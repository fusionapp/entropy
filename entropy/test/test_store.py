"""
@copyright: 2007-2011 Quotemaster cc. See LICENSE for details.

Tests for L{entropy.store}.
"""
from StringIO import StringIO
from datetime import timedelta

from epsilon.extime import Time

from zope.interface import implements

from twisted.trial.unittest import TestCase
from twisted.internet.defer import fail, succeed

from axiom.store import Store
from axiom.item import Item
from axiom.attributes import inmemory, integer
from axiom.errors import ItemNotFound

from nevow.inevow import IResource
from nevow.testutil import FakeRequest
from nevow.static import File

from entropy.ientropy import (
    IContentStore, ISiblingStore, IBackendStore, IUploadScheduler, IMigration)
from entropy.errors import CorruptObject, NonexistentObject
from entropy.store import (
    ContentStore, ImmutableObject, ObjectCreator, MemoryObject, _PendingUpload,
    MigrationManager, LocalStoreMigration, PendingMigration)



class ContentStoreTests(TestCase):
    """
    Tests for L{ContentStore}.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.contentStore = ContentStore(store=self.store, hash=u'sha256')


    def test_storeObject(self):
        """
        Test storing an object.
        """
        content = 'blahblah some data blahblah'
        contentType = u'application/octet-stream'
        expectedDigest = u'9aef0e119873bb0aab04e941d8f76daf21dedcd79e2024004766ee3b22ca9862'

        d = self.contentStore.storeObject(content, contentType)
        def _cb(oid):
            self.oid = oid
        d.addCallback(_cb)
        self.assertEqual(self.oid, u'sha256:' + expectedDigest)


    def test_metadata(self):
        """
        Attempting to store metadata results in an exception as this is not yet
        implemented.
        """
        d = self.contentStore.storeObject(
            'blah', 'blah', metadata={'blah': 'blah'})
        return self.assertFailure(d, NotImplementedError
            ).addCallback(lambda e: self.assertSubstring('metadata', str(e)))


    def test_getObject(self):
        """
        Test retrieving object.
        """
        obj = ImmutableObject(store=self.store,
                              hash=u'somehash',
                              contentDigest=u'quux',
                              content=self.store.newFilePath('foo'),
                              contentType=u'application/octet-stream')
        d = self.contentStore.getObject(u'somehash:quux')
        return d.addCallback(lambda obj2: self.assertIdentical(obj, obj2))


    def test_updateObject(self):
        """
        Storing an object that is already in the store just updates the content
        type and timestamp.
        """
        t1 = Time()
        t2 = t1 - timedelta(seconds=30)
        obj = self.contentStore._storeObject('blah',
                                             u'application/octet-stream',
                                             created=t1)
        obj2 = self.contentStore._storeObject('blah',
                                              u'text/plain',
                                              created=t2)
        self.assertIdentical(obj, obj2)
        self.assertEqual(obj.contentType, u'text/plain')
        self.assertEqual(obj.created, t2)

        self.contentStore._storeObject('blah', u'text/plain')

        self.assertTrue(obj.created > t2)


    def test_importObject(self):
        """
        Importing an object stores an equivalent object in the local store.
        """
        created = Time()

        obj1 = MemoryObject(hash=u'sha256',
                            contentDigest=u'9aef0e119873bb0aab04e941d8f76daf21dedcd79e2024004766ee3b22ca9862',
                            content=u'blahblah some data blahblah',
                            created=created,
                            contentType=u'application/octet-stream')
        obj2 = self.contentStore.importObject(obj1)
        self.assertEqual(obj1.objectId, obj2.objectId)
        self.assertEqual(obj1.created, obj2.created)
        self.assertEqual(obj1.contentType, obj2.contentType)
        self.assertEqual(obj1.getContent(), obj2.getContent())


    def test_nonexistentObject(self):
        """
        Retrieving a nonexistent object results in L{NonexistentObject}.
        """
        objectId = u'sha256:NOSUCHOBJECT'
        d = self.contentStore.getObject(objectId)
        return self.assertFailure(d, NonexistentObject
            ).addCallback(lambda e: self.assertEqual(e.objectId, objectId))



class MigrationTests(TestCase):
    """
    Tests for some migration-related stuff.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.contentStore = ContentStore(store=self.store, hash=u'sha256')
        self.mockStore = MockContentStore(store=self.store)


    def _mkObject(self):
        """
        Inject an object for testing.
        """
        return ImmutableObject(
            store=self.store,
            hash=u'somehash',
            contentDigest=u'quux',
            content=self.store.newFilePath('foo'),
            contentType=u'application/octet-stream')


    def test_migrateTo(self):
        """
        A migration is initialized with the current range of stored objects.
        """
        objs = [self._mkObject() for _ in xrange(5)]

        dest = ContentStore(store=self.store, hash=u'sha256')
        migration = self.contentStore.migrateTo(dest)
        self.assertIdentical(migration.source, self.contentStore)
        self.assertIdentical(migration.destination, dest)
        self.assertEqual(migration.start, 0)
        self.assertEqual(migration.end, objs[-1].storeID)
        self.assertEqual(migration.current, -1)


    def test_migration(self):
        """
        Migration replicates all objects in this store to the destination.
        """
        def _mkObject(content):
            return self.contentStore._storeObject(
                content=content,
                contentType=u'application/octet-stream')

        obj1 = _mkObject(u'object1')
        obj2 = _mkObject(u'object2')

        dest = self.mockStore
        migration = self.contentStore.migrateTo(dest)
        d = migration.run()

        # Already running, so a new run should not be started
        self.assertIdentical(migration.run(), None)

        # This is created after the migration, so should not be migrated
        _mkObject(u'object2')

        def _verify(ign):
            self.assertEqual(
                dest.events,
                [('storeObject', dest, obj1.getContent(), obj1.contentType,
                  obj1.metadata, obj1.created, obj1.objectId),
                 ('storeObject', dest, obj2.getContent(), obj2.contentType,
                  obj2.metadata, obj2.created, obj2.objectId)])
        return d.addCallback(_verify)


    def test_nextObject(self):
        """
        L{LocalStoreMigration._nextObject} obtains the next object after the
        most recently processed object, and flags it for migration.
        """
        migration = LocalStoreMigration(
            store=self.store,
            start=0,
            current=-1,
            end=1000,
            source=self.contentStore,
            destination=self.contentStore)
        obj1 = self._mkObject()
        obj2 = self._mkObject()
        m1 = migration._nextObject()
        self.assertIdentical(m1.obj, obj1)
        m2 = migration._nextObject()
        self.assertIdentical(m2.obj, obj2)
        m3 = migration._nextObject()
        self.assertIdentical(m3, None)


    def _mkMigrationJunk(self):
        """
        Set up some test state for migrations.
        """
        obj = self.contentStore._storeObject(
            content='foo',
            contentType=u'application/octet-stream')
        migration = LocalStoreMigration(
            store=self.store,
            start=0,
            current=-1,
            end=1000,
            source=self.contentStore,
            destination=self.mockStore)
        pendingMigration = PendingMigration(
            store=self.store,
            parent=migration,
            obj=obj)
        return obj, migration, pendingMigration


    def test_attemptMigrationSucceeds(self):
        """
        When a migration attempt succeeds, the tracking object is deleted.
        """
        obj, migration, pendingMigration = self._mkMigrationJunk()
        def _cb(ign):
            # .store is set to None on deletion
            self.assertIdentical(pendingMigration.store, None)
        return pendingMigration.attemptMigration().addCallback(_cb)


    def test_attemptMigrationFails(self):
        """
        When a migration attempt fails, the tracking object is not deleted, and
        the trackback is stored and logged.
        """
        obj, migration, pendingMigration = self._mkMigrationJunk()

        def _explode(*a, **kw):
            return fail(ValueError('42'))
        object.__setattr__(self.mockStore, 'storeObject', _explode)

        def _eb(f):
            # .store is set to None on deletion
            self.assertNotIdentical(pendingMigration.store, None)
            tb = pendingMigration.lastFailure
            [tb2] = self.flushLoggedErrors(ValueError)
            self.assertIn(u'ValueError: 42', tb)
            self.assertEqual(tb.encode('ascii'), tb2.getTraceback())

        d = pendingMigration.attemptMigration()
        return self.assertFailure(d, ValueError).addErrback(_eb)



class MockContentStore(Item):
    """
    Mock content store that just logs calls.

    @ivar events: A list of logged calls.
    """
    implements(IContentStore)

    dummy = integer()
    events = inmemory()
    migrationDestination = inmemory()

    def __init__(self, events=None, **kw):
        super(MockContentStore, self).__init__(**kw)
        if events is None:
            self.events = []
        else:
            self.events = events


    # IContentStore

    def getObject(self, objectId):
        self.events.append(('getObject', self, objectId))
        return fail(NonexistentObject(objectId))


    def storeObject(self, content, contentType, metadata={}, created=None,
                    objectId=None):
        self.events.append(
            ('storeObject', self, content, contentType, metadata, created,
             objectId))
        return succeed(u'sha256:FAKE')


    def migrateTo(self, destination):
        self.migrationDestination = destination
        return TestMigration(store=destination.store)



class MockUploadScheduler(object):
    """
    Mock implementation of L{IUploadScheduler}.
    """
    implements(IUploadScheduler)

    def __init__(self):
        self.uploads = []


    def scheduleUpload(self, objectId, backend):
        self.uploads.append((objectId, backend))



class StoreBackendTests(TestCase):
    """
    Tests for content store backend functionality.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.contentStore1 = ContentStore(store=self.store)
        self.contentStore1.storeObject(content='somecontent',
                                       contentType=u'application/octet-stream')
        self.testObject = self.store.findUnique(ImmutableObject)

        self.contentStore2 = ContentStore(store=self.store)


    def test_getSiblingExists(self):
        """
        Calling getSiblingObject with an object ID that is present in the local
        store just returns the local object.
        """
        d = self.contentStore1.getSiblingObject(self.testObject.objectId)
        def _cb(o):
            self.o = o
        d.addCallback(_cb)
        self.assertIdentical(self.o, self.testObject)


    def _retrievalTest(self):
        d = self.contentStore2.getSiblingObject(self.testObject.objectId)
        def _cb(o):
            self.o = o
        d.addCallback(_cb)

        self.assertEqual(self.o.getContent(), 'somecontent')
        d = self.contentStore2.getObject(self.testObject.objectId)
        def _cb2(o2):
            self.o2 = o2
        d.addCallback(_cb2)
        self.assertIdentical(self.o, self.o2)


    def test_getSiblingExistsRemote(self):
        """
        Calling getSiblingObject with an object ID that is missing locally, but
        present in one of the sibling stores, will retrieve the object, as well
        as inserting it into the local store.
        """
        self.store.powerUp(self.contentStore1, ISiblingStore)
        self._retrievalTest()


    def test_getSiblingExistsBackend(self):
        """
        If an object is missing in local and sibling stores, but present in a
        backend store, the object will be retrieved from the backend store.
        """
        self.store.powerUp(self.contentStore1, IBackendStore)
        self._retrievalTest()


    def test_siblingBeforeBackend(self):
        """
        When looking for a missing object, sibling stores are tried before
        backend stores.
        """
        events = []

        siblingStore = MockContentStore(store=self.store, events=events)
        self.store.powerUp(siblingStore, ISiblingStore)

        backendStore = MockContentStore(store=self.store, events=events)
        self.store.powerUp(backendStore, IBackendStore)

        def _cb(e):
            self.assertEqual(
                events,
                [('getObject', siblingStore, u'sha256:aoeuaoeu'),
                 ('getObject', backendStore, u'sha256:aoeuaoeu')])
        return self.assertFailure(
            self.contentStore2.getSiblingObject(u'sha256:aoeuaoeu'),
            NonexistentObject).addCallback(_cb)


    def test_getSiblingMissing(self):
        """
        Calling getSiblingObject with an object ID that is missing everywhere
        raises L{NonexistentObject}.
        """
        self.store.powerUp(self.contentStore1, ISiblingStore)
        objectId = u'sha256:NOSUCHOBJECT'
        d = self.contentStore2.getSiblingObject(objectId)
        return self.assertFailure(d, NonexistentObject
            ).addCallback(lambda e: self.assertEqual(e.objectId, objectId))


    def test_storeObject(self):
        """
        Storing an object also causes it to be scheduled for storing in all
        backend stores.
        """
        contentStore = ContentStore(store=self.store)
        backendStore = MockContentStore(store=self.store)
        self.store.powerUp(backendStore, IBackendStore)
        backendStore2 = MockContentStore(store=self.store)
        self.store.powerUp(backendStore2, IBackendStore)
        scheduler = MockUploadScheduler()
        self.store.inMemoryPowerUp(scheduler, IUploadScheduler)

        contentStore.storeObject(content='somecontent',
                                 contentType=u'application/octet-stream')
        testObject = self.store.findUnique(ImmutableObject)
        pu = scheduler.uploads
        self.assertEqual(len(pu), 2)
        self.assertEqual(pu[0][0], testObject.objectId)
        self.assertEqual(pu[1][0], testObject.objectId)
        for objectId, backend in pu:
            if backend is backendStore:
                break
        else:
            self.fail('No pending upload for backendStore')

        for objectId, backend in pu:
            if backend is backendStore2:
                break
        else:
            self.fail('No pending upload for backendStore2')



class _PendingUploadTests(TestCase):
    """
    Tests for L{_PendingUpload}.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.contentStore = ContentStore(store=self.store)
        self.store.powerUp(self.contentStore, IContentStore)
        self.contentStore.storeObject(content='somecontent',
                                      contentType=u'application/octet-stream')
        self.testObject = self.store.findUnique(ImmutableObject)
        self.backendStore = MockContentStore(store=self.store)
        self.pendingUpload = _PendingUpload(store=self.store,
                                            objectId=self.testObject.objectId,
                                            backend=self.backendStore)


    def test_successfulUpload(self):
        """
        When an upload attempt is made, the object is stored to the backend
        store. If this succeeds, the L{_PendingUpload} item is deleted.
        """
        def _cb(ign):
            self.assertEqual(
                self.backendStore.events,
                [('storeObject',
                  self.backendStore,
                  'somecontent',
                  u'application/octet-stream',
                  {},
                  self.testObject.created,
                  self.testObject.objectId)])
            self.assertRaises(ItemNotFound,
                              self.store.findUnique,
                              _PendingUpload)
        return self.pendingUpload.attemptUpload().addCallback(_cb)


    def test_failedUpload(self):
        """
        When an upload attempt is made, the object is stored to the backend
        store. If this fails, the L{_PendingUpload} item has its scheduled time
        updated.
        """
        def _storeObject(content, contentType, metadata={}, created=None,
                         objectId=None):
            raise ValueError('blah blah')
        object.__setattr__(self.backendStore, 'storeObject', _storeObject)

        scheduled = self.pendingUpload.scheduled

        def _cb(ign):
            self.assertIdentical(self.store.findUnique(_PendingUpload),
                                 self.pendingUpload)
            self.assertEqual(self.pendingUpload.scheduled,
                             scheduled + timedelta(minutes=2))
            errors = self.flushLoggedErrors(ValueError)
            self.assertEqual(len(errors), 1)

        d = self.pendingUpload.attemptUpload()
        return self.assertFailure(d, ValueError).addCallback(_cb)



class ObjectCreatorTests(TestCase):
    """
    Tests for L{ObjectCreator}.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.contentStore = ContentStore(store=self.store, hash=u'sha256')
        self.creator = ObjectCreator(self.contentStore)


    def test_correctContentMD5(self):
        """
        Submitting a request with a Content-MD5 header that agrees with the
        uploaded data should succeed.
        """
        req = FakeRequest()
        req.received_headers['content-md5'] = '72VMQKtPF0f8aZkV1PcJAg=='
        req.content = StringIO('testdata')
        return self.creator.handlePUT(req)


    def test_incorrectContentMD5(self):
        """
        Submitting a request with a Content-MD5 header that disagrees with the
        uploaded data should fail.
        """
        req = FakeRequest()
        req.received_headers['content-md5'] = '72VMQKtPF0f8aZkV1PcJAg=='
        req.content = StringIO('wrongdata')
        self.assertRaises(ValueError, self.creator.handlePUT, req)


    def test_missingContentMD5(self):
        """
        Submitting a request with no Content-MD5 header should succeed.
        """
        req = FakeRequest()
        req.content = StringIO('wrongdata')
        return self.creator.handlePUT(req)



class ImmutableObjectTests(TestCase):
    """
    Tests for L{ImmutableObject}.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.contentStore = ContentStore(store=self.store)
        self.contentStore.storeObject(content='somecontent',
                                      contentType=u'application/octet-stream')
        self.testObject = self.store.findUnique(ImmutableObject)


    def test_metadata(self):
        """
        Metadata is not yet supported, so L{ImmutableObject.metadata} should be
        an empty dict.
        """
        self.assertEqual(self.testObject.metadata, {})


    def test_verify(self):
        """
        Verification should succeed when the object contents has not been
        altered.
        """
        self.testObject.verify()


    def test_verifyDamaged(self):
        """
        Verification should fail if the object contents is modified.
        """
        self.testObject.content.setContent('garbage!')
        self.assertRaises(CorruptObject, self.testObject.verify)


    def test_getContent(self):
        """
        L{ImmutableObject.getContent} returns the contents of the object.
        """
        self.assertEqual(self.testObject.getContent(), 'somecontent')


    def test_objectId(self):
        """
        The object ID is composed of the digest function and content digest,
        separated by a colon.
        """
        self.assertEqual(
            self.testObject.objectId,
            'sha256:d5a3477d91583e65a7aba6f6db7a53e2de739bc7bf8f4a08f0df0457b637f1fb')


    def test_adaptToResource(self):
        """
        Adapting L{ImmutableObject} to L{IResource} gives us a L{File} instance
        pointing at the on-disk blob.
        """
        res = IResource(self.testObject)
        self.assertIsInstance(res, File)
        self.assertEqual(res.fp, self.testObject.content)
        self.assertEqual(res.type, 'application/octet-stream')
        self.assertEqual(res.encoding, None)


    def test_adaptDamagedObject(self):
        """
        Adapting L{ImmutableObject} to L{IResource} verifies the object
        contents.
        """
        self.testObject.content.setContent('garbage!')
        self.assertRaises(CorruptObject, IResource, self.testObject)



class TestMigration(Item):
    """
    Test double implementing IMigration.
    """
    implements(IMigration)
    powerupInterfaces = [IMigration]

    ran = integer(default=0)

    def run(self):
        self.ran += 1



class MigrationManagerTests(TestCase):
    """
    Tests for L{MigrationManager}.
    """
    def setUp(self):
        self.store = Store()
        self.manager = MigrationManager(store=self.store)


    def test_serviceRunsMigrations(self):
        """
        Starting the service runs all existing migrations.
        """
        m1 = TestMigration(store=self.store)
        m2 = TestMigration(store=self.store)
        self.store.powerUp(m1)
        self.store.powerUp(m2)
        self.assertEqual(m1.ran, 0)
        self.assertEqual(m2.ran, 0)
        self.manager.startService()
        self.assertEqual(m1.ran, 1)
        self.assertEqual(m2.ran, 1)


    def test_startMigration(self):
        """
        Starting a migration invokes the implementation on the source store.
        """
        source = MockContentStore()
        destination = MockContentStore(store=self.store)
        result = self.manager.migrate(source, destination)
        self.assertEqual(result.ran, 1)
        self.assertEqual(source.migrationDestination, destination)
        self.assertEqual(IMigration(self.store), result)
