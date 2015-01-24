"""
@copyright: 2007-2014 Quotemaster cc. See LICENSE for details.

Tests for L{entropy.store}.
"""
from StringIO import StringIO
from datetime import timedelta

from epsilon.extime import Time
from epsilon.structlike import record

from zope.interface import implements

from twisted.trial.unittest import TestCase
from twisted.internet.defer import fail, succeed
from twisted.application.service import IService
from twisted.web import http

from axiom.store import Store
from axiom.item import Item, POWERUP_BEFORE
from axiom.attributes import inmemory, integer
from axiom.errors import ItemNotFound
from axiom.dependency import installOn

from nevow.inevow import IResource
from nevow.testutil import FakeRequest
from nevow.static import File

from entropy.ientropy import (
    IContentStore, IReadStore, IWriteStore, IDeferredWriteStore,
    IUploadScheduler, IMigration)
from entropy.errors import CorruptObject, NonexistentObject
from entropy.store import (
    StorageConfiguration, ImmutableObject, ObjectCreator, _PendingUpload,
    MigrationManager, RemoteEntropyStore)
from entropy.backends.localaxiom import (
    AxiomStore, LocalStoreMigration, PendingMigration)
from entropy.util import MemoryObject
from entropy.test.util import DummyAgent
from entropy.client import Endpoint


def configurationWithLocal(store):
    storage = StorageConfiguration(store=store)
    backend = AxiomStore(store=store)
    storage.powerUp(backend, IReadStore, POWERUP_BEFORE)
    storage.powerUp(backend, IWriteStore)
    return storage



class RemoteEntropyStoreTests(TestCase):
    """
    Tests for L{entropy.store.RemoteEntropyStore}.
    """
    def setUp(self):
        self.uri = u'http://localhost:8080/'
        self.agent = DummyAgent()
        self.store = Store(self.mktemp())
        self.remoteEntropyStore = RemoteEntropyStore(
            store=self.store,
            entropyURI=self.uri)
        object.__setattr__(
            self.remoteEntropyStore,
            '_endpoint',
            Endpoint(uri=self.uri,
                     agent=self.agent))


    def test_nonexistentObject(self):
        """
        Retrieving a nonexistent object results in L{NonexistentObject}.
        """
        objectId = u'sha256:NOSUCHOBJECT'
        d = self.remoteEntropyStore.getObject(objectId)
        response = self.agent.responses.pop()
        self.assertEqual([], self.agent.responses)
        response.code = http.NOT_FOUND
        response.respond('Not found')
        f = self.failureResultOf(d, NonexistentObject)
        self.assertEqual(f.value.objectId, objectId)



class AxiomStoreTests(TestCase):
    """
    Tests for L{AxiomStore}.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.contentStore = AxiomStore(store=self.store, hash=u'sha256')


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
        self.assertEquals(self.oid, u'sha256:' + expectedDigest)


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
        obj = self.contentStore._storeObject(None,
                                             'blah',
                                             u'application/octet-stream',
                                             created=t1)
        obj2 = self.contentStore._storeObject(None,
                                              'blah',
                                              u'text/plain',
                                              created=t2)
        self.assertIdentical(obj, obj2)
        self.assertEquals(obj.contentType, u'text/plain')
        self.assertEquals(obj.created, t2)

        self.contentStore._storeObject(None, 'blah', u'text/plain')

        self.assertTrue(obj.created > t2)


    def test_nonexistentObject(self):
        """
        Retrieving a nonexistent object results in L{NonexistentObject}.
        """
        objectId = u'sha256:NOSUCHOBJECT'
        d = self.contentStore.getObject(objectId)
        return self.assertFailure(d, NonexistentObject
            ).addCallback(lambda e: self.assertEquals(e.objectId, objectId))



class MigrationTests(TestCase):
    """
    Tests for some migration-related stuff.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.contentStore = AxiomStore(store=self.store, hash=u'sha256')
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

        dest = AxiomStore(store=self.store, hash=u'sha256')
        migration = self.contentStore.migrateTo(dest)
        self.assertIdentical(migration.source, self.contentStore)
        self.assertIdentical(migration.destination, dest)
        self.assertEquals(migration.start, 0)
        self.assertEquals(migration.end, objs[-1].storeID)
        self.assertEquals(migration.current, -1)


    def test_migration(self):
        """
        Migration replicates all objects in this store to the destination.
        """
        def _mkObject(content):
            return self.contentStore._storeObject(
                objectId=None,
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
            self.assertEquals(
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
            objectId=None,
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
            self.assertEquals(tb.encode('ascii'), tb2.getTraceback())

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



class MemoryStore(Item):
    """
    In-memory backend.

    This is an Item because it needs to be installed as a powerup on
    L{axiom.store.StorageConfiguration}, and installing more than one in-memory
    powerup for an interface is not currently supported by Axiom.
    """
    implements(IReadStore, IWriteStore)

    dummy = integer()
    objects = inmemory()

    def __init__(self, *a, **kw):
        super(MemoryStore, self).__init__(*a, **kw)
        if getattr(self, 'objects', None) is None:
            self.objects = {}

    # IReadStore

    def getObject(self, objectId):
        try:
            return succeed(self.objects[objectId])
        except KeyError:
            return fail(NonexistentObject(objectId))

    # IWriteStore

    def storeObject(self, content, contentType, metadata={}, created=None, objectId=None):
        if objectId is None:
            raise ValueError('Must provide objectId')
        self.objects[objectId] = MemoryObject(
            content=content, contentType=contentType, metadata=metadata,
            created=created, objectId=objectId)
        return succeed(objectId)



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
    def test_storeObjectImmediate(self):
        """
        When an object is stored in a storage configuration, it is stored in
        all write backends.
        """
        store = Store()
        storage = StorageConfiguration(store=store)
        backend1 = MemoryStore(store=store)
        storage.powerUp(backend1, IWriteStore)
        backend2 = MemoryStore(store=store)
        storage.powerUp(backend2, IWriteStore)

        storage.storeObject(objectId=u'oid',
                            content='somecontent',
                            contentType=u'application/octet-stream')
        self.assertEquals(
            MemoryObject(
                objectId=u'oid', content='somecontent',
                contentType=u'application/octet-stream', created=None,
                metadata={}),
            self.successResultOf(backend1.getObject(u'oid')))


    def test_storeObjectFailure(self):
        """
        When storing an object in any write backend fails, the entire operation
        fails.
        """
        # XXX: write this test!


    def test_storeObjectDeferred(self):
        """
        Storing an object causes it to be scheduled for upload to all deferred
        write backends.
        """
        store = Store(filesdir=self.mktemp())
        storage = configurationWithLocal(store=store)
        backendStore = MemoryStore(store=store)
        storage.powerUp(backendStore, IDeferredWriteStore)
        backendStore2 = MemoryStore(store=store)
        storage.powerUp(backendStore2, IDeferredWriteStore)
        scheduler = MockUploadScheduler()
        store.inMemoryPowerUp(scheduler, IUploadScheduler)

        storage.storeObject(content='somecontent',
                            contentType=u'application/octet-stream')
        testObject = store.findUnique(ImmutableObject)
        pu = scheduler.uploads
        self.assertEquals(len(pu), 2)
        self.assertEquals(pu[0][0], testObject.objectId)
        self.assertEquals(pu[1][0], testObject.objectId)
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
        self.contentStore = configurationWithLocal(store=self.store)
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
            self.assertEquals(
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

        nextScheduled = self.pendingUpload.scheduled + timedelta(minutes=5)
        def _nextAttempt():
            return nextScheduled
        object.__setattr__(self.pendingUpload, '_nextAttempt', _nextAttempt)

        def _cb(ign):
            self.assertIdentical(self.store.findUnique(_PendingUpload),
                                 self.pendingUpload)
            self.assertEquals(self.pendingUpload.scheduled,
                             nextScheduled)
            errors = self.flushLoggedErrors(ValueError)
            self.assertEquals(len(errors), 1)

        d = self.pendingUpload.attemptUpload()
        return self.assertFailure(d, ValueError).addCallback(_cb)



class ObjectCreatorTests(TestCase):
    """
    Tests for L{ObjectCreator}.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.contentStore = configurationWithLocal(store=self.store)
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
        self.contentStore = AxiomStore(store=self.store)
        self.contentStore.storeObject(content='somecontent',
                                      contentType=u'application/octet-stream')
        self.testObject = self.store.findUnique(ImmutableObject)


    def test_metadata(self):
        """
        Metadata is not yet supported, so L{ImmutableObject.metadata} should be
        an empty dict.
        """
        self.assertEquals(self.testObject.metadata, {})


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
        self.assertEquals(self.testObject.getContent(), 'somecontent')


    def test_objectId(self):
        """
        The object ID is composed of the digest function and content digest,
        separated by a colon.
        """
        self.assertEquals(
            self.testObject.objectId,
            'sha256:d5a3477d91583e65a7aba6f6db7a53e2de739bc7bf8f4a08f0df0457b637f1fb')


    def test_adaptToResource(self):
        """
        Adapting L{ImmutableObject} to L{IResource} gives us a L{File} instance
        pointing at the on-disk blob.
        """
        res = IResource(self.testObject)
        self.assertIsInstance(res, File)
        self.assertEquals(res.fp, self.testObject.content)
        self.assertEquals(res.type, 'application/octet-stream')
        self.assertEquals(res.encoding, None)



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


    def test_installService(self):
        """
        The service is started when it is installed into a running store, and
        stopped when it is deleted.
        """
        IService(self.store).startService()
        installOn(self.manager, self.store)
        self.assertTrue(self.manager.running)
        self.manager.deleteFromStore()
        self.assertFalse(self.manager.running)


    def test_serviceRunsMigrations(self):
        """
        Starting the service runs all existing migrations.
        """
        m1 = TestMigration(store=self.store)
        m2 = TestMigration(store=self.store)
        self.store.powerUp(m1)
        self.store.powerUp(m2)
        self.assertEquals(m1.ran, 0)
        self.assertEquals(m2.ran, 0)
        self.manager.startService()
        self.assertEquals(m1.ran, 1)
        self.assertEquals(m2.ran, 1)


    def test_startMigration(self):
        """
        Starting a migration invokes the implementation on the source store.
        """
        source = MockContentStore()
        destination = MockContentStore(store=self.store)
        result = self.manager.migrate(source, destination)
        self.assertEquals(result.ran, 1)
        self.assertEquals(source.migrationDestination, destination)
        self.assertEquals(IMigration(self.store), result)
