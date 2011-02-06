from StringIO import StringIO
from datetime import timedelta

from epsilon.extime import Time

from zope.interface import implements

from twisted.trial.unittest import TestCase
from twisted.internet.defer import fail, succeed
from twisted.internet.task import Clock

from axiom.store import Store
from axiom.item import Item
from axiom.attributes import inmemory, integer
from axiom.errors import ItemNotFound

from nevow.inevow import IResource
from nevow.testutil import FakeRequest
from nevow.static import File

from entropy.ientropy import IBackendStore, IWriteLaterBackend, IReadBackend, IWriteBackend
from entropy.errors import CorruptObject, NonexistentObject
from entropy.store import ObjectCreator, PendingUpload, UploadScheduler, StorageClass
from entropy.backends.localaxiom import ContentStore, ImmutableObject



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

        d = self.contentStore.storeObject(u'athing', content, contentType)
        return d.addCallback(lambda oid: self.assertEqual(oid, u'athing'))


    def test_metadata(self):
        """
        Attempting to store metadata results in an exception as this is not yet
        implemented.
        """
        d = self.contentStore.storeObject(
            u'athing', 'blah', metadata={'blah': 'blah'})
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
                              contentType=u'application/octet-stream',
                              objectId=u'athing')
        d = self.contentStore.getObject(u'athing')
        return d.addCallback(lambda obj2: self.assertIdentical(obj, obj2))


    def test_updateObject(self):
        """
        Storing an object that is already in the store just updates the content
        type and timestamp.
        """
        t1 = Time()
        t2 = t1 - timedelta(seconds=30)
        obj = self.contentStore._storeObject(u'athing',
                                             'blah',
                                             u'application/octet-stream',
                                             created=t1)
        obj2 = self.contentStore._storeObject(u'athing',
                                              'blah',
                                              u'text/plain',
                                              created=t2)
        self.assertIdentical(obj, obj2)
        self.assertEqual(obj.contentType, u'text/plain')
        self.assertEqual(obj.created, t2)

        self.contentStore._storeObject(u'athing', 'blah')

        self.assertTrue(obj.created > t2)


    def test_nonexistentObject(self):
        """
        Retrieving a nonexistent object results in L{NonexistentObject}.
        """
        objectId = u'sha256:NOSUCHOBJECT'
        d = self.contentStore.getObject(objectId)
        return self.assertFailure(d, NonexistentObject
            ).addCallback(lambda e: self.assertEqual(e.objectId, objectId))



class MockContentStore(Item):
    """
    Mock content store that just logs calls.

    @ivar events: A list of logged calls.
    """
    implements(IBackendStore)

    dummy = integer()
    events = inmemory()

    def __init__(self, events=None, **kw):
        super(MockContentStore, self).__init__(**kw)
        if events is None:
            self.events = []
        else:
            self.events = events


    # IBackendStore

    def getObject(self, objectId):
        self.events.append(('getObject', self, objectId))
        return fail(NonexistentObject(objectId))


    def storeObject(self, objectId, content, contentType=None, metadata={}, created=None):
        self.events.append(
            ('storeObject', self, objectId, content, contentType, metadata, created))
        return succeed(u'objectId')



class StorageClassBackendTests(TestCase):
    """
    Tests for storage class backend functionality.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.storageClass = StorageClass(store=self.store, name=u'testclass')

        self.contentStore1 = ContentStore(store=self.store)
        self.storageClass.powerUp(self.contentStore1, IReadBackend, 10)
        self.storageClass.powerUp(self.contentStore1, IWriteBackend, 10)
        self.contentStore1.storeObject(objectId=u'athing',
                                       content='somecontent',
                                       contentType=u'application/octet-stream')
        self.testObject1 = self.store.findUnique(ImmutableObject,
                                                 ImmutableObject.objectId == u'athing')

        self.contentStore2 = ContentStore(store=self.store)
        self.storageClass.powerUp(self.contentStore2, IReadBackend, 5)
        self.contentStore2.storeObject(objectId=u'anotherthing',
                                       content='somemorecontent',
                                       contentType=u'application/octet-stream')
        self.testObject2 = self.store.findUnique(ImmutableObject,
                                                 ImmutableObject.objectId == u'anotherthing')


    def test_getObjectExistsFirst(self):
        """
        Calling getObject with an object ID that exists in the first store will
        retrieve the object.
        """
        d = self.storageClass.getObject(self.testObject1.objectId)
        def _cb(o):
            self.o = o
        d.addCallback(_cb)
        self.assertIdentical(self.o, self.testObject1)


    def test_getObjectExistsSecond(self):
        """
        Calling getObject with an object ID that exists in the second store will
        retrieve the object.
        """
        d = self.storageClass.getObject(self.testObject2.objectId)
        def _cb(o):
            self.o = o
        d.addCallback(_cb)
        self.assertIdentical(self.o, self.testObject2)


    def test_getObjectMissing(self):
        """
        Calling getObject with an object ID that is missing everywhere
        raises L{NonexistentObject}.
        """
        objectId = u'NOSUCHOBJECT'
        d = self.storageClass.getObject(objectId)
        return self.assertFailure(d, NonexistentObject
            ).addCallback(lambda e: self.assertEqual(e.objectId, objectId))


    def test_storeObject(self):
        """
        Storing an object also causes it to be scheduled for storing in all
        backend stores.
        """
        backendStore = MockContentStore(store=self.store)
        self.storageClass.powerUp(backendStore, IWriteLaterBackend)
        backendStore2 = MockContentStore(store=self.store)
        self.storageClass.powerUp(backendStore2, IWriteLaterBackend)

        self.storageClass.storeObject(objectId=u'athirdthing',
                                      content='somecontent',
                                      contentType=u'application/octet-stream')
        testObject = self.store.findUnique(ImmutableObject,
                                           ImmutableObject.objectId == u'athirdthing')
        pu = list(self.store.query(PendingUpload))
        self.assertEqual(len(pu), 2)
        self.assertEqual(pu[0].objectId, testObject.objectId)
        self.assertEqual(pu[1].objectId, testObject.objectId)
        for p in pu:
            if p.backend is backendStore:
                break
        else:
            self.fail('No pending upload for backendStore')

        for p in pu:
            if p.backend is backendStore2:
                break
        else:
            self.fail('No pending upload for backendStore2')



class PendingUploadTests(TestCase):
    """
    Tests for L{PendingUpload}.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.contentStore = ContentStore(store=self.store)
        self.store.powerUp(self.contentStore, IBackendStore)
        self.contentStore.storeObject(objectId=u'athing',
                                      content='somecontent',
                                      contentType=u'application/octet-stream')
        self.testObject = self.store.findUnique(ImmutableObject)
        self.backendStore = MockContentStore(store=self.store)
        self.pendingUpload = PendingUpload(store=self.store,
                                           objectId=self.testObject.objectId,
                                           backend=self.backendStore)


    def test_successfulUpload(self):
        """
        When an upload attempt is made, the object is stored to the backend
        store. If this succeeds, the L{PendingUpload} item is deleted.
        """
        def _cb(ign):
            self.assertEqual(
                self.backendStore.events,
                [('storeObject',
                  self.backendStore,
                  u'athing',
                  'somecontent',
                  u'application/octet-stream',
                  {},
                  self.testObject.created)])
            self.assertRaises(ItemNotFound,
                              self.store.findUnique,
                              PendingUpload)

        return self.pendingUpload.attemptUpload().addCallback(_cb)


    def test_failedUpload(self):
        """
        When an upload attempt is made, the object is stored to the backend
        store. If this fails, the L{PendingUpload} item has its scheduled time
        updated.
        """
        def _storeObject(self, objectId, content, contentType=None, metadata={}, created=None):
            raise ValueError('blah blah')

        object.__setattr__(self.backendStore, 'storeObject', _storeObject)

        scheduled = self.pendingUpload.scheduled

        def _cb(ign):
            self.assertIdentical(self.store.findUnique(PendingUpload),
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
        self.contentStore.storeObject(objectId=u'athing',
                                      content='somecontent',
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
        FIXME: This is no longer the case.
        """
        self.assertEqual(self.testObject.objectId, u'athing')


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



class UploadSchedulerTests(TestCase):
    """
    Tests for L{UploadScheduler}.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.scheduler = UploadScheduler(store=self.store)
        self.clock = self.scheduler._clock = Clock()
        self.contentStore = ContentStore(store=self.store)


    def test_wakeOnStart(self):
        self.scheduler.startService()
        def _wake():
            self.wokeUp = True
        object.__setattr__(self.scheduler, 'wake', _wake)
        self.clock.advance(1)
        self.assertTrue(self.wokeUp)
        self.assertIdentical(self.scheduler._wakeCall, None)


    def test_cancelWake(self):
        self.scheduler.startService()

        self.scheduler._cancelWake()
        self.assertIdentical(self.scheduler._wakeCall, None)
        self.assertEqual(self.clock.getDelayedCalls(), [])

        # Ensure _cancelWake is idempotent
        self.scheduler._cancelWake()
        self.assertIdentical(self.scheduler._wakeCall, None)
        self.assertEqual(self.clock.getDelayedCalls(), [])


    def _mkUpload(self, scheduled):
        pendingUpload = PendingUpload(store=self.store,
                                      objectId=u'aoeu',
                                      backend=self.contentStore,
                                      scheduled=scheduled)

        def _attemptUpload():
            self.uploadsAttempted += 1
            pendingUpload.deleteFromStore()
            return succeed(None)
        object.__setattr__(pendingUpload, 'attemptUpload', _attemptUpload)
        return pendingUpload


    def test_wakeForOne(self):
        now = Time()
        object.__setattr__(self.scheduler, '_now', lambda: now)

        self.uploadsAttempted = 0
        self._mkUpload(now)
        self.assertEqual(self.uploadsAttempted, 0)
        self.scheduler.wake()
        self.assertEqual(self.uploadsAttempted, 1)
        now += timedelta(seconds=1)
        self.clock.advance(1)
        self.assertEqual(self.uploadsAttempted, 1)


    def test_wakeMulti(self):
        now = Time()
        object.__setattr__(self.scheduler, '_now', lambda: now)

        self.uploadsAttempted = 0
        self._mkUpload(now)
        self._mkUpload(now)
        self._mkUpload(now + timedelta(seconds=5))

        self.assertEqual(self.uploadsAttempted, 0)
        self.scheduler.wake()
        self.assertEqual(self.uploadsAttempted, 2)
        now += timedelta(seconds=1)
        self.clock.advance(1)
        self.assertEqual(self.uploadsAttempted, 2)
        now += timedelta(seconds=5)
        self.clock.advance(5)
        self.assertEqual(self.uploadsAttempted, 3)

