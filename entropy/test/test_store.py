from StringIO import StringIO
from datetime import timedelta

from epsilon.extime import Time

from zope.interface import implements

from twisted.trial.unittest import TestCase
from twisted.internet.defer import succeed
from twisted.internet.task import Clock

from axiom.store import Store
from axiom.item import Item
from axiom.attributes import inmemory, integer
from axiom.errors import ItemNotFound

from nevow.testutil import FakeRequest

from entropy.ientropy import (IContentObject, IBackendStore,
                              IWriteLaterBackend, IWriteBackend)
from entropy.errors import NonexistentObject
from entropy.store import ObjectCreator, PendingUpload, UploadScheduler, StorageClass
from entropy.util import deferred



class MockContentObject(object):
    """
    Immutable content object.
    """
    implements(IContentObject)

    contentType = None
    created = None
    metadata = None
    objectId = None

    def __init__(self, objectId, content, contentType=None, metadata=None, created=None):
        self.objectId = objectId
        self.content = content
        self.contentType = contentType
        self.metadata = {}
        if metadata is not None:
            self.metadata = metadata
        self.created = None


    def getContent(self):
        return self.content



class MockBackendStore(Item):
    """
    Mock content store that just logs calls.

    @ivar events: A list of logged calls.
    @ivar objects: A dict of content objects.
    """
    implements(IBackendStore)

    dummy = integer()
    events = inmemory()
    objects = inmemory()

    def __init__(self, events=None, objects=None, **kw):
        super(MockBackendStore, self).__init__(**kw)
        if events is None:
            self.events = []
        else:
            self.events = events
        self.objects = {}
        if objects is not None:
            for obj in objects:
                self.objects[obj.objectId] = obj


    # IBackendStore

    @deferred
    def getObject(self, objectId):
        self.events.append(('getObject', self, objectId))
        obj = self.objects.get(objectId)
        if obj is None:
            raise NonexistentObject(objectId)
        return obj


    def storeObject(self, objectId, content, contentType=None, metadata={}, created=None):
        self.events.append(
            ('storeObject', self, objectId, content, contentType, metadata, created))
        self.objects[objectId] = MockContentObject(objectId,
                                                   content,
                                                   contentType,
                                                   metadata,
                                                   created)
        return succeed(objectId)



class StorageClassBackendTests(TestCase):
    """
    Tests for storage class backend functionality.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.storageClass = StorageClass(store=self.store, name=u'testclass')

        self.testObject1 = MockContentObject(u'thing1', 'somecontent')
        self.testObject2 = MockContentObject(u'thing2', 'somemorecontent')

        self.backendStore1 = MockBackendStore(store=self.store,
                                              objects=[self.testObject1])
        self.storageClass.addBackend(self.backendStore1, 10)

        self.backendStore2 = MockBackendStore(store=self.store,
                                              objects=[self.testObject2])
        self.storageClass.addBackend(self.backendStore2, 5)


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
        backendStore = MockBackendStore(store=self.store)
        self.storageClass.powerUp(backendStore, IWriteLaterBackend)
        backendStore2 = MockBackendStore(store=self.store)
        self.storageClass.powerUp(backendStore2, IWriteLaterBackend)

        testObject = MockContentObject(u'athirdthing', 'differentcontent')
        self.storageClass.storeObject(testObject.objectId, testObject.getContent())

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
        self.testObject = MockContentObject(objectId=u'athing',
                                            content='somecontent',
                                            contentType=u'application/octet-stream')
        self.localStore = MockBackendStore(store=self.store, objects=[self.testObject])
        self.store.powerUp(self.localStore, IBackendStore)

        self.backendStore = MockBackendStore(store=self.store)
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
        self.storageClass = StorageClass(store=self.store)
        self.backendStore = MockBackendStore(store=self.store)
        self.storageClass.powerUp(self.backendStore, IWriteBackend)
        self.creator = ObjectCreator(self.storageClass, u'athing')


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



class UploadSchedulerTests(TestCase):
    """
    Tests for L{UploadScheduler}.
    """
    def setUp(self):
        self.store = Store(self.mktemp())
        self.scheduler = UploadScheduler(store=self.store)
        self.clock = self.scheduler._clock = Clock()
        self.localStore = MockBackendStore(store=self.store)


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
                                      backend=self.localStore,
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

