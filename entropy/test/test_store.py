from StringIO import StringIO
from datetime import timedelta

from epsilon.extime import Time

from twisted.trial.unittest import TestCase

from axiom.store import Store

from nevow.inevow import IResource
from nevow.testutil import FakeRequest
from nevow.static import File

from entropy.ientropy import ISiblingStore
from entropy.errors import CorruptObject, NonexistentObject
from entropy.store import (ContentStore, ImmutableObject, ObjectCreator,
    MemoryObject)



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
        d = self.contentStore.storeObject('blah', 'blah', metadata={'blah': 'blah'})
        return self.assertFailure(d, NotImplementedError
            ).addCallback(lambda e: self.assertSubstring('metadata', e.message))


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

        obj3 = self.contentStore._storeObject('blah',
                                              u'text/plain')

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


    def test_getSiblingExistsRemote(self):
        """
        Calling getSiblingObject with an object ID that is missing locally, but
        present in one of the sibling stores, will retrieve the object, as well
        as inserting it into the local store.
        """
        self.store.powerUp(self.contentStore1, ISiblingStore)
        d = self.contentStore2.getSiblingObject(self.testObject.objectId)
        def _cb(o):
            self.o = o
        d.addCallback(_cb)

        self.assertEqual(self.o.getContent(), 'somecontent')
        d = self.contentStore2.getObject(self.testObject.objectId)
        def _cb(o2):
            self.o2 = o2
        d.addCallback(_cb)
        self.assertIdentical(self.o, self.o2)


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
