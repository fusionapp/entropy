from StringIO import StringIO

from twisted.trial.unittest import TestCase

from axiom.store import Store

from nevow.testutil import FakeRequest

from entropy.store import ContentStore, ImmutableObject, ObjectCreator
from entropy.errors import CorruptObject

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
        return d.addCallback(lambda objectId: self.assertEqual(objectId, u'sha256:%s' % expectedDigest))

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
