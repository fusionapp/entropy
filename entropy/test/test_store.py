from StringIO import StringIO

from twisted.trial.unittest import TestCase

from axiom.store import Store

from nevow.testutil import FakeRequest

from entropy.store import ContentStore, ImmutableObject, ObjectCreator

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
        objectId = self.contentStore.storeObject(content, contentType)
        self.assertEqual(objectId, u'sha256:%s' % expectedDigest)

    def test_getObject(self):
        """
        Test retrieving object.
        """
        obj = ImmutableObject(store=self.store,
                              hash=u'somehash',
                              contentDigest=u'quux',
                              content=self.store.newFilePath('foo'),
                              contentType=u'application/octet-stream')
        obj2 = self.contentStore.getObject(u'somehash:quux')
        self.assertIdentical(obj, obj2)


class ObjectCreatorTests(TestCase):
    def setUp(self):
        self.store = Store(self.mktemp())
        self.contentStore = ContentStore(store=self.store, hash=u'sha256')
        self.creator = ObjectCreator(self.contentStore)

    def test_correctContentMD5(self):
        req = FakeRequest()
        req.received_headers['content-md5'] = '72VMQKtPF0f8aZkV1PcJAg=='
        req.content = StringIO('testdata')
        self.creator.handlePUT(req)

    def test_incorrectContentMD5(self):
        req = FakeRequest()
        req.received_headers['content-md5'] = '72VMQKtPF0f8aZkV1PcJAg=='
        req.content = StringIO('wrongdata')
        self.assertRaises(ValueError, self.creator.handlePUT, req)

    def test_missingContentMD5(self):
        req = FakeRequest()
        req.content = StringIO('wrongdata')
        self.creator.handlePUT(req)
