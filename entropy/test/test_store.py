from twisted.trial.unittest import TestCase

from axiom.store import Store

from entropy.store import ContentStore

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
