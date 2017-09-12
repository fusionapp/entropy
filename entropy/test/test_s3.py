"""
Tests for the S3 store implementation.
"""
from twisted.trial.unittest import SynchronousTestCase
from txaws.testing.service import FakeAWSServiceRegion

from entropy.s3 import S3Store



class S3Tests(SynchronousTestCase):
    def setUp(self):
        region = FakeAWSServiceRegion(access_key=b'a', secret_key=b'b')
        region.get_s3_client().create_bucket(u'mybucket.example.com')
        self.store = S3Store(
            accessKey=u'a', secretKey=u'c', bucket=u'mybucket.example.com')
        object.__setattr__(
            self.store, '_getClient', lambda: region.get_s3_client())


    def test_storeGet(self):
        """
        Storing an object and then getting it returns the same object.
        """
        self.successResultOf(
            self.store.storeObject(
                b'blah', b'application/octet-stream', objectId=b'sha256:1234'))
        o = self.successResultOf(self.store.getObject(b'sha256:1234'))
        self.assertEquals(o.content, b'blah')

    test_storeGet.todo = 'Does not currently work against MemoryS3'


    def test_store(self):
        """
        Storing an object succeeds.
        """
        self.successResultOf(
            self.store.storeObject(
                b'blah', b'application/octet-stream', objectId=b'sha256:1234'))
