from twisted.trial.unittest import TestCase

from entropy.hash import MessageDigestWrapper
from sha import sha

class WrapperTests(TestCase):
    """
    Tests for L{MessageDigestWrapper}.
    """
    def test_initData(self):
        """
        Passing data into the constructor of L{MessageDigestWrapper} is the
        same as calling update().
        """
        d = MessageDigestWrapper('sha256', 'foo')
        d2 = MessageDigestWrapper('sha256')
        d2.update('foo')
        self.assertEqual(d.digest(), d2.digest())

    def test_updateAfterDigest(self):
        """
        Data cannot be added after the digest has been retrieved.
        """
        d = MessageDigestWrapper('sha256')
        d.update('foo')
        d.digest()
        self.assertRaises(RuntimeError, d.update, 'bar')

    def test_hexdigest(self):
        """
        hexdigest() should return a hex-encoded form of the digest.
        """
        d = MessageDigestWrapper('sha256', 'bar')
        self.assertEqual(d.hexdigest(), d.digest().encode('hex'))

    def test_sha1hashes(self):
        """
        SHA1 hashes should match the stdlib SHA1 implementation.
        """
        testdata = ['',
                    'foobar',
                    'x' * 1000,
                    'abc123!@#']

        for s in testdata:
            self.assertEqual(MessageDigestWrapper('sha1', s).digest(), sha(s).digest())
