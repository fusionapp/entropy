"""
@copyright: 2007-2014 Quotemaster cc. See LICENSE for details.
"""
from twisted.trial.unittest import TestCase

from entropy.errors import UnknownHashAlgorithm, DigestMismatch

class ExceptionTests(TestCase):
    """
    Tests for exception classes.
    """
    def test_unknownHashAlgorithm(self):
        """
        Instantiating L{UnknownHashAlgorithm) correctly sets its attributes.
        """
        e = UnknownHashAlgorithm('algo')
        self.assertEquals(e.algo, 'algo')



class DigestMismatchTests(TestCase):
    """
    Tests for DigestMismatch.
    """
    def setUp(self):
        self.e = DigestMismatch('foo', 'bar')


    def test_str(self):
        """
        Verify the __str__ implementation.
        """
        self.assertEquals(
            "Expected digest 'foo' but got digest 'bar'",
            str(self.e))


    def test_repr(self):
        """
        Verify the __repr__ implementation.
        """
        self.assertEquals(
            "<DigestMismatch expected='foo' actual='bar'>",
            repr(self.e))
