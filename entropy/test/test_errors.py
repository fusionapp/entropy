from twisted.trial.unittest import TestCase

from entropy.errors import UnknownHashAlgorithm

class ExceptionTests(TestCase):
    """
    Tests for exception classes.
    """
    def test_unknownHashAlgorithm(self):
        """
        Instantiating L{UnknownHashAlgorithm) correctly sets its attributes.
        """
        e = UnknownHashAlgorithm('algo')
        self.assertEqual(e.algo, 'algo')
