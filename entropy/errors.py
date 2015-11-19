"""
@copyright: 2007 Fusion Dealership Systems (Pty) Ltd. See LICENSE for details.
"""


class UnknownHashAlgorithm(ValueError):
    """
    An unknown hash algorithm was specified.
    """
    def __init__(self, algo):
        ValueError.__init__(self, algo)
        self.algo = algo



class CorruptObject(RuntimeError):
    """
    An object's contents did not have the expected digest.
    """



class NonexistentObject(ValueError):
    """
    The specified object does not exist.
    """
    def __init__(self, objectId):
        ValueError.__init__(self, objectId)
        self.objectId = objectId



class DigestMismatch(ValueError):
    """
    The content did not match the expected digest.

    @ivar expected: The expected digest.
    @ivar actual: The actual digest.
    """
    def __init__(self, expected, actual):
        self.expected = expected
        self.actual = actual


    def __repr__(self):
        return '<DigestMismatch expected=%r actual=%r>' % (
            self.expected, self.actual)


    def __str__(self):
        return 'Expected digest %r but got digest %r' % (
            self.expected, self.actual)



class APIError(RuntimeError):
    """
    A client's interaction with Entropy was interrupted by an error.
    """
    def __init__(self, message, code):
        """
        @type  code: L{int}
        @param code: Error code.
        """
        RuntimeError.__init__(self, message)
        self.code = code



class IrreparableError(RuntimeError):
    """
    An inconsistency was detected that cannot be repaired automatically.
    """
    def __init__(self, objectId):
        RuntimeError.__init__(self, objectId)
        self.objectId = objectId



class NoGoodCopies(IrreparableError):
    """
    No copies of an object could be found which match the content digest.
    """



class UnexpectedDigest(IrreparableError):
    """
    The metadata of object had a different digest when retrieved than expected.

    This exception signals a I{metadata} inconsistency; it does not refer to
    the actual object contents. This likely means that a backend is
    malfunctioning.
    """
