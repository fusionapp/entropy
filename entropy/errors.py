"""
@copyright: 2007-2014 Quotemaster cc. See LICENSE for details.
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



class NoReadBackends(Exception):
    """
    There are no L{entropy.ientropy.IReadStore} backends configured, but at
    least one is required for the requested operation.
    """



class NoWriteBackends(Exception):
    """
    There are no L{entropy.ientropy.IWriteStore} backends configured, but at
    least one is required for the requested operation.
    """



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
        Exception.__init__(self, message)
        self.code = code
