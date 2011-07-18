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
