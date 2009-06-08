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
