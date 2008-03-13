class UnknownHashAlgorithm(ValueError):
    """
    An unknown hash algorithm was specified.
    """
    def __init__(self, algo):
        UnknownHashAlgorithm.__init__(self, algo)
        self.algo = algo


class CorruptObject(RuntimeError):
    """
    An object's contents did not have the expected digest.
    """
