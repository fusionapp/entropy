class UnknownHashAlgorithm(ValueError):
    """
    An unknown hash algorithm was specified.
    """
    def __init__(self, algo):
        UnknownHashAlgorithm.__init__(self, algo)
        self.algo = algo
