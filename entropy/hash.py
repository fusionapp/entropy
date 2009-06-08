from entropy.errors import UnknownHashAlgorithm

from hashlib import sha256

_hashes = {
    u'sha256': sha256,
    }

def getHash(algo):
    try:
        return _hashes[algo]
    except KeyError:
        raise UnknownHashAlgorithm(algo)
