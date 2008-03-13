from entropy.errors import UnknownHashAlgorithm

try:
    from hashlib import sha256
except ImportError:
    sha256 = None

try:
    from M2Crypto.EVP import MessageDigest
except ImportError:
    MessageDigest = None


class MessageDigestWrapper(object):
    """
    hashlib-like wrapper around L{MessageDigest}
    """
    def __init__(self, algo, data=None):
        self.frozen = False
        self.md = MessageDigest(algo)
        if data is not None:
            self.update(data)

    def update(self, data):
        if self.frozen:
            raise RuntimeError('Cannot update() after retrieving digest')
        self.md.update(data)

    def digest(self):
        if self.frozen:
            return self._digest
        else:
            self.frozen = True
            self._digest = self.md.digest()
            return self._digest

    def hexdigest(self):
        return self.digest().encode('hex')


if sha256 is None:
    def sha256(*a, **kw):
        return MessageDigestWrapper('sha256', *a, **kw)


_hashes = {
    u'sha256': sha256,
    }

def getHash(algo):
    try:
        return _hashes[algo]
    except KeyError:
        return UnknownHashAlgorithm(algo)
