from epsilon.extime import Time

from axiom.item import Item
from axiom.attributes import text, path, timestamp

from nevow.static import File

from entropy.errors import CorruptObject
from entropy.hash import getHash


class ImmutableObject(Item):
    """
    An immutable object.

    Immutable objects are addressed by content hash, and consist of the object
    data as a binary blob, and object key/value metadata pairs.
    """
    contentDigest = text(allowNone=False)
    hash = text(allowNone=False)
    content = path(allowNone=False)
    contentType = text(allowNone=False)
    created = timestamp(allowNone=False, defaultFactory=lambda: Time())

    def _getDigest(self):
        fp = self.content.open()
        try:
            h = getHash(self.hash)(fp.read())
            return unicode(h.hexdigest(), 'ascii')
        finally:
            fp.close()

    def verify(self):
        digest = self._getDigest()
        if self.contentHash != digest:
            raise CorruptObject('expected: %r actual: %r' % (self.contentDigest, digest))

def objectResource(obj):
    """
    Adapt L{ImmutableObject) to L{IResource}.
    """
    return File(obj.content.path, defaultType=obj.contentType)


class ContentStore(Item):
    """
    Manager for stored objects.
    """
    hash = text(allowNone=False, default=u'sha256')

    def storeObject(self, content, contentType, metadata={}):
        if metadata != {}:
            raise NotImplementedError('metadata not yet supported')

        contentDigest = unicode(getHash(self.hash)(content).hexdigest(), 'ascii')

        contentFile = self.store.newFile('objects', 'immutable', '%s:%s' % (self.hash, contentDigest))
        try:
            contentFile.write(content)
            contentFile.close()
        except:
            contentFile.abort()
            raise

        obj = ImmutableObject(store=self.store,
                              contentDigest=contentDigest,
                              hash=self.hash,
                              content=contentFile.finalpath,
                              contentType=contentType)
        return obj.objectId
