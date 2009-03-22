"""
Object data store.

This service acts as a cache / access point for a backend object store;
currently Amazon S3 is used as the backend store, but the architecture should
be flexible enough to allow other possibilities. The system is designed to
handle objects in an immutable fashion; once an object is created, it exists in
perpetuity, and the contents will never change.

The service's functionality is two-fold; firstly, it handles requests for
retrieval of objects, servicing them from the local cache, fetching them from a
neighbour cache, or retrieving them from the backend store. Secondly, it
handles requests for storage of a new object; the object is first cached
locally to ensure local view consistency, and then queued for backend storage
in a reliable fashion.
"""
import hashlib

from zope.interface import implements

from epsilon.extime import Time

from axiom.item import Item
from axiom.attributes import text, path, timestamp, AND, inmemory
from axiom.dependency import dependsOn

from twisted.web import http
from twisted.python.components import registerAdapter

from nevow.inevow import IResource, IRequest
from nevow.static import File
from nevow.rend import NotFound
from nevow.url import URL

from entropy.ientropy import IContentStore
from entropy.errors import CorruptObject, NonexistentObject
from entropy.hash import getHash


class ImmutableObject(Item):
    """
    An immutable object.

    Immutable objects are addressed by content hash, and consist of the object
    data as a binary blob, and object key/value metadata pairs.
    """
    hash = text(allowNone=False)
    contentDigest = text(allowNone=False)
    content = path(allowNone=False)
    contentType = text(allowNone=False)
    created = timestamp(allowNone=False, defaultFactory=lambda: Time())

    @property
    def objectId(self):
        return u'%s:%s' % (self.hash, self.contentDigest)

    def _getDigest(self):
        fp = self.content.open()
        try:
            h = getHash(self.hash)(fp.read())
            return unicode(h.hexdigest(), 'ascii')
        finally:
            fp.close()

    def verify(self):
        digest = self._getDigest()
        if self.contentDigest != digest:
            raise CorruptObject('expected: %r actual: %r' % (self.contentDigest, digest))

def objectResource(obj):
    """
    Adapt L{ImmutableObject) to L{IResource}.
    """
    # XXX: Not sure if we should do this on every single resource retrieval.
    obj.verify()
    return File(obj.content.path, defaultType=obj.contentType.encode('ascii'))

registerAdapter(objectResource, ImmutableObject, IResource)


class ContentStore(Item):
    """
    Manager for stored objects.
    """
    implements(IContentStore)

    hash = text(allowNone=False, default=u'sha256')

    # IContentStore
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

    def getObject(self, objectId):
        hash, contentDigest = objectId.split(u':', 1)
        obj = self.store.findUnique(ImmutableObject,
                                    AND(ImmutableObject.hash == hash,
                                        ImmutableObject.contentDigest == contentDigest),
                                    default=None)
        if obj is None:
            raise NonexistentObject(objectId)
        return obj


class ObjectCreator(object):
    """
    Resource for storing new objects.

    @ivar contentStore: The {IContentStore} provider to create objects in.
    """
    implements(IResource)

    def __init__(self, contentStore):
        self.contentStore = contentStore

    # IResource
    def renderHTTP(self, ctx):
        req = IRequest(ctx)
        if req.method == 'GET':
            return 'PUT data here to create an object.'
        elif req.method == 'PUT':
            return self.handlePUT(req)
        else:
            req.setResponseCode(http.NOT_ALLOWED)
            req.setHeader('Content-Type', 'text/plain')
            return 'Method not allowed'

    def handlePUT(self, req):
        data = req.content.read()
        contentType = unicode(req.getHeader('Content-Type') or 'application/octet-stream', 'ascii')

        contentMD5 = req.getHeader('Content-MD5')
        if contentMD5 is not None:
            expectedHash = contentMD5.decode('base64')
            actualHash = hashlib.md5(data).digest()
            if expectedHash != actualHash:
                raise ValueError('Expected hash %r does not match actual hash %r' % (expectedHash, actualHash))

        objectId = self.contentStore.storeObject(data, contentType)
        return objectId.encode('ascii')


class ContentResource(Item):
    """
    Resource for accessing the content store.
    """
    implements(IResource)
    powerupInterfaces = [IResource]

    addSlash = inmemory()

    contentStore = dependsOn(ContentStore)

    def childFactory(self, name):
        """
        Hook up children.

        / is the root, nothing to see her.

        /new is how new objects are stored.

        /<objectId> is where existing objects are retrieved.
        """
        if name == '':
            return self
        elif name == 'new':
            return ObjectCreator(self.contentStore)
        else:
            try:
                obj = self.contentStore.getObject(name)
            except NonexistentObject:
                pass
            else:
                return obj
        return None

    # IResource
    def renderHTTP(self, ctx):
        """
        Nothing to see here.
        """
        return 'Entropy'

    def locateChild(self, ctx, segments):
        """
        Dispatch to L{childFactory}.
        """
        if len(segments) >= 1:
            res = self.childFactory(segments[0])
            if res is not None:
                return IResource(res), segments[1:]
        return NotFound
