"""
Local Axiom object data store.

This stores objects in the local Axiom data store and retrieves them
as required.
"""
from zope.interface import implements

from epsilon.extime import Time

from axiom.item import Item, transacted
from axiom.attributes import text, path, timestamp
from axiom.upgrade import registerUpgrader

from twisted.python import log
from twisted.python.components import registerAdapter
from twisted.application.service import IService

from nevow.inevow import IResource
from nevow.static import File

from entropy.ientropy import IBackendStore, IContentObject
from entropy.errors import CorruptObject, NonexistentObject
from entropy.hash import getHash
from entropy.util import deferred



class ImmutableObject(Item):
    """
    An immutable object.

    Immutable objects are addressed by content hash, and consist of the object
    data as a binary blob, and object key/value metadata pairs.
    """
    schemaVersion = 2

    implements(IContentObject)

    hash = text(allowNone=False)
    contentDigest = text(allowNone=False)
    content = path(allowNone=False)
    contentType = text(allowNone=True)
    created = timestamp(allowNone=False, defaultFactory=lambda: Time())
    objectId = text(allowNone=False)

    @property
    def metadata(self):
        return {}


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
            raise CorruptObject(
                'expected: %r actual: %r' % (self.contentDigest, digest))


    def getContent(self):
        return self.content.getContent()


def immutableObject1to2(old):
    return old.upgradeVersion(
        ImmutableObject.typeName, 1, 2,
        hash=old.hash,
        contentDigest=old.contentDigest,
        content=old.content,
        contentType=old.contentType,
        created=old.created,
        objectId=old.hash + u':' + old.contentDigest)

registerUpgrader(immutableObject1to2, ImmutableObject.typeName, 1, 2)



def objectResource(obj):
    """
    Adapt L{ImmutableObject) to L{IResource}.
    """
    obj.verify()
    res = File(obj.content.path)
    res.type = obj.contentType.encode('ascii')
    res.encoding = None
    return res

registerAdapter(objectResource, ImmutableObject, IResource)



class ContentStore(Item):
    """
    Manager for stored objects.
    """
    implements(IBackendStore, IService)

    powerupInterfaces=[IService]

    hash = text(allowNone=False, default=u'sha256')

    @transacted
    def _storeObject(self, objectId, content, contentType=None, metadata={}, created=None):
        """
        Do the actual work of synchronously storing the object.
        """
        if metadata != {}:
            raise NotImplementedError('metadata not yet supported')

        contentDigest = getHash(self.hash)(content).hexdigest()
        contentDigest = unicode(contentDigest, 'ascii')

        if created is None:
            created = Time()

        obj = self.store.findUnique(
            ImmutableObject,
            ImmutableObject.objectId == objectId,
            default=None)
        if obj is None:
            contentFile = self.store.newFile('objects', 'immutable', objectId)
            contentFile.write(content)
            contentFile.close()

            obj = ImmutableObject(store=self.store,
                                  contentDigest=contentDigest,
                                  hash=self.hash,
                                  content=contentFile.finalpath,
                                  contentType=contentType,
                                  created=created,
                                  objectId=objectId)
        else:
            obj.contentType = contentType
            obj.created = created

        return obj


    # IBackendStore

    @deferred
    def storeObject(self, objectId, content, contentType=None, metadata={}, created=None):
        log.msg("Storing [%s] in localaxiom." % objectId)
        obj = self._storeObject(objectId, content, contentType, metadata, created)
        return obj.objectId


    @deferred
    @transacted
    def getObject(self, objectId):
        log.msg("Getting [%s] from localaxiom." % objectId)
        obj = self.store.findUnique(
            ImmutableObject,
            ImmutableObject.objectId == objectId,
            default=None)
        if obj is None:
            raise NonexistentObject(objectId)
        return obj


    # IService

    def startService(self):
        log.msg("Started localaxiom")
