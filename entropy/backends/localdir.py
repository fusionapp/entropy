"""
Local filesystem data store.

This stores objects in the a directory in the local filesystem and
retrieves them as required.
"""
import os.path

from zope.interface import implements

from axiom.item import Item, transacted
from axiom.attributes import text

from twisted.python import log
from twisted.python.components import registerAdapter
from twisted.application.service import IService

from nevow.inevow import IResource
from nevow.static import File

from entropy.ientropy import IBackendStore, IContentObject
from entropy.errors import NonexistentObject
from entropy.util import deferred



class FilesystemImmutableObject(object):
    """
    An immutable object.
    """
    implements(IContentObject)

    contentType = None
    created = None
    objectId = None
    filepath = None

    def __init__(self, objectId, filepath):
        self.objectId = objectId
        self.filepath = filepath
        self.contentType = u'binary/octet-stream'


    @property
    def metadata(self):
        return {}


    def getContent(self):
        return open(self.filepath).read()


    def exists(self):
        return os.path.isfile(self.filepath)



def objectResource(obj):
    """
    Adapt L{FilesystemImmutableObject) to L{IResource}.
    """
    res = File(obj.filepath)
    res.type = obj.contentType.encode('ascii')
    res.encoding = None
    return res

registerAdapter(objectResource, FilesystemImmutableObject, IResource)



class FilesystemContentStore(Item):
    """
    Manager for stored objects.
    """
    implements(IBackendStore, IService)

    powerupInterfaces=[IService]

    repoDirectory = text(allowNone=False)


    @transacted
    def _storeObject(self, objectId, content, contentType=None, metadata={}, created=None):
        """
        Do the actual work of synchronously storing the object.
        """
        if metadata != {}:
            raise NotImplementedError('metadata not yet supported')

        filepath = os.path.join(self.repoDirectory, objectId)

        contentFile = open(filepath, 'w')
        contentFile.write(content)
        contentFile.close()

        return FilesystemImmutableObject(objectId, filepath)


    # IBackendStore

    @deferred
    def storeObject(self, objectId, content, contentType=None, metadata={}, created=None):
        log.msg("Storing [%s] in localdir." % objectId)
        obj = self._storeObject(objectId, content, contentType, metadata, created)
        return obj.objectId


    @deferred
    @transacted
    def getObject(self, objectId):
        log.msg("Getting [%s] from localdir." % objectId)
        filepath = os.path.join(self.repoDirectory, objectId)
        obj = FilesystemImmutableObject(objectId, filepath)
        if not obj.exists():
            raise NonexistentObject(objectId)
        return obj


    # IService

    def startService(self):
        log.msg("Started localdir")
