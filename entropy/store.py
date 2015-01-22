"""
@copyright: 2007-2015 Quotemaster cc. See LICENSE for details.

Object data store.

This service acts as a cache / access point for one or more backend object
stores. The configuration of backends is flexible, and a variety of backends
are currently available, including a local on-disk store, and Amazon S3.
Objects are addressed by content hash, and thus are immutable; as a result,
storing an object is idempotent. Deletion of objects is not currently
supported.

The service frontend provides a very limited set of operations:

    - GET to retrieve an object.

    - HEAD to retrieve just the metadata of an object.

    - PUT to store an object.

The implementation of these operations is driven by a configuration of
backends; this consists of any number of L{entropy.ientropy.IReadStore},
L{entropy.ientropy.IWriteStore}, and/or L{entropy.ientropy.IDeferredWriteStore}
powerups.

To retrieve an object, each C{IReadStore} will be consulted in priority order
until the object is found, or the available backends are exhausted.

To store an object, it is stored in each C{IWriteStore} backend before
returning, and scheduled to be stored in each C{IDeferredWriteStore} at a later
point in time.
"""
import hashlib
from datetime import timedelta

from zope.interface import implements

from epsilon.extime import Time

from axiom.iaxiom import IScheduler
from axiom.item import Item, transacted, normalize
from axiom.attributes import (
    text, timestamp, inmemory, reference, integer)
from axiom.dependency import dependsOn

from twisted.web import http
from twisted.python import log
from twisted.python.components import registerAdapter
from twisted.internet.defer import succeed, fail
from twisted.application.service import Service, IService

from nevow.inevow import IResource, IRequest
from nevow.static import Data
from nevow.rend import NotFound

from entropy.ientropy import (
    IContentStore, IContentObject, IUploadScheduler, IMigrationManager,
    IMigration, IReadStore)
from entropy.errors import (
    NonexistentObject, DigestMismatch, APIError)
from entropy.hash import getHash
from entropy.client import Endpoint
from entropy.backends.axiomstore import AxiomStore, ImmutableObject



def genericObjectResource(obj):
    """
    Adapt L{IContentObject} to L{IResource}.
    """
    return Data(obj.getContent(), obj.contentType.encode('ascii'))

registerAdapter(genericObjectResource, IContentObject, IResource)



class StorageConfiguration(Item):
    """
    A configuration of storage backends.
    """
    implements(IContentStore)
    powerupInterfaces = [IContentStore]
    typeName = normalize('entropy.store.ContentStore')

    hash = text(allowNone=False, default=u'sha256')


    @transacted
    def getObject(self, objectId):
        """
        Retrieve an object from a backend, if possible.
        """
        backends = iter(list(self.store.powerupsFor(IReadStore)))

        def _eb(f):
            f.trap(NonexistentObject)
            try:
                remoteStore = backends.next()
            except StopIteration:
                raise NonexistentObject(objectId)

            d = remoteStore.getObject(objectId)
            d.addCallbacks(self.importObject, _eb)
            return d

        return self.getObject(objectId).addErrback(_eb)



class ObjectCreator(object):
    """
    Resource for storing new objects.

    @ivar storage: The {StorageConfiguration} to create objects in.
    """
    implements(IResource)

    def __init__(self, storage):
        self.storage = storage


    # IResource
    def renderHTTP(self, ctx):
        req = IRequest(ctx)
        if req.method == 'GET':
            req.setHeader('Content-Type', 'text/plain')
            return 'PUT data here to create an object.'
        elif req.method == 'PUT':
            return self.handlePUT(req)
        else:
            req.setResponseCode(http.NOT_ALLOWED)
            req.setHeader('Content-Type', 'text/plain')
            return 'Method not allowed'


    def handlePUT(self, req):
        data = req.content.read()
        contentType = unicode(
            req.getHeader('Content-Type') or 'application/octet-stream',
            'ascii')

        contentMD5 = req.getHeader('Content-MD5')
        if contentMD5 is not None:
            expectedHash = contentMD5.decode('base64')
            actualHash = hashlib.md5(data).digest()
            if expectedHash != actualHash:
                raise DigestMismatch(expectedHash, actualHash)

        def _cb(objectId):
            req.setHeader('Content-Type', 'text/plain')
            objectId = objectId.encode('ascii')
            return objectId

        d = self.storage.storeObject(data, contentType)
        return d.addCallback(_cb)



class ContentResource(Item):
    """
    Resource for accessing the content store.
    """
    implements(IResource)
    powerupInterfaces = [IResource]

    addSlash = inmemory()

    # attribute name is historic
    contentStore = dependsOn(StorageConfiguration)

    def getObject(self, name):
        def _notFound(f):
            f.trap(NonexistentObject)
            return None
        return self.contentStore.getSiblingObject(name).addErrback(_notFound)


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
            return self.getObject(name)
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
                return res, segments[1:]
        return NotFound



class RemoteEntropyStore(Item):
    """
    IContentStore implementation for remote Entropy services.
    """
    implements(IContentStore)

    entropyURI = text(allowNone=False,
                      doc="""The URI of the Entropy service in use.""")

    _endpoint = inmemory()


    def activate(self):
        self._endpoint = Endpoint(uri=self.entropyURI)


    # IContentStore

    def storeObject(self, content, contentType, metadata={}, created=None):
        return self._endpoint.store(
            content=content,
            contentType=contentType,
            metadata=metadata,
            created=created)


    def getObject(self, objectId):
        def _checkError(f):
            f.trap(APIError)
            if f.value.code == http.NOT_FOUND:
                return fail(NonexistentObject(objectId))
            return f

        d = self._endpoint.get(objectId)
        d.addErrback(_checkError)
        return d



class _PendingUpload(Item):
    """
    Marker for a pending upload to a backend store.
    """
    objectId = text(allowNone=False)
    backend = reference(allowNone=False) # reftype=IBackendStore
    scheduled = timestamp(
        indexed=True, allowNone=False, defaultFactory=lambda: Time())


    def _nextAttempt(self):
        """
        Determine the time to schedule the next attempt.
        """
        return Time() + timedelta(minutes=2)


    def run(self):
        self.attemptUpload()


    def attemptUpload(self):
        """
        Attempt an upload of an object to a backend store.

        If the upload fails, it will be rescheduled; if it succeeds, this item
        will be deleted.
        """
        def _uploadObject(obj):
            return self.backend.storeObject(
                obj.getContent(),
                obj.contentType,
                obj.metadata,
                obj.created,
                objectId=self.objectId)

        def _reschedule(f):
            # We do this instead of returning a Time from attemptUpload,
            # because that can only be done synchronously.
            log.err(f, 'Error uploading object %r to backend store %r' % (
                self.objectId, self.backend))
            self.scheduled = self._nextAttempt()
            self.schedule()
            return f

        d = succeed(None)
        d.addCallback(
            lambda ign: IContentStore(self.store).getObject(self.objectId))
        d.addCallback(_uploadObject)
        d.addCallbacks(lambda ign: self.deleteFromStore(), _reschedule)
        return d


    def schedule(self):
        IScheduler(self.store).schedule(self, self.scheduled)



class UploadScheduler(Item):
    """
    Schedule upload attempts for pending uploads.
    """
    implements(IUploadScheduler)
    powerupInterfaces = [IUploadScheduler]

    dummy = text()

    # IUploadScheduler

    def scheduleUpload(self, objectId, backend):
        upload = _PendingUpload(
            store=self.store,
            objectId=objectId,
            backend=backend)
        upload.schedule()



class MigrationManager(Item, Service):
    """
    Default migration manager implementation.
    """
    implements(IMigrationManager, IService)
    powerupInterfaces = [IMigrationManager, IService]

    dummy = integer()

    # IService
    parent = inmemory()
    name = inmemory()
    running = inmemory()

    def activate(self):
        self.parent = None
        self.name = None
        self.running = False


    def installed(self):
        """
        Callback invoked after this item has been installed on a store.

        This is used to set the service parent to the store's service object.
        """
        self.setServiceParent(self.store)


    def deleted(self):
        """
        Callback invoked after a transaction in which this item has been
        deleted is committed.

        This is used to remove this item from its service parent, if it has
        one.
        """
        if self.parent is not None:
            self.disownServiceParent()


    # IMigrationManager

    def migrate(self, source, destination):
        """
        Initiate a migration between two content stores.

        @see: L{entropy.ientropy.IMigrationManager.migrate}
        """
        migration = source.migrateTo(destination)
        self.store.powerUp(migration, IMigration)
        migration.run()
        return migration


    # IService

    def startService(self):
        self.running = True
        for migration in self.store.powerupsFor(IMigration):
            migration.run()
