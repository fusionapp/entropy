"""
@copyright: 2007-2014 Quotemaster cc. See LICENSE for details.

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
from datetime import timedelta
from itertools import chain

from zope.interface import implements

from epsilon.extime import Time

from axiom.iaxiom import IScheduler
from axiom.item import Item, transacted
from axiom.attributes import (
    text, path, timestamp, AND, inmemory, reference, integer)
from axiom.dependency import dependsOn

from twisted.web import http
from twisted.python import log
from twisted.python.components import registerAdapter
from twisted.internet.defer import succeed, gatherResults, fail
from twisted.application.service import Service, IService
from twisted.internet.task import cooperate

from nevow.inevow import IResource, IRequest
from nevow.static import File
from nevow.rend import NotFound

from entropy.ientropy import (
    IContentStore, IContentObject, ISiblingStore, IBackendStore,
    IUploadScheduler, IMigrationManager, IMigration)
from entropy.errors import (
    CorruptObject, NonexistentObject, DigestMismatch, APIError)
from entropy.hash import getHash
from entropy.util import deferred
from entropy.client import Endpoint



class ImmutableObject(Item):
    """
    An immutable object.

    Immutable objects are addressed by content hash, and consist of the object
    data as a binary blob, and object key/value metadata pairs.
    """
    implements(IContentObject)

    hash = text(allowNone=False)
    contentDigest = text(allowNone=False, indexed=True)
    content = path(allowNone=False)
    contentType = text(allowNone=False)
    created = timestamp(allowNone=False, defaultFactory=lambda: Time())

    @property
    def metadata(self):
        return {}


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
            raise CorruptObject(
                'expected: %r actual: %r' % (self.contentDigest, digest))


    def getContent(self):
        return self.content.getContent()


def objectResource(obj):
    """
    Adapt L{ImmutableObject) to L{IResource}.
    """
    res = File(obj.content.path)
    res.type = obj.contentType.encode('ascii')
    res.encoding = None
    return res

registerAdapter(objectResource, ImmutableObject, IResource)



class PendingMigration(Item):
    """
    An item that tracks the state the migration of an individual object.

    Once a migration process decides to migrate a particular object, an
    instance of this item will be created to track the migration of the object,
    and will only be deleted once the object has been successfully migrated.
    """
    parent = reference(
        allowNone=False,
        doc="The migration to which this object belongs.")
    obj = reference(
        allowNone=False, reftype=ImmutableObject,
        doc="The object being migrated.")
    lastFailure = text(
        doc="A description of the last failed migration attempt, if any.")


    def attemptMigration(self):
        """
        Perform one attempt at migration of the object being tracked.

        If the migration is successful, this item will be deleted; otherwise,
        the failure will be stored in the C{lastFailure} attribute.

        @rtype: Deferred<None>
        """
        def _cb(ign):
            self.deleteFromStore()

        def _eb(f):
            log.err(f, 'Error during migration of %r from %r to %r' % (
                self.obj.objectId, self.parent.source, self.parent.destination))
            self.lastFailure = unicode(
                f.getTraceback(), 'ascii', errors='replace')

        d = self.parent.destination.storeObject(
            content=self.obj.getContent(),
            contentType=self.obj.contentType,
            metadata=self.obj.metadata,
            created=self.obj.created,
            objectId=self.obj.objectId)
        d.addCallbacks(_cb, _eb)
        return d



class LocalStoreMigration(Item):
    """
    Migration from local content store.
    """
    implements(IMigration)
    powerupInterfaces = [IMigration]

    source = reference(
        allowNone=False,
        doc="The content store that is the source of this migration")
    destination = reference(
        allowNone=False,
        doc="The content store that is the destination of this migration")
    start = integer(allowNone=False, doc="Starting storeID")
    current = integer(allowNone=False, doc="Most recent storeID migrated")
    end = integer(allowNone=False, doc="Ending storeID")

    concurrency = 4

    _running = inmemory()

    def activate(self):
        self._running = False


    @transacted
    def _nextObject(self):
        """
        Obtain the next object for which migration should be attempted.
        """
        obj = self.store.findFirst(
            ImmutableObject,
            AND(ImmutableObject.storeID > self.current,
                ImmutableObject.storeID <= self.end),
            sort=ImmutableObject.storeID.asc)
        if obj is None:
            return None
        self.current = obj.storeID
        return PendingMigration(store=self.store, parent=self, obj=obj)


    # IMigration

    def run(self):
        """
        Perform the migration.
        """
        if self._running:
            return
        self._running = True

        def _done(ign):
            self._running = False

        it = (m.attemptMigration()
              for m in chain(self.store.query(PendingMigration),
                             iter(self._nextObject, None)))
        tasks = [cooperate(it) for _ in xrange(self.concurrency)]
        d = gatherResults([task.whenDone() for task in tasks])
        d.addCallback(_done)
        return d



class ContentStore(Item):
    """
    Manager for stored objects.
    """
    implements(IContentStore)
    powerupInterfaces = [IContentStore]

    hash = text(allowNone=False, default=u'sha256')

    @transacted
    def _storeObject(self, content, contentType, metadata={}, created=None):
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
            AND(ImmutableObject.hash == self.hash,
                ImmutableObject.contentDigest == contentDigest),
            default=None)
        if obj is None:
            bucket = contentDigest[:4]
            contentFile = self.store.newFile(
                'objects', 'immutable', bucket,
                '%s:%s' % (self.hash, contentDigest))
            contentFile.write(content)
            contentFile.close()

            obj = ImmutableObject(store=self.store,
                                  contentDigest=contentDigest,
                                  hash=self.hash,
                                  content=contentFile.finalpath,
                                  contentType=contentType,
                                  created=created)
        else:
            obj.contentType = contentType
            obj.created = created

        scheduler = IUploadScheduler(self.store, None)
        for backend in self.store.powerupsFor(IBackendStore):
            if scheduler is None:
                raise RuntimeError('No upload scheduler configured')
            scheduler.scheduleUpload(obj.objectId, backend)

        return obj


    def importObject(self, obj):
        """
        Import an object from elsewhere.

        @param obj: the object to import.
        @type obj: ImmutableObject
        """
        return self._storeObject(obj.getContent(),
                                 obj.contentType,
                                 obj.metadata,
                                 obj.created)


    @transacted
    def getSiblingObject(self, objectId):
        """
        Import an object from a sibling store.

        @returns: the local imported object.
        @type obj: ImmutableObject
        """
        siblings = list(self.store.powerupsFor(ISiblingStore))
        siblings.extend(self.store.powerupsFor(IBackendStore))
        siblings = iter(siblings)

        def _eb(f):
            f.trap(NonexistentObject)
            try:
                remoteStore = siblings.next()
            except StopIteration:
                raise NonexistentObject(objectId)

            d = remoteStore.getObject(objectId)
            d.addCallbacks(self.importObject, _eb)
            return d

        return self.getObject(objectId).addErrback(_eb)


    # IContentStore

    @deferred
    def storeObject(self, content, contentType, metadata={}, created=None):
        obj = self._storeObject(content, contentType, metadata, created)
        return obj.objectId


    @deferred
    @transacted
    def getObject(self, objectId):
        hash, contentDigest = objectId.split(u':', 1)
        obj = self.store.findUnique(
            ImmutableObject,
            AND(ImmutableObject.hash == hash,
                ImmutableObject.contentDigest == contentDigest),
            default=None)
        if obj is None:
            raise NonexistentObject(objectId)
        return obj


    @transacted
    def migrateTo(self, destination):
        latestObject = self.store.findFirst(
            ImmutableObject, sort=ImmutableObject.storeID.desc)
        return LocalStoreMigration(
            store=self.store,
            source=self,
            destination=destination,
            start=0,
            current=-1,
            end=latestObject.storeID)



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

        d = self.contentStore.storeObject(data, contentType)
        return d.addCallback(_cb)



class ContentResource(Item):
    """
    Resource for accessing the content store.
    """
    implements(IResource)
    powerupInterfaces = [IResource]

    addSlash = inmemory()

    contentStore = dependsOn(ContentStore)

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
