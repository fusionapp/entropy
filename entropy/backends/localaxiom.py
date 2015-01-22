from itertools import chain

from axiom.attributes import (
    text, path, timestamp, reference, integer, inmemory, AND)
from axiom.item import Item, transacted, normalize
from epsilon.extime import Time
from nevow.static import File
from nevow.inevow import IResource
from twisted.python import log
from twisted.python.components import registerAdapter
from twisted.internet.task import cooperate
from twisted.internet.defer import gatherResults
from zope.interface import implements

from entropy.hash import getHash
from entropy.ientropy import (
    IReadStore, IWriteStore, IContentObject, IMigration)
from entropy.errors import CorruptObject, NonexistentObject
from entropy.util import deferred



class ImmutableObject(Item):
    """
    An immutable object.

    Immutable objects are addressed by content hash, and consist of the object
    data as a binary blob, and object key/value metadata pairs.
    """
    implements(IContentObject)
    typeName = normalize('entropy.store.ImmutableObject')

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



class AxiomStore(Item):
    """
    A content store backend using Axiom items and files in the store.
    """
    implements(IReadStore, IWriteStore)
    hash = text(allowNone=False, default=u'sha256')


    @transacted
    def _storeObject(self, objectId, content, contentType, metadata={}, created=None):
        """
        Do the actual work of synchronously storing the object.
        """
        if metadata != {}:
            raise NotImplementedError('metadata not yet supported')

        contentDigest = getHash(self.hash)(content).hexdigest()
        contentDigest = unicode(contentDigest, 'ascii')
        calculatedId = u'%s:%s' % (self.hash, contentDigest)
        if objectId is not None and objectId != calculatedId:
            RuntimeError(
                'Object ID %r does not match calculated ID %r' % (
                    objectId, calculatedId))

        if created is None:
            created = Time()

        obj = self.store.findUnique(
            ImmutableObject,
            AND(ImmutableObject.hash == self.hash,
                ImmutableObject.contentDigest == contentDigest),
            default=None)
        if obj is None:
            contentFile = self.store.newFile(
                'objects', 'immutable', '%s:%s' % (self.hash, contentDigest))
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

        # XXX: this logic moves to the storage configuration
        #scheduler = IUploadScheduler(self.store, None)
        #for backend in self.store.powerupsFor(IBackendStore):
        #    if scheduler is None:
        #        raise RuntimeError('No upload scheduler configured')
        #    scheduler.scheduleUpload(obj.objectId, backend)

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


    # IReadStore

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


    # IWriteStore

    @deferred
    def storeObject(self, content, contentType, metadata={}, created=None, objectId=None):
        obj = self._storeObject(objectId, content, contentType, metadata, created)
        return obj.objectId



class PendingMigration(Item):
    """
    An item that tracks the state the migration of an individual object.

    Once a migration process decides to migrate a particular object, an
    instance of this item will be created to track the migration of the object,
    and will only be deleted once the object has been successfully migrated.
    """
    typeName = normalize('entropy.store.PendingMigration')

    parent = reference(
        allowNone=False,
        doc="The migration to which this object belongs.")
    # XXX: Make migrations independent of the local content store
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
    typeName = normalize('entropy.store.LocalStoreMigration')

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
        d = gatherResults(
            [task.whenDone() for task in tasks], consumeErrors=True)
        d.addCallback(_done)
        return d
