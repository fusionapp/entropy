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
from datetime import timedelta

from zope.interface import implements

from epsilon.extime import Time

from axiom.item import Item
from axiom.attributes import text, timestamp, inmemory, reference
from axiom.dependency import dependsOn

from twisted.web import http
from twisted.python import log
from twisted.application.service import Service, IService
from twisted.internet.defer import DeferredList

from nevow.inevow import IResource, IRequest
from nevow.rend import NotFound

from entropy.ientropy import (IBackendStore, IReadBackend, IWriteBackend,
                              IWriteLaterBackend, IUploadScheduler, IStorageClass)
from entropy.errors import NonexistentObject, NonexistentStorageClass, DigestMismatch
from entropy.hash import getHash
from entropy.util import deferred

from entropy.backends.localaxiom import ContentStore



class StorageClass(Item):
    implements(IStorageClass)

    name = text()


    def getReadBackends(self):
        return iter(self.powerupsFor(IReadBackend))


    def getWriteBackends(self):
        return iter(self.powerupsFor(IWriteBackend))


    def getWriteLaterBackends(self):
        return iter(self.powerupsFor(IWriteLaterBackend))


    def getObject(self, objectId):
        """
        Find an object in one of the read stores and return it.

        @returns: the retrieved object.
        @type obj: ImmutableObject
        """
        backends = self.getReadBackends()

        @deferred
        def _getNothing(objectId):
            raise NonexistentObject(objectId)

        def _eb(f):
            f.trap(NonexistentObject)
            try:
                backend = backends.next()
            except StopIteration:
                raise NonexistentObject(objectId)

            d = backend.getObject(objectId)
            d.addErrback(_eb)
            return d

        return _getNothing(objectId).addErrback(_eb)


    def storeObject(self, objectId, content, contentType=None, metadata={}, created=None):
        """
        Store an object in all backends.
        """
        if metadata != {}:
            raise NotImplementedError('metadata not yet supported')

        results = []
        for backend in self.getWriteBackends():
            results.append(backend.storeObject(objectId,
                                               content,
                                               contentType,
                                               metadata,
                                               created))

        for backend in self.getWriteLaterBackends():
            PendingUpload(store=self.store,
                          objectId=objectId,
                          backend=backend)

        return DeferredList(results, fireOnOneErrback=True)



class ObjectCreator(object):
    """
    Resource for storing new objects.

    @ivar sotrageClass: The {IStorageClass} provider to create objects in.
    """
    implements(IResource)

    def __init__(self, storageClass, objectId):
        self.storageClass = storageClass
        self.objectId = objectId


    # IResource
    def renderHTTP(self, ctx):
        req = IRequest(ctx)
        if req.method == 'GET':
            return self.handleGET(req)
        elif req.method == 'PUT':
            return self.handlePUT(req)
        else:
            req.setResponseCode(http.NOT_ALLOWED)
            req.setHeader('Content-Type', 'text/plain')
            return 'Method not allowed'


    def handleGET(self, req):
        req.setHeader('Content-Type', 'text/plain')
        return 'PUT data here to create an object.'


    def handlePUT(self, req):
        data = req.content.read()
        # if self.objectId is None:
        #     contentDigest = getHash(self.contentStore.hash)(data).hexdigest()
        #     self.objectId = u'%s:%s' % (self.contentStore.hash, contentDigest)
        return self.putObject(self.objectId, data, req)


    def putObject(self, objectId, data, req):
        contentType = unicode(
            req.getHeader('Content-Type') or 'application/octet-stream',
            'ascii')

        contentMD5 = req.getHeader('Content-MD5')
        if contentMD5 is not None:
            expectedHash = contentMD5.decode('base64')
            actualHash = hashlib.md5(data).digest()
            if expectedHash != actualHash:
                raise DigestMismatch(expectedHash, actualHash)

        def _cb(storeResults):
            objectId = None
            for (success, objid) in storeResults:
                if not success:
                    raise Exception("Store failed.")
                objectId = objid
            req.setHeader('Content-Type', 'text/plain')
            return objectId.encode('ascii')

        d = self.storageClass.storeObject(objectId, data, contentType)
        return d.addCallback(_cb)



class ContentResource(Item):
    """
    Resource for accessing the content store.
    """
    implements(IResource)
    powerupInterfaces = [IResource]

    addSlash = inmemory()

    defaultStorageClass = dependsOn(StorageClass)

    def getStorageClass(self, storageClassName):
        if storageClassName is None:
            return self.defaultStorageClass
        for storageClass in self.store.powerupsFor(IStorageClass):
            if storageClass.name == storageClassName:
                return storageClass
        raise NonexistentStorageClass(storageClassName)


    def getObject(self, storageClassName, objectId):
        def _notFound(f):
            f.trap(NonexistentObject)
            return None
        d = self.getStorageClass(storageClassName).getObject(objectId)
        return d.addErrback(_notFound)


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
            return ObjectCreator(self.getStorageClass(None))
        else:
            return self.getObject(None, name)
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


def _init(sc):
    sc.name = u'default'
    return sc


class IdContentResource(Item):
    """
    Resource for accessing the content store.
    """
    implements(IResource)
    powerupInterfaces = [IResource]

    addSlash = inmemory()

    defaultStorageClass = dependsOn(StorageClass, _init)

    def getStorageClass(self, storageClassName):
        if storageClassName is None:
            return self.defaultStorageClass
        for storageClass in self.store.powerupsFor(IStorageClass):
            if storageClass.name == storageClassName:
                return storageClass
        raise NonexistentStorageClass(storageClassName)


    def getObject(self, storageClassName, objectId):
        def _notFound(f):
            f.trap(NonexistentObject)
            return None
        d = self.getStorageClass(storageClassName).getObject(objectId)
        return d.addErrback(_notFound)


    def childFactory(self, name, method):
        """
        Hook up children.

        / is the root, nothing to see here.

        /<objectId> is where existing objects are stored retrieved.
        """
        if name == '':
            return self
        elif method in ['PUT', 'POST']:
            return ObjectCreator(self.getStorageClass(None), unicode(name, 'ascii'))
        else:
            return self.getObject(None, unicode(name, 'ascii'))
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
            res = self.childFactory(segments[0], IRequest(ctx).method)
            if res is not None:
                return res, segments[1:]
        return NotFound



class PendingUpload(Item):
    """
    Marker for a pending upload to a backend store.
    """
    objectId = text(allowNone=False)
    backend = reference(allowNone=False) # reftype=IBackendStore
    scheduled = timestamp(indexed=True, allowNone=False, defaultFactory=lambda: Time())


    def attemptUpload(self):
        def _uploadObject(obj):
            return self.backend.storeObject(
                obj.objectId,
                obj.getContent(),
                obj.contentType,
                obj.metadata,
                obj.created)

        def _reschedule(f):
            log.err(f, 'Error uploading to backend store')
            self.scheduled += timedelta(minutes=2)
            return f

        d = IBackendStore(self.store).getObject(self.objectId)
        d.addCallback(_uploadObject)
        d.addCallback(lambda ign: self.deleteFromStore())
        d.addErrback(_reschedule)
        return d



class UploadScheduler(Item, Service):
    """
    Schedule upload attempts for pending uploads.
    """
    implements(IService, IUploadScheduler)
    powerupInterfaces = [IService, IUploadScheduler]

    dummy = text()

    parent = inmemory()
    name = inmemory()
    running = inmemory()
    _wakeCall = inmemory()
    _clock = inmemory()

    def activate(self):
        self.parent = None
        self.name = None
        self.running = False
        self._wakeCall = None

        from twisted.internet import reactor
        self._clock = reactor


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


    def _scheduledWake(self):
        self._wakeCall = None
        self.wake()


    def _cancelWake(self):
        if self._wakeCall is not None:
            self._wakeCall.cancel()
            self._wakeCall = None


    def _now(self):
        return Time()


    # IUploadScheduler

    def wake(self):
        self._cancelWake()
        now = self._now()

        # Find an upload we can try right now
        p = self.store.findFirst(
            PendingUpload,
            PendingUpload.scheduled <= now)
        if p is not None:
            p.attemptUpload().addBoth(lambda ign: self.wake())
        else:
            # If there wasn't anything, schedule a wake for when there will be
            # something
            p = self.store.findFirst(
                PendingUpload,
                sort=PendingUpload.scheduled.ascending)
            if p is not None:
                self._clock.callLater((p.scheduled - now).seconds, self.wake)


    # IService

    def startService(self):
        self.running = True
        self._wakeCall = self._clock.callLater(0, self._scheduledWake)


    def stopService(self):
        self.running = False
        self._cancelWake()
