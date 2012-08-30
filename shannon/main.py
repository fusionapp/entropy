import hashlib

from zope.interface import implements

from axiom.item import Item
from axiom.attributes import text, inmemory

from twisted.web import http
from nevow.inevow import IResource, IRequest
from nevow.rend import NotFound

from entropy.errors import NonexistentObject, DigestMismatch
from entropy.store import RemoteEntropyStore

from shannon.cassandra import CassandraIndex
from shannon.util import metadataFromHeaders, tagsToDict


class ShannonCreator(object):
    """
    Resource for storing new objects in entropy, and metadata in cassandra.

    @ivar remoteStore: The entropy node URL.
    """
    implements(IResource)

    def __init__(self, entropyURI=u'http://localhost:8080/'):
        self.entropyURI = entropyURI


    # IResource
    def renderHTTP(self, ctx):
        req = IRequest(ctx)
        if req.method == 'GET':
            req.setHeader('Content-Type', 'text/plain')
            return 'POST data here to create an object.'
        elif req.method == 'POST':
            return self.handlePOST(req)
        else:
            req.setResponseCode(http.NOT_ALLOWED)
            req.setHeader('Content-Type', 'text/plain')
            return 'Method not allowed'


    def handlePOST(self, req):
        data = req.content.read()
        contentType = req.getHeader('Content-Type') or 'application/octet-stream'
        metadata = metadataFromHeaders(req)
        contentMD5 = req.getHeader('Content-MD5')

        if contentMD5 is not None:
            expectedHash = contentMD5.decode('base64')
            actualHash = hashlib.md5(data).digest()
            if expectedHash != actualHash:
                raise DigestMismatch(expectedHash, actualHash)

        # Checks for required headers.
        if not metadata['X-Entropy-Name']:
            raise ValueError('X-Entropy-Name is manditory')
        if not metadata['X-Shannon-Description']:
            raise ValueError('X-Shannon-Description is manditory')

        def _cb(objectId):
            objectId = objectId.encode('ascii')
            return objectId

        d = RemoteEntropyStore(entropyURI=self.entropyURI).storeObject(
            data, contentType)
        d.addCallback(_cb)

        tags = tagsToDict(metadata['X-Shannon-Tags'])
        d.addCallback(CassandraIndex().insert,
            metadata['X-Entropy-Name'],
            metadata['X-Shannon-Description'],
            tags)
        return d



class CoreResource(Item):
    """
    Resource for retrieving and updating a Shannon entity.
    """
    implements(IResource)
    powerupInterfaces = [IResource]

    addSlash = inmemory()
    hash = text(allowNone=False, default=u'sha256')

    def getObject(self, shannonID):
        """
        Retrieves a Shannon object.

        @type shannonID: C{unicode}
        @param shannonID: The shannonID of the Shannon object.

        @return: A Deferred which will fire the return value of
            CassandraIndex.retrieve(name) if the object is found.
        """
        cassandra = CassandraIndex()
        def _notFound(f):
            f.trap(NonexistentObject)
            return 'Object not found.'

        def _cb(d):
            return repr(d)
        d = cassandra.retrieve(shannonID).addErrback(_notFound)
        d.addCallback(_cb)
        return d


    def handlePOST(self, req, shannonID):
        """
        Updates a Shannon object.

        @rtype: C{Deferred}
        @return: A Deferred which will fire the return value
            of CassandraIndex.update or a Failure.
        """
        data = req.content.read()
        metadata = metadataFromHeaders(req)

        def _cb(entropyID):
            entropyID = entropyID.encode('ascii')
            tags = tagsToDict(metadata['X-Shannon-Tags'])
            return CassandraIndex().update(shannonID,
                shannonDescription=metadata['X-Shannon-Description'],
                entropyID=entropyID,
                entropyName=metadata['X-Entropy-Name'],
                tags=tags)

        if data:
            contentType = req.getHeader('Content-Type') or 'application/octet-stream'
            contentMD5 = req.getHeader('Content-MD5')

            if contentMD5 is not None:
                expectedHash = contentMD5.decode('base64')
                actualHash = hashlib.md5(data).digest()
                if expectedHash != actualHash:
                    raise DigestMismatch(expectedHash, actualHash)

            if not metadata['X-Entropy-Name']:
                raise ValueError('X-Entropy-Name is manditory')

            d = RemoteEntropyStore(entropyURI=u'http://localhost:8080/'
                ).storeObject(data, contentType)
            d.addCallback(_cb)
            d.addCallback(lambda a: 'Updated!')
            return d
        else:
            return CassandraIndex().update(shannonID, metadata)


    # IResource
    def renderHTTP(self, ctx):
        """
        Nothing to see here.
        """
        req = IRequest(ctx)
        shannonID = req.path[1:]

        if req.method == 'GET':
            req.setHeader('Content-Type', 'text/plain')
            return self.getObject(shannonID)
        elif req.method == 'POST':
            return self.handlePOST(req, shannonID)
        else:
            req.setResponseCode(http.NOT_ALLOWED)
            req.setHeader('Content-Type', 'text/plain')
            return 'Method not allowed'


    def childFactory(self, name):
        """
        Hook up children.

        / is the root, nothing to see here.

        /new is how new objects are stored.

        /<uuid> is where existing objects are (GET) retrieved and (POST) updated.
        """
        if name == '':
            return "Shannon"
        if name == 'new':
            return ShannonCreator()
        else:
            return self
        return None


    def locateChild(self, ctx, segments):
        """
        Dispatch to L{childFactory}.
        """
        if len(segments) >= 1:
            res = self.childFactory(segments[0])
            if res is not None:
                return res, segments[1:]
        return NotFound
