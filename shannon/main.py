from base64 import b64encode

import hashlib

from zope.interface import implements

from axiom.item import Item
from axiom.attributes import text, inmemory

from twisted.web import http
from twisted.web.client import getPage
from nevow.inevow import IResource, IRequest
from nevow.rend import NotFound

from entropy.errors import NonexistentObject, DigestMismatch
from entropy.store import RemoteEntropyStore

from shannon.cassandra import CassandraIndex
from shannon.util import metadataFromHeaders


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
        print repr(ctx)
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

        if not metadata['X-Entropy-Name']:
            raise ValueError('X-Entropy-Name is manditory')

        def _cb(objectId):
            objectId = objectId.encode('ascii')
            return objectId

        d = RemoteEntropyStore(entropyURI=self.entropyURI).storeObject(data, contentType)
        d.addCallback(_cb)
        d.addCallback(CassandraIndex().insert, metadata)
        return d



class CoreResource(Item):
    """
    Resource for retrieving and updating a Shannon entity.
    """
    implements(IResource)
    powerupInterfaces = [IResource]

    addSlash = inmemory()
    uuid = inmemory()

    hash = text(allowNone=False, default=u'sha256')

    def getObject(self, uuid):
        """
        Retrieves a Shannon object.

        @type uuid: C{unicode}
        @param uuid: The uuid of the Shannon object.

        @return: A Deferred which will fire the return value of
            CassandraIndex.retrieve(name) if the object is found.
        """
        cassandra = CassandraIndex()
        def _notFound(f):
            f.trap(NonexistentObject)
            return 'Object not found.'
        return cassandra.retrieve(uuid).addErrback(_notFound)


    def handlePOST(self, req):
        """
        Updates a Shannon object.

        @rtype: C{Deferred}
        @return: A Deferred which will fire the return value
            of CassandraIndex.update or a Failure.
        """
        data = req.content.read()
        metadata = metadataFromHeaders(req)

        def _cb(entropyId):
            entropyId = entropyId.encode('ascii')
            return CassandraIndex().update(self.uuid, metadata, entropyId=entropyId)

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
                ).storeObject(data, contentType) # Hardcoded URI :S
            d.addCallback(_cb)
            d.addCallback(lambda a: 'Updated!')
            return d
        else:
            return CassandraIndex().update(self.uuid, metadata)


    # IResource
    def renderHTTP(self, ctx):
        """
        Nothing to see here.
        """
        print repr(ctx)
        req = IRequest(ctx)
        if req.method == 'GET':
            req.setHeader('Content-Type', 'text/plain')
            return self.getObject(self.uuid)
        elif req.method == 'POST':
            return self.handlePOST(req)
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
            self.uuid = unicode(name, 'ascii')
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



def shannonClient():
    """
    Temporary.
    """
    data = 'toehueoutheee'
    digest = hashlib.md5(data).digest()
    return getPage('http://127.0.0.1:9000/new',
                   method='POST',
                   postdata=data,
                   headers={'Content-Length': len(data),
                            'Content-MD5': b64encode(digest),
                            'X-Shannon-Description': 'This is a new description! :D.',
                            'X-Entropy-Name': 'The name of the Entropy object!!.',
                            'X-Shannon-Tags': 'tag1=value, tag2=value, tag4=value, tag3=value'}
                ).addCallback(lambda url: unicode(url, 'ascii'))


if __name__ == '__main__':
    from twisted.internet import reactor
    d = shannonClient()
    reactor.run()
