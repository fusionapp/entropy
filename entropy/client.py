"""
@copyright: 2007-2014 Quotemaster cc. See LICENSE for details.
"""
import hashlib
from base64 import b64encode
from epsilon.extime import Time
from StringIO import StringIO
from twisted.internet import reactor
from twisted.python.failure import Failure
from twisted.python.urlpath import URLPath
from twisted.web import http
from twisted.web.client import Agent, readBody, FileBodyProducer
from twisted.web.http_headers import Headers

from entropy.errors import APIError, NonexistentObject
from entropy.util import MemoryObject



class Endpoint(object):
    """
    Entropy client endpoint.
    """
    def __init__(self, uri, agent=None):
        """
        @type  uri: L{unicode}
        @param uri: Entropy endpoint URI, for example:
            C{http://example.com/entropy/}.

        @type  agent: L{twisted.web.iweb.IAgent}
        @param agent: Twisted Web agent.
        """
        self.uri = URLPath.fromString(uri)
        if agent is None:
            agent = Agent(reactor)
        self._agent = agent


    def _parseResponse(self, response, objectId=None):
        """
        Parse an Entropy HTTP response.
        """
        def _checkResult(result):
            if response.code == http.NOT_FOUND:
                raise APIError(
                    result, response.code,
                    Failure(NonexistentObject(objectId)))
            elif response.code >= http.BAD_REQUEST:
                raise APIError(result, response.code, reason=None)
            return result, response

        d = readBody(response)
        d.addCallback(_checkResult)
        return d


    def store(self, content, contentType, metadata={}, created=None):
        """
        Store an object in an Entropy endpoint.

        @type  content: L{str}
        @param content: Object data.

        @type  contentType: L{unicode}
        @param contentType: MIME type of C{content}.

        @param metadata: Object metadata.
        @type  metadata: C{dict} of C{unicode}:C{unicode}

        @type  created: L{epsilon.extime.Time} or C{None}
        @param created: Creation timestamp; defaults to the current time.

        @raises L{APIError}: If there is an error storing the object in the
            Entropy endpoint.

        @rtype: L{Deferred} firing with L{unicode}
        @return: Object identifier.
        """
        if isinstance(contentType, unicode):
            contentType = contentType.encode('ascii')
        digest = hashlib.md5(content).digest()
        headers = Headers({
            'Content-Length': [len(content)],
            'Content-Type': [contentType],
            'Content-MD5': [b64encode(digest)]})
        bodyProducer = FileBodyProducer(StringIO(content))
        d = self._agent.request(
            'PUT', str(self.uri.child('new')), headers, bodyProducer)
        d.addCallback(self._parseResponse)
        d.addCallback(lambda (result, response): result.decode('utf-8'))
        return d


    def get(self, objectId):
        """
        Retrieve an object from an Entropy endpoint.

        @type  objectId: L{unicode}
        @param objectId: Stored object identifier.

        @raises L{APIError}: If there is an error retrieving the object from
            the Entropy endpoint.

        @rtype: L{Deferred} firing with L{IContentObject}
        @return: Content object.
        """
        def _makeContentObject((data, response)):
            hash, contentDigest = objectId.split(':', 1)
            contentType = response.headers.getRawHeaders(
                'Content-Type', ['application/octet-stream'])[0].decode('ascii')
            # XXX: Actually get the real creation time
            return MemoryObject(
                content=data,
                hash=hash,
                contentDigest=contentDigest,
                contentType=contentType.decode('ascii'),
                metadata={},
                created=Time())

        if not isinstance(objectId, unicode):
            objectId = objectId.decode('ascii')
        d = self._agent.request(
            'GET', str(self.uri.child(objectId.encode('ascii'))))
        d.addCallback(self._parseResponse, objectId=objectId)
        d.addCallback(_makeContentObject)
        return d



__all__ = ['Endpoint']
