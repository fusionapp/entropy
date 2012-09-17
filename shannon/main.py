import hashlib
import json
from uuid import UUID

from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.python.failure import Failure

from entropy.errors import DigestMismatch
from entropy.store import RemoteEntropyStore

from shannon.cassandra import CassandraIndex
from shannon.util import metadataFromHeaders, tagsToDict, ShannonEncoder



def getRootResource(hostname, port, keyspace):
    """
    Returns the root Resource.
    """
    cassandra = CassandraIndex(hostname=hostname, port=port, keyspace=keyspace)
    resource = ShannonDispatch(cassandra)
    resource.putChild("new", ShannonCreator(cassandra))
    return resource


def _writeRequest(d, request):
    """
    Converts data to valid JSON, writes the data to the request and calls
    request.finish().

    @param d: The data to convert to JSON and write to the request.
    @type d: Objects supported by json.dumps also L{UUID} and L{Failure}.

    @param request: The request object to write to.
    @type request: L{twisted.web.iweb.IRequest}
    """
    def _toJSON(d):
        _json= {}
        _json['response'] = d
        return json.dumps(_json, cls=ShannonEncoder)

    if isinstance(d, Failure):
       request.setResponseCode(500)
    else:
        request.setResponseCode(200)

    request.setHeader('content-type', 'application/json')
    request.write(_toJSON(d))
    request.finish()



class InvalidUUID(Resource):
    """
    Resource returned when an invalid UUID is requested.
    """
    def render(self, request):
        """
        Sets the response code to 404.
        Returns JSON explaining that the UUID was invalid.
        """
        request.setResponseCode(404)
        _writeRequest("Invalid Shannon UUID", request)
        return NOT_DONE_YET



class CoreResource(Resource):
    """
    Resource for updating and retrieving shannon objects.
    """
    def __init__(self, cassandra, shannonID):
        self.cassandra = cassandra
        self.shannonID = shannonID


    def getObject(self, shannonID):
        """
        Retrieves a Shannon object.

        @type shannonID: C{unicode}
        @param shannonID: The shannonID of the Shannon object.

        @return: A Deferred which will fire the return value of
            CassandraIndex.retrieve(name) if the object is found.
        """
        def _notFound(f):
            return f.value[0]
        
        d = self.cassandra.retrieve(shannonID).addErrback(_notFound)
        return d


    def render_GET(self, request):
        """
        Retrieves a shannon object.
        """
        d = self.getObject(self.shannonID)
        d.addBoth(_writeRequest, request)
        return NOT_DONE_YET


    def render_POST(self, request):
        """
        Updates a Shannon object.

        @rtype: C{Deferred}
        @return: A Deferred which will fire the return value
            of CassandraIndex.update or a Failure.
        """
        data = request.content.read()
        metadata = metadataFromHeaders(request)

        def _update(entropyID=None):
            if entropyID:
                entropyID = entropyID.encode('ascii')
            tags = tagsToDict(metadata['X-Shannon-Tags'])

            d = self.cassandra.update(self.shannonID,
                shannonDescription=metadata['X-Shannon-Description'],
                entropyID=entropyID,
                entropyName=metadata['X-Entropy-Name'],
                tags=tags)
            d.addCallback(lambda ignore: 'Updated.')
            return d

        # Add a new entropy object.
        if data:
            contentType = request.getHeader('Content-Type') or 'application/octet-stream'
            contentMD5 = request.getHeader('Content-MD5')

            if contentMD5 is not None:
                expectedHash = contentMD5.decode('base64')
                actualHash = hashlib.md5(data).digest()
                if expectedHash != actualHash:
                    raise DigestMismatch(expectedHash, actualHash)

            if not metadata['X-Entropy-Name']:
                raise ValueError('X-Entropy-Name is mandatory')

            d = RemoteEntropyStore(entropyURI=u'http://localhost:8080/'
                ).storeObject(data, contentType)
            d.addCallback(lambda a: _update(entropyID=a))
            d.addBoth(_writeRequest, request)
        else:
            d = _update()
            d.addBoth(_writeRequest, request)

        return NOT_DONE_YET



class ShannonCreator(Resource):
    """
    Resource for storing new objects in entropy, and metadata in Cassandra.
    """
    def __init__(self, cassandra, entropyURI=u'http://localhost:8080/'):
        self.cassandra = cassandra
        self.entropyURI = entropyURI


    def render_GET(self, request):
        """
        This method explains that the correct method to use is a POST.
        """
        return 'POST data here to create an object.'


    def render_POST(self, request):
        """
        Creates a new shannon object.
        """
        data = request.content.read()
        contentType = request.getHeader('Content-Type') or 'application/octet-stream'
        metadata = metadataFromHeaders(request)
        contentMD5 = request.getHeader('Content-MD5')

        if contentMD5 is not None:
            expectedHash = contentMD5.decode('base64')
            actualHash = hashlib.md5(data).digest()
            if expectedHash != actualHash:
                raise DigestMismatch(expectedHash, actualHash)

        # Checks for required headers.
        if not metadata['X-Entropy-Name']:
            raise ValueError('X-Entropy-Name is mandatory')
        if not metadata['X-Shannon-Description']:
            raise ValueError('X-Shannon-Description is mandatory')

        def _cb(objectId):
            objectId = objectId.encode('ascii')
            return objectId

        d = RemoteEntropyStore(entropyURI=self.entropyURI).storeObject(
            data, contentType)
        d.addCallback(_cb)

        tags = tagsToDict(metadata['X-Shannon-Tags'])
        d.addCallback(self.cassandra.insert,
            metadata['X-Entropy-Name'],
            metadata['X-Shannon-Description'],
            tags)

        d.addBoth(_writeRequest, request)
        return NOT_DONE_YET



class ShannonDispatch(Resource):
    """
    Handles the dispatch of dynamic URLs.
    """
    def __init__(self, cassandra):
        Resource.__init__(self)
        self.cassandra = cassandra


    def getChild(self, path, request):
        """
        Checks that the UUID requested is valid.
        
        @return: L{CoreResource}.
        """
        try:
            UUID(path)
        except ValueError:
            return InvalidUUID()
        return CoreResource(self.cassandra, path)
