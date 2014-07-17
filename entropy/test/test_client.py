from StringIO import StringIO
from twisted.internet.defer import succeed
from twisted.python.failure import Failure
from twisted.trial.unittest import TestCase
from twisted.web import http
from twisted.web.client import ResponseDone
from twisted.web.http_headers import Headers
from twisted.web.test.test_agent import DummyResponse

from entropy.client import Endpoint
from entropy.errors import APIError, NonexistentObject



class _DummyAgent(object):
    """
    Dummy L{IAgent} that only uses L{DummyResponse}s.
    """
    def __init__(self):
        self.responses = []


    def request(self, method, uri, headers=None, bodyProducer=None):
        if headers is None:
            headers = Headers({})
        response = DummyResponse()
        response.args = (method, uri, headers, bodyProducer)
        self.responses.append(response)
        return succeed(response)



class _FileConsumer(object):
    """
    Trivial L{IConsumer} that writes its input to another output.
    """
    def __init__(self, outputFile):
        self.outputFile = outputFile


    def write(self, data):
        self.outputFile.write(data)



class EndpointTests(TestCase):
    """
    Tests for L{entropy.client.Endpoint}.
    """
    def setUp(self):
        self.agent = _DummyAgent()
        self.endpoint = Endpoint(u'http://example.com/entropy/', self.agent)


    def test_failure(self):
        """
        If Entropy returns a non-success code, L{Endpoint} raises L{APIError}.
        """
        d = self.endpoint.store('some_data', 'text/plain')
        response = self.agent.responses.pop()
        response.code = http.BAD_REQUEST
        self.assertEqual([], self.agent.responses)
        response.protocol.dataReceived('Oops')
        response.protocol.connectionLost(Failure(ResponseDone()))
        f = self.failureResultOf(d, APIError)
        self.assertEqual(
            (http.BAD_REQUEST, 'Oops', None),
            (f.value.code, f.value.message, f.value.reason))


    def test_success(self):
        """
        Parse a successful Entropy response.
        """
        d = self.endpoint.store('some_data', 'text/plain')
        response = self.agent.responses.pop()
        self.assertEqual([], self.agent.responses)
        response.protocol.dataReceived('an_id')
        response.protocol.connectionLost(Failure(ResponseDone()))
        self.assertEqual('an_id', self.successResultOf(d))


    def test_store(self):
        """
        Store an object in an Entropy endpoint.
        """
        d = self.endpoint.store('some_data', 'text/plain')
        response = self.agent.responses.pop()
        self.assertEqual([], self.agent.responses)
        self.assertEqual(
            ('PUT', 'http://example.com/entropy/new'),
            (response.args[0], response.args[1]))
        self.assertEqual(
            Headers({
                'Content-Length': [9],
                'Content-Type': ['text/plain'],
                'Content-MD5': ['DZJHy840q6SsqNXIh6DwpA==']}),
            response.args[2])
        response.protocol.dataReceived('an_id')
        response.protocol.connectionLost(Failure(ResponseDone()))
        self.assertEqual('an_id', self.successResultOf(d))

        def _checkBody(result):
            self.assertEqual(
                'some_data',
                output.getvalue())
            return result

        output = StringIO()
        consumer = _FileConsumer(output)
        complete = response.args[3].startProducing(consumer)
        complete.addCallback(_checkBody)
        return complete


    def test_get(self):
        """
        Retrieve an existing Entropy object.
        """
        d = self.endpoint.get(u'sha256:an_id')
        response = self.agent.responses.pop()
        response.headers.setRawHeaders('Content-Type', ['applicaton/pdf'])
        self.assertEqual([], self.agent.responses)
        self.assertEqual(
            ('GET', 'http://example.com/entropy/sha256:an_id'),
            (response.args[0], response.args[1]))
        response.protocol.dataReceived('some_data')
        response.protocol.connectionLost(Failure(ResponseDone()))
        obj = self.successResultOf(d)
        self.assertEqual(
            ('some_data', u'sha256', u'an_id', u'applicaton/pdf', {}),
            (obj.content, obj.hash, obj.contentDigest, obj.contentType,
             obj.metadata))


    def test_getNonexistent(self):
        """
        L{NonexistentObject} is the L{APIError} reason given when attempting to
        retrieve a nonexistent Entropy object.
        """
        d = self.endpoint.get(u'sha256:an_id')
        response = self.agent.responses.pop()
        response.code = http.NOT_FOUND
        self.assertEqual([], self.agent.responses)
        response.protocol.dataReceived('Not found')
        response.protocol.connectionLost(Failure(ResponseDone()))
        f = self.failureResultOf(d, APIError)
        self.assertEqual(
            (http.NOT_FOUND, 'Not found'),
            (f.value.code, f.value.message))
        reason = f.value.reason.value
        self.assertIsInstance(reason, NonexistentObject)
        self.assertEqual('sha256:an_id', str(reason))
