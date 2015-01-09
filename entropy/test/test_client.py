"""
@copyright: 2007-2014 Quotemaster cc. See LICENSE for details.
"""
from StringIO import StringIO
from twisted.trial.unittest import TestCase
from twisted.web import http
from twisted.web.http_headers import Headers

from entropy.client import Endpoint
from entropy.errors import APIError
from entropy.test.util import DummyAgent



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
        self.agent = DummyAgent()
        self.endpoint = Endpoint(u'http://example.com/entropy/', self.agent)


    def test_failure(self):
        """
        If Entropy returns a non-success code, L{Endpoint} raises L{APIError}.
        """
        d = self.endpoint.store('some_data', 'text/plain')
        response = self.agent.responses.pop()
        self.assertEqual([], self.agent.responses)
        response.code = http.BAD_REQUEST
        response.respond('Oops')
        f = self.failureResultOf(d, APIError)
        self.assertEqual(
            (http.BAD_REQUEST, 'Oops'),
            (f.value.code, f.value.message))


    def test_success(self):
        """
        Parse a successful Entropy response.
        """
        d = self.endpoint.store('some_data', 'text/plain')
        response = self.agent.responses.pop()
        self.assertEqual([], self.agent.responses)
        response.respond('an_id')
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
                'Content-Type': ['text/plain'],
                'Content-MD5': ['DZJHy840q6SsqNXIh6DwpA==']}),
            response.args[2])
        response.respond('an_id')
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
        self.assertEqual([], self.agent.responses)
        response.headers.setRawHeaders('Content-Type', ['applicaton/pdf'])
        self.assertEqual(
            ('GET', 'http://example.com/entropy/sha256:an_id'),
            (response.args[0], response.args[1]))
        response.respond('some_data')
        obj = self.successResultOf(d)
        self.assertEqual(
            ('some_data', u'sha256', u'an_id', u'applicaton/pdf', {}),
            (obj.content, obj.hash, obj.contentDigest, obj.contentType,
             obj.metadata))


    def test_exists(self):
        """
        Determine if an Entropy object exists.
        """
        d = self.endpoint.exists(u'sha256:an_id')
        response = self.agent.responses.pop()
        self.assertEqual([], self.agent.responses)
        self.assertEqual(
            ('HEAD', 'http://example.com/entropy/sha256:an_id'),
            (response.args[0], response.args[1]))
        response.respond('')
        self.assertTrue(self.successResultOf(d))


    def test_doesNotExist(self):
        """
        Determine if an Entropy object does not exist.
        """
        d = self.endpoint.exists(u'sha256:an_id')
        response = self.agent.responses.pop()
        self.assertEqual([], self.agent.responses)
        response.code = http.NOT_FOUND
        self.assertEqual(
            ('HEAD', 'http://example.com/entropy/sha256:an_id'),
            (response.args[0], response.args[1]))
        response.respond('')
        self.assertFalse(self.successResultOf(d))
