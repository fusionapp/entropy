"""
@copyright: 2007-2014 Quotemaster cc. See LICENSE for details.
"""
from twisted.internet.defer import succeed
from twisted.python.failure import Failure
from twisted.web.client import ResponseDone
from twisted.web.http_headers import Headers
from twisted.web.test.test_agent import DummyResponse as _TwistedDummyResponse



class DummyResponse(_TwistedDummyResponse):
    """
    Dummy agent response.
    """
    def respond(self, data):
        """
        Deliver some data to the underlying protocol and close it.
        """
        self.protocol.dataReceived(data)
        self.protocol.connectionLost(Failure(ResponseDone()))



class DummyAgent(object):
    """
    Dummy L{IAgent} that only uses L{DummyResponse}s.
    """
    def __init__(self):
        self.responses = []


    def request(self, method, uri, headers=None, bodyProducer=None):
        if headers is None:
            headers = Headers({})
        response = DummyResponse()
        response.code = 200
        response.args = (method, uri, headers, bodyProducer)
        self.responses.append(response)
        return succeed(response)
