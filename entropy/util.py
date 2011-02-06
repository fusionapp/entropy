from twisted.internet import defer
from twisted.python.util import mergeFunctionMetadata

from xmantissa.offering import InstalledOffering

# XXX: private import, replace with better API ASAP
# We probably need to wait for Twisted ticket #886 to be closed.
from twisted.web.client import _makeGetterFactory, HTTPClientFactory


def getAppStore(siteStore):
    """
    Retrieve the Entropy app store.
    """
    offering = siteStore.findUnique(
        InstalledOffering,
        InstalledOffering.offeringName == u'Entropy')
    appStore = offering.application.open()
    return appStore


def deferred(f):
    def _wrapper(*a, **kw):
        return defer.execute(f, *a, **kw)
    return mergeFunctionMetadata(f, _wrapper)

def getPageWithHeaders(uri):
    """
    Fetch a resource.

    This function is only necessary because L{twisted.web.client.getPage} is a
    really awful API that doesn't give us access to vital response entity
    headers such as Content-Type.
    """
    factory = _makeGetterFactory(str(uri), HTTPClientFactory)
    return factory.deferred.addCallback(
        lambda data: (data, factory.response_headers))
