from twisted.internet import defer
from twisted.python.util import mergeFunctionMetadata

from xmantissa.offering import InstalledOffering

def getAppStore(siteStore):
    """
    Retrieve the Entropy app store.
    """
    offering = siteStore.findUnique(InstalledOffering, InstalledOffering.offeringName == u'Entropy')
    appStore = offering.application.open()
    return appStore


def deferred(f):
    def _wrapper(*a, **kw):
        return defer.execute(f, *a, **kw)
    return mergeFunctionMetadata(f, _wrapper)
