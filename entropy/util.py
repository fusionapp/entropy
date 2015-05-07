"""
@copyright: 2007-2014 Quotemaster cc. See LICENSE for details.
"""
from zope.interface import implements

from epsilon.structlike import record

from twisted.internet import defer
from twisted.python.util import mergeFunctionMetadata

from xmantissa.offering import InstalledOffering

from entropy.ientropy import IContentObject


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



class MemoryObject(record('content hash contentDigest contentType created '
                          'metadata', metadata={})):
    """
    In-memory implementation of L{IContentObject}.

    This is primarily useful for objects retrieved from a remote system, that
    need to be temporarily held in memory.
    """
    implements(IContentObject)


    @property
    def objectId(self):
        return u'%s:%s' % (self.hash, self.contentDigest)


    def getContent(self):
        return defer.succeed(self.content)
