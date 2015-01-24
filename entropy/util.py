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



class MemoryObject(record('objectId content contentType created metadata',
                          metadata={})):
    """
    In-memory implementation of L{IContentObject}.

    This is primarily useful for objects retrieved from a remote system, that
    need to be temporarily held in memory.
    """
    implements(IContentObject)


    def getContent(self):
        return self.content



def firstSuccess(operation, targets, exceptionType, *args):
    """
    Try an operation on several targets, returning the first successful result.
    """
    def tryOne():
        try:
            target = it.next()
        except StopIteration:
            raise exceptionType(*args)
        return operation(target, *args).addErrback(eb)

    def eb(f):
        f.trap(exceptionType)
        return tryOne()

    it = iter(targets)
    return tryOne()
