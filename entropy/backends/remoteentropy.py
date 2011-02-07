import hashlib

from zope.interface import implements

from epsilon.extime import Time
from epsilon.structlike import record

from axiom.item import Item
from axiom.attributes import text

from twisted.web import error as eweb
from twisted.web.client import getPage
from twisted.python import log

from entropy.ientropy import IBackendStore, IContentObject
from entropy.errors import NonexistentObject
from entropy.util import getPageWithHeaders



class MemoryObject(record('objectId content hash contentDigest contentType '
                          'created metadata', metadata={})):
    implements(IContentObject)


    def getContent(self):
        return self.content



class RemoteEntropyStore(Item):
    """
    IBackendStore implementation for remote Entropy services.
    """
    implements(IBackendStore)

    entropyURI = text(allowNone=False,
                      doc="""The URI of the Entropy service in use.""")

    def getURI(self, documentId):
        """
        Construct an Entropy URI for the specified document.
        """
        return self.entropyURI + documentId


    # IBackendStore
    def storeObject(self, objectId, content, contentType=None, metadata={}, created=None):
        digest = hashlib.md5(data).digest()
        return getPage((self.entropyURI + 'new').encode('ascii'),
                       method='PUT',
                       postdata=data,
                       headers={'Content-Length': len(data),
                                'Content-Type': contentType,
                                'Content-MD5': b64encode(digest)}
                    ).addCallback(lambda url: unicode(url, 'ascii'))


    def getObject(self, objectId):
        def _makeContentObject((data, headers)):
            # XXX: Actually get the real creation time
            return MemoryObject(
                objectId=objectId,
                content=data,
                hash=None,
                contentDigest=None,
                contentType=unicode(headers['content-type'][0], 'ascii'),
                metadata={},
                created=Time())

        def _eb(f):
            f.trap(eweb.Error)
            if f.value.status == '404':
                raise NonexistentObject(objectId)
            return f

        return getPageWithHeaders(self.getURI(objectId)
                    ).addCallbacks(_makeContentObject, _eb)
