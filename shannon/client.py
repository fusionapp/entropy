import hashlib
from base64 import b64encode

from twisted.web.client import getPage

from shannon.util import tagsToStr



class Client(object):
    def __init__(self, uri):
        self.shannonURI = uri
        self.contentType = 'text/html; charset=ISO-8859-1'


    def retrieve(self, shannonID):
        """
        @param shannonID: The shannonID of the entity you want to retrieve.
        @type shannonID: C{str}

        @return: A deferred with the shannon results or a Failure
        """
        return getPage(self.shannonURI+shannonID.encode('ascii'))


    def create(self, entropyData, entropyName, shannonDescription, tags=''):
        """
        @param entropyData: The data you want to store.
        @type entropyData: C{str}.

        @param entropyName: The name of the data.
        @type entropyName: C{str}.

        @param shannonDescription: The description of the shannon entity.
        @type shannonDescription: C{str}.

        @param tags: Optional key-value-tags to store.
        @type tags: C{dict}.

        @return: Deferred containing the shannonID or a Failure.
        """
        digest = hashlib.md5(data).digest()
        if tags:
            tags = tagsToStr(tags)

        return getPage((self.shannonURI + 'new').encode('ascii'),
                       method='POST',
                       postdata=data,
                       headers={'Content-Length': len(data),
                                'Content-Type': self.contentType,
                                'Content-MD5': b64encode(digest),
                                'X-Entropy-Name': entropyName,
                                'X-Shannon-Description': shannonDescription,
                                'X-Shannon-Tags': tags}
                    ).addCallback(lambda uuid: unicode(uuid, 'ascii'))


    def update(self, shannonID, entropyData='', entropyName='', shannonDescription='', tags=''):
        """
        @param shannonID: The shannonID of the entity you want to update.
        @type shannonID: C{str}.

        @param entropyData: The data you want to store.
        @type entropyData: C{str}.

        @param entropyName: The name of the data.
        @type entropyName: C{str}.

        @param shannonDescription: The description of the shannon entity.
        @type shannonDescription: C{str}.

        @param tags: Optional key-value-tags to store.
        @type tags: C{dict}.

        @return: Deferred containing the shannonID or a Failure.
        """
        if tags:
            tags = tagsToStr(tags)

        if entropyData:
            if not entropyName:
                raise ValueError('entropyName is manditory.')
            digest = hashlib.md5(entropyData).digest()
        else:
            digest = ''

        return getPage((self.shannonURI + shannonID).encode('ascii'),
                       method='POST',
                       postdata=entropyData,
                       headers={'Content-Length': len(entropyData),
                                'Content-Type': self.contentType,
                                'Content-MD5': b64encode(digest),
                                'X-Entropy-Name': entropyName,
                                'X-Shannon-Description': shannonDescription,
                                'X-Shannon-Tags': tags}
                    ).addCallback(lambda uuid: unicode(uuid, 'ascii'))
