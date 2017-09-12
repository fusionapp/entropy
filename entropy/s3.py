"""
@copyright: 2011-2014 Quotemaster cc. See LICENSE for details.

Backend implementation using Amazon S3 for storage.
"""
from axiom.attributes import text
from axiom.item import Item
from txaws.credentials import AWSCredentials
from txaws.s3.exception import S3Error
from txaws.service import AWSServiceRegion
from zope.interface import implements

from entropy.errors import NonexistentObject
from entropy.ientropy import IContentStore
from entropy.util import MemoryObject



class S3Store(Item):
    """
    Content store using Amazon S3.
    """
    implements(IContentStore)

    accessKey = text(allowNone=False, doc="AWS access key.")
    secretKey = text(allowNone=False, doc="AWS secret key.")
    bucket = text(allowNone=False, doc="Name of S3 bucket used for storage.")

    def _getClient(self):
        """
        Build a txAWS S3 client using our stored credentials.
        """
        creds = AWSCredentials(
            access_key=self.accessKey.encode('utf-8'),
            secret_key=self.secretKey.encode('utf-8'))
        region = AWSServiceRegion(creds=creds)
        return region.get_s3_client()


    # IContentStore

    def storeObject(self, content, contentType, metadata={}, created=None,
                    objectId=None):
        if objectId is None:
            raise NotImplementedError('Must provide objectId')
        if metadata != {}:
            raise NotImplementedError('Metadata not supported')

        client = self._getClient()
        d = client.put_object(
            bucket=self.bucket.encode('utf-8'),
            object_name=objectId.encode('utf-8'),
            data=content,
            content_type=contentType.encode('utf-8'))
        d.addCallback(lambda ign: objectId)
        return d


    def getObject(self, objectId):
        hash, contentDigest = objectId.split(u':', 1)

        def _makeObject((response, body)):
            return MemoryObject(
                content=body,
                hash=hash,
                contentDigest=contentDigest,
                contentType=unicode(
                    response.headers.getRawHeaders('content-type')[0],
                    'utf-8'),
                created=None)

        def _eb(f):
            f.trap(S3Error)
            raise NonexistentObject(objectId)

        client = self._getClient()
        return (
            client._submit(client._query_factory(client._details(
                method=b"GET",
                url_context=client._url_context(
                    bucket=self.bucket.encode('utf-8'),
                    object_name=objectId.encode('utf-8')))))
            .addCallbacks(_makeObject, _eb))
