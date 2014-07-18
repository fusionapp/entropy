"""
@copyright: 2011-2014 Quotemaster cc. See LICENSE for details.

Backend implementation using Amazon S3 for storage.
"""
from zope.interface import implements

from axiom.item import Item
from axiom.attributes import text

from txaws.service import AWSServiceRegion
from txaws.credentials import AWSCredentials
from txaws.s3.exception import S3Error

from entropy.ientropy import IContentStore
from entropy.util import MemoryObject
from entropy.errors import NonexistentObject


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
        def _makeObject(content):
            headers = query.get_response_headers()
            return MemoryObject(
                content=content,
                hash=hash,
                contentDigest=contentDigest,
                contentType=unicode(headers['content-type'][0], 'utf-8'),
                created=None)

        def _eb(f):
            f.trap(S3Error)
            raise NonexistentObject(objectId)

        client = self._getClient()
        query = client.query_factory(
            action='GET', creds=client.creds, endpoint=client.endpoint,
            bucket=self.bucket.encode('utf-8'),
            object_name=objectId.encode('utf-8'))
        d = query.submit()
        d.addCallbacks(_makeObject, _eb)
        return d
