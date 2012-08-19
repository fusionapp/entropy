import time
from uuid import uuid1

from cql.connection import Connection

from twisted.enterprise.adbapi import ConnectionPool

from entropy.errors import NonexistentObject

from shannon.util import parseTags



class CassandraIndex(object):
    def __init__(self, hostname='localhost', port='9160', keyspace='shannon'):
        self.pool = ConnectionPool(
            'cql', hostname, port, keyspace,
            cql_version='3.0.0',
            cp_reconnect=True)
        Connection.rollback = lambda self: None


#    def _insertInteraction(self, txn, entropyId, metadata):
#        shannonId = str(uuid1())
#
#        txn.execute('''INSERT INTO shannon (shannonID, created )
#            VALUES (:shannonId, :created)''',
#            dict(shannonId=shannonId, created=self._time()))
#
#        txn.execute('''INSERT INTO attachments (shannonID, name, entropyID)
#            VALUES (:shannonId, :name, :entropyId)''',
#            dict(shannonId=shannonId,
#                 name=metadata['X-Entropy-Name'],
#                 entropyId=entropyId))
#
#        if metadata['X-Shannon-Tag']:
#            for key, value in metadata['X-Shannon-Tag']:
#                txn.execute('''INSERT INTO tags (shannonID, key, value)
#                    VALUES (:shannonId, :key, :value)''',
#                    dict(shannonId=shannonId, key=key, value=value))
#
#        return shannonId


    def _time(self):
        """
        Epoch in milliseconds
        """
        return int(time.time() * 1000)


    def _insertTag(self, shannonId, tags):
        for key, value in tags.iteritems():
            self.pool.runOperation('''
            INSERT INTO tags (
            shannonID, key, value) VALUES (:shannonId, :key, :value)''',
            dict(shannonId=shannonId, key=key, value=value))
        return None


    def _insertAttachment(self, shannonId, name, entropyId):
        d = self.pool.runOperation('''
            INSERT INTO attachments (shannonID, name, entropyID)
            VALUES (:shannonId, :name, :entropyId)''',
            dict(shannonId=shannonId, name=name, entropyId=entropyId))
        return d


    def _updateShannon(self, shannonId, description):
        d = self.pool.runOperation('''
            UPDATE shannon SET description = :description
            WHERE shannonID = :shannonId''',
            dict(shannonId=shannonId, description=description))
        return d


    def _insertShannon(self, shannonId, description):
        d = self.pool.runOperation('''
            INSERT INTO shannon (shannonID, created )
            VALUES (:shannonId, :created)''',
            dict(shannonId=shannonId, created=self._time()))

        if description:
            d.addCallback(lambda ign: self._updateShannon(shannonId, description))
        return d


    def insert(self, entropyId, metadata):
        shannonId = str(uuid1())

        d = self._insertShannon(shannonId, metadata['X-Shannon-Description'])
        d.addCallback(lambda ign: self._insertAttachment(shannonId,
            metadata['X-Entropy-Name'], entropyId))

        if metadata['X-Shannon-Tags']:
            tags = parseTags(metadata['X-Shannon-Tags'])
            d.addCallback(lambda ign: self._insertTag(
                shannonId, tags))

        d.addCallback(lambda d: shannonId)
        return d


    def update(self, shannonId, metadata, entropyId=None):
        d = self.retrieve(shannonId) #Check object exists before attempting update.
        if entropyId:
            d.addCallback(lambda ign: self._insertAttachment(
                shannonId, metadata['X-Entropy-Name'], entropyId))

        if metadata['X-Shannon-Description']:
            d.addCallback(lambda ign: self._updateShannon(
                shannonId, metadata['X-Shannon-Description']))

        if metadata['X-Shannon-Tags']:
            tags = parseTags(metadata['X-Shannon-Tags'])
            d.addCallback(lambda ign: self._insertTag(
                shannonId, tags))
        return d


    def retrieve(self, shannonId):
        """
        Not fully implemented.
        Only returns the shannon column family data.
        """
        d = self.pool.runQuery('''
            SELECT * FROM shannon WHERE shannonID = :shannonId''',
            dict(shannonId=str(shannonId)))

        def _cb(results):
            if not results:
                raise NonexistentObject(shannonId)
            return repr(results).encode('ascii')

        d.addCallback(_cb)
        return d
