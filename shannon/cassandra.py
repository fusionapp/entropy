import time
from uuid import uuid1

from cql.connection import Connection

from twisted.internet.defer import gatherResults
from twisted.enterprise.adbapi import ConnectionPool

from entropy.errors import NonexistentObject

from shannon.util import parseTags



class CassandraIndex(object):
    def __init__(self, hostname='localhost', port=9160, keyspace='shannon', factory=None):
        if not factory:
            factory = ConnectionPool

        self.pool = factory(
            'cql', hostname, port, keyspace,
            cql_version='3.0.0',
            cp_reconnect=True)
        Connection.rollback = lambda self: None


    def _time(self):
        """
        Epoch in milliseconds
        """
        return int(time.time() * 1000)


    def _insertTag(self, shannonID, key, value):
        d = self.pool.runOperation('''
        INSERT INTO tags (
        shannonID, key, value) VALUES (:shannonID, :key, :value)''',
        dict(shannonID=shannonID, key=key, value=value))

        d.addCallback(lambda ignore: shannonID)
        return d


    def _insertTags(self, shannonID, tags):
        ds = []
        for key, value in tags.iteritems():
            ds.append(self._insertTag(shannonID, key, value))

        d = gatherResults(ds)
        d.addCallback(lambda ignore: shannonID)
        return d


    def _insertAttachment(self, shannonID, name, entropyID):
        d = self.pool.runOperation('''
            INSERT INTO attachments (shannonID, name, entropyID)
            VALUES (:shannonID, :name, :entropyID)''',
            dict(shannonID=shannonID, name=name, entropyID=entropyID))

        d.addCallback(lambda ignore: shannonID)
        return d


    def _updateShannon(self, shannonID, description):
        d = self.pool.runOperation('''
            UPDATE shannon SET description = :description
            WHERE shannonID = :shannonID''',
            dict(shannonID=shannonID, description=description))
        d.addCallback(lambda ignore: shannonID)
        return d


    def _insertShannon(self, shannonID, description):
        d = self.pool.runOperation('''
            INSERT INTO shannon (shannonID, created )
            VALUES (:shannonID, :created)''',
            dict(shannonID=shannonID, created=self._time()))
        d.addCallback(lambda ignore: shannonID)

        if description:
            d.addCallback(self._updateShannon, description)
        return d


    def insert(self, entropyID, entropyName, shannonDescription, tags=None):
        shannonID = str(uuid1())

        d = self._insertShannon(shannonID, shannonDescription)
        d.addCallback(self._insertAttachment, entropyName, entropyID)

        if tags:
            d.addCallback(self._insertTags, tags)
        return d


    def update(self, shannonID, shannonDescription=None, entropyID=None, entropyName=None, tags=None):
        # Check shannon entity exists before attempting update it.
        d = self.retrieve(shannonID)
        if entropyID:
            d.addCallback(self._insertAttachment, entropyName, entropyID)

        if shannonDescription:
            d.addCallback(self._updateShannon, shannonDescription)

        if tags:
            d.addCallback(self._insertTags, tags)
        return d


    def retrieve(self, shannonID):
        """
        Not fully implemented.
        Only returns the shannon column family data.
        """
        d = self.pool.runQuery('''
            SELECT * FROM shannon WHERE shannonID = :shannonID''',
            dict(shannonID=str(shannonID)))

        def _cb(results):
            if not results:
                raise NonexistentObject(shannonID)
            return results

        d.addCallback(_cb)
        return d
