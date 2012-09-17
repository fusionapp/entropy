import time
from uuid import uuid1

from cql.connection import Connection

from twisted.internet.defer import gatherResults
from twisted.enterprise.adbapi import ConnectionPool

from entropy.errors import NonexistentObject



class CassandraIndex(object):
    """
    Object for retrieving, creating and updating shannon objects.
    """
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
        """
        Inserts a single tag into the 'tags' column family.

        @param shannonID: The shannonID to associate the tag with.
        @type shannonID: C{str}.

        @param key: The key of the tag.
        @type key: C{str}.

        @param value: The value of the tag.
        @type value: C{str}.

        @return: A Deferred which eventualy results in a shannonID.
        """
        d = self.pool.runOperation('''
        INSERT INTO tags (
        shannonID, key, value) VALUES (:shannonID, :key, :value)''',
        dict(shannonID=shannonID, key=key, value=value))

        d.addCallback(lambda ignore: shannonID)
        return d


    def _insertTags(self, shannonID, tags):
        """
        Inserts multiple tags into the 'tags' column family.

        @param shannonID: The shannonID to associate all the tags with.
        @type shannonID: C{str}.

        @param tags: A list of tags to be inserted.
        @type tags: C{dict}.

        @return: A Deferred which eventualy results in a shannonID.
        """
        ds = []
        for key, value in tags.iteritems():
            ds.append(self._insertTag(shannonID, key, value))

        d = gatherResults(ds)
        d.addCallback(lambda ignore: shannonID)
        return d


    def _insertAttachment(self, shannonID, name, entropyID):
        """
        Inserts an attachment into the 'attachments' column family.

        @param shannonID: The shannonID to associate the attachment with.
        @type shannonID: C{str}.

        @param name: The name of the attachment.
        @type name: C{str}.

        @param entropyID: The ID for the entropy object attached.
        @type entropyID: C{str}.

        @return: A Deferred which eventualy results in a shannonID.
        """
        d = self.pool.runOperation('''
            INSERT INTO attachments (shannonID, name, entropyID)
            VALUES (:shannonID, :name, :entropyID)''',
            dict(shannonID=shannonID, name=name, entropyID=entropyID))

        d.addCallback(lambda ignore: shannonID)
        return d


    def _updateShannon(self, shannonID, description):
        """
        Updates a shannon object. The description is the only mutable column.

        @param shannonID: The ID of the Shannon object.
        @type shannonID: C{str}.

        @param description: The new Shannon description.
        @type description: C{str}.

        @return: A Deferred which eventualy results in a shannonID.
        """
        d = self.pool.runOperation('''
            UPDATE shannon SET description = :description
            WHERE shannonID = :shannonID''',
            dict(shannonID=shannonID, description=description))
        d.addCallback(lambda ignore: shannonID)
        return d


    def _insertShannon(self, shannonID, description):
        """
        Inserts a new shannon row into the 'shannon' column family.

        @param shannonID: The ID of the Shannon object.
        @type shannonID: C{str}.

        @param description: The description of the Shannon object.
        @type description: C{str}.

        @return: A Deferred which eventualy results in a shannonID.
        """
        d = self.pool.runOperation('''
            INSERT INTO shannon (shannonID, created )
            VALUES (:shannonID, :created)''',
            dict(shannonID=shannonID, created=self._time()))
        d.addCallback(lambda ignore: shannonID)

        if description:
            d.addCallback(self._updateShannon, description)
        return d


    def insert(self, entropyID, entropyName, shannonDescription, tags=None):
        """
        Creates a new Shannon object.

        @param entropyID: The ID of the entropy object to insert.
        @type entropyID: C{str}.

        @param entropyName: The name of the entropy object.
        @type entropyName: C{str}.

        @param shannonDescription: The description of the shannon object.
        @type shannonDescription: C{str}.

        @param tags: Optional. A dictionary of key-value tags for the shannon object.
        @type: C{dict}.
        """
        shannonID = str(uuid1())

        d = self._insertShannon(shannonID, shannonDescription)
        d.addCallback(self._insertAttachment, entropyName, entropyID)

        if tags:
            d.addCallback(self._insertTags, tags)
        return d


    def update(self, shannonID, shannonDescription=None, entropyID=None, entropyName=None, tags=None):
        """
        Updates an existing Shannon object.

        @param shannonID: The shannonID of the shannon object.
        @type shannonID: C{str}.

        @param entropyID: Optional. The ID of the entropy object to insert.
        @type entropyID: C{str}.

        @param entropyName: Optional only if no entropyID is provided.
            The name of the entropy object.
        @type entropyName: C{str}.

        @param shannonDescription: Optional. The description of the shannon object.
        @type shannonDescription: C{str}.

        @param tags: Optional. A dictionary of key-value tags for the shannon object.
        @type: C{dict}.
        """
        # Check shannon entity exists before attempting update it.
        d = self.retrieve(shannonID)
        # Get the shannonID
        d.addCallback(lambda get: get[0]['shannon'][0])

        if entropyID:
            d.addCallback(self._insertAttachment, entropyName, entropyID)

        if shannonDescription:
            d.addCallback(self._updateShannon, shannonDescription)

        if tags:
            d.addCallback(self._insertTags, tags)
        return d


    def _retrieveShannon(self, shannonID):
        """
        Retrieves the 'shannon' column family row associated with the shannonID.

        @param shannonID: The ID of the Shannon object to retrieve.
        @type shannonID: C{str}.
        """
        def _checkResult(result):
            if not result:
                raise NonexistentObject(shannonID)
            return result
        d = self.pool.runQuery('''
            SELECT * FROM shannon WHERE shannonID = :shannonID''',
            dict(shannonID=str(shannonID)))
        d.addCallback(_checkResult)
        d.addCallback(lambda res: {'shannon':res[0]})
        return d


    def _retrieveAttachments(self, shannonID):
        """
        Retrieves the attachmens associated with the C{shannonID}.

        @param shannonID: The shannonID associated with the attachments.
        @type shannonID: C{str}.
        """
        d = self.pool.runQuery('''
            SELECT * FROM attachments WHERE shannonID = :shannonID''',
            dict(shannonID=shannonID))
        d.addCallback(lambda res: {'attachments':res})
        return d


    def _retrieveTags(self, shannonID):
        """
        Retrieves the tags associated with the shannonID.

        @param shannonID: The shannonID associated with the tags.
        @type shannonID: C{str}.
        """
        d = self.pool.runQuery('''
            SELECT * FROM tags WHERE shannonID = :shannonID''',
            dict(shannonID=shannonID))
        d.addCallback(lambda res: {'tags':res})
        return d


    def retrieve(self, shannonID):
        """
        Retrieves a Shannon object.

        @param shannonID: The shannonID of the object to retrieve.
        @type shannonID: C{str}.

        @return: A list containing attachments, tags and the shannon entity data.
        """
        ds = [
            self._retrieveShannon(shannonID),
            self._retrieveAttachments(shannonID),
            self._retrieveTags(shannonID)]

        d = gatherResults(ds, consumeErrors=True)
        return d
