import time
from uuid import UUID

from twisted.trial.unittest import TestCase
from twisted.internet import defer

from shannon.cassandra import CassandraIndex



class FakeConnectionPool(object):
    """
    A connection pool for testing.
    """
    def __init__(self, dbapiName, *connargs, **connkw):
        pass


    def runOperation(self, *args, **kw):
        """
        Returns a Deferred that results in C{None}.
        """
        d = defer.Deferred()
        d.callback(None)
        return d


    def runQuery(self, *args, **kw):
        """
        Returns an example of a Shannon entity.

        @rtype: A C{list} containing a single C{list} which contains Shannon data.
        """
        d = defer.Deferred()
        result = [[UUID('88d2698a-f131-11e1-98f2-0800278d227d'),
            1346173084.9, u'Description.']]
        d.callback(result)
        return d



class CassandraIndexTests(TestCase):
    """
    Tests for L{shannon.cassandra.CassandraIndex}.
    """
    def setUp(self):
        self.index = CassandraIndex(factory=FakeConnectionPool)


    def test_time(self):
        """
        L{CassandraIndex.time} returns the current epoch in milliseconds.
        This is tested by checking the number of digits in the returned time.
        """
        current = len(str(int(time.time() * 1000)))
        self.assertEqual(current, len(str(self.index._time())))


    def test_insert(self):
        """
        L{CassandraIndex.insert} executes without Failures.
        """
        tags = {'name':'value','name2':None}
        self.index.insert('entropyID', 'name', 'description', tags)


    def test_update(self):
        """
        L{CassandraIndex.update} executes without Failures.
        """
        self.index.update(
            'shannonId',
            shannonDescription='description',
            entropyID = 'entropyID',
            entropyName='entropyName',
            tags={'tag1':'value', 'tag2':'value2'})


    def test_retrieve(self):
        """
        L{CassandraIndex.retrieve} returns a C{list} containing a
        C{list} holding Shannon data.
        """
        def _cb(d):
            self.assertEqual('88d2698a-f131-11e1-98f2-0800278d227d',
                str(d[0]['shannon'][0]))
        d = self.index.retrieve('shannonId')
        d.addCallback(_cb)
