from uuid import UUID
from twisted.trial.unittest import TestCase
from twisted.internet import defer

from shannon.cassandra import CassandraIndex



class FakeConnectionPool(object):
    def __init__(self, dbapiName, *connargs, **connkw):
        pass


    def runOperation(self, *args, **kw):
        d = defer.Deferred()
        d.callback(None)
        return d


    def runQuery(self, *args, **kw):
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
        An epoch represented in milliseconds
        """
        self.assertEqual(13, len(str(self.index._time())))


    def test_insert(self):
        tags = {'name':'value','name2':None}
        self.index.insert('entropyID', 'name', 'description', tags)


    def test_update(self):
        self.index.update(
            'shannonId',
            shannonDescription='description',
            entropyID = 'entropyID',
            entropyName='entropyName',
            tags={'tag1':'value', 'tag2':'value2'})


    def test_retrieve(self):
        def _cb(d):
            self.assertEqual('88d2698a-f131-11e1-98f2-0800278d227d',
                str(d[0]['shannon'][0]))
        d = self.index.retrieve('shannonId')
        d.addCallback(_cb)
