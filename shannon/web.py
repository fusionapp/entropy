from zope.interface import implements

from twisted.web.server import Site

from axiom.item import Item
from axiom.attributes import integer, text

from xmantissa.ixmantissa import IProtocolFactoryFactory

from shannon.main import getRootResource


class ShannonSiteFactory(Item):
    """
    Configuration object for a Mantissa HTTP server.
    """
    powerupInterfaces = [IProtocolFactoryFactory]
    implements(*powerupInterfaces)

    hostname = text(default=u'localhost')
    port = integer(default=9160)
    keyspace = text(default=u'shannon')

    def getFactory(self):
        return Site(getRootResource(self.hostname.encode('ascii'),
            self.port, self.keyspace.encode('ascii')))
