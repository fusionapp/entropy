from zope.interface import implements

from twisted.web.server import Site

from axiom.item import Item
from axiom.attributes import path

from xmantissa.ixmantissa import IProtocolFactoryFactory

from shannon.main import getResourceTree


class SimpleSiteFactory(Item):
    """
    Configuration object for a Mantissa HTTP server.
    """
    powerupInterfaces = [IProtocolFactoryFactory]
    implements(*powerupInterfaces)

    httpLog = path(default=None)

    def getFactory(self):
        return Site(getResourceTree())
