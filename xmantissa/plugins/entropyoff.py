"""
Dropin with Entropy offering.
"""
from xmantissa.offering import Offering

from entropy.web import SimpleSiteFactory
from entropy.store import ContentResource


plugin = Offering(
    name=u'Entropy',
    description=u'A persistent, immutable object store.',
    siteRequirements=[(None, SimpleSiteFactory)],
    appPowerups=[ContentResource],
    installablePowerups=[],
    loginInterfaces=[],
    themes=[])
