"""
Dropin with Entropy offering.
"""
from xmantissa.offering import Offering

from entropy.web import SimpleSiteFactory
from entropy.store import ContentStore


plugin = Offering(
    name=u'Entropy',
    description=u'A persistent, immutable object store.',
    siteRequirements=[(None, SimpleSiteFactory)],
    appPowerups=[ContentStore],
    installablePowerups=[],
    loginInterfaces=[],
    themes=[])
