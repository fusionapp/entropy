"""
Dropin with Entropy offering.
"""
from xmantissa.offering import Offering

from entropy.web import SimpleSiteFactory
from entropy.store import ContentResource

from shannon.main import CoreResource
from shannon.web import SimpleSiteFactory as ShannonSiteFactory


plugin = Offering(
    name=u'Entropy',
    description=u'A persistent, immutable object store.',
    siteRequirements=[(None, SimpleSiteFactory)],
    appPowerups=[ContentResource],
    installablePowerups=[],
    loginInterfaces=[],
    themes=[])


plugin = Offering(
    name=u'Shannon',
    description=u'A persistent, immutable object store.',
    siteRequirements=[(None, ShannonSiteFactory)],
    appPowerups=[CoreResource],
    installablePowerups=[],
    loginInterfaces=[],
    themes=[])
