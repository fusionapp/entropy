"""
Dropin with Entropy offering.
"""
from xmantissa.offering import Offering

from shannon.main import CoreResource
from shannon.web import SimpleSiteFactory as ShannonSiteFactory


plugin = Offering(
    name=u'Shannon',
    description=u'',
    siteRequirements=[(None, ShannonSiteFactory)],
    appPowerups=[CoreResource],
    installablePowerups=[],
    loginInterfaces=[],
    themes=[])
