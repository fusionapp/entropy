"""
Dropin with Entropy offering.
"""
from xmantissa.offering import Offering

from shannon.web import ShannonSiteFactory


plugin = Offering(
    name=u'Shannon',
    description=u'',
    siteRequirements=[(None, ShannonSiteFactory)],
    appPowerups=[],
    installablePowerups=[],
    loginInterfaces=[],
    themes=[])
