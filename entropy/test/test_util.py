from twisted.trial.unittest import TestCase
from twisted.cred.portal import IRealm

from axiom.store import Store
from axiom.userbase import LoginSystem
from axiom.dependency import installOn

from xmantissa.ixmantissa import IOfferingTechnician
from xmantissa.offering import getOfferings

from entropy.util import getAppStore

class GetAppStoreTests(TestCase):
    def setUp(self):
        self.siteStore = Store(self.mktemp())
        installOn(LoginSystem(store=self.siteStore), self.siteStore)

        for offering in getOfferings():
            if offering.name == u'Entropy':
                break
        else:
            raise RuntimeError(u'Could not find Entropy offering')

        IOfferingTechnician(self.siteStore).installOffering(offering)

    def test_getAppStore(self):
        appStore = IRealm(self.siteStore).accountByAddress(u'Entropy', None).avatars.open()
        self.assertIdentical(appStore, getAppStore(self.siteStore))
