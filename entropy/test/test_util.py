from twisted.trial.unittest import TestCase
from twisted.cred.portal import IRealm

from axiom.store import Store
from axiom.userbase import LoginSystem
from axiom.dependency import installOn

from xmantissa.ixmantissa import IOfferingTechnician
from xmantissa.offering import getOfferings

from entropy.util import getAppStore, deferred

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


@deferred
def testfn(arg):
    if arg == 42:
        raise ValueError('Oh noes')
    return arg


class DeferredTests(TestCase):
    def test_success(self):
        d = testfn(50)
        return d.addCallback(lambda result: self.assertEqual(result, 50))

    def test_failure(self):
        d = self.assertFailure(testfn(42), ValueError)
        return d.addCallback(lambda e: self.assertEqual(e.message, 'Oh noes'))
