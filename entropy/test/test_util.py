"""
@copyright: 2007-2014 Quotemaster cc. See LICENSE for details.
"""
from twisted.trial.unittest import TestCase
from twisted.cred.portal import IRealm

from axiom.store import Store
from axiom.userbase import LoginSystem
from axiom.dependency import installOn

from xmantissa.ixmantissa import IOfferingTechnician
from xmantissa.offering import getOfferings

from entropy.util import getAppStore, deferred

class GetAppStoreTests(TestCase):
    """
    Tests for L{getAppStore}.
    """
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
        """
        L{getAppStore} should return the Entropy application store.
        """
        appStore = IRealm(self.siteStore).accountByAddress(u'Entropy', None).avatars.open()
        self.assertIdentical(appStore, getAppStore(self.siteStore))


@deferred
def testfn(arg):
    """
    Test function for L{deferred} decorator.

    Raises L{ValueError} if 42 is passed, otherwise returns whatever was
    passed.
    """
    if arg == 42:
        raise ValueError('Oh noes')
    return arg


class DeferredTests(TestCase):
    """
    Tests for L{deferred} decorator.
    """
    def test_success(self):
        """
        Returning a value should result in a Deferred that callbacks with that
        value.
        """
        d = testfn(50)
        return d.addCallback(lambda result: self.assertEquals(result, 50))

    def test_failure(self):
        """
        Raising an exception should result in a Deferred that errbacks with
        that exception.
        """
        d = self.assertFailure(testfn(42), ValueError)
        return d.addCallback(lambda e: self.assertIn('Oh noes', str(e)))
