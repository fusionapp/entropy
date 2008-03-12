from xmantissa.offering import InstalledOffering

def getAppStore(siteStore):
    """
    Retrieve the Entropy app store.
    """
    offering = siteStore.findUnique(InstalledOffering, InstalledOffering.offeringName == u'Entropy')
    appStore = offering.application.open()
    return appStore
