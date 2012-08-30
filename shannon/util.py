from xmantissa.offering import InstalledOffering


def getAppStore(siteStore):
    """
    Retrieve the Shannon app store.
    """
    offering = siteStore.findUnique(
        InstalledOffering,
        InstalledOffering.offeringName == u'Shannon')
    appStore = offering.application.open()
    return appStore


def metadataFromHeaders(req):
    """
    @type req: L{IRequest}
    @param req: The request to retrieve metadata from.

    @rtype: C{dict}
    @return: Metadata used in Shannon.
    """
    headers = ['X-Shannon-Description', 'X-Shannon-Tags', 'X-Entropy-Name']
    headerData = [req.getHeader(x) for x in headers]
    return dict(zip(headers, headerData))


def tagsToStr(tags):
    """
    @param tags: A dictionary of tags to convert a string.
    @type tags: C{dict}.

    @return: String formatted tags.
    @rtype: C{str}
    """
    if not tags:
        return ''
    return ', '.join(['%s=%s' % (key, value) for key, value in tags.iteritems()])


def tagsToDict(tags):
    """
    @type tags: C{string}
    @param tags: The tags to parse.

    @return: A dictionary of tags.
    @rtype: C{dict}
    """
    if not tags:
        return None
    return dict(item.split('=', 1) for item in tags.split(', '))
