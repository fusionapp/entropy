from json import JSONEncoder
from uuid import UUID

from twisted.python.failure import Failure


def metadataFromHeaders(req):
    """
    Extracts Shannon-specific data from a request's headers.

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
    Converts a C{dict} of tags to a C{str}

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
    Converts a C{str} of tags to a C{dict}.

    @type tags: C{string}
    @param tags: The tags to parse.

    @return: A dictionary of tags.
    @rtype: C{dict}
    """
    if not tags:
        return None
    return dict(item.split('=', 1) for item in tags.split(', '))



class ShannonEncoder(JSONEncoder):
    """
    Handles encoding of L{Failure} and L{UUID} objects.
    """
    def default(self, obj):
        """
        Failures are returned as C{dict} with the key being the 
        contained exception's name and the value being the exception's message.

        UUID objects are turned into a C{str}.
        """
        if isinstance(obj, Failure):
            return {obj.value.__class__.__name__: obj.value.message}
        if isinstance(obj, UUID):
            return str(obj)
