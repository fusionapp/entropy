from zope.interface import Interface

class IContentStore(Interface):
    """
    Interface for storing and retrieving immutable content objects.
    """
    def storeObject(content, contentType, metadata={}):
        """
        Store an object.

        @param content: the data to store.
        @type content: C{str}
        @param contentType: the MIME type of the content.
        @type contentType: C{unicode}
        @param metadata: a dictionary of metadata entries.
        @type metadata: C{dict} of C{unicode}:C{unicode}
        @returns: the object identifier.
        @rtype: C{unicode}
        """
