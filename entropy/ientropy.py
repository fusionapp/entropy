from zope.interface import Interface, Attribute



class IContentObject(Interface):
    """
    Immutable content object.
    """
    # hash = Attribute("""The hash function used to calculate the content digest.""")
    # contentDigest = Attribute("""A digest of the object content.""")
    contentType = Attribute("""The MIME type describing the content of this object.""")
    created = Attribute("""Creation timestamp of this object.""")
    metadata = Attribute("""Object metadata.""")
    objectId = Attribute("""Object id.""")

    def getContent(self):
        """
        Get the data contained in this object.

        @rtype: C{str}
        """



class IContentStore(Interface):
    """
    Interface for storing and retrieving immutable content objects.
    """
    def storeObject(objectId, content, contentType=None, metadata={}, created=None):
        """
        Store an object.

        @param objectId: the object identifier.
        @type objectId: C{str}

        @param content: the data to store.
        @type content: C{str}

        @param contentType: the MIME type of the content.
        @type contentType: C{unicode}

        @param metadata: a dictionary of metadata entries.
        @type metadata: C{dict} of C{unicode}:C{unicode}

        @param created: the creation timestamp; defaults to the current time.
        @type created: L{epsilon.extime.Time} or C{None}

        @returns: the object identifier.
        @rtype: C{Deferred<unicode>}
        """

    def getObject(objectId):
        """
        Retrieve an object.

        @param objectId: the object identifier.
        @type objectId: C{unicode}
        @returns: the content object.
        @rtype: C{Deferred<IContentObject>}
        """



class ISiblingStore(IContentStore):
    """
    Sibling content store.
    """



class IBackendStore(IContentStore):
    """
    Backend content store.
    """



class IUploadScheduler(Interface):
    """
    Manager of pending uploads.
    """
    def wake():
        """
        Notify the scheduler that new pending uploads have been created.
        """
