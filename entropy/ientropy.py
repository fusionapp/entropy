from zope.interface import Interface, Attribute



class IStorageClass(Interface):
    """
    Collection of backend data stores.
    """
    name = Attribute("""Storage class name.""")

    def getReadBackends(self):
        """
        Get the read backends for this storage class.
        """

    def getWriteBackends(self):
        """
        Get the write backends for this storage class.
        """

    def getWriteLaterBackends(self):
        """
        Get the asynchronous background write backends for this storage class.
        """



class IContentObject(Interface):
    """
    Immutable content object.
    """
    contentType = Attribute("""The MIME type describing the content of this object.""")
    created = Attribute("""Creation timestamp of this object.""")
    metadata = Attribute("""Object metadata.""")
    objectId = Attribute("""Object id.""")

    def getContent(self):
        """
        Get the data contained in this object.

        @rtype: C{str}
        """



class IReadBackend(Interface):
    """
    Interface for retrieving immutable content objects.
    """
    def getObject(objectId):
        """
        Retrieve an object.

        @param objectId: the object identifier.
        @type objectId: C{unicode}
        @returns: the content object.
        @rtype: C{Deferred<IContentObject>}
        """



class IWriteBackend(Interface):
    """
    Interface for storing immutable content objects.
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



class IWriteLaterBackend(Interface):
    """
    Interface for storing immutable content objects at leisure.
    """



class IBackendStore(IReadBackend, IWriteBackend, IWriteLaterBackend):
    """
    Interface for storing and retrieving immutable content objects.
    """



class IUploadScheduler(Interface):
    """
    Manager of pending uploads.
    """
    def wake():
        """
        Notify the scheduler that new pending uploads have been created.
        """
