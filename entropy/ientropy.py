"""
@copyright: 2007-2014 Quotemaster cc. See LICENSE for details.

Interface definitions for Entropy.
"""
from zope.interface import Interface, Attribute



class IContentObject(Interface):
    """
    Immutable content object.
    """
    hash = Attribute("""The hash function used to calculate the content digest.""")
    contentDigest = Attribute("""A digest of the object content.""")
    contentType = Attribute("""The MIME type describing the content of this object.""")
    created = Attribute("""Creation timestamp of this object.""")
    metadata = Attribute("""Object metadata.""")

    def getContent():
        """
        Get the data contained in this object.

        @rtype: C{str}
        """



class IContentStore(Interface):
    """
    Interface for storing and retrieving immutable content objects.
    """
    def storeObject(content, contentType, metadata={}, created=None):
        """
        Store an object.

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


    def getObject(objectID):
        """
        Retrieve an object.

        @param objectId: the object identifier.
        @type objectId: C{unicode}
        @returns: the content object.
        @rtype: C{Deferred<IContentObject>}
        """


    def migrateTo(destination):
        """
        Initiate a migration to another content store.

        All objects present in this content store at the moment the migration
        is initiated MUST be replicated to the destination store before the
        migration is considered complete. Objects created after the migration
        is initiated MUST NOT be replicated.

        NOTE: This method is optional, as some storage backends may be unable
        to support enumerating all objects which is usually necessary to
        implement migration.

        @type  destination: L{IContentStore}
        @param destination: The destination store.

        @rtype: L{IMigration}
        @return: The migration powerup tracking the requested migration.

        @raise NotImplementedError: if this implementation does not support
            migration.
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
    def scheduleUpload(objectId, backend):
        """
        Notify the scheduler that an object needs to be uploaded to a backend.
        """



class IMigrationManager(Interface):
    """
    Manager for migrations from one content store to another.
    """
    def migrate(source, destination):
        """
        Initiate a migration between two content stores. Some content stores
        may not support migration, as some storage backends cannot support
        enumerating all stored objects.

        @type  source: L{IContentStore}
        @param source: The source content store; must support migration.

        @type  destination: L{IContentStore}
        @param destination: The destination store; does not need any special
            support for migration.

        @rtype: L{IMigration}
        @return: The migration powerup responsible for tracking the requested
            migration.
        """



class IMigration(Interface):
    """
    Powerup tracking a migration in progress.
    """
    def run():
        """
        Run this migration.

        If the migration is already running, this is a noop.
        """
