from epsilon.extime import Time

from axiom.item import Item
from axiom.attributes import text, path, timestamp


class ImmutableObject(Item):
    """
    An immutable object.

    Immutable objects are addressed by content hash, and consist of the object
    data as a binary blob, and object key/value metadata pairs.
    """
    contentHash = text(allowNone=False)
    content = path(allowNone=False)
    contentType = text(allowNone=False)
    created = timestamp(allowNone=False, defaultFactory=lambda: Time())


class ContentStore(Item):
    """
    Manager for stored objects.
    """
    hashAlgorithm = text(allowNone=False, default=u'sha256')
