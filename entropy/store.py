from epsilon.extime import Time

from axiom.item import Item
from axiom.attributes import text, path, timestamp

class ImmutableObject(Item):
    contentHash = text(allowNone=False)
    content = path(allowNone=False)
    contentType = text(allowNone=False)
    created = timestamp(allowNone=False, defaultFactory=lambda: Time())
