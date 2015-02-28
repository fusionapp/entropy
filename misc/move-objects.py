from sys import argv

from axiom.store import Store
from entropy.store import ImmutableObject
from entropy.util import getAppStore


def moveObjects(appStore, start, limit):
    obj = None
    skipped = 0
    for obj in appStore.query(
            ImmutableObject,
            ImmutableObject.storeID >= start,
            limit=limit):
        oldPath = obj.content
        bucket = obj.contentDigest[:4]
        newPath = appStore.newFilePath(
            'objects', 'immutable', bucket,
            '%s:%s' % (obj.hash, obj.contentDigest))
        if oldPath == newPath:
            skipped += 1
            continue
        if not newPath.parent().exists():
            newPath.parent().makedirs()
        oldPath.moveTo(newPath)
        obj.content = newPath
    if obj is None:
        print 'No objects selected'
    else:
        print 'Last object seen: %s' % (obj.storeID,)
    if skipped > 0:
        print 'Skipped %d objects' % (skipped,)


siteStore = Store(argv[1])
appStore = getAppStore(siteStore)
limit = int(argv[3])
appStore.transact(moveObjects, appStore, int(argv[2]), int(argv[3]))
