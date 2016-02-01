from twisted.web.client import HTTPClientFactory
HTTPClientFactory.noisy = False
del HTTPClientFactory

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
