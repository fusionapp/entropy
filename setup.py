import versioneer
from setuptools import find_packages, setup
setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    name='entropy-store',
    maintainer='Entropy developers',
    description='A simple content-addressed immutable object store, with flexible backend support.',
    url='https://github.com/fusionapp/entropy',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Framework :: Twisted',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.5',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Archiving'],
    packages=find_packages() + ['xmantissa.plugins'],
    install_requires=['Twisted[tls] >= 15.2.1',
                      'Epsilon >= 0.7.0',
                      'Axiom >= 0.7.4',
                      'Nevow >= 0.9.8',
                      'txAWS >= 0.2',
                      'Mantissa >= 0.8.0'])
