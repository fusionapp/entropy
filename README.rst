=======
Entropy
=======

Entropy is an immutable object store, with an HTTP API, that replicates in
real-time to the configured backends.

Quick start
-----------

Begin by following the `recommended git workflow`_ to create and checkout a fork
of Entropy. Development is intended to take place in a Linux or macOS environment
and requires Python 2.7 (not Python 3).

.. code-block:: shell

   $ cd path/to/entropy/checkout
   $ pip install -r requirements.txt
   # It's recommended to install with `-e` to avoid having to constantly
   # reinstall Entropy when changing branches or making changes.
   $ pip install -e .

.. _recommended git workflow: https://github.com/fusionapp/fusion/wiki/Recommended-git-workflow
.. _virtual environment: https://virtualenv.pypa.io/en/stable/

After successfully installing the software and its dependencies, an instance of
it will need to be configured for development:

.. code-block:: shell

   # Create an alias to avoid having to continually pass the database argument.
   $ alias axiomatic='axiomatic -d entropy.axiom'

   # Create a directory for deployment artefacts.
   $ mkdir -p ~/deployment/entropy
   $ cd ~/deployment/entropy

   # Install the offering.
   $ axiomatic offering install Entropy
   # Configure the port to listen on.
   $ axiomatic port create --strport=tcp:8000 --factory-identifier=1

   # Start the instance.
   $ axiomatic start -n
