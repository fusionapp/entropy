Building the Entropy Docker container
=====================================

Entropy is built off the standard fusionapp/base container.

Build process
-------------

1. Pull the base container.

   .. code-block:: shell-session

      $ docker pull fusionapp/base

2. Run the base container to build the necessary wheels.

   .. code-block:: shell-session

      $ docker run --rm --interactive --volume=${PWD}:/application --volume=${PWD}/wheelhouse:/wheelhouse fusionapp/base

   The built wheels will be placed in the "wheelhouse" directory at the root
   of the repository.

3. Build the entropy container.

   .. code-block:: shell-session

      $ docker build --tag=fusionapp/entropy --file=docker/entropy.docker .
