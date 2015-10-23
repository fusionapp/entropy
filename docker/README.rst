Building the Entropy Docker container
=====================================

There are three Docker containers defined: run.docker defines the actual
container used to run Entropy, build.docker is used to build the wheels
required for the "run" container, and base.docker is used to define a base
container which is shared between the "build" and "run" containers as an
optimization.

Build process
-------------

1. Build the base container.

   .. code-block:: shell-session

      $ docker build -t fusionapp/entropy-base -f docker/base.docker .

2. Build the build container.

   .. code-block:: shell-session

      $ docker build -t fusionapp/entropy-build -f docker/build.docker .

3. Run the build container to build the necessary wheels.

   .. code-block:: shell-session

      $ docker run --rm -ti -v "${PWD}:/application" -v "${PWD}/wheelhouse:/wheelhouse" fusionapp/entropy-build

   The built wheels will be placed in the "wheelhouse" directory at the root
   of the repository. This is necessary for building the final container.

4. Build the run container.

   .. code-block:: shell-session

      $ docker build -t fusionapp/entropy -f docker/run.docker .
