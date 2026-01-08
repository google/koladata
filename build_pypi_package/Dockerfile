# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds a Python wheel package with the specified version, outputting
# the result to the host's current directory.
#
# Usage:
#
#     docker buildx build . \
#         --output=. \
#         --build-arg PACKAGE_VERSION=<version> \
#         --build-arg PYTHON_VERSION=<3.11|3.12|3.13>
#
# Uploading (after build):
#
#     python3 -m twine upload *.whl
#
#
# Recommended workflow:
#
#   1. Build `arolla` and `koladata` packages:
#      docker buildx build . \
#          --output=. \
#          --build-arg PACKAGE_VERSION=<version>
#
#   2. Upload the resulting arolla-*.whl and koladata-*.whl files to
#      Google Colab (left sidebar -> Files -> Upload to sessions storage),
#      and do some tests.
#
#      # Installing the packages.
#      !pip install arolla-*.whl koladata-*.whl
#
#      # Test Arolla.
#      from arolla import arolla
#      arolla.eval(arolla.M.math.trig.sin(arolla.L.x), x=1)
#
#      # Test Koladata.
#      from koladata import kd
#      kd.M.math.exp(1)
#
#   3. Upload the resulting packages:
#      python3 -m twine upload arolla-*.whl koladata-*.whl
#

# Pre-installing packages in the base stage.
FROM ubuntu:22.04 AS base
COPY --chmod=0755 /base.sh /base.sh
RUN /base.sh

FROM base AS build
COPY --chmod=0755 build.sh /build.sh
ARG PYTHON_VERSION=3.11
ENV PYTHON_VERSION=$PYTHON_VERSION
RUN /build.sh

# Note: Separating the 'build' and 'pack' stages allows caching for the build
# artifacts.
FROM base AS pack
COPY --from=build /build/pkg/ /build/pkg/
COPY --chmod=0755 pack.sh /pack.sh
ARG PACKAGE_VERSION=0.0.0-0 PYTHON_VERSION=3.11
ENV PACKAGE_VERSION=$PACKAGE_VERSION PYTHON_VERSION=$PYTHON_VERSION
RUN /pack.sh

FROM scratch
COPY --from=pack /build/pkg/dist/*.whl /
