#!/usr/bin/env bash

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

# This script is designed to be run within a Docker container. It installs
# additional dependencies the will be used by the build.sh and pack.sh scripts.

set -euxo pipefail

# Add Bazel distribution URI as a package source
# (based on https://bazel.build/install/ubuntu#install-on-ubuntu)
apt update
apt install -y \
    apt-transport-https \
    curl \
    gnupg \
    ;
curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor >bazel-archive-keyring.gpg
mv bazel-archive-keyring.gpg /usr/share/keyrings
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/bazel-archive-keyring.gpg] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list

# Install build dependencies.
apt update
apt install -y \
    bazel \
    g++-12 \
    git \
    patchelf \
    python3 \
    python3-setuptools \
    python3-wheel \
    twine \
    ;
