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

# External contributions to Koda.

load(
    "@com_google_arolla//py/arolla/optools:optools.bzl",
    "arolla_operator_package_snapshot",
)
load("@rules_python//python:defs.bzl", "py_library")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "functions",
    srcs = ["functions.py"],
    deps = ["//py:python_path"],
)

py_library(
    name = "operators",
    srcs = ["operators.py"],
    visibility = [
        "//koladata:internal",
        "//py/koladata/ext/operators:__pkg__",
    ],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

arolla_operator_package_snapshot(
    name = "operator_package.pb2",
    imports = [],
)
