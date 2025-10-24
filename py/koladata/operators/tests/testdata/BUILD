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

# Shared test data between various implementations of the same operator.

load("@rules_python//python:defs.bzl", "py_library")

package(default_visibility = [
    "//koladata:internal",
    "//py/koladata/ext/view:__subpackages__",
])

licenses(["notice"])

py_library(
    name = "lists_implode_testdata",
    testonly = True,
    srcs = ["lists_implode_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/types:data_slice",
    ],
)

py_library(
    name = "lists_explode_testdata",
    testonly = True,
    srcs = ["lists_explode_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/types:data_slice",
    ],
)

py_library(
    name = "slices_align_testdata",
    testonly = True,
    srcs = ["slices_align_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
    ],
)
