# Copyright 2024 Google LLC
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

# Koda functions (non operators or have different behavior from operators).

load("@rules_python//python:defs.bzl", "py_library")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "functions",
    srcs = ["functions.py"],
    deps = [
        ":attrs",
        ":object_factories",
        ":py_conversions",
        ":schema",
    ],
)

py_library(
    name = "schema",
    srcs = ["schema.py"],
    visibility = ["//visibility:private"],
    deps = [
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
    ],
)

py_library(
    name = "attrs",
    srcs = ["attrs.py"],
    visibility = ["//visibility:private"],
    deps = [
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
    ],
)

py_library(
    name = "object_factories",
    srcs = ["object_factories.py"],
    visibility = ["//visibility:private"],
    deps = [
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
    ],
)

py_library(
    name = "py_conversions",
    srcs = ["py_conversions.py"],
    visibility = ["//visibility:private"],
    deps = [
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
    ],
)
