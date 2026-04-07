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

load("@com_google_arolla//py/arolla/dynamic_deps:build_defs.bzl", "arolla_py_cc_deps")
load("@rules_python//python:defs.bzl", "py_library")
load("//py/koladata/operators:optools.bzl", "koladata_cc_operator_package")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

# C++-only kd_ext.contrib operator package. Use this one for serving.
# In Python `from koladata import kd_ext` instead.
koladata_cc_operator_package(
    name = "cc_operators",
    arolla_initializer = {
        "name": "arolla_operators/kd_ext_contrib",
        "deps": ["arolla_operators/koda_base"],
    },
    imports = [
        "koladata.ext.contrib.build_cc_operators",
    ],
    preimports = ["koladata.kd"],
    tool_deps = [
        ":build_cc_operators",
        "//py/koladata:kd",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//koladata/contrib:operators",
        "//py/koladata/operators:cc_operators",
    ],
)

arolla_py_cc_deps(
    name = "cc_operators_py_clib",
    dynamic_deps = [
        "//py/koladata/dynamic_deps:koladata_so",
        "//py/koladata/dynamic_deps:koladata_operators_so",
    ],
    deps = [
        ":cc_operators",
    ],
)

py_library(
    name = "kde_operators",
    srcs = ["kde_operators.py"],
    visibility = [
        "//koladata:internal",
        "//py/koladata:__subpackages__",
    ],
    deps = [
        ":cc_operators_py_clib",
        ":operators",
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

py_library(
    name = "build_cc_operators",
    srcs = ["build_cc_operators.py"],
    deps = [
        ":operators",
        "//py:python_path",
        "//py/koladata/operators:optools",
    ],
)

py_library(
    name = "operators",
    srcs = ["operators.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/operators:qtype_utils",
        "//py/koladata/operators:schema",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "functions",
    srcs = ["functions.py"],
    visibility = [
        "//koladata:internal",
        "//py/koladata:__subpackages__",
    ],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
    ],
)
