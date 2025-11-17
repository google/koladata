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

# A lightweight library with Koda syntax for working with Python objects.

load("@pip_deps//:requirements.bzl", "requirement")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_py_extension")

package(default_visibility = [
    "//koladata:internal",
    "//py/koladata/ext/view:__subpackages__",
])

licenses(["notice"])

py_test(
    name = "benchmarks",
    srcs = ["benchmarks.py"],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata:kd_ext",
        "//py/koladata/functions:py_conversions",
        requirement("google-benchmark"),
    ],
)

py_test(
    name = "view_test",
    srcs = ["view_test.py"],
    deps = [
        ":mask_constants",
        ":test_utils",
        ":view",
        "//py:python_path",
        requirement("optree"),
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "kv_test",
    srcs = ["kv_test.py"],
    deps = [
        ":kv",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "mask_constants_test",
    srcs = ["mask_constants_test.py"],
    deps = [
        ":mask_constants",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "clib_py_ext_test",
    srcs = ["clib_py_ext_test.py"],
    deps = [
        ":clib_py_ext",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "test_utils_test",
    srcs = ["test_utils_test.py"],
    deps = [
        ":mask_constants",
        ":test_utils",
        ":view",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "view",
    srcs = ["view.py"],
    deps = [
        ":clib_py_ext",
        ":mask_constants",
        "//py:python_path",
        requirement("optree"),
    ],
)

py_library(
    name = "mask_constants",
    srcs = ["mask_constants.py"],
    deps = ["//py:python_path"],
)

py_library(
    name = "kv",
    srcs = ["kv.py"],
    deps = [
        ":mask_constants",
        ":operators",
        ":view",
        "//py:python_path",
    ],
)

py_library(
    name = "operators",
    srcs = ["operators.py"],
    deps = [
        ":view",
        "//py:python_path",
        requirement("optree"),
    ],
)

py_library(
    name = "test_utils",
    testonly = True,
    srcs = ["test_utils.py"],
    deps = [
        ":mask_constants",
        ":view",
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

cc_library(
    name = "clib_cc_lib",
    srcs = ["clib.cc"],
    hdrs = ["clib.h"],
    deps = [
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//py/arolla/py_utils",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

koladata_py_extension(
    name = "clib_py_ext",
    srcs = ["clib_module.cc"],
    deps = [
        ":clib_cc_lib",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)
