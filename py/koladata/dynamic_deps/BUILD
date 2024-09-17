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

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_shared_library(
    name = "koladata_so",
    dynamic_deps = [
        "@com_google_arolla//py/arolla/dynamic_deps:base_so",
        "@com_google_arolla//py/arolla/dynamic_deps:arolla_so",
        "@com_google_arolla//py/arolla/dynamic_deps:arolla_py_abc_so",
        "@com_google_arolla//py/arolla/dynamic_deps:arolla_standard_operators_so",
    ],
    exports_filter = [
        "//koladata:__subpackages__",
        "//py/koladata:__subpackages__",
    ],
    deps = [
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/expr:expr_operators",
        "//koladata/expr:expr_operators_repr",
        "//koladata/functor:operators",
        "//koladata/operators",
        "//koladata/s11n",
        "//py/koladata/exceptions:py_exception_utils",
        "//py/koladata/types:data_bag_cc_lib",
        "//py/koladata/types:data_slice_cc_lib",
    ],
)

cc_shared_library(
    name = "koladata_test_so",
    testonly = True,
    dynamic_deps = [
        "@com_google_arolla//py/arolla/dynamic_deps:base_so",
        "@com_google_arolla//py/arolla/dynamic_deps:arolla_so",
        "@com_google_arolla//py/arolla/dynamic_deps:arolla_py_abc_so",
        "@com_google_arolla//py/arolla/dynamic_deps:arolla_standard_operators_so",
        ":koladata_so",
    ],
    exports_filter = [
        "//py/koladata/types/testing:__subpackages__",
    ],
    deps = [
        "//py/koladata/types/testing:operator_test_invalid_unicode_op",
    ],
)

bzl_library(
    name = "py_extension_bzl",
    srcs = ["py_extension.bzl"],
    visibility = ["//koladata:internal"],
)
