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

load("@bazel_skylib//:bzl_library.bzl", "bzl_library")
load("@rules_cc//cc:cc_shared_library.bzl", "cc_shared_library")

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
        "@com_google_arolla//py/arolla/dynamic_deps:jagged_shape_so",
        "@com_google_arolla//py/arolla/dynamic_deps:derived_qtype_so",
    ],
    exports_filter = [
        "//koladata:__subpackages__",
        "//py/koladata:__subpackages__",
    ],
    deps = [
        "//koladata:all_qexpr_operators",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/expr:init",
        "//koladata/functor/parallel:init_expr_operators",
        "//koladata/iterables:init_expr_operators",
        "//koladata/s11n",
        "//py/koladata/fstring:fstring_cc_lib",
        "//py/koladata/types:data_bag_cc_lib",
        "//py/koladata/types:data_slice_cc_lib",
    ],
)

bzl_library(
    name = "py_extension_bzl",
    srcs = ["py_extension.bzl"],
    visibility = ["//koladata:internal"],
)
