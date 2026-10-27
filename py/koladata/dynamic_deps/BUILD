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

default_visibility = ["//koladata:internal"]

package(default_visibility = default_visibility)

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
        "@com_google_arolla//py/arolla/dynamic_deps:objects_so",
    ],
    exports_filter = [
        "//koladata:__subpackages__",
        "//py/koladata:__subpackages__",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//koladata:all_qexpr_operators",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/expr:init",
        "//koladata/functor/parallel:async_eval",
        "//koladata/functor/parallel:basic_routine",
        "//koladata/functor/parallel:context_guard",
        "//koladata/functor/parallel:create_transform_config",
        "//koladata/functor/parallel:derived_executor",
        "//koladata/functor/parallel:eager_executor",
        "//koladata/functor/parallel:executor",
        "//koladata/functor/parallel:future",
        "//koladata/functor/parallel:future_qtype",
        "//koladata/functor/parallel:get_default_executor",
        "//koladata/functor/parallel:init_expr_operators",
        "//koladata/functor/parallel:make_executor",
        "//koladata/functor/parallel:parallel_call_utils",
        "//koladata/functor/parallel:stream",
        "//koladata/functor/parallel:stream_call",
        "//koladata/functor/parallel:stream_composition",
        "//koladata/functor/parallel:stream_filter_json",
        "//koladata/functor/parallel:stream_for",
        "//koladata/functor/parallel:stream_loop_internal",
        "//koladata/functor/parallel:stream_map",
        "//koladata/functor/parallel:stream_qtype",
        "//koladata/functor/parallel:stream_reduce",
        "//koladata/functor/parallel:stream_reduce_stack_or_concat",
        "//koladata/functor/parallel:stream_while",
        "//koladata/functor/parallel:transform",
        "//koladata/functor/parallel:transform_config",
        "//koladata/functor/parallel:transform_config_registry",
        "//koladata/iterables:init_expr_operators",
        "//koladata/s11n",
        "//py/koladata/fstring:fstring_cc_lib",
        "//py/koladata/types:data_bag_cc_lib",
        "//py/koladata/types:data_slice_cc_lib",
    ],
)

cc_shared_library(
    name = "koladata_operators_so",
    dynamic_deps = [
        "//py/koladata/dynamic_deps:koladata_so",
    ],
    exports_filter = [
        "//py/koladata/operators:__subpackages__",
    ],
    visibility = ["//visibility:public"],
    deps = ["//py/koladata/operators:cc_operators"],
)

cc_shared_library(
    name = "koladata_ext_operators_so",
    dynamic_deps = [
        "//py/koladata/dynamic_deps:koladata_so",
        "//py/koladata/dynamic_deps:koladata_operators_so",
    ],
    exports_filter = [
        "//py/koladata/ext:cc_operators",
        "//koladata/contrib:operators",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//koladata/contrib:operators",
        "//py/koladata/ext:cc_operators",
    ],
)

bzl_library(
    name = "py_extension_bzl",
    srcs = ["py_extension.bzl"],
    visibility = ["//visibility:private"],
)
