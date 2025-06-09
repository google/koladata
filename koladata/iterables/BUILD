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

# This package contains code working with Koda iterables.
#
# A Koda iterable is a derived type from an arolla Sequence, which is intended to be
# used to represent streams that need streaming processing in multithreaded
# evaluation.

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "iterable_qtype",
    srcs = ["iterable_qtype.cc"],
    hdrs = ["iterable_qtype.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/synchronization",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/sequence",
        "@com_google_arolla//arolla/util",
    ],
)

cc_library(
    name = "operators",
    srcs = ["operators.cc"],
    deps = [
        ":iterable_qtype",
        ":reduce_operators",
        ":sequence_operators",
        "//koladata:data_slice_qtype",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
    ],
    alwayslink = 1,
)

cc_library(
    name = "expr_operators",
    srcs = ["expr_operators.cc"],
    hdrs = ["expr_operators.h"],
    deps = [
        ":iterable_qtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators:bootstrap",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "init_expr_operators",
    srcs = ["init_expr_operators.cc"],
    deps = [
        ":expr_operators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
    alwayslink = 1,
)

cc_library(
    name = "sequence_operators",
    srcs = ["sequence_operators.cc"],
    hdrs = ["sequence_operators.h"],
    deps = [
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/operators:lib",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/sequence",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "reduce_operators",
    srcs = ["reduce_operators.cc"],
    hdrs = ["reduce_operators.h"],
    deps = [
        ":iterable_qtype",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/internal/op_utils:qexpr",
        "//koladata/operators:lib",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/sequence",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "iterable_qtype_test",
    srcs = ["iterable_qtype_test.cc"],
    deps = [
        ":iterable_qtype",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/sequence",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)
