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

# Parallel execution tools for Koda.

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "executor",
    srcs = ["executor.cc"],
    hdrs = ["executor.h"],
    deps = [
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/status",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "executor_test",
    srcs = ["executor_test.cc"],
    deps = [
        ":executor",
        "@com_google_absl//absl/status",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "eager_executor",
    srcs = ["eager_executor.cc"],
    hdrs = ["eager_executor.h"],
    deps = [
        ":executor",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/status",
    ],
)

cc_test(
    name = "eager_executor_test",
    srcs = ["eager_executor_test.cc"],
    deps = [
        ":eager_executor",
        ":executor",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "init_expr_operators",
    srcs = ["init_expr_operators.cc"],
    deps = [
        ":eager_executor",
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
    name = "operators",
    srcs = ["operators.cc"],
    deps = [
        ":async_eval_operator",
        ":future_operators",
        ":future_qtype",
        "//koladata:data_slice_qtype",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
    ],
    alwayslink = 1,
)

cc_library(
    name = "future",
    srcs = ["future.cc"],
    hdrs = ["future.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "future_test",
    srcs = ["future_test.cc"],
    deps = [
        ":future",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "future_operators",
    srcs = ["future_operators.cc"],
    hdrs = ["future_operators.h"],
    deps = [
        ":future",
        ":future_qtype",
        "//koladata:data_slice_qtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/core:lib",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "future_qtype",
    srcs = ["future_qtype.cc"],
    hdrs = ["future_qtype.h"],
    deps = [
        ":future",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/synchronization",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_library(
    name = "expr_operators",
    srcs = ["expr_operators.cc"],
    hdrs = ["expr_operators.h"],
    deps = [
        ":executor",
        ":future_qtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "async_eval_operator",
    srcs = ["async_eval_operator.cc"],
    hdrs = ["async_eval_operator.h"],
    deps = [
        ":async_eval",
        ":executor",
        ":future",
        ":future_qtype",
        "//koladata:data_slice_qtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "async_eval",
    srcs = ["async_eval.cc"],
    hdrs = ["async_eval.h"],
    deps = [
        ":executor",
        ":future",
        ":future_qtype",
        "//koladata/expr:expr_eval",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "future_qtype_test",
    srcs = ["future_qtype_test.cc"],
    deps = [
        ":future",
        ":future_qtype",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)
