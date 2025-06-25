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

# Parallel execution tools for Koda.

load("@com_google_protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("@com_google_protobuf//bazel:py_proto_library.bzl", "py_proto_library")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

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
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "executor_test",
    srcs = ["executor_test.cc"],
    deps = [
        ":executor",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "transform",
    srcs = ["transform.cc"],
    hdrs = ["transform.h"],
    deps = [
        ":execution_config_cc_proto",
        ":execution_context",
        ":executor",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "//koladata/expr:expr_eval",
        "//koladata/expr:expr_operators",
        "//koladata/expr:non_determinism",
        "//koladata/functor",
        "//koladata/functor:auto_variables",
        "//koladata/functor:signature",
        "//koladata/functor:signature_storage",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/internal:non_deterministic_token",
        "@com_google_absl//absl/algorithm:container",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/visitors",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "transform_test",
    srcs = ["transform_test.cc"],
    deps = [
        ":create_execution_context",
        ":eager_executor",
        ":executor",
        ":future",
        ":future_qtype",
        ":transform",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/expr:expr_operators",
        "//koladata/functor",
        "//koladata/functor:call",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/internal:non_deterministic_token",
        "//koladata/testing:eval_python_declared_ops_in_cpp_tests",
        "//koladata/testing:matchers",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
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
    name = "get_default_executor",
    srcs = ["get_default_executor.cc"],
    hdrs = ["get_default_executor.h"],
    deps = [
        ":default_asio_executor",
        ":executor",
        "@com_google_absl//absl/base:nullability",
    ],
)

cc_library(
    name = "make_executor",
    srcs = ["make_executor.cc"],
    hdrs = ["make_executor.h"],
    deps = [
        ":asio_executor",
        ":executor",
        "@com_google_absl//absl/base:nullability",
    ],
)

cc_library(
    name = "default_asio_executor",
    srcs = ["default_asio_executor.cc"],
    hdrs = ["default_asio_executor.h"],
    deps = [
        ":executor",
        "@boost.asio//:boost.asio",
        "@boost.throw_exception//:boost.throw_exception",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
    ],
)

cc_test(
    name = "default_asio_executor_test",
    srcs = ["default_asio_executor_test.cc"],
    deps = [
        ":default_asio_executor",
        ":executor",
        "@com_google_absl//absl/synchronization",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "asio_executor",
    srcs = ["asio_executor.cc"],
    hdrs = ["asio_executor.h"],
    deps = [
        ":executor",
        ":get_default_executor",
        "@boost.asio//:boost.asio",
        "@boost.throw_exception//:boost.throw_exception",
        "@com_google_absl//absl/base:nullability",
    ],
)

cc_test(
    name = "asio_executor_test",
    srcs = ["asio_executor_test.cc"],
    deps = [
        ":asio_executor",
        ":executor",
        ":get_default_executor",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "stream",
    srcs = ["stream.cc"],
    hdrs = ["stream.h"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "stream_test",
    srcs = ["stream_test.cc"],
    deps = [
        ":executor",
        ":get_default_executor",
        ":stream",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "stream_qtype",
    srcs = ["stream_qtype.cc"],
    hdrs = ["stream_qtype.h"],
    deps = [
        ":stream",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "stream_qtype_test",
    srcs = ["stream_qtype_test.cc"],
    deps = [
        ":stream",
        ":stream_qtype",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "basic_routine",
    srcs = ["basic_routine.cc"],
    hdrs = ["basic_routine.h"],
    deps = [
        ":executor",
        ":stream",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "basic_routine_test",
    srcs = ["basic_routine_test.cc"],
    deps = [
        ":basic_routine",
        ":eager_executor",
        ":get_default_executor",
        ":make_executor",
        ":stream",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "stream_composition",
    srcs = ["stream_composition.cc"],
    hdrs = ["stream_composition.h"],
    deps = [
        ":stream",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/synchronization",
        "@com_google_arolla//arolla/qtype",
    ],
)

cc_test(
    name = "stream_composition_test",
    srcs = ["stream_composition_test.cc"],
    deps = [
        ":get_default_executor",
        ":make_executor",
        ":stream",
        ":stream_composition",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/synchronization",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "stream_map",
    srcs = ["stream_map.cc"],
    hdrs = ["stream_map.h"],
    deps = [
        ":executor",
        ":stream",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "stream_map_test",
    srcs = ["stream_map_test.cc"],
    deps = [
        ":eager_executor",
        ":executor",
        ":get_default_executor",
        ":make_executor",
        ":stream",
        ":stream_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/synchronization",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "stream_reduce",
    srcs = ["stream_reduce.cc"],
    hdrs = ["stream_reduce.h"],
    deps = [
        ":basic_routine",
        ":executor",
        ":stream",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "stream_reduce_test",
    srcs = ["stream_reduce_test.cc"],
    deps = [
        ":eager_executor",
        ":executor",
        ":get_default_executor",
        ":make_executor",
        ":stream",
        ":stream_reduce",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/synchronization",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "stream_loop_internal",
    srcs = ["stream_loop_internal.cc"],
    hdrs = ["stream_loop_internal.h"],
    deps = [
        ":stream",
        ":stream_qtype",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "stream_loop_internal_test",
    srcs = ["stream_loop_internal_test.cc"],
    deps = [
        ":stream",
        ":stream_loop_internal",
        ":stream_qtype",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:test_utils",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "stream_while",
    srcs = ["stream_while.cc"],
    hdrs = ["stream_while.h"],
    deps = [
        ":basic_routine",
        ":executor",
        ":stream",
        ":stream_loop_internal",
        "//koladata:data_slice_qtype",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "stream_while_test",
    srcs = ["stream_while_test.cc"],
    deps = [
        ":eager_executor",
        ":stream",
        ":stream_while",
        "//koladata:data_slice_qtype",
        "//koladata:test_utils",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "stream_for",
    srcs = ["stream_for.cc"],
    hdrs = ["stream_for.h"],
    deps = [
        ":basic_routine",
        ":executor",
        ":stream",
        ":stream_loop_internal",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "stream_for_test",
    srcs = ["stream_for_test.cc"],
    deps = [
        ":eager_executor",
        ":stream",
        ":stream_for",
        "//koladata:data_slice_qtype",
        "//koladata:test_utils",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
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
        ":get_default_executor",
        "//koladata/expr:expr_operators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
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
        ":create_execution_context",
        ":execution_context",
        ":executor",
        ":future_operators",
        ":future_qtype",
        ":get_default_executor",
        ":make_executor",
        ":stream_operators",
        ":stream_qtype",
        ":testing_stream_prime",
        ":transform",
        "//koladata:data_slice_qtype",
        "//koladata/internal:non_deterministic_token",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
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
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/functional:any_invocable",
        "@com_google_absl//absl/log",
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
        ":stream",
        ":stream_composition",
        ":stream_qtype",
        "//koladata:data_slice_qtype",
        "//koladata/iterables:iterable_qtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/sequence",
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
        "@com_google_absl//absl/log:check",
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
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "async_eval_test",
    srcs = ["async_eval_test.cc"],
    deps = [
        ":async_eval",
        ":eager_executor",
        ":future",
        ":future_qtype",
        ":make_executor",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "//koladata/testing:eval_python_declared_ops_in_cpp_tests",
        "//koladata/testing:matchers",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators:bootstrap",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "stream_operators",
    srcs = ["stream_operators.cc"],
    hdrs = ["stream_operators.h"],
    deps = [
        ":executor",
        ":stream",
        ":stream_composition",
        ":stream_for",
        ":stream_map",
        ":stream_qtype",
        ":stream_reduce",
        ":stream_while",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/functor:call",
        "//koladata/iterables:iterable_qtype",
        "//koladata/operators:lib",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/sequence",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "execution_context",
    srcs = ["execution_context.cc"],
    hdrs = ["execution_context.h"],
    deps = [
        ":execution_config_cc_proto",
        ":executor",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
    ],
)

cc_test(
    name = "execution_context_test",
    srcs = ["execution_context_test.cc"],
    deps = [
        ":eager_executor",
        ":execution_config_cc_proto",
        ":execution_context",
        ":executor",
        "//koladata/testing:test_env",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators:bootstrap",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "create_execution_context",
    srcs = ["create_execution_context.cc"],
    hdrs = ["create_execution_context.h"],
    deps = [
        ":execution_config_cc_proto",
        ":execution_context",
        ":executor",
        "//koladata:data_slice",
        "//koladata/proto:to_proto",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "create_execution_context_test",
    srcs = ["create_execution_context_test.cc"],
    deps = [
        ":create_execution_context",
        ":eager_executor",
        ":execution_config_cc_proto",
        ":executor",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata/proto:from_proto",
        "//koladata/testing:eval_python_declared_ops_in_cpp_tests",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
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

cc_library(
    name = "testing_stream_prime",
    srcs = ["testing_stream_prime.cc"],
    hdrs = ["testing_stream_prime.h"],
    deps = [
        ":executor",
        ":stream",
        ":stream_qtype",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/iterables:iterable_qtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/sequence",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

proto_library(
    name = "execution_config_proto",
    srcs = ["execution_config.proto"],
)

py_proto_library(
    name = "execution_config_py_proto",
    deps = [
        ":execution_config_proto",
    ],
)

cc_proto_library(
    name = "execution_config_cc_proto",
    deps = [
        ":execution_config_proto",
    ],
)
