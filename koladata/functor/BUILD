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

# Functor tools for Koda.

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "signature_utils",
    srcs = ["signature_utils.cc"],
    hdrs = ["signature_utils.h"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "functor",
    srcs = ["functor.cc"],
    hdrs = ["functor.h"],
    deps = [
        ":default_signature",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:object_factories",
        "//koladata/expr:expr_eval",
        "//koladata/internal:data_item",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "functor_test",
    srcs = ["functor_test.cc"],
    deps = [
        ":functor",
        ":signature_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:object_factories",
        "//koladata:test_utils",
        "//koladata/expr:constants",
        "//koladata/testing:matchers",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/qexpr/operators/all",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "auto_variables",
    srcs = ["auto_variables.cc"],
    hdrs = ["auto_variables.h"],
    deps = [
        ":functor",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "//koladata/expr:expr_operators",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/visitors",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "cpp_function_bridge",
    srcs = ["cpp_function_bridge.cc"],
    hdrs = ["cpp_function_bridge.h"],
    deps = [
        ":functor",
        ":signature_utils",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/expr:expr_operators",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators:bootstrap",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "cpp_function_bridge_test",
    srcs = ["cpp_function_bridge_test.cc"],
    deps = [
        ":call",
        ":cpp_function_bridge",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata/expr:expr_eval",
        "//koladata/functor/parallel:future",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/internal:non_deterministic_token",
        "//koladata/internal:object_id",
        "//koladata/s11n",
        "//koladata/testing:eval_python_declared_ops_in_cpp_tests",
        "//koladata/testing:matchers",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/qexpr/operators/all",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_arolla//arolla/serialization",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "call",
    srcs = ["call.cc"],
    hdrs = ["call.h"],
    deps = [
        ":signature_utils",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/expr:expr_eval",
        "//koladata/internal:data_item",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//arolla/util:traceme",
    ],
)

cc_library(
    name = "default_signature",
    srcs = ["default_signature.cc"],
    hdrs = ["default_signature.h"],
    deps = [
        ":signature_utils",
        "//koladata:data_slice",
        "//koladata/expr:constants",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "call_operator",
    srcs = ["call_operator.cc"],
    hdrs = ["call_operator.h"],
    deps = [
        ":call",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/internal:non_deterministic_token",
        "//koladata/operators:lib",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "with_assertion_operator",
    srcs = ["with_assertion_operator.cc"],
    hdrs = ["with_assertion_operator.h"],
    deps = [
        ":call",
        "//koladata:arolla_utils",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "expr_fn_operator",
    srcs = ["expr_fn_operator.cc"],
    hdrs = ["expr_fn_operator.h"],
    deps = [
        ":auto_variables",
        ":functor",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/expr:expr_operators",
        "//koladata/internal/op_utils:qexpr",
        "//koladata/operators:lib",
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
    name = "bind_operator",
    srcs = ["bind_operator.cc"],
    hdrs = ["bind_operator.h"],
    deps = [
        ":functor",
        ":signature_utils",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/expr:expr_operators",
        "//koladata/expr:non_determinism",
        "//koladata/internal/op_utils:qexpr",
        "//koladata/operators:lib",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "operators",
    srcs = ["operators.cc"],
    deps = [
        ":bind_operator",
        ":call_operator",
        ":expr_fn_operator",
        ":is_fn_operator",
        ":map_operator",
        ":while_operator",
        ":with_assertion_operator",
        "//koladata:data_slice_qtype",
        "@com_google_arolla//arolla/qexpr",
    ],
    alwayslink = 1,
)

cc_library(
    name = "map_operator",
    srcs = ["map_operator.cc"],
    hdrs = ["map_operator.h"],
    deps = [
        ":map",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/operators:lib",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "is_fn_operator",
    srcs = ["is_fn_operator.cc"],
    hdrs = ["is_fn_operator.h"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal/op_utils:presence_and",
        "//koladata/operators:lib",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "map",
    srcs = ["map.cc"],
    hdrs = ["map.h"],
    deps = [
        ":call",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/operators:lib",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "while_operator",
    srcs = ["while_operator.cc"],
    hdrs = ["while_operator.h"],
    deps = [
        ":while",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/iterables:iterable_qtype",
        "//koladata/operators:lib",
        "@com_google_absl//absl/log:check",
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

cc_library(
    name = "while",
    srcs = ["while.cc"],
    hdrs = ["while.h"],
    deps = [
        ":call",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/sequence",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "while_test",
    srcs = ["while_test.cc"],
    deps = [
        ":cpp_function_bridge",
        ":functor",
        ":signature_utils",
        ":while",
        "//koladata:data_slice",
        "//koladata:test_utils",
        "//koladata/internal:dtype",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/sequence",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "map_test",
    srcs = ["map_test.cc"],
    deps = [
        ":functor",
        ":map",
        ":signature_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal/testing:matchers",
        "//koladata/testing:matchers",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/jagged_shape/testing",
        "@com_google_arolla//arolla/qexpr/operators/all",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "default_signature_test",
    srcs = ["default_signature_test.cc"],
    deps = [
        ":default_signature",
        "//koladata:data_slice",
        "//koladata/expr:constants",
        "//koladata/testing:matchers",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "call_test",
    srcs = ["call_test.cc"],
    deps = [
        ":call",
        ":functor",
        ":signature_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:object_factories",
        "//koladata:test_utils",
        "//koladata/operators:lib",
        "//koladata/s11n",
        "//koladata/testing:matchers",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/qexpr/operators/all",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "signature_utils_test",
    srcs = ["signature_utils_test.cc"],
    deps = [
        ":signature_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/testing:matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)
