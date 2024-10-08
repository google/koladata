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

# Arolla expression tools for Koda.

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "expr_operators",
    srcs = ["expr_operators.cc"],
    hdrs = ["expr_operators.h"],
    tags = ["keep_dep"],
    deps = [
        "//koladata:data_slice_qtype",
        "//koladata/internal:ellipsis",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators:bootstrap",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "expr_operators_repr",
    srcs = ["expr_operators_repr.cc"],
    tags = ["keep_dep"],
    deps = [
        ":expr_operators",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "expr_eval",
    srcs = ["expr_eval.cc"],
    hdrs = ["expr_eval.h"],
    deps = [
        ":expr_operators",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/io",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/serving",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "constants",
    srcs = ["constants.cc"],
    hdrs = ["constants.h"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:object_factories",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "constants_test",
    srcs = ["constants_test.cc"],
    deps = [
        ":constants",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "expr_eval_test",
    srcs = ["expr_eval_test.cc"],
    deps = [
        ":expr_eval",
        ":expr_operators",
        "//koladata/operators:compile_expr_operators",
        "//koladata/testing:test_env",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/qexpr/operators/all",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)
