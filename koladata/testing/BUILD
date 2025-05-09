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

# Testing utilities for koladata.

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "matchers",
    testonly = True,
    hdrs = ["matchers.h"],
    deps = [
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/internal/testing:matchers",
        "@com_google_arolla//arolla/jagged_shape/testing",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "matchers_test",
    srcs = ["matchers_test.cc"],
    deps = [
        ":matchers",
        "//koladata:data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "test_env",
    testonly = True,
    deps = ["@com_google_arolla//arolla/util/testing"],
    alwayslink = True,
)

cc_library(
    name = "eval_python_declared_ops_in_cpp_tests",
    testonly = True,
    deps = [
        ":test_env",
        "//koladata:all_qexpr_operators",
        "//koladata/s11n",
        "//py/koladata/operators:base_cc_operators",
        "//py/koladata/operators:extra_cc_operators",
        "@com_google_arolla//arolla/qexpr/operators/all",
    ],
)
