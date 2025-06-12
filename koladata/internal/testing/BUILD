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

# Testing utilities for koladata/internal.

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
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:triples",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest",
    ],
)

cc_test(
    name = "matchers_test",
    srcs = ["matchers_test.cc"],
    deps = [
        ":matchers",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "deep_op_utils",
    testonly = True,
    hdrs = ["deep_op_utils.h"],
    deps = [
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_googletest//:gtest",
    ],
)
