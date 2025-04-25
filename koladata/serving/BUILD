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

# Serving tools for Koda.

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

# Standard dependencies for evaluating Koda functors from C++.
cc_library(
    name = "standard_deps",
    visibility = ["//visibility:public"],
    deps = [
        "//koladata:all_qexpr_operators",
        "//koladata/s11n",
        "//py/koladata/operators:cc_operators",
        "@com_google_arolla//arolla/expr/operators:bootstrap",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qexpr",
        "@com_google_arolla//arolla/qexpr/operators/all:operators_on_dense_arrays",
        "@com_google_arolla//arolla/serialization_codecs/dense_array/decoders",
        "@com_google_arolla//arolla/serialization_codecs/generic/decoders",
    ],
    alwayslink = 1,
)

cc_library(
    name = "slice_registry",
    srcs = ["slice_registry.cc"],
    hdrs = ["slice_registry.h"],
    visibility = ["//visibility:public"],
    deps = [
        "//koladata:data_slice",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_test(
    name = "slice_registry_test",
    srcs = ["slice_registry_test.cc"],
    deps = [
        ":slice_registry",
        "//koladata:test_utils",
        "//koladata/testing:matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)
