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

# C++-only parts of Koda Contrib. See the rest in //py/koladata/ext/contrib.

load("@rules_cc//cc:cc_library.bzl", "cc_library")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

cc_library(
    name = "operators",
    srcs = ["operators.cc"],
    visibility = ["//py/koladata/ext/contrib:__pkg__"],
    deps = [
        ":flatten_cyclic_references",
        "//koladata:data_slice_qtype",
        "//koladata/internal/op_utils:qexpr",
    ],
    alwayslink = 1,
)

cc_library(
    name = "flatten_cyclic_references",
    srcs = ["flatten_cyclic_references.cc"],
    hdrs = ["flatten_cyclic_references.h"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:non_deterministic_token",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/internal/op_utils:traverse_helper",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/log",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/jagged_shape/dense_array/util",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)
