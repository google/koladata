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

# C++-only parts of Koda Ids. See the rest in //py/koladata/ext/ids.

load("@rules_cc//cc:cc_library.bzl", "cc_library")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

cc_library(
    name = "operators",
    srcs = ["operators.cc"],
    visibility = [
        "//py/koladata/ext:__pkg__",
        "//py/koladata/ext/ids:__pkg__",
    ],
    deps = [
        ":auto_id_cleanup_update",
        ":auto_id_update",
        ":auto_reference_update",
        "//koladata:data_slice_qtype",
        "//koladata/internal/op_utils:qexpr",
    ],
    alwayslink = 1,
)

cc_library(
    name = "auto_id_update",
    srcs = ["auto_id_update.cc"],
    hdrs = ["auto_id_update.h"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal/op_utils:auto_values_update",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "auto_reference_update",
    srcs = ["auto_reference_update.cc"],
    hdrs = ["auto_reference_update.h"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal/op_utils:auto_values_update",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "auto_id_cleanup_update",
    srcs = ["auto_id_cleanup_update.cc"],
    hdrs = ["auto_id_cleanup_update.h"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata/internal/op_utils:auto_values_update",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)
