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

load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("@rules_python//python:proto.bzl", "py_proto_library")

# Tools for working with external libraries, such as numpy, pandas, etc.

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "persisted_data",
    srcs = ["persisted_data.py"],
    deps = [
        ":file_system_interaction",
        ":persisted_incremental_data_bag_manager",
    ],
)

py_test(
    name = "persisted_data_test",
    srcs = ["persisted_data_test.py"],
    deps = [
        ":persisted_data",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "persisted_incremental_data_bag_manager",
    srcs = ["persisted_incremental_data_bag_manager.py"],
    deps = [
        ":file_system_interaction",
        ":persisted_incremental_data_bag_manager_metadata_py_pb2",
        "//py/koladata:kd",
    ],
)

py_test(
    name = "persisted_incremental_data_bag_manager_test",
    srcs = ["persisted_incremental_data_bag_manager_test.py"],
    deps = [
        ":file_system_interaction",
        ":persisted_incremental_data_bag_manager",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

proto_library(
    name = "persisted_incremental_data_bag_manager_metadata_proto",
    srcs = ["persisted_incremental_data_bag_manager_metadata.proto"],
)

py_proto_library(
    name = "persisted_incremental_data_bag_manager_metadata_py_pb2",
    deps = [
        ":persisted_incremental_data_bag_manager_metadata_proto",
    ],
)

py_library(
    name = "file_system_interaction",
    srcs = ["file_system_interaction.py"],
    deps = [
        "//py/koladata:kd",
    ],
)

py_test(
    name = "file_system_interaction_test",
    srcs = ["file_system_interaction_test.py"],
    deps = [
        ":file_system_interaction",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)
