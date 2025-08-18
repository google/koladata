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

load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("@com_google_protobuf//bazel:py_proto_library.bzl", "py_proto_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")

# Tools for working with external libraries, such as numpy, pandas, etc.

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "persisted_data",
    srcs = ["persisted_data.py"],
    deps = [
        ":fs_implementation",
        ":fs_interface",
        ":fs_util",
        ":persisted_incremental_data_bag_manager",
        "//py:python_path",
    ],
)

py_test(
    name = "persisted_data_test",
    srcs = ["persisted_data_test.py"],
    deps = [
        ":persisted_data",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "persisted_incremental_data_slice_manager",
    srcs = ["persisted_incremental_data_slice_manager.py"],
    deps = [
        ":data_slice_manager_interface",
        ":data_slice_path",
        ":fs_interface",
        ":fs_util",
        ":persisted_incremental_data_bag_manager",
        ":persisted_incremental_data_slice_manager_metadata_py_pb2",
        ":schema_helper",
        ":stubs_and_minimal_bags_lib",
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

py_test(
    name = "persisted_incremental_data_slice_manager_test",
    srcs = ["persisted_incremental_data_slice_manager_test.py"],
    deps = [
        ":data_slice_manager_interface",
        ":data_slice_path",
        ":persisted_incremental_data_slice_manager",
        ":schema_helper",
        ":simple_in_memory_data_slice_manager",
        ":test_only_schema_node_name_helper",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

proto_library(
    name = "persisted_incremental_data_slice_manager_metadata_proto",
    srcs = ["persisted_incremental_data_slice_manager_metadata.proto"],
)

py_proto_library(
    name = "persisted_incremental_data_slice_manager_metadata_py_pb2",
    deps = [
        ":persisted_incremental_data_slice_manager_metadata_proto",
    ],
)

py_library(
    name = "persisted_incremental_data_bag_manager",
    srcs = ["persisted_incremental_data_bag_manager.py"],
    deps = [
        ":fs_interface",
        ":fs_util",
        ":persisted_incremental_data_bag_manager_metadata_py_pb2",
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

py_test(
    name = "persisted_incremental_data_bag_manager_test",
    srcs = ["persisted_incremental_data_bag_manager_test.py"],
    deps = [
        ":fs_implementation",
        ":persisted_incremental_data_bag_manager",
        "//py:python_path",
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
    name = "fs_interface",
    srcs = ["fs_interface.py"],
    deps = ["//py:python_path"],
)

py_library(
    name = "fs_implementation",
    srcs = ["fs_implementation.py"],
    deps = [
        ":fs_interface",
        "//py:python_path",
    ],
)

py_library(
    name = "fs_util",
    srcs = ["fs_util.py"],
    deps = [
        ":fs_implementation",
        ":fs_interface",
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

py_test(
    name = "fs_util_test",
    srcs = ["fs_util_test.py"],
    deps = [
        ":fs_util",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "schema_helper",
    srcs = ["schema_helper.py"],
    deps = [
        ":data_slice_path",
        ":stubs_and_minimal_bags_lib",
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

py_test(
    name = "schema_helper_test",
    srcs = ["schema_helper_test.py"],
    deps = [
        ":data_slice_path",
        ":schema_helper",
        ":test_only_schema_node_name_helper",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "data_slice_path",
    srcs = ["data_slice_path.py"],
    deps = [
        ":stubs_and_minimal_bags_lib",
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

py_test(
    name = "data_slice_path_test",
    srcs = ["data_slice_path_test.py"],
    deps = [
        ":data_slice_path",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "test_only_schema_node_name_helper",
    testonly = True,
    srcs = ["test_only_schema_node_name_helper.py"],
    deps = [
        ":data_slice_path",
        ":schema_helper",
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

py_test(
    name = "test_only_schema_node_name_helper_test",
    srcs = ["test_only_schema_node_name_helper_test.py"],
    deps = [
        ":data_slice_path",
        ":schema_helper",
        ":test_only_schema_node_name_helper",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "value_presence_util",
    srcs = ["value_presence_util.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

py_test(
    name = "value_presence_util_test",
    srcs = ["value_presence_util_test.py"],
    deps = [
        ":value_presence_util",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "stubs_and_minimal_bags_lib",
    srcs = ["stubs_and_minimal_bags_lib.py"],
    deps = [
        ":value_presence_util",
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

py_test(
    name = "stubs_and_minimal_bags_lib_test",
    srcs = ["stubs_and_minimal_bags_lib_test.py"],
    deps = [
        ":stubs_and_minimal_bags_lib",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "data_slice_manager_interface",
    srcs = ["data_slice_manager_interface.py"],
    deps = [
        ":data_slice_path",
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

py_library(
    name = "simple_in_memory_data_slice_manager",
    testonly = True,
    srcs = ["simple_in_memory_data_slice_manager.py"],
    deps = [
        ":data_slice_manager_interface",
        ":data_slice_path",
        ":schema_helper",
        "//py:python_path",
        "//py/koladata:kd",
    ],
)
