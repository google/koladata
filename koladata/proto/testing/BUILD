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

# Testing utilities for koladata.

load("@com_google_protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

proto_library(
    name = "test_proto2_proto",
    srcs = ["test_proto2.proto"],
)

cc_proto_library(
    name = "test_proto2_cc_proto",
    deps = [":test_proto2_proto"],
)

proto_library(
    name = "test_proto3_proto",
    srcs = ["test_proto3.proto"],
)

cc_proto_library(
    name = "test_proto3_cc_proto",
    deps = [":test_proto3_proto"],
)
