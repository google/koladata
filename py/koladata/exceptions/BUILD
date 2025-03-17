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

# Custom C++/Python bindings for Koda errors.

load("@bazel_skylib//rules:copy_file.bzl", "copy_file")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("@rules_python//python:defs.bzl", "py_library")
load("@rules_python//python:proto.bzl", "py_proto_library")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

# NOTE: b/352697901 - We copy the proto file here to make in available in the OSS py package.
copy_file(
    name = "error_proto_copy",
    src = "//koladata/internal:error.proto",
    out = "error.proto",
)

proto_library(
    name = "error_proto",
    srcs = ["error.proto"],
    deps = [
        "@com_google_arolla//arolla/serialization_base:base_proto",
    ],
)

py_proto_library(
    name = "error_py_proto",
    deps = [":error_proto"],
)

py_library(
    name = "exceptions",
    srcs = ["exceptions.py"],
    deps = ["//py:python_path"],  # Adds //py to the path to allow convenient imports.
)
