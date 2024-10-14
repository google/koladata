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
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("@rules_python//python:proto.bzl", "py_proto_library")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_py_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

koladata_py_extension(
    name = "py_exceptions_py_ext",
    srcs = ["py_exceptions_module.cc"],
    deps = [":py_exception_utils"],
)

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
    deps = [
        ":error_py_proto",
        ":py_exceptions_py_ext",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
    ],
)

py_test(
    name = "exceptions_test",
    srcs = ["exceptions_test.py"],
    deps = [
        ":error_py_proto",
        ":exceptions",
        "//py/koladata/exceptions/testing:testing_pybind",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "exceptions_not_registered_test",
    srcs = ["exceptions_not_registered_test.py"],
    deps = [
        ":error_py_proto",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "//py/koladata/exceptions/testing:testing_pybind",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

cc_library(
    name = "py_exception_utils",
    srcs = ["py_exception_utils.cc"],
    hdrs = ["py_exception_utils.h"],
    deps = [
        "//koladata:data_slice_qtype",
        "//koladata/internal:error_cc_proto",
        "//koladata/internal:error_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//py/arolla/py_utils",
        "@com_google_arolla//py/arolla/py_utils:status_payload_handler_registry",
    ],
)
