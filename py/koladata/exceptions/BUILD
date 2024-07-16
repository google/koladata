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

load("//py/koladata:py_extension.bzl", "py_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_extension(
    name = "py_exceptions_py_ext",
    srcs = ["py_exceptions_module.cc"],
    deps = [":py_exception_utils"],
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
        "@com_google_arolla//py/arolla/py_utils",
    ],
)
