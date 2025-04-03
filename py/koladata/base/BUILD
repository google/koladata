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

# Base utilities shared between different parts of Koda.

load("@rules_python//python:defs.bzl", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_pybind_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "to_py_object",
    srcs = ["to_py_object.cc"],
    hdrs = ["to_py_object.h"],
    deps = [
        ":wrap_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:missing_value",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

koladata_pybind_extension(
    name = "to_py_object_testing_clib",
    testonly = True,
    srcs = ["to_py_object_testing_clib.cc"],
    deps = [
        ":to_py_object",
        "//koladata:data_slice",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//py/arolla/abc:pybind11_utils",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

py_test(
    name = "to_py_object_test",
    srcs = ["to_py_object_test.py"],
    deps = [
        ":to_py_object_testing_clib",
        "//py/koladata/functions",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

cc_library(
    name = "wrap_utils",
    srcs = ["wrap_utils.cc"],
    hdrs = ["wrap_utils.h"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/jagged_shape/array/qtype",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qtype",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

koladata_pybind_extension(
    name = "wrap_utils_testing_clib",
    testonly = True,
    srcs = ["wrap_utils_testing_clib.cc"],
    deps = [
        ":wrap_utils",
        "//koladata:data_slice_qtype",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qtype",
        "@com_google_arolla//py/arolla/abc:pybind11_utils",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
    ],
)

py_test(
    name = "wrap_utils_test",
    srcs = ["wrap_utils_test.py"],
    deps = [
        ":wrap_utils",
        ":wrap_utils_testing_clib",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)
