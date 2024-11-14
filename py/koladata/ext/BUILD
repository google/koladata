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

load("@rules_python//python:defs.bzl", "py_library", "py_test")

# Tools for working with external libraries, such as numpy, pandas, etc.

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "npkd",
    srcs = ["npkd.py"],
    visibility = ["//visibility:public"],
    deps = [
        "//py/koladata:kd",
        "//py/koladata/types:data_slice",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/experimental",
    ],
)

py_test(
    name = "npkd_test",
    srcs = ["npkd_test.py"],
    deps = [
        ":npkd",
        "//py/koladata:kd",
        "//py/koladata/testing",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "pdkd",
    srcs = ["pdkd.py"],
    visibility = ["//visibility:public"],
    deps = [
        ":npkd",
        "//py/koladata:kd",
        "//py/koladata/types:data_slice",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "pdkd_test",
    srcs = ["pdkd_test.py"],
    tags = ["nooss"],
    deps = [
        ":pdkd",
        "//py/koladata:kd",
        "//py/koladata/testing",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "kd_ext",
    srcs = ["kd_ext.py"],
    visibility = ["//visibility:public"],
    deps = [
        ":nested_data",
        ":npkd",
        ":pdkd",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "kd_ext_test",
    srcs = ["kd_ext_test.py"],
    tags = ["nooss"],
    deps = [
        ":kd_ext",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "functools",
    srcs = ["functools.py"],
    visibility = ["//visibility:public"],
    deps = [
        "//py/koladata:kd",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "functools_test",
    srcs = ["functools_test.py"],
    deps = [
        ":functools",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "nested_data",
    srcs = ["nested_data.py"],
    visibility = ["//visibility:public"],
    deps = [
        ":functools",
        "//py/koladata:kd",
    ],
)

py_test(
    name = "nested_data_test",
    srcs = ["nested_data_test.py"],
    deps = [
        ":nested_data",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)
