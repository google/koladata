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

load("@pip_deps//:requirements.bzl", "requirement")
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
        "//py:python_path",
        "//py/koladata:kd",
        requirement("numpy"),
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/experimental",
    ],
)

py_test(
    name = "npkd_test",
    srcs = ["npkd_test.py"],
    deps = [
        ":npkd",
        "//py:python_path",
        "//py/koladata:kd",
        requirement("numpy"),
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
        "//py:python_path",
        "//py/koladata:kd",
        requirement("pandas"),
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "pdkd_test",
    srcs = ["pdkd_test.py"],
    deps = [
        ":pdkd",
        "//py:python_path",
        "//py/koladata:kd",
        requirement("numpy"),
        requirement("pandas"),
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "py_cloudpickle",
    srcs = ["py_cloudpickle.py"],
    deps = [
        "//py:python_path",
        requirement("cloudpickle"),
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "py_cloudpickle_test",
    srcs = ["py_cloudpickle_test.py"],
    deps = [
        ":py_cloudpickle",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "functools",
    srcs = ["functools.py"],
    visibility = ["//visibility:public"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "functools_test",
    srcs = ["functools_test.py"],
    deps = [
        ":functools",
        "//py:python_path",
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
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

py_test(
    name = "nested_data_test",
    srcs = ["nested_data_test.py"],
    deps = [
        ":nested_data",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "vis",
    srcs = ["vis.py"],
    data = [
        "//ts:data_slice_webcomponents",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//py:python_path",
        requirement("ipython"),
        "//py/koladata:kd",
        "@com_google_arolla//py/arolla",
        "@rules_python//python/runfiles",
    ],
)

py_test(
    name = "vis_test",
    srcs = ["vis_test.py"],
    deps = [
        ":vis",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "benchmarks",
    srcs = ["benchmarks.py"],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        ":nested_data",
        "//py:python_path",
        "//py/koladata:kd",
        requirement("google-benchmark"),
        # Enables python functions in profile.
        # Enables --cpu_profile.
        # Enables --peakheap_profile.
        # Memory usage counters.
    ],
)
