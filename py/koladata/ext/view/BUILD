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

# A lightweight library with Koda syntax for working with Python objects.

load("@pip_deps//:requirements.bzl", "requirement")
load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_test(
    name = "benchmarks",
    srcs = ["benchmarks.py"],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        "//py:python_path",
        "//py/koladata:kd_ext",
        requirement("google-benchmark"),
    ],
)

py_test(
    name = "view_test",
    srcs = ["view_test.py"],
    deps = [
        ":view",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "kv_test",
    srcs = ["kv_test.py"],
    deps = [
        ":kv",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "view",
    srcs = ["view.py"],
    deps = [
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "kv",
    srcs = ["kv.py"],
    deps = [
        ":view",
        "//py:python_path",
    ],
)
