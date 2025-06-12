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

# Small utility libraries that are not specific to Koda.

load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "kd_functools",
    srcs = ["kd_functools.py"],
    deps = ["//py:python_path"],
)

py_test(
    name = "kd_functools_test",
    srcs = ["kd_functools_test.py"],
    deps = [
        ":kd_functools",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)
