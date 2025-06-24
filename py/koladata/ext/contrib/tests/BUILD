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

# Tests for external contributions.

load("@rules_python//python:defs.bzl", "py_test")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_test(
    name = "value_counts_test",
    srcs = ["value_counts_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata:kd_ext",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)
