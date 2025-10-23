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

# Tests for operators in "kv" module.
# The implementations for the operators tested here are either in view.py or in operators.py.

load("@rules_python//python:defs.bzl", "py_test")

licenses(["notice"])

py_test(
    name = "align_test",
    srcs = ["align_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/ext/view:kv",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "view_test",
    srcs = ["view_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/ext/view:kv",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "map_test",
    srcs = ["map_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/ext/view:kv",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)
