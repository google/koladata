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

licenses(["notice"])

py_library(
    name = "call",
    srcs = ["call.py"],
    visibility = ["//py/koladata/ext:__pkg__"],
    deps = [
        ":task_repository",
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/operators:optools",
        "//py/koladata/types:py_boxing",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "call_test",
    srcs = ["call_test.py"],
    deps = [
        ":call",
        "//py/koladata:kd",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "task_repository",
    srcs = ["task_repository.py"],
)
