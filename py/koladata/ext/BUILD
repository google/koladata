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
        "//py/koladata/types:data_slice",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/experimental",
    ],
)

py_test(
    name = "npkd_test",
    srcs = ["npkd_test.py"],
    tags = ["nooss"],
    deps = [
        ":npkd",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)
