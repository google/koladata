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

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "types",
    srcs = ["types.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/ext/tqdm/proto:progress_py_pb2",
    ],
)

py_library(
    name = "context",
    srcs = ["context.py"],
    deps = [
        ":types",
        "//py:python_path",
    ],
)

py_library(
    name = "tqdm",
    srcs = ["tqdm.py"],
    deps = [
        ":context",
        ":types",
        "//py:python_path",
        requirement("tqdm"),
    ],
)

py_library(
    name = "kd_ext_tqdm",
    srcs = ["kd_ext_tqdm.py"],
    visibility = ["//visibility:public"],
    deps = [
        ":context",
        ":tqdm",
        ":types",
        "//py:python_path",
        "//py/koladata/ext/tqdm/proto:progress_py_pb2",
    ],
)

py_test(
    name = "tqdm_test",
    srcs = ["tqdm_test.py"],
    deps = [
        ":context",
        ":tqdm",
        ":types",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "context_test",
    srcs = ["context_test.py"],
    deps = [
        ":context",
        ":types",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "types_test",
    srcs = ["types_test.py"],
    deps = [
        ":types",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)
