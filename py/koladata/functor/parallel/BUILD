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

# Parallel execution primitives for Kola.

load("@rules_python//python:defs.bzl", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_pybind_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

koladata_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    visibility = ["//koladata:internal"],
    deps = [
        ":py_executor",
        "//koladata/functor/parallel:init_expr_operators",
        "@com_google_arolla//py/arolla/abc:pybind11_utils",
    ],
)

cc_library(
    name = "py_executor",
    srcs = ["py_executor.cc"],
    hdrs = ["py_executor.h"],
    deps = [
        "//koladata/functor/parallel:executor",
        "@com_google_absl//absl/cleanup",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

py_test(
    name = "executor_test",
    size = "small",
    srcs = ["executor_test.py"],
    deps = [
        ":clib",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)
