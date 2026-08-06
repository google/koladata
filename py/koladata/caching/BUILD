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

# Caching utilities.

load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_pybind_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

koladata_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_utils",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/synchronization",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:pybind11_utils",
        "@com_google_arolla//py/arolla/py_utils",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
    ],
)

py_test(
    name = "clib_test",
    srcs = ["clib_test.py"],
    deps = [
        ":clib",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_library(
    name = "lru_cache",
    srcs = ["lru_cache.py"],
    deps = [
        ":clib",
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:tracing_mode",
        "//py/koladata/types:data_slice",
    ],
)

py_test(
    name = "lru_cache_test",
    srcs = ["lru_cache_test.py"],
    deps = [
        ":lru_cache",
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)
