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

load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "extension_types",
    srcs = ["extension_types.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/expr:view",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:extension_type_registry",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "//py/koladata/types:schema_item",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/derived_qtype",
        "@com_google_arolla//py/arolla/objects",
    ],
)

py_library(
    name = "functions",
    srcs = ["functions.py"],
    deps = [
        ":extension_types",
        "//py:python_path",
        "//py/koladata/types:extension_type_registry",
    ],
)

py_test(
    name = "extension_types_test",
    srcs = ["extension_types_test.py"],
    deps = [
        ":extension_types",
        "//py:python_path",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:extension_type_registry",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/derived_qtype",
        "@com_google_arolla//py/arolla/objects",
    ],
)
