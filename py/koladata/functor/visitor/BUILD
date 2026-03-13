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
    name = "visitor",
    srcs = ["visitor.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/types:data_item",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "visitor_test",
    srcs = ["visitor_test.py"],
    deps = [
        ":visitor",
        "//py:python_path",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/functions",
        "//py/koladata/functions:predicates",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/functor:tracing_decorator",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)
