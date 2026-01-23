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

# Utlities for Koda Expr(s).

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_py_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "input_container",
    srcs = ["input_container.py"],
    deps = [
        "//koladata/expr:init",
        "//py:python_path",
        "//py/koladata/types:literal_operator",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "input_container_test",
    srcs = ["input_container_test.py"],
    deps = [
        ":expr_eval",
        ":input_container",
        ":view",
        "//py:python_path",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "view",
    srcs = ["view.py"],
    deps = [
        ":expr_eval",
        ":input_container",
        ":introspection",
        ":source_location",
        ":tracing_mode",
        "//py:python_path",
        "//py/koladata/fstring",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "//py/koladata/util:kd_functools",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "expr_eval",
    srcs = ["expr_eval.py"],
    deps = [
        ":input_container",
        ":py_expr_eval_py_ext",
        "//py:python_path",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "introspection",
    srcs = ["introspection.py"],
    deps = [
        ":input_container",
        "//py:python_path",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "tracing_mode",
    srcs = ["tracing_mode.py"],
    deps = ["//py:python_path"],
)

py_test(
    name = "tracing_mode_test",
    srcs = ["tracing_mode_test.py"],
    deps = [
        ":tracing_mode",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "tracing",
    srcs = ["tracing.py"],
    deps = [
        ":input_container",
        ":introspection",
        ":tracing_mode",
        "//py:python_path",
        "//py/koladata/types:extension_type_registry",
        "//py/koladata/types:py_boxing",
        "//py/koladata/util:kd_functools",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "source_location",
    srcs = ["source_location.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/util:kd_functools",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "source_location_test",
    srcs = ["source_location_test.py"],
    deps = [
        ":input_container",
        ":source_location",
        "//py:python_path",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/util:kd_functools",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "tracing_test",
    srcs = ["tracing_test.py"],
    deps = [
        ":tracing",
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/extension_types",
        "//py/koladata/functions:parallel",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/functor:tracing_decorator",
        "//py/koladata/testing",
        "//py/koladata/types:extension_type_registry",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "tracing_with_future_annotations_test",
    srcs = ["tracing_with_future_annotations_test.py"],
    deps = [
        ":tracing",
        "//py:python_path",
        "//py/koladata/extension_types",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "introspection_test",
    srcs = ["introspection_test.py"],
    deps = [
        ":input_container",
        ":introspection",
        "//py:python_path",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "view_test",
    srcs = ["view_test.py"],
    deps = [
        ":input_container",
        ":introspection",
        ":tracing_mode",
        ":view",
        "//py:python_path",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:view_overloads",
        "//py/koladata/testing",
        "//py/koladata/testing:signature_test_utils",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:ellipsis",
        "//py/koladata/types:jagged_shape",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "expr_eval_test",
    srcs = ["expr_eval_test.py"],
    deps = [
        ":expr_eval",
        ":input_container",
        ":py_expr_eval_py_ext",
        ":view",
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:ellipsis",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "eval_op_test",
    srcs = ["eval_op_test.py"],
    deps = [
        ":input_container",
        ":py_expr_eval_py_ext",
        "//py:python_path",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

cc_library(
    name = "py_expr_eval_cc_lib",
    srcs = ["py_expr_eval.cc"],
    hdrs = ["py_expr_eval.h"],
    deps = [
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/expr:constants",
        "//koladata/expr:expr_eval",
        "//koladata/expr:non_determinism",
        "//koladata/functor/parallel:get_default_executor",
        "//koladata/internal:non_deterministic_token",
        "//koladata/operators:lib",
        "//py/koladata/base:py_args",
        "//py/koladata/base:wrap_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

koladata_py_extension(
    name = "py_expr_eval_py_ext",
    srcs = ["py_expr_eval_module.cc"],
    deps = [
        ":py_expr_eval_cc_lib",
        "//koladata/expr:non_determinism",
        "@com_google_arolla//py/arolla/py_utils",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)
