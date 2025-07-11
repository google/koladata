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

# Python tools for working with Koda functors.

load("@pip_deps//:requirements.bzl", "requirement")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_py_extension", "koladata_pybind_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "expr_container",
    srcs = ["expr_container.py"],
    visibility = ["//visibility:public"],
    deps = [
        ":tracing_decorator",
        "//py:python_path",
        "//py/koladata/expr:tracing_mode",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
    ],
)

py_test(
    name = "expr_container_test",
    srcs = ["expr_container_test.py"],
    deps = [
        ":expr_container",
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/expr:introspection",
        "//py/koladata/expr:tracing_mode",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

cc_library(
    name = "py_functors_cc_lib",
    srcs = ["py_functors.cc"],
    hdrs = ["py_functors.h"],
    deps = [
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/functor:auto_variables",
        "//py/koladata/base:py_args",
        "//py/koladata/base:wrap_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

cc_library(
    name = "error_converters",
    srcs = ["error_converters.cc"],
    deps = [
        "//koladata/functor:stack_trace",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/py_utils",
        "@com_google_arolla//py/arolla/py_utils:error_converter_registry",
        "@rules_python//python/cc:current_py_cc_headers",
    ],
    alwayslink = True,
)

py_test(
    name = "error_converters_test",
    srcs = ["error_converters_test.py"],
    deps = [
        ":stack_trace",
        ":testing_clib",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

koladata_pybind_extension(
    name = "testing_clib",
    testonly = True,
    srcs = ["testing_clib.cc"],
    deps = [
        ":error_converters",  # Register error converters when run outside of google environment.
        "//koladata:data_slice",
        "//koladata/functor:stack_trace",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//py/arolla/abc:pybind11_utils",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

koladata_py_extension(
    name = "py_functors_py_ext",
    srcs = ["py_functors_module.cc"],
    deps = [
        ":error_converters",
        ":py_functors_cc_lib",
        "//koladata:data_slice",
        "@com_google_arolla//py/arolla/py_utils",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

py_library(
    name = "functions",
    srcs = ["functions.py"],
    deps = [
        ":functor_factories",
        ":tracing_decorator",
        "//py:python_path",
        "//py/koladata/types:signature_utils",
    ],
)

py_library(
    name = "functor_factories",
    srcs = ["functor_factories.py"],
    deps = [
        ":py_functors_py_ext",
        ":stack_trace",
        "//koladata/expr:expr_operators",
        "//py:python_path",
        "//py/koladata/base:py_functors_base_py_ext",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/expr:tracing",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:signature_utils",
        "//py/koladata/util:kd_functools",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "functor_factories_test",
    srcs = ["functor_factories_test.py"],
    deps = [
        ":boxing",
        ":functor_factories",
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:schema_constants",
        "//py/koladata/types:signature_utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "auto_variables_test",
    srcs = ["auto_variables_test.py"],
    deps = [
        ":boxing",
        ":functor_factories",
        ":py_functors_py_ext",
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/expr:source_location",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "tracing_decorator",
    srcs = ["tracing_decorator.py"],
    deps = [
        ":functor_factories",
        ":stack_trace",
        "//py:python_path",
        "//py/koladata/expr:tracing_mode",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/util:kd_functools",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "boxing",
    srcs = ["boxing.py"],
    deps = [
        ":functor_factories",
        "//py:python_path",
        "//py/koladata/types:py_boxing",
    ],
)

py_library(
    name = "stack_trace",
    srcs = ["stack_trace.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/types:data_slice",
        "//py/koladata/util:kd_functools",
    ],
)

py_test(
    name = "stack_trace_test",
    srcs = ["stack_trace_test.py"],
    deps = [
        ":stack_trace",
        ":testing_clib",
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/testing",
        "//py/koladata/util:kd_functools",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "boxing_test",
    srcs = ["boxing_test.py"],
    deps = [
        ":boxing",
        ":functor_factories",
        ":tracing_decorator",
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "tracing_decorator_test",
    srcs = ["tracing_decorator_test.py"],
    deps = [
        ":boxing",
        ":functor_factories",
        ":tracing_decorator",
        "//py:python_path",
        requirement("ipython"),
        "//py/koladata:kd",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/functions",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)
