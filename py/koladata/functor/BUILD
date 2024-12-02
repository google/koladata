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

# Python tools for working with Koda functors.

load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_py_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "py_functors_cc_lib",
    srcs = ["py_functors.cc"],
    hdrs = ["py_functors.h"],
    deps = [
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/expr:expr_operators",
        "//koladata/functor",
        "//koladata/functor:signature_storage",
        "//py/koladata/types:py_utils",
        "//py/koladata/types:wrap_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

koladata_py_extension(
    name = "py_functors_py_ext",
    srcs = ["py_functors_module.cc"],
    deps = [":py_functors_cc_lib"],
)

py_library(
    name = "functions",
    srcs = ["functions.py"],
    deps = [
        ":functor_factories",
        ":signature_utils",
        ":tracing_decorator",
    ],
)

py_library(
    name = "functor_factories",
    srcs = ["functor_factories.py"],
    deps = [
        ":py_functors_py_ext",
        ":signature_utils",
        "//koladata/expr:expr_operators",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/expr:tracing",
        "//py/koladata/functions",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "functor_factories_test",
    srcs = ["functor_factories_test.py"],
    deps = [
        ":functor_factories",
        ":signature_utils",
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
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "signature_utils",
    srcs = ["signature_utils.py"],
    deps = [
        ":py_functors_py_ext",
        "//py/koladata/functions",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
    ],
)

py_library(
    name = "tracing_decorator",
    srcs = ["tracing_decorator.py"],
    deps = [
        ":functor_factories",
        "//py/koladata/expr:tracing_mode",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
    ],
)

py_test(
    name = "tracing_decorator_test",
    srcs = ["tracing_decorator_test.py"],
    deps = [
        ":functor_factories",
        ":tracing_decorator",
        "//py/koladata:kd",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "signature_utils_test",
    srcs = ["signature_utils_test.py"],
    deps = [
        ":signature_utils",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)
