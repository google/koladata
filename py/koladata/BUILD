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

# Koda, a library for advanced data manipulation.

load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(default_visibility = ["//py/koladata:__subpackages__"])

licenses(["notice"])

exports_files(["LICENSE"])

py_library(
    name = "koladata",
    visibility = ["//visibility:public"],
    deps = [
        ":kd",
        "//py:python_path",
    ],
)

py_library(
    name = "kd",
    srcs = ["kd.py"],
    tags = [
    ],
    visibility = ["//visibility:public"],
    deps = [
        ":kd_dynamic",
        ":kd_types",
        "//py:python_path",
        "//py/koladata/base:init",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/expr:source_location",
        "//py/koladata/expr:tracing_mode",
        "//py/koladata/extension_types",
        "//py/koladata/extension_types:functions",
        "//py/koladata/functions",
        "//py/koladata/functions:attrs",
        "//py/koladata/functions:object_factories",
        "//py/koladata/functions:parallel",
        "//py/koladata/functions:predicates",
        "//py/koladata/functions:proto_conversions",
        "//py/koladata/functions:py_conversions",
        "//py/koladata/functions:s11n",
        "//py/koladata/functions:schema",
        "//py/koladata/functor:boxing",
        "//py/koladata/functor:expr_container",
        "//py/koladata/functor:functions",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/functor:tracing_decorator",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators:qtype_utils",
        "//py/koladata/testing",
        "//py/koladata/type_checking",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
    ],
)

py_test(
    name = "kd_test",
    srcs = ["kd_test.py"],
    deps = [
        ":kd",
        "//py:python_path",
        "//py/koladata/expr:tracing_mode",
        "//py/koladata/functions",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:schema_constants",
        "//py/koladata/types:signature_utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "kd_ext",
    srcs = ["kd_ext.py"],
    visibility = ["//visibility:public"],
    deps = [
        ":kd",
        "//py:python_path",
        "//py/koladata/expr:source_location",
        "//py/koladata/expr:tracing_mode",
        "//py/koladata/ext:nested_data",
        "//py/koladata/ext:npkd",
        "//py/koladata/ext:pdkd",
        "//py/koladata/ext:py_cloudpickle",
        "//py/koladata/ext:vis",
        "//py/koladata/ext/contrib:functions",
        "//py/koladata/ext/operators:kde_operators",
        "//py/koladata/ext/persisted_data",
        "//py/koladata/ext/view:kv",
        "//py/koladata/operators:eager_op_utils",
    ],
)

py_library(
    name = "kd_dynamic",
    srcs = ["kd_dynamic.py"],
    deps = ["//py:python_path"],
)

py_library(
    name = "kd_types",
    srcs = ["kd_types.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functor/parallel:clib",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:iterable_qvalue",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:list_item",
        "//py/koladata/types:schema_item",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "kd_ext_test",
    srcs = ["kd_ext_test.py"],
    deps = [
        ":kd",
        ":kd_ext",
        "//py:python_path",
        "//py/koladata/ext:npkd",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)
