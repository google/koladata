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

# Tests for Koda functions.

load("@com_google_protobuf//bazel:cc_proto_library.bzl", "cc_proto_library")
load("@com_google_protobuf//bazel:proto_library.bzl", "proto_library")
load("@com_google_protobuf//bazel:py_proto_library.bzl", "py_proto_library")
load("@rules_python//python:defs.bzl", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_py_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_test(
    name = "bag_test",
    srcs = ["bag_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "list_test",
    srcs = ["list_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:list_item",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "is_expr_test",
    srcs = ["is_expr_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "is_item_test",
    srcs = ["is_item_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "is_slice_test",
    srcs = ["is_slice_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "list_like_test",
    srcs = ["list_like_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:list_item",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "list_shaped_test",
    srcs = ["list_shaped_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:list_item",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "list_shaped_as_test",
    srcs = ["list_shaped_as_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:list_item",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "implode_test",
    srcs = ["implode_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "concat_lists_test",
    srcs = ["concat_lists_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "dict_test",
    srcs = ["dict_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dict_like_test",
    srcs = ["dict_like_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dict_shaped_test",
    srcs = ["dict_shaped_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_test(
    name = "dict_shaped_as_test",
    srcs = ["dict_shaped_as_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "schema_from_py_test",
    srcs = ["schema_from_py_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "new_test",
    srcs = ["new_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:list_item",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "new_shaped_test",
    srcs = ["new_shaped_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:list_item",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "new_shaped_as_test",
    srcs = ["new_shaped_as_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "new_like_test",
    srcs = ["new_like_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:list_item",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "obj_test",
    srcs = ["obj_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:list_item",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "obj_shaped_test",
    srcs = ["obj_shaped_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:list_item",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "obj_shaped_as_test",
    srcs = ["obj_shaped_as_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "obj_like_test",
    srcs = ["obj_like_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:list_item",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "uu_test",
    srcs = ["uu_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "uuobj_test",
    srcs = ["uuobj_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "set_attr_test",
    srcs = ["set_attr_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:comparison",
        "//py/koladata/operators:core",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "set_attrs_test",
    srcs = ["set_attrs_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "del_attr_test",
    srcs = ["del_attr_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "embed_schema_test",
    srcs = ["embed_schema_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "set_schema_test",
    srcs = ["set_schema_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "from_proto_test",
    srcs = ["from_proto_test.py"],
    deps = [
        ":test_py_pb2",
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "from_py_test",
    srcs = ["from_py_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "to_pylist_test",
    srcs = ["to_pylist_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "to_py_test",
    srcs = ["to_py_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/base/py_conversions:dataclasses_py_util",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "container_test",
    srcs = ["container_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "update_schema_test",
    srcs = ["update_schema_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "s11n_test",
    srcs = ["s11n_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/functions:s11n",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "int32_test",
    srcs = ["int32_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "int64_test",
    srcs = ["int64_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "float32_test",
    srcs = ["float32_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "float64_test",
    srcs = ["float64_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "str_test",
    srcs = ["str_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "bytes_test",
    srcs = ["bytes_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "bool_test",
    srcs = ["bool_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "mask_test",
    srcs = ["mask_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "expr_quote_test",
    srcs = ["expr_quote_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "to_proto_test",
    srcs = ["to_proto_test.py"],
    deps = [
        ":test_py_pb2",
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "dir_test",
    srcs = ["dir_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "get_attr_names_test",
    srcs = ["get_attr_names_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "slice_test",
    srcs = ["slice_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "item_test",
    srcs = ["item_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "schema_from_proto_test",
    srcs = ["schema_from_proto_test.py"],
    deps = [
        ":test_py_pb2",
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/testing",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "call_multithreaded_test",
    srcs = ["call_multithreaded_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/expr:input_container",
        "//py/koladata/functions",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/functor:tracing_decorator",
        "//py/koladata/operators:functor",
        "//py/koladata/operators:iterables",
        "//py/koladata/operators:tuple",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:signature_utils",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "tuples_get_nth_test",
    srcs = ["tuples_get_nth_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "tuples_get_namedtuple_field_test",
    srcs = ["tuples_get_namedtuple_field_test.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

proto_library(
    name = "test_proto",
    srcs = ["test.proto"],
)

py_proto_library(
    name = "test_py_pb2",
    deps = [":test_proto"],
)

cc_proto_library(
    name = "test_cc_proto",
    deps = [":test_proto"],
)

# Contains static C++ references for the messages in test.proto to force them
# to be registered in the C++ generated descriptor pool.
koladata_py_extension(
    name = "test_cc_proto_py_ext",
    testonly = 1,
    srcs = ["test_cc_proto_module.cc"],
    deps = [
        ":test_cc_proto",
        "//py/koladata/base:py_utils",  # buildcleaner: keep
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)
