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

# Tests for Koda Expr operators.

load("@rules_python//python:defs.bzl", "py_test")
load(
    "//py/koladata/operators/tests/util:koda_operator_test_def.bzl",
    "koda_operator_coverage_test",
)

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

##### Tests for public operators #####

py_test(
    name = "allocation_new_itemid_test",
    srcs = ["allocation_new_itemid_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "allocation_new_itemid_shaped_test",
    srcs = ["allocation_new_itemid_shaped_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "allocation_new_itemid_like_test",
    srcs = ["allocation_new_itemid_like_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "allocation_new_itemid_shaped_as_test",
    srcs = ["allocation_new_itemid_shaped_as_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "allocation_new_listid_test",
    srcs = ["allocation_new_listid_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:list_item",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "allocation_new_listid_shaped_test",
    srcs = ["allocation_new_listid_shaped_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "allocation_new_listid_like_test",
    srcs = ["allocation_new_listid_like_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "allocation_new_listid_shaped_as_test",
    srcs = ["allocation_new_listid_shaped_as_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "allocation_new_dictid_test",
    srcs = ["allocation_new_dictid_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "allocation_new_dictid_shaped_test",
    srcs = ["allocation_new_dictid_shaped_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "allocation_new_dictid_like_test",
    srcs = ["allocation_new_dictid_like_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "allocation_new_dictid_shaped_as_test",
    srcs = ["allocation_new_dictid_shaped_as_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "bags_new_test",
    srcs = ["bags_new_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_size_test",
    srcs = ["dicts_size_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_dict_update_test",
    srcs = ["dicts_dict_update_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_with_dict_update_test",
    srcs = ["dicts_with_dict_update_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "lists_like_test",
    srcs = ["lists_like_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "lists_shaped_test",
    srcs = ["lists_shaped_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "lists_shaped_as_test",
    srcs = ["lists_shaped_as_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "lists_size_test",
    srcs = ["lists_size_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:list_item",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_is_entity_test",
    srcs = ["core_is_entity_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_has_entity_test",
    srcs = ["core_has_entity_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "lists_is_list_test",
    srcs = ["lists_is_list_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:list_item",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "lists_has_list_test",
    srcs = ["lists_has_list_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_is_dict_test",
    srcs = ["dicts_is_dict_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:dict_item",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_has_dict_test",
    srcs = ["dicts_has_dict_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_add_test",
    srcs = ["math_add_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_subtract_test",
    srcs = ["math_subtract_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_multiply_test",
    srcs = ["math_multiply_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_divide_test",
    srcs = ["math_divide_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_log_test",
    srcs = ["math_log_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_log10_test",
    srcs = ["math_log10_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_exp_test",
    srcs = ["math_exp_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_sigmoid_test",
    srcs = ["math_sigmoid_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_abs_test",
    srcs = ["math_abs_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_neg_test",
    srcs = ["math_neg_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_sign_test",
    srcs = ["math_sign_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_pos_test",
    srcs = ["math_pos_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_ceil_test",
    srcs = ["math_ceil_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_floor_test",
    srcs = ["math_floor_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_round_test",
    srcs = ["math_round_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_pow_test",
    srcs = ["math_pow_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_floordiv_test",
    srcs = ["math_floordiv_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_mod_test",
    srcs = ["math_mod_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_maximum_test",
    srcs = ["math_maximum_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_minimum_test",
    srcs = ["math_minimum_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_cum_max_test",
    srcs = ["math_cum_max_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_cum_min_test",
    srcs = ["math_cum_min_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_cum_sum_test",
    srcs = ["math_cum_sum_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_cdf_test",
    srcs = ["math_cdf_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_softmax_test",
    srcs = ["math_softmax_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_agg_inverse_cdf_test",
    srcs = ["math_agg_inverse_cdf_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_inverse_cdf_test",
    srcs = ["math_inverse_cdf_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_get_attr_test",
    srcs = ["core_get_attr_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_has_attr_test",
    srcs = ["core_has_attr_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_is_empty_test",
    srcs = ["slices_is_empty_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_maybe_test",
    srcs = ["core_maybe_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_attrs_test",
    srcs = ["core_attrs_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_attr_test",
    srcs = ["core_attr_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_with_attrs_test",
    srcs = ["core_with_attrs_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_with_attr_test",
    srcs = ["core_with_attr_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "entities_new_test",
    srcs = ["entities_new_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "entities_shaped_test",
    srcs = ["entities_shaped_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "entities_shaped_as_test",
    srcs = ["entities_shaped_as_test.py"],
    deps = [
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "objs_new_test",
    srcs = ["objs_new_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "objs_shaped_test",
    srcs = ["objs_shaped_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "objs_shaped_as_test",
    srcs = ["objs_shaped_as_test.py"],
    deps = [
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "objs_like_test",
    srcs = ["objs_like_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "entities_like_test",
    srcs = ["entities_like_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_new_test",
    srcs = ["dicts_new_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_shaped_test",
    srcs = ["dicts_shaped_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_shaped_as_test",
    srcs = ["dicts_shaped_as_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_like_test",
    srcs = ["dicts_like_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_get_item_test",
    srcs = ["core_get_item_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators:view_overloads",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_get_keys_test",
    srcs = ["dicts_get_keys_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_get_values_test",
    srcs = ["dicts_get_values_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_get_bag_test",
    srcs = ["core_get_bag_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_apply_mask_test",
    srcs = ["masking_apply_mask_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_mask_and_test",
    srcs = ["masking_mask_and_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_mask_or_test",
    srcs = ["masking_mask_or_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_mask_equal_test",
    srcs = ["masking_mask_equal_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_mask_not_equal_test",
    srcs = ["masking_mask_not_equal_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "functor_maybe_call_test",
    srcs = ["functor_maybe_call_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_select_test",
    srcs = ["slices_select_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_select_present_test",
    srcs = ["slices_select_present_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "lists_select_items_test",
    srcs = ["lists_select_items_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_select_keys_test",
    srcs = ["dicts_select_keys_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dicts_select_values_test",
    srcs = ["dicts_select_values_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_inverse_select_test",
    srcs = ["slices_inverse_select_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_reverse_test",
    srcs = ["slices_reverse_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_isin_test",
    srcs = ["slices_isin_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "ids_encode_decode_itemid_test",
    srcs = ["ids_encode_decode_itemid_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_coalesce_test",
    srcs = ["masking_coalesce_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_disjoint_coalesce_test",
    srcs = ["masking_disjoint_coalesce_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_cond_test",
    srcs = ["masking_cond_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "comparison_equal_test",
    srcs = ["comparison_equal_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "comparison_full_equal_test",
    srcs = ["comparison_full_equal_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "comparison_not_equal_test",
    srcs = ["comparison_not_equal_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "comparison_greater_test",
    srcs = ["comparison_greater_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "comparison_greater_equal_test",
    srcs = ["comparison_greater_equal_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "comparison_less_test",
    srcs = ["comparison_less_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "comparison_less_equal_test",
    srcs = ["comparison_less_equal_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_split_test",
    srcs = ["strings_split_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_agg_join_test",
    srcs = ["strings_agg_join_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_strip_test",
    srcs = ["strings_strip_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_lstrip_test",
    srcs = ["strings_lstrip_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_rstrip_test",
    srcs = ["strings_rstrip_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_substr_test",
    srcs = ["strings_substr_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_has_test",
    srcs = ["masking_has_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_has_not_test",
    srcs = ["masking_has_not_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_agg_sum_test",
    srcs = ["math_agg_sum_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_sum_test",
    srcs = ["math_sum_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_agg_mean_test",
    srcs = ["math_agg_mean_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_mean_test",
    srcs = ["math_mean_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_agg_median_test",
    srcs = ["math_agg_median_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_median_test",
    srcs = ["math_median_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_agg_std_test",
    srcs = ["math_agg_std_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_agg_var_test",
    srcs = ["math_agg_var_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_agg_max_test",
    srcs = ["math_agg_max_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_max_test",
    srcs = ["math_max_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_agg_min_test",
    srcs = ["math_agg_min_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "math_min_test",
    srcs = ["math_min_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_agg_any_test",
    srcs = ["masking_agg_any_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_any_test",
    srcs = ["masking_any_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_agg_has_test",
    srcs = ["masking_agg_has_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_agg_all_test",
    srcs = ["masking_agg_all_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_all_test",
    srcs = ["masking_all_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_agg_count_test",
    srcs = ["slices_agg_count_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_cum_count_test",
    srcs = ["slices_cum_count_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_count_test",
    srcs = ["slices_count_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_agg_size_test",
    srcs = ["slices_agg_size_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_size_test",
    srcs = ["slices_size_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_uu_schema_test",
    srcs = ["schema_uu_schema_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_new_schema_test",
    srcs = ["schema_new_schema_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_is_primitive_test",
    srcs = ["core_is_primitive_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_has_primitive_test",
    srcs = ["core_has_primitive_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_dict_schema_test",
    srcs = ["schema_dict_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_list_schema_test",
    srcs = ["schema_list_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "ids_uuid_test",
    srcs = ["ids_uuid_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "ids_uuid_for_list_test",
    srcs = ["ids_uuid_for_list_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "ids_uuid_for_dict_test",
    srcs = ["ids_uuid_for_dict_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "objs_uu_test",
    srcs = ["objs_uu_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "entities_uu_test",
    srcs = ["entities_uu_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "ids_agg_uuid_test",
    srcs = ["ids_agg_uuid_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:list_item",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_collapse_test",
    srcs = ["slices_collapse_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_concat_test",
    srcs = ["slices_concat_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators:slices",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_stack_test",
    srcs = ["slices_stack_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators:slices",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_zip_test",
    srcs = ["slices_zip_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_take_test",
    srcs = ["slices_take_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_align_test",
    srcs = ["slices_align_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_expand_to_test",
    srcs = ["slices_expand_to_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_tile_test",
    srcs = ["slices_tile_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_is_expandable_to_test",
    srcs = ["slices_is_expandable_to_test.py"],
    deps = [
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_is_shape_compatible_test",
    srcs = ["slices_is_shape_compatible_test.py"],
    deps = [
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "lists_appended_list_test",
    srcs = ["lists_appended_list_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "lists_list_append_update_test",
    srcs = ["lists_list_append_update_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "lists_explode_test",
    srcs = ["lists_explode_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "lists_implode_test",
    srcs = ["lists_implode_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "lists_concat_lists_test",
    srcs = ["lists_concat_lists_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_subslice_test",
    srcs = ["slices_subslice_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
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
    name = "core_nofollow_test",
    srcs = ["core_nofollow_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_freeze_test",
    srcs = ["core_freeze_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_index_test",
    srcs = ["slices_index_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_range_test",
    srcs = ["slices_range_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_no_bag_test",
    srcs = ["core_no_bag_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_with_bag_test",
    srcs = ["core_with_bag_test.py"],
    deps = [
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_reify_test",
    srcs = ["core_reify_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_enriched_test",
    srcs = ["core_enriched_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_updated_test",
    srcs = ["core_updated_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "bags_enriched_test",
    srcs = ["bags_enriched_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "bags_updated_test",
    srcs = ["bags_updated_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "shapes_expand_to_shape_test",
    srcs = ["shapes_expand_to_shape_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_repeat_test",
    srcs = ["slices_repeat_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_repeat_present_test",
    srcs = ["slices_repeat_present_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "shapes_new_test",
    srcs = ["shapes_new_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:literal_operator",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "shapes_size_test",
    srcs = ["shapes_size_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "shapes_rank_test",
    srcs = ["shapes_rank_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "shapes_dim_sizes_test",
    srcs = ["shapes_dim_sizes_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "shapes_dim_mapping_test",
    srcs = ["shapes_dim_mapping_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "shapes_reshape_test",
    srcs = ["shapes_reshape_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "shapes_get_shape_test",
    srcs = ["shapes_get_shape_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "shapes_flatten_test",
    srcs = ["shapes_flatten_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

##### Tests for internal implementation operators #####

py_test(
    name = "koda_internal_input_test",
    srcs = ["koda_internal_input_test.py"],
    deps = [
        "//koladata/expr:init",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_internal_non_deterministic_identity_test",
    srcs = ["koda_internal_non_deterministic_identity_test.py"],
    deps = [
        "//koladata/expr:init",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/operators:kde_operators",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_internal_to_arolla_boolean_test",
    srcs = ["koda_internal_to_arolla_boolean_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:arolla_bridge",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_internal_to_arolla_int64_test",
    srcs = ["koda_internal_to_arolla_int64_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:arolla_bridge",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_internal_to_arolla_float64_test",
    srcs = ["koda_internal_to_arolla_float64_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:arolla_bridge",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_internal_to_data_slice_test",
    srcs = ["koda_internal_to_data_slice_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:arolla_bridge",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:literal_operator",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/operator_tests:utils",
    ],
)

py_test(
    name = "koda_internal_to_arolla_dense_array_int64_test",
    srcs = ["koda_internal_to_arolla_dense_array_int64_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:arolla_bridge",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_clone_test",
    srcs = ["core_clone_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_deep_clone_test",
    srcs = ["core_deep_clone_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "ids_deep_uuid_test",
    srcs = ["ids_deep_uuid_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_extract_test",
    srcs = ["core_extract_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_shallow_clone_test",
    srcs = ["core_shallow_clone_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_internal_to_arolla_dense_array_unit_test",
    srcs = ["koda_internal_to_arolla_dense_array_unit_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:arolla_bridge",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_internal_flatten_last_ndim_test",
    srcs = ["koda_internal_flatten_last_ndim_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:jagged_shape",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_internal_remove_last_ndim_test",
    srcs = ["koda_internal_remove_last_ndim_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:jagged_shape",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_get_primitive_schema_test",
    srcs = ["schema_get_primitive_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "assert_assert_ds_has_primitives_of_test",
    srcs = ["assert_assert_ds_has_primitives_of_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_group_by_indices_test",
    srcs = ["slices_group_by_indices_test.py"],
    deps = [
        "//py/koladata:kd",
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_group_by_test",
    srcs = ["slices_group_by_test.py"],
    deps = [
        "//py/koladata:kd",
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_unique_test",
    srcs = ["slices_unique_test.py"],
    deps = [
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_ordinal_rank_test",
    srcs = ["slices_ordinal_rank_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_dense_rank_test",
    srcs = ["slices_dense_rank_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_inverse_mapping_test",
    srcs = ["slices_inverse_mapping_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "shapes_is_expandable_to_shape_test",
    srcs = ["shapes_is_expandable_to_shape_test.py"],
    deps = [
        "//py/koladata:kd",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_sort_test",
    srcs = ["slices_sort_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "random_sample_test",
    srcs = ["random_sample_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "random_sample_n_test",
    srcs = ["random_sample_n_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "random_randint_shaped_test",
    srcs = ["random_randint_shaped_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "random_randint_shaped_as_test",
    srcs = ["random_randint_shaped_as_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "random_randint_like_test",
    srcs = ["random_randint_like_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_cast_to_test",
    srcs = ["schema_cast_to_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_int32_test",
    srcs = ["schema_to_int32_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_val_shaped_test",
    srcs = ["slices_val_shaped_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_val_shaped_as_test",
    srcs = ["slices_val_shaped_as_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_val_like_test",
    srcs = ["slices_val_like_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_present_shaped_test",
    srcs = ["masking_present_shaped_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_present_shaped_as_test",
    srcs = ["masking_present_shaped_as_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "masking_present_like_test",
    srcs = ["masking_present_like_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_int64_test",
    srcs = ["schema_to_int64_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_float32_test",
    srcs = ["schema_to_float32_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_float64_test",
    srcs = ["schema_to_float64_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_mask_test",
    srcs = ["schema_to_mask_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_bool_test",
    srcs = ["schema_to_bool_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_bytes_test",
    srcs = ["schema_to_bytes_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_str_test",
    srcs = ["schema_to_str_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_expr_test",
    srcs = ["schema_to_expr_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_schema_test",
    srcs = ["schema_to_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_itemid_test",
    srcs = ["schema_to_itemid_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_object_test",
    srcs = ["schema_to_object_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_any_test",
    srcs = ["schema_to_any_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_to_none_test",
    srcs = ["schema_to_none_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_with_schema_test",
    srcs = ["schema_with_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_decode_test",
    srcs = ["strings_decode_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_encode_test",
    srcs = ["strings_encode_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_internal_to_arolla_dense_array_text_test",
    srcs = ["koda_internal_to_arolla_dense_array_text_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:arolla_bridge",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_translate_test",
    srcs = ["slices_translate_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_translate_group_test",
    srcs = ["slices_translate_group_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_get_schema_test",
    srcs = ["schema_get_schema_test.py"],
    deps = [
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_get_obj_schema_test",
    srcs = ["schema_get_obj_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_get_item_schema_test",
    srcs = ["schema_get_item_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_get_key_schema_test",
    srcs = ["schema_get_key_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_get_value_schema_test",
    srcs = ["schema_get_value_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_length_test",
    srcs = ["strings_length_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_lower_test",
    srcs = ["strings_lower_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_upper_test",
    srcs = ["strings_upper_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_printf_test",
    srcs = ["strings_printf_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_format_test",
    srcs = ["strings_format_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_fstr_test",
    srcs = ["strings_fstr_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "py_apply_py_test",
    srcs = ["py_apply_py_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "py_apply_py_on_cond_test",
    srcs = ["py_apply_py_on_cond_test.py"],
    tags = ["cpu:4"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "py_map_py_test",
    srcs = ["py_map_py_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "py_map_py_on_cond_test",
    srcs = ["py_map_py_on_cond_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
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
    name = "py_apply_py_on_selected_test",
    srcs = ["py_apply_py_on_selected_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "py_map_py_on_selected_test",
    srcs = ["py_map_py_on_selected_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "functor_map_test",
    srcs = ["functor_map_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/functor:functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_join_test",
    srcs = ["strings_join_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_contains_test",
    srcs = ["strings_contains_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_find_test",
    srcs = ["strings_find_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_rfind_test",
    srcs = ["strings_rfind_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_count_test",
    srcs = ["strings_count_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "tuple_make_tuple_test",
    srcs = ["tuple_make_tuple_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_cast_to_implicit_test",
    srcs = ["schema_cast_to_implicit_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_cast_to_narrow_test",
    srcs = ["schema_cast_to_narrow_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "functor_call_test",
    srcs = ["functor_call_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/functor:signature_utils",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "annotation_with_name_test",
    srcs = ["annotation_with_name_test.py"],
    deps = [
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_ref_test",
    srcs = ["core_ref_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_with_schema_from_obj_test",
    srcs = ["schema_with_schema_from_obj_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_get_ndim_test",
    srcs = ["slices_get_ndim_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_internal_to_arolla_optional_unit_test",
    srcs = ["koda_internal_to_arolla_optional_unit_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:arolla_bridge",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "assertion_with_assertion_test",
    srcs = ["assertion_with_assertion_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_regex_extract_test",
    srcs = ["strings_regex_extract_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_regex_match_test",
    srcs = ["strings_regex_match_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_replace_test",
    srcs = ["strings_replace_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_with_merged_bag_test",
    srcs = ["core_with_merged_bag_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_stub_test",
    srcs = ["core_stub_test.py"],
    deps = [
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "shapes_new_with_size_test",
    srcs = ["shapes_new_with_size_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:literal_operator",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_is_dict_schema_test",
    srcs = ["schema_is_dict_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_is_entity_schema_test",
    srcs = ["schema_is_entity_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_is_struct_schema_test",
    srcs = ["schema_is_struct_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_is_list_schema_test",
    srcs = ["schema_is_list_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_is_primitive_schema_test",
    srcs = ["schema_is_primitive_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "pointwise_arolla_op_error_test",
    srcs = ["pointwise_arolla_op_error_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "ids_uuids_with_allocation_size_test",
    srcs = ["ids_uuids_with_allocation_size_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "ids_hash_itemid_test",
    srcs = ["ids_hash_itemid_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

##### Arolla coverage tests #####

koda_operator_coverage_test(
    name = "kd_core_add_covers_math_add_test",
    arolla_op = "math.add",
    koda_op = "math.add",
    skip_test_patterns = [".*weak_float.*"],
)

koda_operator_coverage_test(
    name = "kd_core_add_covers_strings_join_test",
    arolla_op = "strings.join",
    koda_op = "math.add",
    skip_test_patterns = ["(?!(.*arity=2)).*"],
)

koda_operator_coverage_test(
    name = "kd_slices_unique_covers_array_unique_test",
    arolla_op = "array.unique",
    koda_op = "slices.unique",
    skip_test_patterns = [
        ".*weak_float.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_masking_agg_any_covers_core_any_test",
    arolla_op = "core.any",
    koda_op = "masking.agg_any",
    # Arolla operator supports scalar inputs that we don't.
    skip_test_patterns = [
        r".*\(missing.*",
        r".*\(present.*",
        r".*\(unit.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_masking_agg_all_covers_core_all_test",
    arolla_op = "core.all",
    koda_op = "masking.agg_all",
    # Arolla operator supports scalar inputs that we don't.
    skip_test_patterns = [
        r".*\(missing.*",
        r".*\(present.*",
        r".*\(unit.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_subtract_covers_math_subtract_test",
    arolla_op = "math.subtract",
    koda_op = "math.subtract",
    skip_test_patterns = [".*weak_float.*"],
)

koda_operator_coverage_test(
    name = "kd_math_multiply_covers_math_multiply_test",
    arolla_op = "math.multiply",
    koda_op = "math.multiply",
    skip_test_patterns = [".*weak_float.*"],
)

koda_operator_coverage_test(
    name = "kd_math_divide_covers_math_divide_test",
    arolla_op = "math.divide",
    koda_op = "math.divide",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_pow_covers_math_pow_test",
    arolla_op = "math.pow",
    koda_op = "math.pow",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_exp_covers_math_exp_test",
    arolla_op = "math.exp",
    koda_op = "math.exp",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_sigmoid_covers_math_sigmoid_test",
    arolla_op = "math.sigmoid",
    koda_op = "math.sigmoid",
    shard_count = 50,  # TODO: Enormous test
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_log_covers_math_log_test",
    arolla_op = "math.log",
    koda_op = "math.log",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_log10_covers_math_log10_test",
    arolla_op = "math.log",
    koda_op = "math.log",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_abs_covers_math_abs_test",
    arolla_op = "math.abs",
    koda_op = "math.abs",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_neg_covers_math_neg_test",
    arolla_op = "math.neg",
    koda_op = "math.neg",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_sign_covers_math_sign_test",
    arolla_op = "math.sign",
    koda_op = "math.sign",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_pos_covers_math_pos_test",
    arolla_op = "math.pos",
    koda_op = "math.pos",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_ceil_covers_math_ceil_test",
    arolla_op = "math.ceil",
    koda_op = "math.ceil",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_floor_covers_math_floor_test",
    arolla_op = "math.floor",
    koda_op = "math.floor",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_round_covers_math_round_test",
    arolla_op = "math.round",
    koda_op = "math.round",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_floordiv_covers_math_floordiv_test",
    arolla_op = "math.floordiv",
    koda_op = "math.floordiv",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_mod_covers_math_mod_test",
    arolla_op = "math.mod",
    koda_op = "math.mod",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_math_maximum_covers_math_maximum_test",
    arolla_op = "math.maximum",
    koda_op = "math.maximum",
    skip_test_patterns = [".*weak_float.*"],
)

koda_operator_coverage_test(
    name = "kd_math_minimum_covers_math_minimum_test",
    arolla_op = "math.minimum",
    koda_op = "math.minimum",
    skip_test_patterns = [".*weak_float.*"],
)

koda_operator_coverage_test(
    name = "kd_masking_apply_mask_covers_core_presence_and_test",
    arolla_op = "core.presence_and",
    koda_op = "masking.apply_mask",
    skip_test_patterns = [
        ".*weak_float.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_masking_cond_covers_core_where_test",
    arolla_op = "core.where",
    koda_op = "masking.cond",
    skip_test_patterns = [
        ".*weak_float.*",
        ".*uint64.*",
        # TODO: Include int64|int64 and float64|float64 tests.
        ".*int64.*",  # DataSlices with MIXED type can't be converted to Arolla value
        ".*float64.*",  # DataSlices with MIXED type can't be converted to Arolla value
    ],
)

koda_operator_coverage_test(
    name = "kd_masking_coalesce_covers_core_presence_or_test",
    arolla_op = "core.presence_or",
    koda_op = "masking.coalesce",
    skip_test_patterns = [
        ".*weak_float.*",
        ".*uint64.*",
        # TODO: Include int64|int64 and float64|float64 tests.
        ".*int64.*",  # DataSlices with MIXED type can't be converted to Arolla value
        ".*float64.*",  # DataSlices with MIXED type can't be converted to Arolla value
    ],
)

koda_operator_coverage_test(
    name = "kd_comparison_equal_covers_core_equal_test",
    arolla_op = "core.equal",
    koda_op = "comparison.equal",
    skip_test_patterns = [
        ".*weak_float.*",
        ".*uint64.*",
        ".*INT32.*",  # Unsupported test with qtypes comparison.
    ],
)

koda_operator_coverage_test(
    name = "kd_comparison_not_equal_covers_core_not_equal_test",
    arolla_op = "core.not_equal",
    koda_op = "comparison.not_equal",
    skip_test_patterns = [
        ".*weak_float.*",
        ".*uint64.*",
        ".*INT32.*",  # Unsupported test with qtypes comparison.
    ],
)

koda_operator_coverage_test(
    name = "kd_comparison_greater_covers_core_greater_test",
    arolla_op = "core.greater",
    koda_op = "comparison.greater",
    skip_test_patterns = [
        ".*weak_float.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_comparison_greater_equal_covers_core_greater_equal_test",
    arolla_op = "core.greater_equal",
    koda_op = "comparison.greater_equal",
    skip_test_patterns = [
        ".*weak_float.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_comparison_less_covers_core_less_test",
    arolla_op = "core.less",
    koda_op = "comparison.less",
    skip_test_patterns = [
        ".*weak_float.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_comparison_less_equal_covers_core_less_equal_test",
    arolla_op = "core.less_equal",
    koda_op = "comparison.less_equal",
    skip_test_patterns = [
        ".*weak_float.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_masking_has_covers_core_has_test",
    arolla_op = "core.has",
    koda_op = "masking.has",
    skip_test_patterns = [
        ".*weak_float.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_masking_has_not_covers_core_presence_not_test",
    arolla_op = "core.presence_not",
    koda_op = "masking.has_not",
    skip_test_patterns = [
        ".*weak_float.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_agg_sum_covers_math_sum_test",
    arolla_op = "math.sum",
    koda_op = "math.agg_sum",
    shard_count = 16,  # Huge test.
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_agg_mean_covers_math_mean_test",
    arolla_op = "math.mean",
    koda_op = "math.agg_mean",
    shard_count = 16,  # Huge test.
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_agg_median_covers_math_median_test",
    arolla_op = "math.median",
    koda_op = "math.agg_median",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_agg_std_covers_math_std_test",
    arolla_op = "math.std",
    koda_op = "math.agg_std",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_agg_var_covers_math_var_test",
    arolla_op = "math.var",
    koda_op = "math.agg_var",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_max_covers_math_max_test",
    arolla_op = "math.max",
    koda_op = "math.agg_max",
    shard_count = 16,  # Huge test.
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_cum_max_covers_math_cum_max_test",
    arolla_op = "math.cum_max",
    koda_op = "math.cum_max",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_min_covers_math_min_test",
    arolla_op = "math.min",
    koda_op = "math.agg_min",
    shard_count = 16,  # Huge test.
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_cum_min_covers_math_cum_min_test",
    arolla_op = "math.cum_min",
    koda_op = "math.cum_min",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_cum_sum_covers_math_cum_sum_test",
    arolla_op = "math.cum_sum",
    koda_op = "math.cum_sum",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_cdf_covers_math_cdf_test",
    args_for_reshape = [
        "x",
        "weights",
    ],
    arolla_op = "math.cdf",
    koda_op = "math.cdf",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_softmax_covers_math_softmax_test",
    arolla_op = "math.softmax",
    koda_op = "math.softmax",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_math_agg_inverse_cdf_covers_math_inverse_cdf_test",
    arolla_op = "math.inverse_cdf",
    koda_op = "math.agg_inverse_cdf",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_slices_agg_count_covers_array_count_test",
    arolla_op = "array.count",
    koda_op = "slices.agg_count",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_slices_cum_count_covers_array_cum_count_test",
    arolla_op = "array.cum_count",
    koda_op = "slices.cum_count",
)

koda_operator_coverage_test(
    name = "kd_slices_index_covers_array_agg_index_test",
    arolla_op = "array.agg_index",
    koda_op = "slices.index",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_strings_agg_join_covers_strings_agg_join_test",
    arolla_op = "strings.agg_join",
    koda_op = "strings.agg_join",
)

koda_operator_coverage_test(
    name = "kd_strings_extract_regex_covers_strings_regex_extract_test",
    arolla_op = "strings.extract_regex",
    koda_op = "strings.regex_extract",
)

koda_operator_coverage_test(
    name = "kd_strings_contains_regex_covers_strings_regex_match_test",
    arolla_op = "strings.contains_regex",
    koda_op = "strings.regex_match",
)

koda_operator_coverage_test(
    name = "kd_strings_strip_covers_strings_strip_test",
    arolla_op = "strings.strip",
    koda_op = "strings.strip",
)

koda_operator_coverage_test(
    name = "kd_strings_lstrip_covers_strings_lstrip_test",
    arolla_op = "strings.lstrip",
    koda_op = "strings.lstrip",
)

koda_operator_coverage_test(
    name = "kd_strings_rstrip_covers_strings_rstrip_test",
    arolla_op = "strings.rstrip",
    koda_op = "strings.rstrip",
)

koda_operator_coverage_test(
    name = "kd_strings_substr_covers_strings_substr_test",
    arolla_op = "strings.substr",
    koda_op = "strings.substr",
)

koda_operator_coverage_test(
    name = "kd_slices_collapse_covers_array_collapse_test",
    arolla_op = "array.collapse",
    koda_op = "slices.collapse",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
        ".*edge_mapping.*",  # JaggedShape edges cannot be mapping edges
    ],
)

koda_operator_coverage_test(
    name = "kd_slices_at_covers_covers_array_at_test",
    arolla_op = "array.at",
    koda_op = "at",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
        ".*out of range.*",  # Out-of-range results in missing items rather than error in Koda
    ],
)

koda_operator_coverage_test(
    name = "kd_slices_ordinal_rank_covers_array_ordinal_rank_test",
    args_for_reshape = [
        "x",
        "tie_breaker",
    ],
    arolla_op = "array.ordinal_rank",
    koda_op = "slices.ordinal_rank",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
        ".*empty.*",  # test_empty uses M.edge.from_mapping
        ".*mapping_edge.*",  # JaggedShape edges cannot be mapping edges
    ],
)

koda_operator_coverage_test(
    name = "kd_slices_dense_rank_covers_array_dense_rank_test",
    arolla_op = "array.dense_rank",
    koda_op = "slices.dense_rank",
    skip_test_patterns = [
        "(?i).*[WEAK_FLOAT].*",
        ".*uint64.*",
        ".*empty.*",  # test_empty uses M.edge.from_mapping
        ".*mapping_edge.*",  # JaggedShape edges cannot be mapping edges
    ],
)

koda_operator_coverage_test(
    name = "kd_slices_inverse_mapping_covers_covers_array_inverse_mapping_test",
    arolla_op = "array.inverse_mapping",
    koda_op = "inverse_mapping",
)

koda_operator_coverage_test(
    name = "kd_schema_to_int32_covers_core_to_int32_test",
    arolla_op = "core.to_int32",
    koda_op = "schema.to_int32",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_schema_to_int64_covers_core_to_int64_test",
    arolla_op = "core.to_int64",
    koda_op = "schema.to_int64",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_schema_to_float32_covers_core_to_float32_test",
    arolla_op = "core.to_float32",
    koda_op = "schema.to_float32",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_schema_to_float64_covers_core_to_float64_test",
    arolla_op = "core.to_float64",
    koda_op = "schema.to_float64",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_schema_to_bool_covers_core_to_bool_test",
    arolla_op = "core.to_bool",
    koda_op = "schema.to_bool",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_schema_to_str_covers_strings_as_text_test",
    arolla_op = "strings.as_text",
    koda_op = "schema.to_str",
    skip_test_patterns = [
        "(?i).*WEAK_FLOAT.*",
        ".*uint64.*",
    ],
)

koda_operator_coverage_test(
    name = "kd_strings_encode_covers_strings_encode_test",
    arolla_op = "strings.encode",
    koda_op = "strings.encode",
)

koda_operator_coverage_test(
    name = "kd_strings_decode_covers_strings_decode_test",
    arolla_op = "strings.decode",
    koda_op = "strings.decode",
)

koda_operator_coverage_test(
    name = "kd_strings_length_covers_strings_length_test",
    arolla_op = "strings.length",
    koda_op = "strings.length",
)

koda_operator_coverage_test(
    name = "kd_strings_lower_covers_strings_lower_test",
    arolla_op = "strings.lower",
    koda_op = "strings.lower",
)

koda_operator_coverage_test(
    name = "kd_strings_upper_covers_strings_upper_test",
    arolla_op = "strings.upper",
    koda_op = "strings.upper",
)

koda_operator_coverage_test(
    name = "kd_strings_printf_covers_strings_printf_test",
    arolla_op = "strings.printf",
    koda_op = "strings.printf",
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_strings_format_covers_strings_format_test",
    arolla_op = "strings.format",
    koda_op = "strings._test_only_format_wrapper",
    shard_count = 7,  # Quite a lot of test cases.
    skip_test_patterns = ["(?i).*WEAK_FLOAT.*"],
)

koda_operator_coverage_test(
    name = "kd_strings_join_covers_strings_join_test",
    arolla_op = "strings.join",
    koda_op = "strings.join",
)

koda_operator_coverage_test(
    name = "kd_strings_contains_covers_strings_contains_test",
    arolla_op = "strings.contains",
    koda_op = "strings.contains",
)

koda_operator_coverage_test(
    name = "kd_strings_find_covers_strings_find_test",
    arolla_op = "strings.find",
    koda_op = "strings.find",
)

koda_operator_coverage_test(
    name = "kd_strings_rfind_covers_strings_rfind_test",
    arolla_op = "strings.rfind",
    koda_op = "strings.rfind",
)

koda_operator_coverage_test(
    name = "kd_strings_replace_covers_strings_replace_test",
    arolla_op = "strings.replace",
    koda_op = "strings.replace",
)

koda_operator_coverage_test(
    name = "kd_strings_count_covers_strings_count_test",
    arolla_op = "strings.count",
    koda_op = "strings.count",
)

py_test(
    name = "simple_agg_arolla_op_error_test",
    srcs = ["simple_agg_arolla_op_error_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "schema_named_schema_test",
    srcs = ["schema_named_schema_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_internal_maybe_named_schema_test",
    srcs = ["schema_internal_maybe_named_schema_test.py"],
    deps = [
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_internal_view_get_item_test",
    srcs = ["koda_internal_view_get_item_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:view",
        "//py/koladata/operators:view_overloads",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "tuple_get_nth_test",
    srcs = ["tuple_get_nth_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:view_overloads",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_extract_bag_test",
    srcs = ["core_extract_bag_test.py"],
    deps = [
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_encode_base64_test",
    srcs = ["strings_encode_base64_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "strings_decode_base64_test",
    srcs = ["strings_decode_base64_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_agg_common_schema_test",
    srcs = ["schema_agg_common_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_common_schema_test",
    srcs = ["schema_common_schema_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "json_to_json_test",
    srcs = ["json_to_json_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "core_freeze_bag_test",
    srcs = ["core_freeze_bag_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_item_test",
    srcs = ["slices_item_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_slice_test",
    srcs = ["slices_slice_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_int32_test",
    srcs = ["slices_int32_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_int64_test",
    srcs = ["slices_int64_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_float64_test",
    srcs = ["slices_float64_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_float32_test",
    srcs = ["slices_float32_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_expr_quote_test",
    srcs = ["slices_expr_quote_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_bytes_test",
    srcs = ["slices_bytes_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_mask_test",
    srcs = ["slices_mask_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_str_test",
    srcs = ["slices_str_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "slices_bool_test",
    srcs = ["slices_bool_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "tuple_make_slice_test",
    srcs = ["tuple_make_slice_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "koda_internal_to_arolla_text_test",
    srcs = ["koda_internal_to_arolla_text_test.py"],
    deps = [
        "//py/koladata/exceptions",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:arolla_bridge",
        "//py/koladata/operators/tests/util:qtypes",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "tuple_make_namedtuple_test",
    srcs = ["tuple_make_namedtuple_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "tuple_get_namedtuple_field_test",
    srcs = ["tuple_get_namedtuple_field_test.py"],
    deps = [
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:view_overloads",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)
