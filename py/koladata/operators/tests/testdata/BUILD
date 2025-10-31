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

# Shared test data between various implementations of the same operator.

load("@rules_python//python:defs.bzl", "py_library")

package(
    default_testonly = True,
    default_visibility = [
        "//koladata:internal",
        "//py/koladata/ext/view:__subpackages__",
    ],
)

licenses(["notice"])

py_library(
    name = "lists_implode_testdata",
    srcs = ["lists_implode_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/types:data_slice",
    ],
)

py_library(
    name = "lists_explode_testdata",
    srcs = ["lists_explode_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/types:data_slice",
    ],
)

py_library(
    name = "slices_align_testdata",
    srcs = ["slices_align_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
    ],
)

py_library(
    name = "shapes_flatten_testdata",
    srcs = ["shapes_flatten_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "py_map_py_testdata",
    srcs = ["py_map_py_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
    ],
)

py_library(
    name = "slices_expand_to_testdata",
    srcs = ["slices_expand_to_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "core_get_attr_testdata",
    srcs = ["core_get_attr_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:schema_constants",
    ],
)

py_library(
    name = "core_get_item_testdata",
    srcs = ["core_get_item_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
    ],
)

py_library(
    name = "slices_take_testdata",
    srcs = ["slices_take_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
    ],
)

py_library(
    name = "slices_group_by_testdata",
    srcs = ["slices_group_by_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
    ],
)

py_library(
    name = "slices_collapse_testdata",
    srcs = ["slices_collapse_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "masking_apply_mask_testdata",
    srcs = ["masking_apply_mask_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "masking_coalesce_testdata",
    srcs = ["masking_coalesce_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "comparison_equal_testdata",
    srcs = ["comparison_equal_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "comparison_not_equal_testdata",
    srcs = ["comparison_not_equal_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "comparison_greater_testdata",
    srcs = ["comparison_greater_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "comparison_greater_equal_testdata",
    srcs = ["comparison_greater_equal_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "comparison_less_testdata",
    srcs = ["comparison_less_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "comparison_less_equal_testdata",
    srcs = ["comparison_less_equal_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "slices_select_testdata",
    srcs = ["slices_select_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "slices_inverse_select_testdata",
    srcs = ["slices_inverse_select_testdata.py"],
    deps = [
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)
