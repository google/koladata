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

# Custom C++/Python bindings for Kola types.

load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_py_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "wrap_utils",
    srcs = ["wrap_utils.cc"],
    hdrs = ["wrap_utils.h"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/jagged_shape/array/qtype",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qtype",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//py/arolla/abc:py_abc",
    ],
)

py_test(
    name = "py_ext_test",
    srcs = ["py_ext_test.py"],
    deps = [
        ":data_item_py_ext",
        ":data_slice_py_ext",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

cc_library(
    name = "boxing",
    srcs = ["boxing.cc"],
    hdrs = ["boxing.h"],
    deps = [
        ":wrap_utils",
        "//koladata:arolla_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:missing_value",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_utils",
        "//koladata/internal:types",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/array/qtype",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/dense_array/qtype",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

py_test(
    name = "boxing_test",
    srcs = ["boxing_test.py"],
    tags = ["nooss"],  # TODO: b/352697901 - Remove once we support Koda exceptions in OSS.
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":qtypes",
        ":schema_constants",
        "//py/koladata/exceptions",
        "//py/koladata/testing",
        "//py/koladata/types/testing:operator_test_invalid_unicode_op",  # buildcleaner: keep
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

cc_library(
    name = "py_utils",
    srcs = ["py_utils.cc"],
    hdrs = ["py_utils.h"],
    deps = [
        ":boxing",
        ":wrap_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
    ],
)

py_library(
    name = "py_boxing",
    srcs = ["py_boxing.py"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":ellipsis",
        ":literal_operator",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "py_boxing_test",
    srcs = ["py_boxing_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":data_bag",
        ":data_slice",
        ":dict_item",
        ":ellipsis",
        ":literal_operator",
        ":py_boxing",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "operator_lookup",
    srcs = ["operator_lookup.py"],
    deps = [
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "operator_lookup_test",
    srcs = ["operator_lookup_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":operator_lookup",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

cc_library(
    name = "data_slice_cc_lib",
    srcs = ["data_slice.cc"],
    hdrs = ["data_slice.h"],
    deps = [
        ":boxing",
        ":py_utils",
        ":wrap_utils",
        "//koladata:arolla_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/s11n",
        "//py/koladata/exceptions:py_exception_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qtype",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

koladata_py_extension(
    name = "data_slice_py_ext",
    srcs = ["data_slice_module.cc"],
    deps = [":data_slice_cc_lib"],
)

py_library(
    name = "data_slice",
    srcs = ["data_slice.py"],
    deps = [
        ":data_slice_py_ext",
        ":general_eager_ops",
        ":jagged_shape",
        ":operator_lookup",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "data_slice_test",
    srcs = ["data_slice_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":dict_item",
        ":jagged_shape",
        ":list_item",
        ":qtypes",
        ":schema_constants",
        "//py/koladata/exceptions",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

cc_library(
    name = "data_item_cc_lib",
    srcs = ["data_item.cc"],
    hdrs = ["data_item.h"],
    deps = [
        ":data_slice_cc_lib",
        ":wrap_utils",
        "//koladata:data_slice",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/base:nullability",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

koladata_py_extension(
    name = "data_item_py_ext",
    srcs = ["data_item_module.cc"],
    deps = [":data_item_cc_lib"],
)

py_library(
    name = "data_item",
    srcs = ["data_item.py"],
    deps = [
        ":data_item_py_ext",
        ":data_slice",
        ":operator_lookup",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "data_item_test",
    srcs = ["data_item_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":jagged_shape",
        ":schema_constants",
        "//py/koladata/expr:input_container",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "list_item",
    srcs = ["list_item.py"],
    deps = [
        ":data_item",
        ":data_slice",
        ":operator_lookup",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "list_item_test",
    srcs = ["list_item_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":jagged_shape",
        ":list_item",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "dict_item",
    srcs = ["dict_item.py"],
    deps = [
        ":data_item",
        ":data_slice",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dict_item_test",
    srcs = ["dict_item_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":dict_item",
        ":jagged_shape",
        ":schema_constants",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "schema_item",
    srcs = ["schema_item.py"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":operator_lookup",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_item_test",
    srcs = ["schema_item_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":jagged_shape",
        ":schema_constants",
        ":schema_item",
        "//py/koladata/functions",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "qtypes",
    srcs = ["qtypes.py"],
    deps = [
        ":data_bag_py_ext",
        ":data_slice_py_ext",
        ":ellipsis",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_test(
    name = "qtypes_test",
    srcs = ["qtypes_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":ellipsis",
        ":qtypes",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_library(
    name = "schema_constants",
    srcs = ["schema_constants.py"],
    deps = [
        ":py_misc_py_ext",
        ":schema_item",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_constants_test",
    srcs = ["schema_constants_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":schema_constants",
        ":schema_item",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

cc_library(
    name = "data_bag_cc_lib",
    srcs = ["data_bag.cc"],
    hdrs = ["data_bag.h"],
    deps = [
        ":boxing",
        ":py_utils",
        ":wrap_utils",
        "//koladata:data_bag",
        "//koladata:data_bag_comparison",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//py/koladata/exceptions:py_exception_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

koladata_py_extension(
    name = "data_bag_py_ext",
    srcs = ["data_bag_module.cc"],
    deps = [":data_bag_cc_lib"],
)

py_library(
    name = "data_bag",
    srcs = ["data_bag.py"],
    deps = [
        ":data_bag_py_ext",
        ":data_slice_py_ext",
        ":general_eager_ops",
        ":jagged_shape",
        "@com_google_arolla//py/arolla",
    ],
)

cc_library(
    name = "py_misc_cc_lib",
    srcs = ["py_misc.cc"],
    hdrs = ["py_misc.h"],
    deps = [
        ":wrap_utils",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:schema_constants",
        "//koladata/expr:expr_operators",
        "//koladata/expr:expr_operators_repr",
        "//koladata/operators:compile_expr_operators",
        "//koladata/s11n",
        "@com_google_absl//absl/base:nullability",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

koladata_py_extension(
    name = "py_misc_py_ext",
    srcs = ["py_misc_module.cc"],
    deps = [":py_misc_cc_lib"],
)

py_library(
    name = "literal_operator",
    srcs = ["literal_operator.py"],
    deps = [
        ":py_misc_py_ext",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "literal_operator_test",
    srcs = ["literal_operator_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":data_bag",
        ":data_slice",
        ":literal_operator",
        ":qtypes",
        ":schema_constants",
        "//py/koladata/expr:view",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "jagged_shape",
    srcs = ["jagged_shape.py"],
    deps = [
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_test(
    name = "data_bag_test",
    srcs = ["data_bag_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":dict_item",
        ":jagged_shape",
        ":list_item",
        ":schema_constants",
        "//py/koladata/exceptions",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_test(
    name = "jagged_shape_test",
    srcs = ["jagged_shape_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":data_slice",
        ":jagged_shape",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "ellipsis",
    srcs = ["ellipsis.py"],
    deps = [
        "//koladata/expr:expr_operators",
        "//koladata/s11n",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "mask_constants",
    srcs = ["mask_constants.py"],
    deps = [
        ":data_item",
        ":schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "general_eager_ops",
    srcs = ["general_eager_ops.py"],
    deps = ["@com_google_arolla//py/arolla"],
)

py_test(
    name = "general_eager_ops_test",
    srcs = ["general_eager_ops_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":data_slice",
        ":general_eager_ops",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "mask_constants_test",
    srcs = ["mask_constants_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":data_item",
        ":mask_constants",
        ":schema_constants",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "ellipsis_test",
    srcs = ["ellipsis_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":ellipsis",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)
