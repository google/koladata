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

# Custom C++/Python bindings for Kola types.

load("@pip_deps//:requirements.bzl", "requirement")
load("@pybind11_bazel//:build_defs.bzl", "pybind_library")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_py_extension")

package(default_visibility = ["//koladata:internal"])

licenses(["notice"])

py_test(
    name = "py_ext_test",
    srcs = ["py_ext_test.py"],
    deps = [
        ":data_item_py_ext",
        ":data_slice_py_ext",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
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
        ":jagged_shape",
        ":literal_operator",
        ":schema_item",
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "py_boxing_test",
    srcs = ["py_boxing_test.py"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":ellipsis",
        ":jagged_shape",
        ":literal_operator",
        ":py_boxing",
        ":schema_item",
        "//py:python_path",
        "//py/koladata/expr:input_container",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

cc_library(
    name = "data_slice_cc_lib",
    srcs = ["data_slice.cc"],
    hdrs = ["data_slice.h"],
    deps = [
        ":pybind11_protobuf_wrapper",
        "//koladata:arolla_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:uuid_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/operators:lib",
        "//koladata/proto:to_proto",
        "//koladata/s11n",
        "//py/koladata/base:boxing",
        "//py/koladata/base:py_args",
        "//py/koladata/base:py_utils",
        "//py/koladata/base:to_py_object",
        "//py/koladata/base:wrap_utils",
        "//py/koladata/base/py_conversions:to_py",
        "//py/koladata/fstring:fstring_processor",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qtype",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
        "@com_google_protobuf//:protobuf",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

py_library(
    name = "type_defs",
    srcs = ["type_defs.py"],
    pyi_deps = [
        "@com_google_arolla//py/arolla",
    ],
    pyi_srcs = ["type_defs.pyi"],
    deps = ["//py:python_path"],
)

koladata_py_extension(
    name = "data_slice_py_ext",
    srcs = ["data_slice_module.cc"],
    pytype_deps = [
        ":type_defs",
    ],
    pytype_srcs = ["data_slice_py_ext.pyi"],
    deps = [
        ":data_slice_cc_lib",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

py_library(
    name = "data_slice",
    srcs = ["data_slice.py"],
    deps = [
        ":data_bag_py_ext",
        ":data_item_py_ext",
        ":data_slice_py_ext",
        ":general_eager_ops",
        ":jagged_shape",
        "//py:python_path",
        "//py/koladata/base/py_conversions:dataclasses_py_util",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "data_slice_test",
    srcs = ["data_slice_test.py"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":dict_item",
        ":jagged_shape",
        ":list_item",
        ":mask_constants",
        ":qtypes",
        ":schema_constants",
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/expr:input_container",
        "//py/koladata/functions",
        "//py/koladata/functions/tests:test_py_pb2",
        "//py/koladata/functor:boxing",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/functor:tracing_decorator",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/operators:optools",
        "//py/koladata/testing",
        "//py/koladata/testing:signature_test_utils",
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
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//py/koladata/base:boxing",
        "//py/koladata/base:py_args",
        "//py/koladata/base:to_py_object",
        "//py/koladata/base:wrap_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/log:check",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

koladata_py_extension(
    name = "data_item_py_ext",
    srcs = ["data_item_module.cc"],
    deps = [
        ":data_item_cc_lib",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

py_library(
    name = "data_item",
    srcs = ["data_item.py"],
    pyi_deps = [":data_bag"],
    pyi_srcs = ["data_item.pyi"],
    deps = [
        ":data_item_py_ext",
        ":data_slice",
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/util:kd_functools",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "data_item_test",
    srcs = ["data_item_test.py"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":jagged_shape",
        ":schema_constants",
        "//py:python_path",
        "//py/koladata:kd",
        "//py/koladata/expr:input_container",
        "//py/koladata/functions",
        "//py/koladata/functor:functor_factories",
        "//py/koladata/operators:kde_operators",
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
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "list_item_test",
    srcs = ["list_item_test.py"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":jagged_shape",
        ":list_item",
        ":mask_constants",
        "//py:python_path",
        "//py/koladata/operators:kde_operators",
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
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "dict_item_test",
    srcs = ["dict_item_test.py"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":dict_item",
        ":jagged_shape",
        ":schema_constants",
        "//py:python_path",
        "//py/koladata/operators:kde_operators",
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
        ":data_item",
        ":data_slice",
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "signature_utils",
    srcs = ["signature_utils.py"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":py_boxing",
        "//py:python_path",
        "//py/koladata/base:py_functors_base_py_ext",
    ],
)

py_test(
    name = "schema_item_test",
    srcs = ["schema_item_test.py"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":jagged_shape",
        ":schema_constants",
        ":schema_item",
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "signature_utils_test",
    srcs = ["signature_utils_test.py"],
    deps = [
        ":data_item",
        ":data_slice",
        ":schema_constants",
        ":signature_utils",
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
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
        ":jagged_shape",
        "//koladata/functor/parallel:init_expr_operators",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "qtypes_test",
    srcs = ["qtypes_test.py"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":ellipsis",
        ":qtypes",
        "//py:python_path",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "schema_constants",
    srcs = ["schema_constants.py"],
    deps = [
        ":py_misc_py_ext",
        ":schema_item",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "schema_constants_test",
    srcs = ["schema_constants_test.py"],
    deps = [
        ":schema_constants",
        ":schema_item",
        "//py:python_path",
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
        ":pybind11_protobuf_wrapper",
        "//koladata:data_bag",
        "//koladata:data_bag_comparison",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/operators:lib",
        "//koladata/proto:from_proto",
        "//py/koladata/base:boxing",
        "//py/koladata/base:py_args",
        "//py/koladata/base:py_utils",
        "//py/koladata/base:wrap_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/functional:function_ref",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
        "@com_google_protobuf//:protobuf",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

koladata_py_extension(
    name = "data_bag_py_ext",
    srcs = ["data_bag_module.cc"],
    pytype_deps = [
        ":type_defs",
    ],
    pytype_srcs = ["data_bag_py_ext.pyi"],
    deps = [
        ":data_bag_cc_lib",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

py_library(
    name = "data_bag",
    srcs = ["data_bag.py"],
    deps = [
        ":data_bag_py_ext",
        ":data_slice_py_ext",
        ":general_eager_ops",
        ":jagged_shape",
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "@com_google_arolla//py/arolla",
    ],
)

cc_library(
    name = "py_misc_cc_lib",
    srcs = ["py_misc.cc"],
    hdrs = ["py_misc.h"],
    deps = [
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:jagged_shape_qtype",
        "//koladata:schema_constants",
        "//koladata/expr:expr_operators",
        "//koladata/expr:init",
        "//koladata/s11n",
        "//py/koladata/base:boxing",
        "//py/koladata/base:wrap_utils",
        "@com_google_absl//absl/base:nullability",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qtype",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

pybind_library(
    name = "pybind11_protobuf_wrapper",
    srcs = ["pybind11_protobuf_wrapper.cc"],
    hdrs = ["pybind11_protobuf_wrapper.h"],
    deps = [
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//py/arolla/py_utils",
        "@com_google_protobuf//:protobuf",
        "@pybind11_protobuf//pybind11_protobuf:native_proto_caster",
    ],
)

koladata_py_extension(
    name = "py_misc_py_ext",
    srcs = ["py_misc_module.cc"],
    deps = [
        ":py_misc_cc_lib",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

py_library(
    name = "literal_operator",
    srcs = ["literal_operator.py"],
    deps = [
        ":py_misc_py_ext",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "literal_operator_test",
    srcs = ["literal_operator_test.py"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":literal_operator",
        ":qtypes",
        "//py:python_path",
        "//py/koladata/expr:view",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/operators:kde_operators",
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
        ":data_slice_py_ext",
        ":py_misc_py_ext",
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_test(
    name = "data_bag_test",
    srcs = ["data_bag_test.py"],
    deps = [
        ":data_bag",
        ":data_item",
        ":data_slice",
        ":jagged_shape",
        ":schema_constants",
        "//py:python_path",
        "//py/koladata/functions/tests:test_py_pb2",
        "//py/koladata/operators:kde_operators",
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
    deps = [
        ":data_slice",
        ":jagged_shape",
        ":schema_constants",
        "//py:python_path",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "ellipsis",
    srcs = ["ellipsis.py"],
    deps = [
        "//koladata/s11n",
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "mask_constants",
    srcs = ["mask_constants.py"],
    deps = [
        ":data_item",
        ":schema_constants",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "general_eager_ops",
    srcs = ["general_eager_ops.py"],
    deps = [
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "iterable_qvalue",
    srcs = ["iterable_qvalue.py"],
    deps = [
        ":py_boxing",
        ":qtypes",
        "//py:python_path",
        "//py/koladata/operators:kde_operators",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "iterable_qvalue_test",
    srcs = ["iterable_qvalue_test.py"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":iterable_qvalue",
        ":py_boxing",
        ":qtypes",
        "//py:python_path",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "extension_type_registry",
    srcs = ["extension_type_registry.py"],
    deps = [
        ":py_boxing",
        "//py:python_path",
        requirement("bidict"),
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/derived_qtype",
        "@com_google_arolla//py/arolla/objects",
    ],
)

py_test(
    name = "extension_type_registry_test",
    srcs = ["extension_type_registry_test.py"],
    deps = [
        ":data_bag",
        ":data_slice",
        ":extension_type_registry",
        ":py_boxing",
        ":qtypes",
        ":schema_constants",
        "//py:python_path",
        "//py/koladata/expr:input_container",
        "//py/koladata/functions",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/derived_qtype",
        "@com_google_arolla//py/arolla/objects",
    ],
)

py_test(
    name = "general_eager_ops_test",
    srcs = ["general_eager_ops_test.py"],
    deps = [
        ":data_slice",
        ":general_eager_ops",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "mask_constants_test",
    srcs = ["mask_constants_test.py"],
    deps = [
        ":data_item",
        ":mask_constants",
        ":schema_constants",
        "//py:python_path",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

py_test(
    name = "ellipsis_test",
    srcs = ["ellipsis_test.py"],
    deps = [
        ":ellipsis",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "py_misc_test",
    srcs = ["py_misc_test.py"],
    deps = [
        ":data_slice",
        ":jagged_shape",
        ":py_misc_py_ext",
        "//py:python_path",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "merge_test",
    srcs = ["merge_test.py"],
    deps = [
        ":data_bag",
        ":data_slice",
        "//py:python_path",
        "//py/koladata/operators:kde_operators",
        "//py/koladata/testing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)
