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

# Arolla Expr Operators on Koda abstractions.

load("@com_google_arolla//py/arolla/dynamic_deps:build_defs.bzl", "arolla_py_cc_deps")
load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_pybind_extension")
load("//py/koladata/operators:optools.bzl", "koladata_cc_operator_package")

package(default_visibility = ["//visibility:private"])

licenses(["notice"])

# Base Koda Expr operators available in C++.
#
# Depending on this target will make the operators available to call and evaluate, but will not give
# Python-specific addons like custom repr. You need to also depend on their QExpr implementations in
# //koladata/operators and //koladata/functor:operators.
#
# In Python use `import kde_operators` instead.
koladata_cc_operator_package(
    name = "cc_operators",
    arolla_initializer = {
        "name": "arolla_operators/koda_base",
        "deps": [
            "arolla_operators/jagged_shape",
            "arolla_operators/derived_qtype",
            "arolla_operators/objects",
            "arolla_operators/koda_internal",
            "arolla_operators/koda_iterables",
            "arolla_operators/koda_functor_parallel",
        ],
    },
    imports = [
        "koladata.operators.build_cc_operators",
    ],
    preimports = [
        "arolla.jagged_shape.jagged_shape",
        "arolla.objects.objects",
        "arolla.derived_qtype.derived_qtype",
        "koladata.types.data_slice",
    ],
    tool_deps = [
        ":build_cc_operators",
        "@com_google_arolla//py/arolla/derived_qtype",
        "@com_google_arolla//py/arolla/jagged_shape",
        "@com_google_arolla//py/arolla/objects",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "//koladata/expr:init",
        "//koladata/functor/parallel:init_expr_operators",
        "//koladata/functor/parallel:operators",
        "//koladata/iterables:init_expr_operators",
        "//koladata/iterables:operators",
        "//koladata/operators",
        "//koladata/s11n",
        "@com_google_arolla//arolla/derived_qtype/operators",
        "@com_google_arolla//arolla/jagged_shape/expr",
        "@com_google_arolla//arolla/objects/operators",
        "@com_google_arolla//arolla/qexpr/operators/all:operators_on_dense_arrays",
    ],
)

arolla_py_cc_deps(
    name = "cc_operators_py_clib",
    dynamic_deps = [
        "//py/koladata/dynamic_deps:koladata_so",
    ],
    deps = [
        ":cc_operators",
    ],
)

# All Koda Expr operators, including the Python-specific ones.
# (You need to also depend on their QExpr implementations in
# //koladata:all_qexpr_operators.)
py_library(
    name = "kde_operators",
    srcs = ["kde_operators.py"],
    visibility = [
        "//koladata:internal",
    ],
    deps = [
        ":cc_operators_py_clib",
        ":functor",
        ":operators_in_cc_operator_package",
        ":py",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "bitwise",
    srcs = ["bitwise.py"],
    deps = [
        ":optools",
        ":qtype_utils",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "operators_in_cc_operator_package",
    srcs = ["operators_in_cc_operator_package.py"],
    deps = [
        ":allocation",
        ":annotation",
        ":arolla_bridge",
        ":assertion",
        ":bags",
        ":bitwise",
        ":comparison",
        ":core",
        ":curves",
        ":dicts",
        ":entities",
        ":extension_types",
        ":functor",
        ":ids",
        ":iterables",
        ":jagged_shape",
        ":json",
        ":koda_internal",
        ":koda_internal_iterables",
        ":koda_internal_parallel",
        ":lists",
        ":masking",
        ":math",
        ":objs",
        ":proto",
        ":random",
        ":schema",
        ":slices",
        ":streams",
        ":strings",
        ":tuple",
        ":view_overloads",
        "//py:python_path",
    ],
)

py_library(
    name = "build_cc_operators",
    srcs = ["build_cc_operators.py"],
    deps = [
        ":operators_in_cc_operator_package",
        ":optools",
        "//py:python_path",
    ],
)

koladata_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    deps = [
        ":py_unified_binding_policy",
        "//koladata/expr:non_determinism",
        "@com_google_absl//absl/strings:string_view",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
    ],
)

cc_library(
    name = "py_unified_binding_policy",
    srcs = ["py_unified_binding_policy.cc"],
    hdrs = ["py_unified_binding_policy.h"],
    deps = [
        "//koladata/expr:expr_operators",
        "//koladata/expr:non_determinism",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
        "@rules_python//python/cc:current_py_cc_headers",
    ],
)

py_library(
    name = "unified_binding_policy",
    srcs = ["unified_binding_policy.py"],
    deps = [
        ":clib",
        "//py:python_path",
        "//py/koladata/types:py_boxing",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "unified_binding_policy_test",
    srcs = ["unified_binding_policy_test.py"],
    deps = [
        ":aux_policies",
        ":clib",
        ":op_repr",
        ":unified_binding_policy",
        "//py:python_path",
        "//py/koladata/testing",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "classic_binding_policy_test",
    srcs = ["classic_binding_policy_test.py"],
    deps = [
        ":aux_policies",
        ":kde_operators",
        "//py:python_path",
        "//py/koladata/expr:input_container",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:ellipsis",
        "//py/koladata/types:literal_operator",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "optools",
    srcs = ["optools.py"],
    visibility = ["//koladata:internal"],
    deps = [
        ":aux_policies",
        ":op_repr",
        ":qtype_utils",
        ":unified_binding_policy",
        "//py:python_path",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:tracing_mode",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "optools_test",
    srcs = ["optools_test.py"],
    deps = [
        ":aux_policies",
        ":comparison",
        ":jagged_shape",
        ":koda_internal",
        ":math",
        ":optools",
        ":qtype_utils",
        ":tuple",
        "//py:python_path",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "qtype_utils",
    srcs = ["qtype_utils.py"],
    visibility = [
        "//koladata:internal",
    ],
    deps = [
        "//koladata/functor/parallel:init_expr_operators",
        "//koladata/operators",
        "//py:python_path",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_test(
    name = "qtype_utils_test",
    srcs = ["qtype_utils_test.py"],
    deps = [
        ":kde_operators",
        ":qtype_utils",
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:iterable_qvalue",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "assertion",
    srcs = ["assertion.py"],
    deps = [
        ":arolla_bridge",
        ":optools",
        ":qtype_utils",
        "//koladata/operators",
        "//py:python_path",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "allocation",
    srcs = ["allocation.py"],
    deps = [
        ":jagged_shape",
        ":optools",
        ":qtype_utils",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "arolla_bridge",
    srcs = ["arolla_bridge.py"],
    deps = [
        ":optools",
        ":qtype_utils",
        "//koladata/operators",
        "//py:python_path",
        "//py/koladata/expr:view",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/derived_qtype",
    ],
)

py_library(
    name = "ids",
    srcs = ["ids.py"],
    deps = [
        ":arolla_bridge",
        ":jagged_shape",
        ":optools",
        ":qtype_utils",
        ":schema",
        "//koladata/operators",
        "//py:python_path",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "schema",
    srcs = ["schema.py"],
    deps = [
        ":jagged_shape",
        ":masking",
        ":optools",
        ":qtype_utils",
        "//koladata/operators",
        "//py:python_path",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "math",
    srcs = ["math.py"],
    deps = [
        ":jagged_shape",
        ":op_repr",
        ":optools",
        ":qtype_utils",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "comparison",
    srcs = ["comparison.py"],
    deps = [
        ":masking",
        ":op_repr",
        ":optools",
        ":qtype_utils",
        "//koladata/operators",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "strings",
    srcs = ["strings.py"],
    deps = [
        ":arolla_bridge",
        ":assertion",
        ":aux_policies",
        ":jagged_shape",
        ":masking",
        ":optools",
        ":qtype_utils",
        ":slices",
        "//koladata/operators",
        "//py:python_path",
        "//py/koladata/expr:view",
        "//py/koladata/fstring",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "tuple",
    srcs = ["tuple.py"],
    deps = [
        ":arolla_bridge",
        ":aux_policies",
        ":optools",
        ":qtype_utils",
        ":view_overloads",
        "//py:python_path",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "core",
    srcs = ["core.py"],
    deps = [
        ":arolla_bridge",
        ":assertion",
        ":aux_policies",
        ":bags",
        ":entities",
        ":masking",
        ":op_repr",
        ":optools",
        ":qtype_utils",
        ":schema",
        ":tuple",
        ":view_overloads",
        "//koladata/operators",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "lists",
    srcs = ["lists.py"],
    deps = [
        ":arolla_bridge",
        ":core",
        ":jagged_shape",
        ":koda_internal",
        ":op_repr",
        ":optools",
        ":qtype_utils",
        ":view_overloads",
        "//koladata/operators",
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "masking",
    srcs = ["masking.py"],
    deps = [
        ":assertion",
        ":jagged_shape",
        ":op_repr",
        ":optools",
        ":qtype_utils",
        "//koladata/operators",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "jagged_shape",
    srcs = ["jagged_shape.py"],
    deps = [
        ":arolla_bridge",
        ":assertion",
        ":optools",
        ":qtype_utils",
        "//koladata/operators",
        "//py:python_path",
        "//py/koladata/expr:view",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "objs",
    srcs = ["objs.py"],
    deps = [
        ":jagged_shape",
        ":optools",
        ":qtype_utils",
        "//koladata/operators",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "entities",
    srcs = ["entities.py"],
    deps = [
        ":jagged_shape",
        ":optools",
        ":qtype_utils",
        ":schema",
        "//koladata/operators",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "random",
    srcs = ["random.py"],
    deps = [
        ":allocation",
        ":arolla_bridge",
        ":assertion",
        ":core",
        ":ids",
        ":jagged_shape",
        ":lists",
        ":masking",
        ":optools",
        ":qtype_utils",
        ":schema",
        ":slices",
        "//koladata/operators",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "py",
    srcs = ["py.py"],
    deps = [
        ":optools",
        ":qtype_utils",
        ":schema",
        ":slices",
        "//py:python_path",
        "//py/koladata/base/py_conversions:clib",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "slices",
    srcs = ["slices.py"],
    deps = [
        ":arolla_bridge",
        ":assertion",
        ":aux_policies",
        ":comparison",
        ":jagged_shape",
        ":masking",
        ":math",
        ":op_repr",
        ":optools",
        ":qtype_utils",
        ":schema",
        ":view_overloads",
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:py_misc_py_ext",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "koda_internal",
    srcs = ["koda_internal.py"],
    deps = [
        ":optools",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "kde_operators_test",
    srcs = ["kde_operators_test.py"],
    deps = [
        ":kde_operators",
        "//py:python_path",
        "//py/koladata/functions",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "eager_op_utils",
    srcs = ["eager_op_utils.py"],
    visibility = [
        "//koladata:internal",
    ],
    deps = [
        ":kde_operators",
        "//py:python_path",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/abc:py_abc",
    ],
)

py_library(
    name = "op_repr",
    srcs = ["op_repr.py"],
    deps = [
        ":aux_policies",
        ":unified_binding_policy",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "functor",
    srcs = ["functor.py"],
    deps = [
        ":arolla_bridge",
        ":assertion",
        ":dicts",
        ":iterables",
        ":koda_internal_iterables",
        ":lists",
        ":masking",
        ":objs",
        ":op_repr",
        ":optools",
        ":qtype_utils",
        ":schema",
        ":slices",
        ":tuple",
        "//koladata/functor:operators",
        "//py:python_path",
        "//py/koladata/base:py_functors_base_py_ext",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:mask_constants",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "//py/koladata/types:signature_utils",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "annotation",
    srcs = ["annotation.py"],
    deps = [
        ":optools",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "view_overloads",
    srcs = ["view_overloads.py"],
    visibility = [
        "//koladata:internal",
    ],
    deps = [
        ":op_repr",
        ":optools",
        "//py:python_path",
    ],
)

py_library(
    name = "optools_test_utils",
    testonly = True,
    srcs = ["optools_test_utils.py"],
    deps = [
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "curves",
    srcs = ["curves.py"],
    deps = [
        ":optools",
        ":qtype_utils",
        "//py:python_path",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "dicts",
    srcs = ["dicts.py"],
    deps = [
        ":arolla_bridge",
        ":core",
        ":jagged_shape",
        ":op_repr",
        ":optools",
        ":qtype_utils",
        ":slices",
        ":view_overloads",
        "//py:python_path",
        "//py/koladata/expr:view",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "json",
    srcs = ["json.py"],
    deps = [
        ":optools",
        ":qtype_utils",
        ":view_overloads",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "bags",
    srcs = ["bags.py"],
    deps = [
        ":aux_policies",
        ":optools",
        ":qtype_utils",
        ":view_overloads",
        "//py:python_path",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "proto",
    srcs = ["proto.py"],
    deps = [
        ":optools",
        ":qtype_utils",
        ":view_overloads",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "koda_internal_iterables",
    srcs = ["koda_internal_iterables.py"],
    deps = [
        ":arolla_bridge",
        ":jagged_shape",
        ":math",
        ":optools",
        ":qtype_utils",
        ":random",
        ":schema",
        ":slices",
        "//koladata/iterables:init_expr_operators",
        "//koladata/iterables:operators",
        "//py:python_path",
        "//py/koladata/expr:view",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/derived_qtype",
    ],
)

py_library(
    name = "iterables",
    srcs = ["iterables.py"],
    deps = [
        ":koda_internal_iterables",
        ":optools",
        ":qtype_utils",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "streams",
    srcs = ["streams.py"],
    deps = [
        ":koda_internal_parallel",
        ":optools",
        ":qtype_utils",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "koda_internal_parallel",
    srcs = ["koda_internal_parallel.py"],
    deps = [
        ":assertion",
        ":core",
        ":functor",
        ":iterables",
        ":jagged_shape",
        ":koda_internal_functor",
        ":koda_internal_iterables",
        ":masking",
        ":optools",
        ":proto",
        ":qtype_utils",
        ":schema",
        ":slices",
        ":tuple",
        ":view_overloads",
        "//koladata/functor/parallel:init_expr_operators",
        "//koladata/functor/parallel:operators",
        "//koladata/functor/parallel:transform_config_py_proto",
        "//py:python_path",
        "//py/koladata/base:py_functors_base_py_ext",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:introspection",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/expr:view",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "//py/koladata/types:signature_utils",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/derived_qtype",
        "@com_google_protobuf//:protobuf_python",
    ],
)

py_library(
    name = "koda_internal_functor",
    srcs = ["koda_internal_functor.py"],
    deps = [
        ":optools",
        "//py:python_path",
        "//py/koladata/types:qtypes",
    ],
)

py_library(
    name = "extension_types",
    srcs = ["extension_types.py"],
    deps = [
        ":arolla_bridge",
        ":optools",
        "//py:python_path",
        "//py/koladata/expr:view",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/derived_qtype",
        "@com_google_arolla//py/arolla/objects",
    ],
)

py_library(
    name = "aux_policies",
    srcs = ["aux_policies.py"],
    deps = [
        ":clib",
        "//py:python_path",
        "//py/koladata/expr:view",
        "//py/koladata/types:py_boxing",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "aux_policies_test",
    srcs = ["aux_policies_test.py"],
    deps = [
        ":aux_policies",
        "//py:python_path",
        "//py/koladata/expr:view",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)

py_test(
    name = "eager_op_utils_test",
    srcs = ["eager_op_utils_test.py"],
    deps = [
        ":eager_op_utils",
        ":optools",
        "//py:python_path",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "op_repr_test",
    srcs = ["op_repr_test.py"],
    deps = [
        ":kde_operators",
        ":op_repr",
        "//py:python_path",
        "//py/koladata/expr:input_container",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

cc_test(
    name = "operator_package_test",
    srcs = ["operator_package_test.cc"],
    deps = [
        ":cc_operators",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)
