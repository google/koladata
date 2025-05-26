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

# Arolla Expr Operators on Koda abstractions.

load(
    "@com_google_arolla//py/arolla/optools:optools.bzl",
    "arolla_cc_operator_package",
    "arolla_operator_package_snapshot",
)
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_py_extension")

default_visibility = [
    "//koladata:internal",
]

package(default_visibility = default_visibility)

licenses(["notice"])

# Base Koda Expr operators available in C++.
#
# Depending on this target will make the operators available to call and evaluate, but will not give
# Python-specific addons like custom repr. You need to also depend on their QExpr implementations in
# //koladata/operators and //koladata/functor:operators.
#
# In Python use `import kde_operators` instead.
arolla_cc_operator_package(
    name = "base_cc_operators",
    arolla_initializer = {
        "name": "arolla_operators/koda_base",
        "deps": [
            "arolla_operators/jagged_shape",
            "arolla_operators/koda_internal",
            "arolla_operators/koda_iterables",
        ],
    },
    snapshot = ":operator_package.pb2",
    visibility = ["//visibility:public"],
    deps = [
        "//koladata/expr:init",
        "//koladata/iterables:init_expr_operators",
        "//koladata/iterables:operators",
        "//koladata/operators",
        "//koladata/s11n",
        "@com_google_arolla//arolla/jagged_shape/expr",
        "@com_google_arolla//arolla/qexpr/operators/all:operators_on_dense_arrays",
    ],
)

# Extra Koda Expr operators available in C++.
#
# Depending on this target will make the operators available to call and evaluate, but will not give
# Python-specific addons like custom repr. You need to also depend on their QExpr implementations in
# //koladata/iterables:operators and //koladata/functor/parallel:operators.
#
# In Python use `import kde_operators` instead.
arolla_cc_operator_package(
    name = "extra_cc_operators",
    arolla_initializer = {
        "name": "arolla_operators/extra",
        "deps": [
            "arolla_operators/koda_base",
            "arolla_operators/koda_functor_parallel",
        ],
    },
    snapshot = ":extra_operator_package.pb2",
    visibility = ["//visibility:public"],
    deps = [
        ":base_cc_operators",
        "//koladata/functor/parallel:init_expr_operators",
        "//koladata/functor/parallel:operators",
        "//koladata/s11n",
        "@com_google_arolla//arolla/qexpr/operators/all:operators_on_dense_arrays",
    ],
)

# All Koda Expr operators, including the Python-specific ones.
# (You need to also depend on their QExpr implementations in
# //koladata:all_qexpr_operators.)
py_library(
    name = "kde_operators",
    srcs = ["kde_operators.py"],
    deps = [
        ":base_cc_operators_lib",
        ":extra_cc_operators_lib",
        ":koda_internal_parallel",
        ":optools",
        ":py",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

arolla_operator_package_snapshot(
    name = "operator_package.pb2",
    imports = [
        "koladata.operators.base_cc_operators",
    ],
    preimports = [
        "arolla.jagged_shape.jagged_shape",
        "koladata.types.data_slice",
    ],
    visibility = ["//visibility:private"],
    deps = [
        ":base_cc_operators_lib",
    ],
)

arolla_operator_package_snapshot(
    name = "extra_operator_package.pb2",
    imports = [
        "koladata.operators.extra_cc_operators",
    ],
    preimports = [
        "arolla.jagged_shape.jagged_shape",
        "koladata.types.data_slice",
        "koladata.operators.base_cc_operators",
    ],
    visibility = ["//visibility:private"],
    deps = [
        ":base_cc_operators_lib",
        ":extra_cc_operators_lib",
    ],
)

py_library(
    name = "base_cc_operators_lib",
    srcs = ["base_cc_operators.py"],
    visibility = ["//visibility:private"],
    deps = [
        ":allocation",
        ":annotation",
        ":assertion",
        ":bags",
        ":bootstrap",
        ":comparison",
        ":core",
        ":dicts",
        ":entities",
        ":functor",
        ":ids",
        ":iterables",
        ":jagged_shape",
        ":json",
        ":koda_internal",
        ":koda_internal_iterables",
        ":lists",
        ":masking",
        ":math",
        ":objs",
        ":proto",
        ":random",
        ":schema",
        ":slices",
        ":strings",
        ":tuple",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "extra_cc_operators_lib",
    srcs = ["extra_cc_operators.py"],
    visibility = ["//visibility:private"],
    deps = [
        ":koda_internal_parallel",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

koladata_py_extension(
    name = "py_optools_py_ext",
    srcs = [
        "py_optools_module.cc",
        "py_unified_binding_policy.cc",
        "py_unified_binding_policy.h",
    ],
    deps = [
        "//koladata/expr:expr_operators",
        "//koladata/expr:non_determinism",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:inlined_vector",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/random",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
        "@rules_python//python/cc:current_py_cc_headers",  # buildcleaner: keep
    ],
)

py_library(
    name = "unified_binding_policy",
    srcs = ["unified_binding_policy.py"],
    deps = [
        ":py_optools_py_ext",
        "//py:python_path",
        "//py/koladata/types:py_boxing",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "unified_binding_policy_test",
    srcs = ["unified_binding_policy_test.py"],
    deps = [
        ":op_repr",
        ":unified_binding_policy",
        "//py:python_path",
        "//py/koladata/testing",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "optools",
    srcs = ["optools.py"],
    deps = [
        ":op_repr",
        ":qtype_utils",
        ":unified_binding_policy",
        "//py:python_path",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "optools_test",
    srcs = ["optools_test.py"],
    deps = [
        ":comparison",
        ":jagged_shape",
        ":koda_internal",
        ":math",
        ":optools",
        ":optools_test_utils",
        ":qtype_utils",
        ":tuple",
        "//py:python_path",
        "//py/koladata/expr:expr_eval",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
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
    deps = [
        ":bootstrap",
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
        ":koda_internal_parallel",
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
        "//py/koladata/types:jagged_shape",
        "@com_google_arolla//py/arolla",
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
        ":jagged_shape",
        ":masking",
        ":optools",
        ":qtype_utils",
        ":slices",
        "//koladata/operators",
        "//py:python_path",
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
        ":optools",
        ":qtype_utils",
        ":view_overloads",
        "//py:python_path",
        "//py/koladata/types:py_boxing",
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
        ":optools",
        ":qtype_utils",
        ":view_overloads",
        "//koladata/operators",
        "//py:python_path",
        "//py/koladata/types:data_slice",
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
        ":unified_binding_policy",
        "//py:python_path",
        "//py/koladata/types:data_slice",
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
        "//py/koladata/types:py_boxing",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "view_overloads",
    srcs = ["view_overloads.py"],
    deps = [
        ":op_repr",
        ":optools",
        "//py:python_path",
        "@com_google_arolla//py/arolla",
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
    name = "dicts",
    srcs = ["dicts.py"],
    deps = [
        ":core",
        ":jagged_shape",
        ":optools",
        ":qtype_utils",
        ":view_overloads",
        "//py:python_path",
        "//py/koladata/types:data_slice",
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
        ":optools",
        ":qtype_utils",
        ":view_overloads",
        "//py:python_path",
        "//py/koladata/types:py_boxing",
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
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "iterables",
    srcs = ["iterables.py"],
    deps = [
        ":bootstrap",
        ":koda_internal_iterables",
        ":optools",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "bootstrap",
    srcs = ["bootstrap.py"],
    deps = [
        "//py:python_path",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "koda_internal_parallel",
    srcs = ["koda_internal_parallel.py"],
    deps = [
        ":assertion",
        ":bootstrap",
        ":masking",
        ":optools",
        ":qtype_utils",
        ":schema",
        ":slices",
        ":tuple",
        "//koladata/functor/parallel:init_expr_operators",
        "//koladata/functor/parallel:operators",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
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
