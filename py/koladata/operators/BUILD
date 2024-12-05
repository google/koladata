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

default_visibility = [
    "//quality/arolla/koladata:__subpackages__",
    "//koladata:internal",
]

# TODO(b/379873449); remove once we have a public API to define user operators.
user_ops_visibility = [
    "//quality/arolla/projects/rankchain/experimental/koda_v1/llm:__subpackages__",
    "//quality/arolla/doc:__subpackages__",
]

package(default_visibility = default_visibility)

licenses(["notice"])

# The package allows to use operators defined in Python from C++.
arolla_cc_operator_package(
    name = "operators",
    arolla_initializer = {
        "deps": [
            "arolla_operators/jagged_shape",
            "arolla_operators/koda_internal",
        ],
    },
    snapshot = ":operator_package.pb2",
    deps = [
        "//koladata/expr:init",
        "@com_google_arolla//arolla/jagged_shape/expr",
    ],
)

arolla_operator_package_snapshot(
    name = "operator_package.pb2",
    imports = [
        "koladata.operators.allocation",
        "koladata.operators.arolla_bridge",
        "koladata.operators.assertion",
        "koladata.operators.comparison",
        "koladata.operators.core",
        "koladata.operators.jagged_shape",
        "koladata.operators.logical",
        "koladata.operators.math",
        "koladata.operators.random",
        "koladata.operators.strings",
        "koladata.operators.uuid",
    ],
    preimports = [
        "arolla.jagged_shape.jagged_shape",
        "koladata.types.data_slice",
    ],
    deps = [
        ":allocation",
        ":assertion",
        ":comparison",
        ":core",
        ":jagged_shape",
        ":logical",
        ":math",
        ":object_factories",
        ":predicates",
        ":random",
        ":schema",
        ":strings",
        ":tuple",
        ":uuid",
    ],
)

py_library(
    name = "unified_binding_policy",
    srcs = ["unified_binding_policy.py"],
    visibility = default_visibility + user_ops_visibility,
    deps = [
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
    visibility = default_visibility + user_ops_visibility,
    deps = [
        ":op_repr",
        ":qtype_utils",
        ":unified_binding_policy",
        "//py/koladata/expr:input_container",
        "//py/koladata/expr:view",
        "//py/koladata/types:py_boxing",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "optools_test",
    srcs = ["optools_test.py"],
    deps = [
        ":comparison",
        ":core",
        ":jagged_shape",
        ":optools",
        ":optools_test_utils",
        ":qtype_utils",
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
        "//koladata/operators",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "qtype_utils_test",
    srcs = ["qtype_utils_test.py"],
    deps = [
        ":kde_operators",
        ":qtype_utils",
        "//py/koladata/expr:py_expr_eval_py_ext",
        "//py/koladata/types:data_bag",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:jagged_shape",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
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
        "//py/koladata/types:py_boxing",
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
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
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
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "predicates",
    srcs = ["predicates.py"],
    deps = [
        ":optools",
        ":qtype_utils",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "uuid",
    srcs = ["uuid.py"],
    deps = [
        ":optools",
        ":qtype_utils",
        "//koladata/operators",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "object_factories",
    srcs = ["object_factories.py"],
    deps = [
        ":optools",
        ":qtype_utils",
        ":schema",
        "//koladata/operators",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "schema",
    srcs = ["schema.py"],
    deps = [
        ":assertion",
        ":jagged_shape",
        ":optools",
        ":qtype_utils",
        "//koladata/operators",
        "//py/koladata/types:py_boxing",
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
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "comparison",
    srcs = ["comparison.py"],
    deps = [
        ":logical",
        ":op_repr",
        ":optools",
        ":qtype_utils",
        "//koladata/operators",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "strings",
    srcs = ["strings.py"],
    deps = [
        ":jagged_shape",
        ":logical",
        ":optools",
        ":qtype_utils",
        "//koladata/operators",
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
        ":functor",
        ":jagged_shape",
        ":logical",
        ":math",
        ":op_repr",
        ":optools",
        ":qtype_utils",
        ":schema",
        ":tuple",
        ":view_overloads",
        "//koladata/operators",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "logical",
    srcs = ["logical.py"],
    deps = [
        ":assertion",
        ":jagged_shape",
        ":op_repr",
        ":optools",
        ":qtype_utils",
        ":schema",
        "//koladata/operators",
        "//py/koladata/types:qtypes",
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
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:qtypes",
        "//py/koladata/types:schema_constants",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
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
        ":jagged_shape",
        ":logical",
        ":optools",
        ":qtype_utils",
        ":schema",
        "//koladata/operators",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/jagged_shape",
    ],
)

py_library(
    name = "py",
    srcs = ["py.py"],
    deps = [
        ":core",
        ":logical",
        ":optools",
        ":qtype_utils",
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
    name = "kde_operators",
    srcs = ["kde_operators.py"],
    deps = [
        ":allocation",
        ":annotation",
        ":assertion",
        ":comparison",
        ":core",
        ":functor",
        ":jagged_shape",
        ":logical",
        ":math",
        ":object_factories",
        ":predicates",
        ":py",
        ":random",
        ":schema",
        ":strings",
        ":tuple",
        ":uuid",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "kde_operators_test",
    srcs = ["kde_operators_test.py"],
    deps = [
        ":kde_operators",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "eager_op_utils",
    srcs = ["eager_op_utils.py"],
    visibility = default_visibility + user_ops_visibility,
    deps = [
        ":kde_operators",
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
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "functor",
    srcs = ["functor.py"],
    deps = [
        ":optools",
        ":qtype_utils",
        "//koladata/functor:operators",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "annotation",
    srcs = ["annotation.py"],
    deps = [
        ":optools",
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
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "optools_test_utils",
    testonly = True,
    srcs = ["optools_test_utils.py"],
    deps = ["@com_google_arolla//py/arolla"],
)

py_test(
    name = "eager_op_utils_test",
    srcs = ["eager_op_utils_test.py"],
    deps = [
        ":eager_op_utils",
        ":optools",
        ":qtype_utils",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
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
        "//py/koladata/expr:input_container",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)
