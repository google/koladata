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

# Utlities for Koda Expr(s).

load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/koladata:py_extension.bzl", "py_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "input_container",
    srcs = ["input_container.py"],
    deps = [
        "//koladata/expr:expr_operators",
        "//koladata/expr:expr_operators_repr",
        "//koladata/operators:compile_expr_operators",
        "//py/koladata/types:literal_operator",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "input_container_test",
    srcs = ["input_container_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":expr_eval",
        ":input_container",
        ":view",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:literal_operator",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "view",
    srcs = ["view.py"],
    deps = [
        ":expr_eval",
        ":input_container",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:qtypes",
        "@com_google_arolla//py/arolla",
    ],
)

py_library(
    name = "expr_eval",
    srcs = ["expr_eval.py"],
    deps = [
        ":input_container",
        ":py_expr_eval_py_ext",
        "//py/koladata/types:py_boxing",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "view_test",
    srcs = ["view_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":input_container",
        ":view",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:ellipsis",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "expr_eval_test",
    srcs = ["expr_eval_test.py"],
    tags = ["nooss"],  # TODO: b/352697900 - Remove once we support operators in OSS.
    deps = [
        ":expr_eval",
        ":input_container",
        ":py_expr_eval_py_ext",
        ":view",
        "//py/koladata/functions",
        "//py/koladata/testing",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:ellipsis",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
    ],
)

cc_library(
    name = "py_expr_eval_cc_lib",
    srcs = ["py_expr_eval.cc"],
    hdrs = ["py_expr_eval.h"],
    deps = [
        "//koladata:data_slice_qtype",
        "//koladata/expr:expr_eval",
        "//koladata/expr:expr_operators",
        "//py/koladata/exceptions:py_exception_utils",
        "//py/koladata/types:py_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/abc:py_abc",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

py_extension(
    name = "py_expr_eval_py_ext",
    srcs = ["py_expr_eval_module.cc"],
    deps = [":py_expr_eval_cc_lib"],
)
