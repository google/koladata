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

# Package for utilities and fakes used in testing.

load(
    "@com_google_arolla//arolla/codegen/qexpr:register_operator.bzl",
    "operator_libraries",
    "operator_overload_list",
)
load("@rules_python//python:defs.bzl", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_py_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "test_invalid_unicode_op",
    testonly = True,
    hdrs = ["test_invalid_unicode_op.h"],
    deps = [
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
    ],
)

operator_libraries(
    name = "operator_test_invalid_unicode_op",
    testonly = True,
    operator_name = "test.invalid_unicode_op",
    overloads = operator_overload_list(
        hdrs = ["test_invalid_unicode_op.h"],
        arg_lists = [[]],
        op_class = "::koladata::ops::testing::InvalidUnicodeOp",
        deps = [":test_invalid_unicode_op"],
    ),
)

koladata_py_extension(
    name = "py_utils_py_ext",
    testonly = True,
    srcs = ["py_utils_module.cc"],
    deps = [
        "//py/koladata/types:py_utils",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/log:check",
    ],
)

py_test(
    name = "py_utils_py_ext_test",
    srcs = ["py_utils_py_ext_test.py"],
    deps = [
        ":py_utils_py_ext",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "@com_google_absl_py//absl/testing:absltest",
    ],
)
