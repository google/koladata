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

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_python//python:defs.bzl", "py_library", "py_test")
load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_pybind_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "to_py",
    srcs = ["to_py.cc"],
    hdrs = ["to_py.h"],
    deps = [
        ":dataclasses_util",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata/internal:data_bag",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/internal/op_utils:extract",
        "//koladata/internal/op_utils:traverser",
        "//py/koladata/base:to_py_object",
        "//py/koladata/base:wrap_utils",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/py_utils",
        "@rules_python//python/cc:current_py_cc_headers",
    ],
)

cc_library(
    name = "from_py",
    srcs = ["from_py.cc"],
    hdrs = ["from_py.h"],
    deps = [
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata:object_factories",
        "//koladata/internal:casting",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:schema_attrs",
        "//koladata/internal:schema_utils",
        "//py/koladata/base:boxing",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/util:status_backport",
        "@rules_python//python/cc:current_py_cc_headers",
    ],
)

py_library(
    name = "dataclasses_py_util",
    srcs = ["dataclasses_util.py"],
    deps = [
        "//py:python_path",
    ],
)

py_test(
    name = "dataclasses_py_util_test",
    srcs = ["dataclasses_py_util_test.py"],
    deps = [
        ":dataclasses_py_util",
        "//py:python_path",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)

cc_library(
    name = "dataclasses_util",
    srcs = ["dataclasses_util.cc"],
    hdrs = ["dataclasses_util.h"],
    deps = [
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//py/arolla/py_utils",
        "@rules_python//python/cc:current_py_cc_headers",
    ],
)

koladata_pybind_extension(
    name = "clib",
    srcs = ["clib.cc"],
    deps = [
        ":from_py",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//py/koladata/base:wrap_utils",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//py/arolla/abc:pybind11_utils",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)

koladata_pybind_extension(
    name = "testing_clib",
    testonly = True,
    srcs = ["testing_clib.cc"],
    deps = [
        ":dataclasses_util",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//py/arolla/abc:pybind11_utils",
        "@com_google_arolla//py/arolla/py_utils",
        "@pybind11_abseil//pybind11_abseil:absl_casters",
    ],
)

py_test(
    name = "dataclasses_util_test",
    srcs = ["dataclasses_util_test.py"],
    deps = [
        ":testing_clib",
        "//py:python_path",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)
