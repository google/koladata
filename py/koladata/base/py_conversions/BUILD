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
        "//koladata/internal:dtype",
        "//koladata/internal:schema_utils",
        "//koladata/internal/op_utils:extract",
        "//koladata/internal/op_utils:traverser",
        "//py/koladata/base:to_py_object",
        "//py/koladata/types:wrap_utils",
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
    ],
)

py_library(
    name = "dataclasses_py_util",
    srcs = ["dataclasses_util.py"],
)

py_test(
    name = "dataclasses_py_util_test",
    srcs = ["dataclasses_py_util_test.py"],
    deps = [
        ":dataclasses_py_util",
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
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
        "//py:python_path",  # Adds //py to the path to allow convenient imports.
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)
