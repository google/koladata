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

# Type checker for Koda functions.

load("@rules_python//python:defs.bzl", "py_library", "py_test")

package(default_visibility = [
    "//koladata:internal",
])

py_library(
    name = "type_checking",
    srcs = ["type_checking.py"],
    deps = [
        "//py/koladata/expr:tracing_mode",
        "//py/koladata/operators:eager_op_utils",
        "//py/koladata/types:data_item",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:py_boxing",
        "//py/koladata/types:schema_item",
        "@com_google_arolla//py/arolla",
    ],
)

py_test(
    name = "type_checking_test",
    srcs = ["type_checking_test.py"],
    deps = [
        ":type_checking",
        "//py/koladata:kd",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "@com_google_absl_py//absl/testing:absltest",
    ],
)
