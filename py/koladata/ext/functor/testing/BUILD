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

load("@rules_python//python:defs.bzl", "py_binary", "py_library", "py_test")

package(default_visibility = [
    "//koladata:internal",
])

py_library(
    name = "test_cases",
    srcs = glob(["test_cases/*.py"]),
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

py_binary(
    name = "fn_to_py_generator",
    srcs = ["fn_to_py_generator.py"],
    deps = [
        ":test_cases",  # buildcleaner: keep
        "//py/koladata:kd_ext",
        "@com_google_absl_py//absl:app",
        "@com_google_absl_py//absl/flags",
        "@pip_deps//pyink",
    ],
)

# Dynamically find all test cases.
#
# To add a new test case:
# 1. Create a new Python file under `test_cases/` (e.g., `test_cases/my_model.py`).
#    The file MUST define:
#    - `MODEL`: The Koda functor to be converted to Python.
#    - `INPUTS`: A list of dictionaries representing inputs to the functor,
#      used to verify execution correctness. Inputs are passed to the functor
#      as keyword arguments.
TEST_CASES = [
    f[len("test_cases/"):-3]
    for f in glob(["test_cases/*.py"])
    if not f.endswith("__init__.py")
]

# Generate outputs from test cases.
[
    genrule(
        name = "generate_%s_output" % m,
        srcs = ["test_cases/%s.py" % m],
        outs = ["reference/%s.py.output" % m],
        cmd = "$(location :fn_to_py_generator) --model_module=koladata.ext.functor.testing.test_cases.%s --output_file=$@" % m,
        tools = [":fn_to_py_generator"],
    )
    for m in TEST_CASES
]

py_test(
    name = "fn_to_py_execution_test",
    srcs = ["fn_to_py_execution_test.py"],
    data = ["reference/%s.py.output" % m for m in TEST_CASES],
    deps = [
        ":test_cases",  # buildcleaner: keep
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_absl_py//absl/testing:parameterized",
    ],
)
