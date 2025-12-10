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

load("@com_google_arolla//arolla/codegen:utils.bzl", "call_python_function")
load("@rules_cc//cc:cc_test.bzl", "cc_test")
load("@rules_python//python:defs.bzl", "py_library")
load(
    "//py/koladata/serving:serving.bzl",
    "koladata_cc_embedded_slices",
    "koladata_export_dataslice",
    "koladata_parallel_transform",
    "koladata_serialized_slices",
    "koladata_trace_py_fn",
)

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_library(
    name = "test_functors",
    srcs = ["test_functors.py"],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
    ],
)

koladata_serialized_slices(
    name = "serialized_test_functors",
    testonly = True,
    slices = {
        "ask_about_serving": koladata_trace_py_fn("koladata.serving.testing.test_functors.ask_about_serving"),
        "non_deterministic_functor": koladata_trace_py_fn(
            "koladata.serving.testing.test_functors.non_deterministic_functor",
            experimental_deterministic_mode = True,
        ),
    } | {
        # Generating 10 functors to check there is no ordering issue.
        "plus_{}".format(n): call_python_function(
            "koladata.serving.testing.test_functors.make_plus_n_functor",
            args = [n],
            deps = [":test_functors"],
        )
        for n in range(1, 11)
    },
    tool_deps = [":test_functors"],  # Can also come from "deps =" argument of koladata_trace_py_fn above.
)

koladata_serialized_slices(
    name = "other_serialized_test_functors_for_determinism_test",
    testonly = True,
    slices = {
        "non_deterministic_functor": koladata_trace_py_fn(
            "koladata.serving.testing.test_functors.non_deterministic_functor",
            experimental_deterministic_mode = True,
        ),
        "non_deterministic_functor_with_different_name": koladata_trace_py_fn(
            "koladata.serving.testing.test_functors.non_deterministic_functor_with_different_name",
            experimental_deterministic_mode = True,
        ),
    },
    tool_deps = [":test_functors"],  # Can also come from "deps =" argument of koladata_trace_py_fn above.
)

koladata_cc_embedded_slices(
    name = "cc_test_functors",
    testonly = True,
    cc_function_name = "koladata_serving_test::TestFunctors",
    slices = {
        "plus_one": koladata_trace_py_fn("koladata.serving.testing.test_functors.plus_one"),
        "parallel_plus_one": koladata_parallel_transform(koladata_trace_py_fn(
            "koladata.serving.testing.test_functors.plus_one",
        )),
        "ask_about_serving": koladata_trace_py_fn("koladata.serving.testing.test_functors.ask_about_serving"),
        "TEST_DS": koladata_export_dataslice("koladata.serving.testing.test_functors.TEST_DS"),
    },
    tool_deps = [":test_functors"],  # Can also come from "deps =" argument of koladata_trace_py_fn above.
    deps = ["//py/koladata/serving:standard_deps"],
)

cc_test(
    name = "cc_test_functors_test",
    srcs = ["cc_test_functors_test.cc"],
    data = [":serialized_test_functors"],
    deps = [
        ":cc_test_functors",
        "//koladata:data_slice",
        "//koladata:test_utils",
        "//koladata/functor:call",
        "//koladata/functor:cpp_function_bridge",
        "//koladata/functor/parallel:eager_executor",
        "//koladata/functor/parallel:future",
        "//koladata/functor/parallel:parallel_call_utils",
        "//koladata/serving:slice_registry",
        "//koladata/testing:matchers",
        "//py/koladata/serving:serialized_slices",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)
