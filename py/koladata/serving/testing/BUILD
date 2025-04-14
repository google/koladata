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

load("@rules_python//python:defs.bzl", "py_library")
load(
    "//py/koladata/serving:serving.bzl",
    "koladata_cc_embedded_slices",
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
        "//py/koladata:kd",
    ],
)

# TODO: b/409476740 - Add a test on koladata_serialized_functors as well.
koladata_cc_embedded_slices(
    name = "cc_test_functors",
    testonly = True,
    cc_function_name = "koladata_serving_test::TestFunctors",
    slices = {
        "plus_one": koladata_trace_py_fn("koladata.serving.testing.test_functors.plus_one"),
        "ask_about_serving": koladata_trace_py_fn("koladata.serving.testing.test_functors.ask_about_serving"),
    },
    tool_deps = [":test_functors"],  # Can also come from "deps =" argument of koladata_trace_py_fn above.
    deps = ["//py/koladata/serving:standard_deps"],
)

cc_test(
    name = "cc_test_functors_test",
    srcs = ["cc_test_functors_test.cc"],
    deps = [
        ":cc_test_functors",
        "//koladata:data_slice",
        "//koladata:test_utils",
        "//koladata/functor:call",
        "//koladata/testing:matchers",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/qtype/testing",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
    ],
)
