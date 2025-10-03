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

# Benchmarks for Koladata.

load("@pip_deps//:requirements.bzl", "requirement")
load("@rules_cc//cc:cc_test.bzl", "cc_test")
load("@rules_python//python:defs.bzl", "py_test")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_test(
    name = "benchmarks",
    srcs = [
        "benchmarks.py",
        "boxing_benchmarks.py",
        "create_benchmarks.py",
        "from_to_json_benchmarks.py",
        "from_to_proto_benchmarks.py",
        "from_to_py_benchmarks.py",
        "misc_benchmarks.py",
        "operator_benchmarks.py",
        "set_get_attr_benchmarks.py",
    ],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata:kd",
        "@com_google_arolla//py/arolla",
        requirement("google-benchmark"),
        "//py/koladata/functions/tests:test_py_pb2",
        # Enables python functions in profile.
        # Enables --cpu_profile.
        # Enables --peakheap_profile.
        # Memory usage counters.
    ],
)

py_test(
    name = "boxing_benchmarks",
    srcs = ["boxing_benchmarks.py"],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_arolla//py/arolla",
        requirement("google-benchmark"),
        "//py/koladata/functions/tests:test_py_pb2",
        # Enables python functions in profile.
        # Enables --cpu_profile.
        # Enables --peakheap_profile.
        # Memory usage counters.
    ],
)

py_test(
    name = "create_benchmarks",
    srcs = ["create_benchmarks.py"],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        "//py:python_path",
        "@com_google_arolla//py/arolla",
        requirement("google-benchmark"),
        "//py/koladata:kd",
        # Enables python functions in profile.
        # Enables --cpu_profile.
        # Enables --peakheap_profile.
        # Memory usage counters.
    ],
)

py_test(
    name = "from_to_json_benchmarks",
    srcs = ["from_to_json_benchmarks.py"],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        "//py:python_path",
        "@com_google_arolla//py/arolla",
        requirement("google-benchmark"),
        "//py/koladata/functions/tests:test_py_pb2",
        "//py/koladata:kd",
        # Enables python functions in profile.
        # Enables --cpu_profile.
        # Enables --peakheap_profile.
        # Memory usage counters.
    ],
)

py_test(
    name = "from_to_proto_benchmarks",
    srcs = ["from_to_proto_benchmarks.py"],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        requirement("google-benchmark"),
        "//py/koladata/functions/tests:test_py_pb2",
        # Enables python functions in profile.
        # Enables --cpu_profile.
        # Enables --peakheap_profile.
        # Memory usage counters.
    ],
)

py_test(
    name = "from_to_py_benchmarks",
    srcs = ["from_to_py_benchmarks.py"],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        "//py:python_path",
        "//py/koladata/functions",
        "//py/koladata:kd",
        requirement("google-benchmark"),
        # Enables python functions in profile.
        # Enables --cpu_profile.
        # Enables --peakheap_profile.
        # Memory usage counters.
    ],
)

py_test(
    name = "misc_benchmarks",
    srcs = ["misc_benchmarks.py"],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_arolla//py/arolla",
        requirement("google-benchmark"),
        # Enables python functions in profile.
        # Enables --cpu_profile.
        # Enables --peakheap_profile.
        # Memory usage counters.
    ],
)

py_test(
    name = "operator_benchmarks",
    srcs = ["operator_benchmarks.py"],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_arolla//py/arolla",
        requirement("google-benchmark"),
        "//py/koladata/functions/tests:test_py_pb2",
        # Enables python functions in profile.
        # Enables --cpu_profile.
        # Enables --peakheap_profile.
        # Memory usage counters.
    ],
)

py_test(
    name = "set_get_attr_benchmarks",
    srcs = ["set_get_attr_benchmarks.py"],
    tags = [
        "manual",
        "notap",
    ],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",
        "@com_google_arolla//py/arolla",
        requirement("google-benchmark"),
        # Enables python functions in profile.
        # Enables --cpu_profile.
        # Enables --peakheap_profile.
        # Memory usage counters.
    ],
)

cc_test(
    name = "cc_benchmarks",
    srcs = ["cc_benchmarks.cc"],
    deps = [
        "//koladata:all_qexpr_operators",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/functor",
        "//koladata/functor:call",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/s11n",
        "//py/koladata/operators:base_cc_operators",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/io",
        "@com_google_arolla//arolla/jagged_shape/array/qexpr",
        "@com_google_arolla//arolla/jagged_shape/dense_array/qexpr",
        "@com_google_arolla//arolla/jagged_shape/dense_array/serialization_codecs",
        "@com_google_arolla//arolla/jagged_shape/expr",
        "@com_google_arolla//arolla/jagged_shape/qexpr",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/qexpr/operators/all",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/serialization_codecs:all_decoders",
        "@com_google_arolla//arolla/serving",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
        "@com_google_googletest//:gtest_main",
    ],
)
