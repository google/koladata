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

load("@rules_python//python:defs.bzl", "py_test")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

py_test(
    name = "data_bag_s11n_test",
    srcs = ["data_bag_s11n_test.py"],
    deps = [
        "//koladata/s11n:codec_py_proto",
        "//py/koladata:kd",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/s11n/testing:codec_test_case",
    ],
)

py_test(
    name = "jagged_shape_s11n_test",
    srcs = ["jagged_shape_s11n_test.py"],
    deps = [
        "//koladata/s11n:codec_py_proto",
        "//py/koladata/functions:s11n",
        "//py/koladata/types:jagged_shape",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//arolla/jagged_shape/dense_array/serialization_codecs:jagged_shape_codec_py_proto",
        "@com_google_arolla//arolla/serialization_codecs/dense_array:dense_array_codec_py_proto",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/s11n/testing:codec_test_case",
    ],
)

py_test(
    name = "data_slice_s11n_test",
    srcs = ["data_slice_s11n_test.py"],
    deps = [
        "//koladata/s11n:codec_py_proto",
        "//py/koladata:kd",
        "//py/koladata/testing",
        "//py/koladata/types:data_slice",
        "//py/koladata/types:schema_constants",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//arolla/jagged_shape/dense_array/serialization_codecs:jagged_shape_codec_py_proto",
        "@com_google_arolla//arolla/serialization_codecs/dense_array:dense_array_codec_py_proto",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/s11n/testing:codec_test_case",
        "@com_google_protobuf//:protobuf_python",
    ],
)

py_test(
    name = "literal_operator_s11n_test",
    srcs = ["literal_operator_s11n_test.py"],
    deps = [
        "//koladata/s11n:codec_py_proto",
        "//py/koladata/types:literal_operator",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//arolla/serialization_codecs/generic:scalar_codec_py_proto",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/s11n/testing:codec_test_case",
    ],
)

py_test(
    name = "ellipsis_s11n_test",
    srcs = ["ellipsis_s11n_test.py"],
    deps = [
        "//koladata/s11n:codec_py_proto",
        "//py/koladata/types:ellipsis",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/s11n/testing:codec_test_case",
    ],
)

py_test(
    name = "non_deterministic_s11n_test",
    srcs = ["non_deterministic_s11n_test.py"],
    deps = [
        "//koladata/s11n:codec_py_proto",
        "//py/koladata/types:qtypes",
        "@com_google_absl_py//absl/testing:absltest",
        "@com_google_arolla//py/arolla",
        "@com_google_arolla//py/arolla/s11n/testing:codec_test_case",
    ],
)
