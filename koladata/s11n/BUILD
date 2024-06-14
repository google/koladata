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

# Koda serialization codecs.

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "s11n",
    srcs = [
        "decoder.cc",
        "encoder.cc",
    ],
    hdrs = [],
    deps = [
        ":codec_cc_proto",
        ":codec_names",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/expr:expr_operators",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:ellipsis",
        "//koladata/internal:missing_value",
        "//koladata/internal:object_id",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/serialization_base",
        "@com_google_arolla//arolla/serialization_base:base_cc_proto",
        "@com_google_arolla//arolla/serialization_codecs:registry",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
    alwayslink = True,
)

cc_test(
    name = "serialization_test",
    srcs = ["serialization_test.cc"],
    deps = [
        ":s11n",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/serialization",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "codec_names",
    hdrs = ["codec_names.h"],
    deps = ["@com_google_absl//absl/strings:string_view"],
)

proto_library(
    name = "codec_proto",
    srcs = ["codec.proto"],
    deps = ["@com_google_arolla//arolla/serialization_base:base_proto"],
)

cc_proto_library(
    name = "codec_cc_proto",
    deps = [":codec_proto"],
)
