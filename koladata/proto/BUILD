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

# Proto conversions for Koda.

load("@rules_cc//cc:cc_library.bzl", "cc_library")
load("@rules_cc//cc:cc_test.bzl", "cc_test")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "from_proto",
    srcs = ["from_proto.cc"],
    hdrs = ["from_proto.h"],
    deps = [
        ":proto_schema_utils",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:object_factories",
        "//koladata:uuid_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:schema_attrs",
        "//koladata/internal/op_utils:trampoline_executor",
        "//koladata/operators:lib",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "from_proto_test",
    srcs = ["from_proto_test.cc"],
    deps = [
        ":from_proto",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:object_factories",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal/testing:matchers",
        "//koladata/operators:lib",
        "//koladata/proto/testing:test_proto2_cc_proto",
        "//koladata/proto/testing:test_proto3_cc_proto",
        "//koladata/testing:matchers",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/jagged_shape/testing",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "to_proto",
    srcs = ["to_proto.cc"],
    hdrs = ["to_proto.h"],
    deps = [
        "//koladata:data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:schema_attrs",
        "//koladata/internal/op_utils:trampoline_executor",
        "//koladata/operators:lib",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "proto_schema_utils",
    hdrs = ["proto_schema_utils.h"],
    deps = ["@com_google_absl//absl/strings:string_view"],
)

cc_test(
    name = "to_proto_test",
    srcs = ["to_proto_test.cc"],
    deps = [
        ":from_proto",
        ":to_proto",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:object_factories",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/internal:object_id",
        "//koladata/internal:schema_attrs",
        "//koladata/operators:lib",
        "//koladata/proto/testing:test_proto2_cc_proto",
        "//koladata/proto/testing:test_proto3_cc_proto",
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_absl//absl/strings",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_arolla//arolla/util/testing",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:differencer",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "to_proto_benchmark",
    srcs = ["to_proto_benchmark.cc"],
    deps = [
        ":from_proto",
        ":to_proto",
        "//koladata:data_bag",
        "//koladata/proto/testing:test_proto2_cc_proto",
        "//koladata/proto/testing:test_proto3_cc_proto",
        "@com_google_absl//absl/log:check",
        "@com_google_benchmark//:benchmark_main",
        "@com_google_googletest//:gtest",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "message_factory",
    srcs = ["message_factory.cc"],
    hdrs = ["message_factory.h"],
    deps = [
        "@com_google_absl//absl/base:nullability",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "message_factory_test",
    srcs = ["message_factory_test.cc"],
    deps = [
        ":message_factory",
        "//koladata/proto/testing:test_proto2_cc_proto",
        "//koladata/proto/testing:test_proto3_cc_proto",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:status_matchers",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_googletest//:gtest_main",
    ],
)
