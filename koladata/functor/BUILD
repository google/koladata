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

# Functor tools for Koda.

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "signature",
    srcs = ["signature.cc"],
    hdrs = ["signature.h"],
    deps = [
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/container:flat_hash_set",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "signature_storage",
    srcs = ["signature_storage.cc"],
    hdrs = ["signature_storage.h"],
    deps = [
        ":signature",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:object_factories",
        "//koladata/internal:data_item",
        "//koladata/internal:data_slice",
        "//koladata/internal:dtype",
        "//koladata/operators:lib",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_library(
    name = "functor",
    srcs = ["functor.cc"],
    hdrs = ["functor.h"],
    deps = [
        ":signature_storage",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:object_factories",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "@com_google_absl//absl/base:no_destructor",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "functor_test",
    srcs = ["functor_test.cc"],
    deps = [
        ":functor",
        ":signature",
        ":signature_storage",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:object_factories",
        "//koladata/expr:expr_operators",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/testing:matchers",
        "//koladata/testing:status_matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/qexpr/operators/all",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_library(
    name = "call",
    srcs = ["call.cc"],
    hdrs = ["call.h"],
    deps = [
        ":functor",
        ":signature",
        ":signature_storage",
        "//koladata:data_slice",
        "//koladata:data_slice_qtype",
        "//koladata/expr:expr_eval",
        "//koladata/internal:data_item",
        "@com_google_absl//absl/container:flat_hash_map",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:str_format",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_absl//absl/types:span",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "call_test",
    srcs = ["call_test.cc"],
    deps = [
        ":call",
        ":functor",
        ":signature",
        ":signature_storage",
        "//koladata:data_slice",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/s11n",
        "//koladata/testing:matchers",
        "//koladata/testing:status_matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/expr",
        "@com_google_arolla//arolla/expr/operators/all",
        "@com_google_arolla//arolla/qexpr/operators/all",
        "@com_google_arolla//arolla/qtype",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//arolla/util:status_backport",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "signature_storage_test",
    srcs = ["signature_storage_test.cc"],
    deps = [
        ":signature",
        ":signature_storage",
        "//koladata:data_bag",
        "//koladata:data_slice",
        "//koladata:object_factories",
        "//koladata:test_utils",
        "//koladata/internal:data_item",
        "//koladata/internal:dtype",
        "//koladata/operators:lib",
        "//koladata/s11n",
        "//koladata/testing:matchers",
        "//koladata/testing:status_matchers",
        "@com_google_absl//absl/status",
        "@com_google_arolla//arolla/memory",
        "@com_google_arolla//arolla/util",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "signature_test",
    srcs = ["signature_test.cc"],
    deps = [
        ":signature",
        "//koladata:data_slice",
        "//koladata:test_utils",
        "//koladata/internal:dtype",
        "//koladata/testing:matchers",
        "//koladata/testing:status_matchers",
        "@com_google_absl//absl/status",
        "@com_google_arolla//arolla/qtype",
        "@com_google_googletest//:gtest_main",
    ],
)
