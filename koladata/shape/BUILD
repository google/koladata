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

# Koda specific JaggedShape utilities.

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

cc_library(
    name = "shape_utils",
    hdrs = ["shape_utils.h"],
    deps = [
        "@com_google_absl//absl/log:check",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/util:status_backport",
    ],
)

cc_test(
    name = "shape_utils_test",
    srcs = ["shape_utils_test.cc"],
    deps = [
        ":shape_utils",
        "//koladata:data_slice",
        "//koladata/internal:data_slice",
        "//koladata/testing:matchers",
        "//koladata/testing:status_matchers",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/status:statusor",
        "@com_google_arolla//arolla/dense_array",
        "@com_google_googletest//:gtest_main",
    ],
)
