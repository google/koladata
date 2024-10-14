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

# Package for utilities and fakes in testing.

load("//py/koladata/dynamic_deps:py_extension.bzl", "koladata_pybind_extension")

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

koladata_pybind_extension(
    name = "testing_pybind",
    testonly = True,
    srcs = ["testing_pybind.cc"],
    deps = [
        "//koladata/internal:error_cc_proto",
        "//koladata/internal:error_utils",
        "//py/koladata/exceptions:py_exception_utils",
        "@com_google_absl//absl/status",
        "@com_google_absl//absl/strings:cord",
        "@com_google_absl//absl/strings:string_view",
        "@com_google_arolla//arolla/util",
        "@com_google_arolla//py/arolla/py_utils",
    ],
)
