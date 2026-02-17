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

load("@pip_deps//:requirements.bzl", "requirement")
load("@rules_python//python:defs.bzl", "py_test")

# Package for KolaData documentation.

package(default_visibility = [
    "//koladata:internal",
])

licenses(["notice"])

exports_files(
    glob(["*.md"]),
)

filegroup(
    name = "docs",
    data = glob(["*.md"]),
)

py_test(
    name = "doc_test",
    srcs = ["doc_test.py"],
    data = [
        ":10_min_intro.md",
        ":common_pitfalls.md",
        ":quick_recipes.md",
    ],
    deps = [
        "//py:python_path",
        "//py/koladata:kd",  # buildcleaner: keep
        "//py/koladata:kd_ext",  # buildcleaner: keep
        requirement("numpy"),  # buildcleaner: keep
        "@com_google_absl_py//absl/testing:absltest",
        "@rules_python//python/runfiles",
    ],
)
