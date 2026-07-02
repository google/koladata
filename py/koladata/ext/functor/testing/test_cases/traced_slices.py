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

"""Test case for traced slices."""

from koladata import kd


def my_model(x, y):
  # 2D slice with Expr args, mixed with constants, and explicit schema
  return kd.slice([[x, x], [y, 3.0]], schema=kd.FLOAT64)  # pyrefly: ignore[missing-attribute]


MODEL = kd.fn(my_model)

INPUTS = [
    {
        'x': kd.item(1.5),  # pyrefly: ignore[missing-attribute]
        'y': kd.item(2.5),  # pyrefly: ignore[missing-attribute]
    },
    {
        'x': kd.item(-1.0),  # pyrefly: ignore[missing-attribute]
        'y': kd.item(2.0),  # pyrefly: ignore[missing-attribute]
    },
]
