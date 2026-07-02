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

"""Test case for simple expression."""

from koladata import kd

MODEL = kd.fn(lambda x, y: x * 2 + y)
INPUTS = [
    {'x': kd.item(1), 'y': kd.item(2)},  # pyrefly: ignore[missing-attribute]
    {'x': kd.item(-5), 'y': kd.item(10)},  # pyrefly: ignore[missing-attribute]
]
