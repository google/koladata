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

"""Test case for subfunctors."""

from koladata import kd

_g_sub = kd.fn(lambda x: x * 2)
_h_sub = kd.fn(lambda y: y + 1)

MODEL = kd.fn(kd.V.g(kd.I.x) + kd.V.h(kd.I.y), g=_g_sub, h=_h_sub)
INPUTS = [
    {'x': kd.item(3), 'y': kd.item(4)},
]
