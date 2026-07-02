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

"""Test case for complex example."""

from koladata import kd


@kd.trace_as_fn()
def sub_with_default(a, b=kd.float64(2.5)):  # pyrefly: ignore[missing-attribute]
  return a * b


@kd.trace_as_fn()
def sub_kw_only(x, *, factor=kd.int64(10)):  # pyrefly: ignore[missing-attribute]
  return x + factor


def _complex_top(x, y):
  entity = kd.uu(  # pyrefly: ignore[missing-attribute]
      a=x,
      b=3.14,
      schema=kd.uu_schema(a=x.get_schema(), b=kd.FLOAT64),  # pyrefly: ignore[missing-attribute]
  )
  entity = kd.with_attrs(entity, y=y)  # pyrefly: ignore[missing-attribute]
  return sub_with_default(entity.a.a) + sub_kw_only(entity.b, factor=entity.y)


MODEL = kd.fn(_complex_top)
INPUTS = [
    {'x': kd.new(a=kd.item(2.0)), 'y': kd.item(3.0)},  # pyrefly: ignore[missing-attribute]
    {'x': kd.new(a=kd.item(10.0)), 'y': kd.item(20.0)},  # pyrefly: ignore[missing-attribute]
]
