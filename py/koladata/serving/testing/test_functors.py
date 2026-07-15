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

"""Test functor definitions."""

from collections.abc import Callable

from koladata import kd


def plus_one(x: kd.types.DataSlice) -> kd.types.DataSlice:
  return x + 1


def make_plus_n_functor(
    n: kd.types.DataSlice,
) -> Callable[[kd.types.DataSlice], kd.types.DataSlice]:
  return kd.fn(lambda x: x + n)


def ask_about_serving(
    call_external_fn: kd.types.DataSlice,
) -> kd.types.DataSlice:
  return call_external_fn("How to serve Koda functors?")


XYSchema = kd.schema.uu_schema(x=kd.INT32, y=kd.INT32)


def non_deterministic_functor(x, y) -> kd.types.DataSlice:
  literal = kd.eager.new(x=57, y=7, schema=XYSchema)
  return kd.obj(args=XYSchema.new(x=x, y=y), literal=literal)


non_deterministic_functor_with_different_name = non_deterministic_functor


TEST_DS = kd.slice([1, 2, 3])
