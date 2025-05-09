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

"""Koladata benchmarks related to boxing."""

import google_benchmark
from koladata import kd


I = kd.I
V = kd.V
S = kd.S
kde = kd.lazy
kdi = kd.eager
kdf = kd.functor


# pylint: disable=missing-function-docstring
@google_benchmark.register
def create_item(state):
  while state:
    _ = kd.item(42)


@google_benchmark.register
def create_item_through_slice(state):
  while state:
    _ = kd.slice(42)


@google_benchmark.register
def from_py(state):
  lst = [12] * 1000 + ['abc'] * 2000 + [b'abc'] * 3000 + [3.14] * 4000
  while state:
    _ = kd.slice(lst)


@google_benchmark.register
@google_benchmark.option.range(1000, 50000)
def from_py_big_text(state):
  text_size = state.range(0)
  lst = [str(i % 10) * text_size for i in range(10)]
  while state:
    _ = kd.slice(lst)


@google_benchmark.register
@google_benchmark.option.range(1000, 50000)
def from_py_big_text_and_big_slice(state):
  text_size = state.range(0)
  lst = [str(i % 10) * text_size for i in range(1000)]
  while state:
    _ = kd.slice(lst)


@google_benchmark.register
def from_py_with_casting(state):
  lst = [12] * 5000 + [3.14] * 5000
  while state:
    _ = kd.slice(lst)


@google_benchmark.register
def from_py_dtype(state):
  lst = [42] * 10000
  while state:
    _ = kd.slice(lst, kd.INT32)


@google_benchmark.register
def from_py_dtype_with_casting(state):
  lst = [42] * 10000
  while state:
    _ = kd.slice(lst, kd.FLOAT32)


@google_benchmark.register
def from_py_deep_nesting(state):
  lst = [3.14] * 4096  # 2 ^ 12 elements
  for _ in range(11):  # 12 levels-deep
    new_lst = []
    for i in range(0, len(lst), 2):
      new_lst.append([lst[i], lst[i + 1]])
    lst = new_lst
  while state:
    _ = kd.slice(lst)


if __name__ == '__main__':
  google_benchmark.main()
