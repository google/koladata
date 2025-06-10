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

# Typing annotations for koladata.functor.parallel.clib.

from __future__ import annotations

from typing import Callable, Iterator

from arolla import arolla

class Executor(arolla.QValue):
  def schedule(self, task_fn: Callable[[], None], /) -> None: ...

class Stream(arolla.QValue):
  def make_reader(self) -> StreamReader: ...
  def read_all(self, *, timeout: float | None) -> list[arolla.AnyQValue]: ...
  def yield_all(
      self, *, timeout: float | None
  ) -> Iterator[arolla.AnyQValue]: ...

class StreamWriter:
  def orphaned(self) -> bool: ...
  def write(self, item: arolla.QValue, /) -> None: ...
  def close(self, exception: Exception | None = None) -> None: ...

class StreamReader:
  def read_available(
      self, limit: int | None = None
  ) -> list[arolla.AnyQValue] | None: ...
  def subscribe_once(
      self, executor: Executor, callback: Callable[[], None]
  ) -> None: ...

def make_stream(
    value_qtype: arolla.QType, /
) -> tuple[Stream, StreamWriter]: ...
