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

# Typing annotations for koladata.functor.parallel.clib.

from __future__ import annotations

from typing import Callable, Generic, Iterator, TypeVar

from arolla import arolla

class Executor(arolla.QValue):
  def schedule(self, task_fn: Callable[[], None], /) -> None: ...

QValueT_co = TypeVar('QValueT_co', bound=arolla.QValue, covariant=True)
QValueT_contra = TypeVar('QValueT_contra', bound=arolla.QValue, contravariant=True)

class Stream(Generic[QValueT_co], arolla.QValue):
  @classmethod
  def new(
      cls, value_qtype: arolla.QType, /
  ) -> tuple[Stream[arolla.AnyQValue], StreamWriter[arolla.AnyQValue]]: ...
  def make_reader(self) -> StreamReader[QValueT_co]: ...
  def read_all(self, *, timeout: float | None) -> list[QValueT_co]: ...
  def yield_all(self, *, timeout: float | None) -> Iterator[QValueT_co]: ...

class StreamWriter(Generic[QValueT_contra]):
  def orphaned(self) -> bool: ...
  def write(self, item: QValueT_contra, /) -> None: ...
  def close(self, exception: Exception | None = None) -> None: ...

class StreamReader(Generic[QValueT_co]):
  def read_available(
      self, limit: int | None = None
  ) -> list[QValueT_co] | None: ...
  def subscribe_once(
      self, executor: Executor, callback: Callable[[], None]
  ) -> None: ...
