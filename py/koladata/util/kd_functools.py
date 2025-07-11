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

"""Utility functions for working with functions."""

from collections.abc import Callable
import inspect
import types as py_types
from typing import Any


def unwrap(fn: Callable[..., Any]) -> Callable[..., Any]:
  """Unwraps the function from all the decorators."""
  visited = set()
  while (
      fn not in visited
      and hasattr(fn, '__wrapped__')
      and callable(fn.__wrapped__)
  ):
    visited.add(fn)
    fn = fn.__wrapped__
  return fn


_F_CODES_SKIPPED_FROM_STACK_TRACE = set()
_FILES_SKIPPED_FROM_STACK_TRACE = set()


def skip_from_functor_stack_trace(
    func: py_types.FunctionType,
) -> py_types.FunctionType:
  """Annotates a function to be skipped by current_stack_trace_frame().

  The decorator is intended mark the functions used during functor tracing,
  which may otherwise be saved as a stack frame for the currently traced
  function.

  Args:
    func: The function to annotate.

  Returns:
    The annotated function.
  """
  assert isinstance(func, py_types.FunctionType)
  _F_CODES_SKIPPED_FROM_STACK_TRACE.add(func.__code__)
  return func


def skip_file_from_functor_stack_trace(file_name: str):
  """Skips all the functions defined in the provided __file__ from current_stack_trace_frame()."""
  _FILES_SKIPPED_FROM_STACK_TRACE.add(file_name)


@skip_from_functor_stack_trace
def current_stack_trace_frame() -> py_types.FrameType | None:
  """Returns the closest stack frame not marked with @skip_from_functor_stack_trace."""
  frame = inspect.currentframe()
  while frame and (
      frame.f_code in _F_CODES_SKIPPED_FROM_STACK_TRACE
      or frame.f_code.co_filename in _FILES_SKIPPED_FROM_STACK_TRACE
  ):
    frame = frame.f_back
  return frame
