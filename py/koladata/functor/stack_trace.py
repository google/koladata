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

"""Tools for working with functor evaluation stack traces."""

import contextlib
import inspect
import linecache
import re
from typing import Any, Callable

from koladata.functions import functions as fns
from koladata.types import data_slice
from koladata.util import kd_functools


def create_stack_trace_frame(
    function_name: str, file_name: str, line_number: int, line_text: str
) -> data_slice.DataSlice:
  """Returns a DataSlice representing a stack trace frame.

  The returned DataSlice may be used by functor::MaybeAddStackTraceFrame on the
  C++ side.

  Args:
    function_name: name of the function.
    file_name: path to the sourcefile.
    line_number: line number in the source file. O indicates an unknown line.
    line_text: line text in the source file.
  """
  return fns.new(
      # LINT.IfChange
      function_name=fns.str(function_name),
      file_name=fns.str(file_name),
      line_number=fns.int32(line_number),
      line_text=fns.str(line_text),
      # LINT.ThenChange(//koladata/functor/stack_trace.h)
      schema='_functor_stack_trace_frame',
  )


_GOES_BEFORE_FUNCTION_DEF = re.compile(r'\s*([#@].*|)\n')


def _guess_line(func: Callable[..., Any]) -> tuple[int, str]:
  """Like inspect.getsourcelines, but skips decorators and comments."""
  lines, first_line_number = inspect.getsourcelines(func)
  i = 0
  while i < len(lines) and re.fullmatch(_GOES_BEFORE_FUNCTION_DEF, lines[i]):
    i += 1
  if i != len(lines):
    return first_line_number + i, lines[i]
  else:
    return first_line_number, lines[0] if lines else ''


def function_frame(func: Callable[..., Any]) -> data_slice.DataSlice | None:
  """Returns the stack frame of the function with the line pointing to its definition."""
  func = kd_functools.unwrap(func)

  if not isinstance(func, Callable):
    return None

  function_name = func.__name__ if hasattr(func, '__name__') else f'({func})'

  file_name, line_number, line_text = None, None, None
  search_for_source_code_in = [func, getattr(func, '__call__', None)]
  for f in search_for_source_code_in:
    with contextlib.suppress(TypeError):
      file_name = inspect.getsourcefile(f)
      line_number, line_text = _guess_line(f)
      break

  return create_stack_trace_frame(
      function_name=function_name, file_name=file_name, line_number=line_number,
      line_text=line_text,
  )


@kd_functools.skip_from_functor_stack_trace
def current_frame() -> data_slice.DataSlice | None:
  """Returns the best traceback frame to represent the current function call.

  The function searches for the first frame coming from outside this module and
  not marked using @kd_functools.skip_from_stack_trace.
  """
  frame = kd_functools.current_stack_trace_frame()
  if not frame:
    return None

  line_text = linecache.getline(
      frame.f_code.co_filename, frame.f_lineno
  ).rstrip('\n')
  return create_stack_trace_frame(
      function_name=frame.f_code.co_name,
      file_name=frame.f_code.co_filename,
      line_number=frame.f_lineno,
      line_text=line_text,
  )
