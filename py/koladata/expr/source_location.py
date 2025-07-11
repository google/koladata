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

"""Utilities for working with source location annotations."""

from collections.abc import Callable
import functools
import linecache
from typing import Any, Self

from arolla import arolla
from koladata.util import kd_functools


def _strip_build_system_prefix(file_name: str) -> str:
  """A heuristic to strip the build system prefix from the file name."""
  this_file_name = 'py/koladata/expr/source_location.py'
  assert __file__ is not None and __file__.endswith(this_file_name)
  build_system_prefix = __file__[: -len(this_file_name)]
  return file_name.removeprefix(build_system_prefix)


@kd_functools.skip_from_functor_stack_trace
def annotate_with_current_source_location(expr: arolla.Expr) -> arolla.Expr:
  """Deduces the "current" source location and annotates the expression with it."""
  frame = kd_functools.current_stack_trace_frame()
  if not frame:
    return expr

  file_name = _strip_build_system_prefix(frame.f_code.co_filename)
  line_text = linecache.getline(
      frame.f_code.co_filename, frame.f_lineno
  ).rstrip('\n')

  # Using bind_op instead of "from koladata.operators import annotation" to
  # avoid a cyclic dependency between koladata/expr and koladata/operators
  # directories.
  return arolla.abc.bind_op(
      'kd.annotation.source_location',
      expr,
      function_name=arolla.text(frame.f_code.co_name),
      file_name=arolla.text(file_name),
      line=arolla.int32(frame.f_lineno),
      column=arolla.int32(0),
      line_text=arolla.text(line_text),
  )


class _OperatorsContainerWrapper:
  """Wraps arolla.OperatorsContainer to attach source locations."""

  _HAS_DYNAMIC_ATTRIBUTES = True

  def __init__(self, container: arolla.OperatorsContainer):
    self._container = container

  def __getattr__(self, key: str) -> Callable[..., Any] | Self:
    res = getattr(self._container, key)
    return attaching_source_location(res)


def attaching_source_location(
    op_or_container,
) -> Callable[..., Any] | _OperatorsContainerWrapper:
  """Wraps an operator / OperatorsContainer to attach source locations.

  Args:
    op_or_container: The operator or OperatorsContainer to wrap.

  Returns:
    A callable or an object that mimics the behavior of the operator /
    OperatorsContainer provided, but attaches source location to the resulting
    expression.
  """
  if isinstance(op_or_container, arolla.types.Operator):

    @kd_functools.skip_from_functor_stack_trace
    @functools.wraps(op_or_container)
    def wrapper(*args, **kwargs):
      bound = op_or_container(*args, **kwargs)
      if not isinstance(bound, arolla.Expr):
        raise TypeError(
            'unsupported result of operator binding: expected'
            f' Expr, got {type(bound)}'
        )
      return annotate_with_current_source_location(bound)

    return wrapper

  elif isinstance(op_or_container, arolla.OperatorsContainer):
    return _OperatorsContainerWrapper(op_or_container)
  else:
    raise AssertionError(
        'attaching_source_location supports only operators and'
        f' OperatorsContainers, got: {type(op_or_container)}'
    )
