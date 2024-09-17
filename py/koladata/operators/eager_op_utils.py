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

"""Utilities for defining eager operators."""

from __future__ import annotations

from typing import Any, Callable

from arolla import arolla
from koladata.operators import kde_operators


class _OperatorsContainer:
  """Used to access and classify operators based on their purpose.

  The access to namespaces and operators is fetched dynamically from Arolla's
  operators and cached for faster follow-up accesses.
  """

  __slots__ = ('__dict__', '_arolla_container')

  def __init__(self, rl_container: arolla.OperatorsContainer):
    self._arolla_container = rl_container

  def __dir__(self):
    return dir(self._arolla_container)

  def __getitem__(self, op_name: str) -> Callable[..., Any]:
    eager_op = self.__dict__.get(op_name)
    if eager_op is None:
      eager_op = self._arolla_container[op_name]._eval
      self.__dict__[op_name] = eager_op
    assert callable(eager_op)
    return eager_op

  # NOTE: Adding an operator / container to __dict__, causes __getattr__ to not
  # be invoked the next time by Python runtime.
  def __getattr__(
      self, op_or_container_name: str
  ) -> _OperatorsContainer | Callable[..., Any]:
    rl_op_or_container = getattr(self._arolla_container, op_or_container_name)
    if isinstance(rl_op_or_container, arolla.OperatorsContainer):
      ret = _OperatorsContainer(rl_op_or_container)
    else:
      ret = rl_op_or_container._eval  # pylint: disable=protected-access
    self.__dict__[op_or_container_name] = ret
    return ret


_GLOBAL_OPERATORS_CONTAINER = _OperatorsContainer(
    arolla.OperatorsContainer(kde_operators)
)


def reset_operators_container():
  _GLOBAL_OPERATORS_CONTAINER.__dict__ = {}


def operators_container(namespace: str | None = None) -> _OperatorsContainer:
  """Access the container based on `namespace`.

  If `namespace` is None, access the top-level operator namespace.

  Args:
    namespace: name of group of operators grouped by their purpose/function.

  Returns:
    _OperatorsContainer
  """
  container = _GLOBAL_OPERATORS_CONTAINER
  if namespace is not None:
    for name in namespace.split('.'):
      container = getattr(container, name)
  if not isinstance(container, _OperatorsContainer):
    raise ValueError(f'{namespace} is not an OperatorsContainer')
  return container
