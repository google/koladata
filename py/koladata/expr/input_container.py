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

"""InputContainer that supports Koda like syntax."""

import functools

from arolla import arolla
from koladata.types import literal_operator

_KODA_INPUT_OP = arolla.abc.lookup_operator('koda_internal.input')


@functools.lru_cache()
def _get_input(container_name: str, input_key: str) -> arolla.Expr:
  container_name = literal_operator.literal(arolla.text(container_name))
  input_key = literal_operator.literal(arolla.text(input_key))
  return _KODA_INPUT_OP(container_name, input_key)


class InputContainer:
  """Helper container to create Koda specific inputs.

  Supports __getattr__ and __getitem__.

  Note that I.x is not an Arolla leaf and arolla.get_leaf_keys will not include
  'x' - use `get_input_names` instead. Similarly, `arolla.sub_leaves` should not
  be used to replace these inputs, so rely on `arolla.sub_by_fingerprint` or
  `sub_inputs`, instead.
  """

  def __init__(self, container_name: str):
    self.name = container_name

  def __getattr__(self, input_key: str) -> arolla.Expr:
    return _get_input(self.name, input_key)

  def __getitem__(self, input_key: str) -> arolla.Expr:
    if not isinstance(input_key, str):
      raise TypeError(
          'Input key must be str, not {}.'.format(
              arolla.abc.get_type_name(type(input_key))
          )
      )
    return _get_input(self.name, input_key)

  def __repr__(self) -> str:
    return f"InputContainer('{self.name}')"


def get_input_name(expr: arolla.Expr, container: InputContainer) -> str | None:
  """Returns the input name of `expr` if it comes from `container` else None."""
  if (
      expr.op == _KODA_INPUT_OP
      and expr.node_deps[0].qvalue.py_value() == container.name  # pytype: disable=attribute-error
  ):
    return expr.node_deps[1].qvalue.py_value()  # pytype: disable=attribute-error
  else:
    return None
