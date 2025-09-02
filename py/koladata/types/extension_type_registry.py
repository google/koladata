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

"""Extension type registry."""

import functools
from typing import Any
from arolla import arolla
from arolla.derived_qtype import derived_qtype
from arolla.objects import objects
import bidict


M = arolla.M | derived_qtype.M

BASE_QTYPE = objects.OBJECT


@functools.lru_cache
def _get_downcast_expr(qtype):
  return M.derived_qtype.downcast(qtype, arolla.L.x)


_UPCAST_EXPR = M.derived_qtype.upcast(M.qtype.qtype_of(arolla.L.x), arolla.L.x)

_EXTENSION_TYPE_REGISTRY = bidict.bidict()  # cls -> QType.


def _is_extension_qtype(qtype: arolla.QType) -> bool:
  """Checks if the given qtype is a Koda extension type."""
  return (
      arolla.abc.invoke_op('derived_qtype.get_qtype_label', (qtype,)) != ''  # pylint: disable=g-explicit-bool-comparison
  ) and (
      arolla.abc.invoke_op('qtype.decay_derived_qtype', (qtype,)) == BASE_QTYPE
  )


def register_extension_type(
    cls: type[Any], qtype: arolla.QType, *, unsafe_override=False
):
  """Registers an extension type with its corresponding qtype."""
  if not _is_extension_qtype(qtype):
    raise ValueError(f'expected an extension type, got: {qtype}')
  if unsafe_override:
    if qtype in _EXTENSION_TYPE_REGISTRY.inverse:
      del _EXTENSION_TYPE_REGISTRY.inverse[qtype]  # pytype: disable=unsupported-operands
  elif (reg_qtype := _EXTENSION_TYPE_REGISTRY.get(cls, qtype)) != qtype:
    raise ValueError(
        f'{cls} is already registered with a different qtype: {reg_qtype}'
    )
  elif (reg_cls := _EXTENSION_TYPE_REGISTRY.inverse.get(qtype, cls)) != cls:
    raise ValueError(
        f'{qtype} is already registered with a different class: {reg_cls}'
    )
  _EXTENSION_TYPE_REGISTRY[cls] = qtype


def get_extension_qtype(cls: type[Any]) -> arolla.QType:
  """Returns the QType for the given extension type class."""
  try:
    return _EXTENSION_TYPE_REGISTRY[cls]
  except KeyError:
    raise ValueError(f'{cls} is not a registered extension type') from None


def _get_extension_cls(qtype: arolla.QType) -> type[Any]:
  """Returns the extension type class for the given QType."""
  try:
    return _EXTENSION_TYPE_REGISTRY.inverse[qtype]
  except KeyError:
    raise ValueError(
        'there is no registered extension type corresponding to the QType'
        f' {qtype}'
    ) from None


def is_koda_extension_type(cls: type[Any]) -> bool:
  """Returns True iff the given type is a registered Koda extension type."""
  return cls in _EXTENSION_TYPE_REGISTRY


def is_koda_extension(x: Any) -> bool:
  """Returns True iff the given object is an instance of a Koda extension type."""
  if not isinstance(x, arolla.QValue):
    return False
  return x.qtype in _EXTENSION_TYPE_REGISTRY.inverse


def wrap(x: objects.Object, qtype: arolla.QType) -> Any:
  """Wraps `x` into an instance of the given extension type."""
  _ = _get_extension_cls(qtype)  # Check that it's registered
  return arolla.eval(_get_downcast_expr(qtype), x=x)


def unwrap(x: Any) -> arolla.types.Tuple:
  """Unwraps an extension type into the underlying type."""
  if not is_koda_extension(x):
    raise ValueError(f'expected an extension type, got: {type(x)}')
  return arolla.eval(_UPCAST_EXPR, x=x)


def get_dummy_value(cls: Any) -> arolla.AnyQValue:
  """Returns a dummy value for the given extension type class."""
  extension_qtype = get_extension_qtype(cls)
  return wrap(objects.Object(), extension_qtype)


def dynamic_cast(value: arolla.QValue, qtype: arolla.QType) -> arolla.AnyQValue:
  """Up-, down-, and side-casts `value` to `qtype`."""
  dc = arolla.abc.lookup_operator('kd.extension_types.dynamic_cast')
  return arolla.eval(dc(arolla.L.x, qtype), x=value)
