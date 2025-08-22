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
import bidict
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import qtypes


M = arolla.M | derived_qtype.M


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
      # Heuristic check to catch the worst offenders: while there can be many
      # different types of extension types, they are at least all tuples.
      arolla.is_tuple_qtype(
          arolla.abc.invoke_op('qtype.decay_derived_qtype', (qtype,))
      )
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


def wrap(x: arolla.types.Tuple, qtype: arolla.QType) -> Any:
  """Wraps `x` into an instance of the given extension type."""
  _ = _get_extension_cls(qtype)  # Check that it's registered
  return arolla.eval(_get_downcast_expr(qtype), x=x)


def unwrap(x: Any) -> arolla.types.Tuple:
  """Unwraps an extension type into the underlying type."""
  if not is_koda_extension(x):
    raise ValueError(f'expected an extension type, got: {type(x)}')
  return arolla.eval(_UPCAST_EXPR, x=x)


@functools.cache
def _get_dummy_bag():
  return data_bag.DataBag.empty()


def get_dummy_value(cls: Any) -> arolla.AnyQValue:
  """Returns a dummy value for the given extension type class."""
  extension_qtype = get_extension_qtype(cls)
  tpl_qtype = arolla.abc.invoke_op(
      'qtype.decay_derived_qtype', (extension_qtype,)
  )
  dummy_fields = []
  # NOTE: This should be kept up-to-date with `_get_class_meta`.
  for qtype in arolla.abc.get_field_qtypes(tpl_qtype):
    if qtype == qtypes.DATA_SLICE:
      dummy_fields.append(data_slice.DataSlice.from_vals(None))
    elif qtype == qtypes.DATA_BAG:
      dummy_fields.append(_get_dummy_bag())
    elif qtype == qtypes.JAGGED_SHAPE:
      dummy_fields.append(jagged_shape.create_shape())
    elif extension_cls := _EXTENSION_TYPE_REGISTRY.inverse.get(qtype):
      dummy_fields.append(get_dummy_value(extension_cls))
    else:
      raise ValueError(f'unexpected qtype field: {qtype}')
  return wrap(arolla.tuple(*dummy_fields), extension_qtype)
