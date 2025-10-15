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

from typing import Any
from arolla import arolla
from arolla.derived_qtype import derived_qtype
from arolla.objects import objects
import bidict
from koladata.types import py_boxing


M = arolla.M | derived_qtype.M

BASE_QTYPE = objects.OBJECT
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
  # Allow extension types to be used as type annotations in functors.
  py_boxing.register_dummy_qvalue_handler(cls, get_dummy_value)


def get_extension_qtype(cls: type[Any]) -> arolla.QType:
  """Returns the QType for the given extension type class."""
  try:
    return _EXTENSION_TYPE_REGISTRY[cls]
  except KeyError:
    raise ValueError(f'{cls} is not a registered extension type') from None


def get_extension_cls(qtype: arolla.QType) -> type[Any]:
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


def get_dummy_value(cls: Any) -> arolla.AnyQValue:
  """Returns a dummy value for the given extension type class."""
  extension_qtype = get_extension_qtype(cls)
  return wrap(objects.Object(), extension_qtype)


# Eager versions of kde operators that require specific implementations (due to
# requiring literal inputs).


def wrap(x: Any, qtype: arolla.QType) -> arolla.AnyQValue:
  """Wraps `x` into an instance of the given extension type."""
  _ = get_extension_cls(qtype)  # Check that it's registered
  wrap_op = arolla.abc.lookup_operator('kd.extension_types.wrap')
  x = py_boxing.as_qvalue(x)
  return arolla.eval(wrap_op(arolla.L.x, qtype), x=x)


def dynamic_cast(value: Any, qtype: arolla.QType) -> arolla.AnyQValue:
  """Up-, down-, and side-casts `value` to `qtype`."""
  dc = arolla.abc.lookup_operator('kd.extension_types.dynamic_cast')
  value = py_boxing.as_qvalue(value)
  return arolla.eval(dc(arolla.L.value, qtype), value=value)


def get_attr(
    ext: Any, attr: str | arolla.QValue, qtype: arolla.QType
) -> arolla.AnyQValue:
  """Returns the attribute of `ext` with name `attr` and type `qtype`."""
  ga = arolla.abc.lookup_operator('kd.extension_types.get_attr')
  ext = py_boxing.as_qvalue(ext)
  return arolla.eval(ga(arolla.L.ext, attr, qtype), ext=ext)


def make(
    qtype: arolla.QType,
    prototype: objects.Object | None = None,
    /,
    **attrs: Any,
):
  """Returns an extension type of the given `qtype` with the given `attrs`.

  Args:
    qtype: the output qtype of the extension type.
    prototype: parent object (arolla.Object).
    **attrs: attributes of the extension type.
  """
  if prototype is None:
    prototype = arolla.unspecified()
  attrs = arolla.abc.aux_eval_op('kd.namedtuple', **attrs)
  return arolla.eval(
      arolla.abc.bind_op(
          'kd.extension_types.make', qtype, arolla.L.prototype, arolla.L.attrs
      ),
      prototype=prototype,
      attrs=attrs,
  )


def make_null(qtype: arolla.QType) -> arolla.AnyQValue:
  """Returns a null instance of an extension type."""
  return arolla.eval(
      arolla.abc.bind_op('kd.extension_types.make_null', qtype), qtype=qtype
  )
