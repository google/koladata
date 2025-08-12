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

"""Helper functions for working with extension types."""

import dataclasses
import functools
import inspect
from typing import Any, Callable, Mapping
from arolla import arolla
from arolla.derived_qtype import derived_qtype
import bidict
from koladata.expr import view
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes


M = arolla.M | derived_qtype.M


@functools.lru_cache
def _get_downcast_expr(qtype):
  return M.derived_qtype.downcast(qtype, arolla.L.x)


_UPCAST_EXPR = M.derived_qtype.upcast(M.qtype.qtype_of(arolla.L.x), arolla.L.x)

_EXTENSION_TYPE_REGISTRY = bidict.bidict()  # cls -> QType.


def register_extension_type(
    cls: type[Any], qtype: arolla.QType, *, unsafe_override=False
):
  """Registers an extension type with its corresponding qtype."""
  if not _is_extension_type(qtype):
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
  """Returns the qtype for the given extension type class."""
  try:
    return _EXTENSION_TYPE_REGISTRY[cls]
  except KeyError:
    raise ValueError(f'{cls} is not a registered extension type') from None


def is_koda_extension_type(cls: type[Any]) -> bool:
  """Returns True iff the given type is a registered Koda extension type."""
  return cls in _EXTENSION_TYPE_REGISTRY


def _is_extension_type(qtype: arolla.QType) -> bool:
  """Checks if the given qtype is a Koda extension type."""
  return (
      arolla.abc.invoke_op('derived_qtype.get_qtype_label', (qtype,)) != ''  # pylint: disable=g-explicit-bool-comparison
  ) and (
      arolla.abc.invoke_op('qtype.decay_derived_qtype', (qtype,))
      == qtypes.DATA_SLICE
  )


def is_koda_extension(x: Any) -> bool:
  """Returns True iff the given object is instance of a Koda extension type."""
  if not isinstance(x, arolla.QValue):
    return False
  return _is_extension_type(x.qtype)


def wrap(x: data_slice.DataSlice, qtype: arolla.QType) -> Any:
  """Wraps a DataSlice into an instance of the given extension type."""
  if not isinstance(x, data_slice.DataSlice):
    raise ValueError(f'expected a DataSlice, got: {type(x)}')
  if not _is_extension_type(qtype):
    raise ValueError(f'expected an extension type, got: {qtype}')
  return arolla.eval(_get_downcast_expr(qtype), x=x)


def unwrap(x: Any) -> data_slice.DataSlice:
  """Unwraps an extension type into the underlying DataSlice."""
  if not is_koda_extension(x):
    raise ValueError(f'expected an extension type, got: {type(x)}')
  return arolla.eval(_UPCAST_EXPR, x=x)


# Dummy dataclass used to get the list of default dataclassfields.
@dataclasses.dataclass()
class _DummyDC:
  _dummy_field: int


_DEFAULT_FIELDS = frozenset(dir(_DummyDC))


@dataclasses.dataclass(frozen=True)
class _ClassMeta:
  signature: inspect.Signature
  methods: Mapping[str, Callable[..., Any]]


def _get_class_meta(original_class: type[Any]) -> _ClassMeta:
  """Returns meta information about the given class."""
  data_class = dataclasses.dataclass()(original_class)
  fields = {f.name: f for f in dataclasses.fields(data_class)}

  methods = {}
  for attr in dir(data_class):
    if attr in fields or attr in _DEFAULT_FIELDS:
      continue
    method = getattr(data_class, attr)
    if not callable(method):
      continue
    methods[attr] = method
  return _ClassMeta(
      signature=inspect.signature(data_class, eval_str=True), methods=methods
  )


def extension_type(
    unsafe_override=False,
) -> Callable[[type[Any]], type[arolla.AnyQValue]]:
  """Creates a Koda extension type from the the given original class.

  This function is intended to be used as a class decorator. The decorated class
  serves as a schema for the new extension type.

  Internally, this function creates the following:
  -  A new `QType` for the extension type, which is a labeled `QType` on top of
  `DATA_SLICE`.
  - A `QValue` class for representing evaluated instances of the extension type.
  - An `ExprView` class for representing expressions that will evaluate to
        the extension type.

  It replaces the decorated class with a new class that acts as a factory. This
  factory's `__new__` method dispatches to either create an `Expr` or a `QValue`
  instance, depending on the types of the arguments provided.

  The fields of the dataclass are exposed as properties on both the `QValue` and
  `ExprView` classes. Any methods defined on the dataclass are also carried
  over.

  Note:
  - The decorated class must not have its own `__new__` method.
  - The type annotations on the fields of the dataclass are used to determine
    the schema of the underlying `DataSlice`.
  - All fields must have type annotations.

  Example:
    @extension_type()
    class MyPoint:
      x: kd.FLOAT32
      y: kd.FLOAT32

      def norm(self):
        return (self.x**2 + self.y**2)**0.5

    # Creates a QValue instance of MyPoint.
    p1 = MyPoint(x=1.0, y=2.0)

  Args:
    unsafe_override: Overrides existing registered extension types.

  Returns:
    A new class that serves as a factory for the extension type.
  """

  def impl(original_class: type[Any]) -> type[arolla.AnyQValue]:
    class_meta = _get_class_meta(original_class)

    # QValue construction.
    qvalue_class_attrs = {}
    for name in class_meta.signature.parameters:
      qvalue_class_attrs[name] = property(
          lambda self, k=name: getattr(unwrap(self), k)
      )
    qvalue_class_attrs |= class_meta.methods
    qvalue_class = type(
        f'{original_class.__name__}QValue', (arolla.QValue,), qvalue_class_attrs
    )

    extension_qtype = M.derived_qtype.get_labeled_qtype(
        qtypes.DATA_SLICE, original_class.__name__
    ).qvalue
    arolla.abc.register_qvalue_specialization(extension_qtype, qvalue_class)

    # ExprView construction.
    expr_view_class_attrs = {}
    for name in class_meta.signature.parameters:
      expr_view_class_attrs[name] = property(
          lambda self, k=name: getattr(
              M.derived_qtype.upcast(extension_qtype, self), k
          )
      )
    expr_view_class_attrs |= class_meta.methods

    expr_view_class = type(
        f'{original_class.__name__}ExprView',
        (view.KodaView,),
        expr_view_class_attrs,
    )
    arolla.abc.set_expr_view_for_qtype(extension_qtype, expr_view_class)

    schema = arolla.abc.aux_eval_op(
        'kd.named_schema',
        original_class.__name__,
        **{
            p.name: p.annotation
            for p in class_meta.signature.parameters.values()
        },
    )

    def dispatching_new(cls, *args, **kwargs):  # pylint: disable=unused-argument
      bound_args = class_meta.signature.bind(*args, **kwargs)
      bound_args.apply_defaults()
      field_values = bound_args.arguments

      if arolla.Expr in (type(v) for v in field_values.values()):
        return M.annotation.qtype(
            M.derived_qtype.downcast(
                extension_qtype,
                arolla.abc.lookup_operator('kd.new')(
                    **field_values, schema=schema
                ),
            ),
            extension_qtype,
        )
      else:
        return wrap(
            data_bag.DataBag._new_no_bag(**field_values, schema=schema),  # pylint: disable=protected-access
            extension_qtype,
        )

    cls_p = inspect.Parameter('cls', inspect.Parameter.POSITIONAL_OR_KEYWORD)
    dispatching_new.__signature__ = class_meta.signature.replace(
        parameters=[cls_p] + list(class_meta.signature.parameters.values()),
        return_annotation=inspect.Parameter.empty,
    )

    class_attrs = {}
    class_attrs['__new__'] = dispatching_new
    class_attrs['qtype'] = extension_qtype

    new_class = type(original_class.__name__, (object,), class_attrs)
    register_extension_type(
        new_class, extension_qtype, unsafe_override=unsafe_override
    )

    return new_class

  return impl
