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
from koladata.types import jagged_shape
from koladata.types import qtypes
from koladata.types import schema_item


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
  return data_bag.DataBag.empty().freeze()


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


# Dummy dataclass used to get the list of default dataclassfields.
@dataclasses.dataclass()
class _DummyDC:
  _dummy_field: int


_DEFAULT_FIELDS = frozenset(dir(_DummyDC))


@dataclasses.dataclass(frozen=True)
class _ClassMeta:
  signature: inspect.Signature
  methods: Mapping[str, Callable[..., Any]]
  field_annotations: Mapping[str, Any]
  field_qtypes: Mapping[str, arolla.QType]


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

  sig = inspect.signature(data_class, eval_str=True)
  field_annotations = {}
  field_qtypes = {}
  # NOTE: This should be kept up-to-date with `get_dummy_value`.
  for param in sig.parameters.values():
    if isinstance(param.annotation, schema_item.SchemaItem):
      field_qtypes[param.name] = qtypes.DATA_SLICE
    elif issubclass(param.annotation, data_slice.DataSlice):
      field_qtypes[param.name] = qtypes.DATA_SLICE
    elif issubclass(param.annotation, data_bag.DataBag):
      field_qtypes[param.name] = qtypes.DATA_BAG
    elif issubclass(param.annotation, jagged_shape.JaggedShape):
      field_qtypes[param.name] = qtypes.JAGGED_SHAPE
    elif is_koda_extension_type(param.annotation):
      field_qtypes[param.name] = get_extension_qtype(param.annotation)
    else:
      raise ValueError(
          f'unsupported extension type annotation: {param.annotation}'
      )
    field_annotations[param.name] = param.annotation

  return _ClassMeta(
      signature=sig,
      methods=methods,
      field_annotations=field_annotations,
      field_qtypes=field_qtypes,
  )


def _sidecast_expr(value: Any, to_qtype: arolla.QType) -> arolla.Expr:
  return M.derived_qtype.downcast(
      to_qtype,
      M.derived_qtype.upcast(M.qtype.qtype_of(value), value),
  )


def _sidecast_qvalue(
    value: arolla.QValue, to_qtype: arolla.QType
) -> arolla.AnyQValue:
  tpl = arolla.eval(_UPCAST_EXPR, x=value)
  return arolla.eval(_get_downcast_expr(to_qtype), x=tpl)


def _cast_input_qvalue(value: Any, annotation: Any) -> arolla.AnyQValue:
  if isinstance(annotation, schema_item.SchemaItem):
    return arolla.abc.aux_eval_op('kd.schema.cast_to_narrow', value, annotation)
  else:
    return value


def _cast_input_expr(value: arolla.Expr, annotation: Any) -> arolla.Expr:
  if isinstance(annotation, schema_item.SchemaItem):
    return arolla.abc.aux_bind_op('kd.schema.cast_to_narrow', value, annotation)
  else:
    return value


def extension_type(
    unsafe_override=False,
) -> Callable[[type[Any]], type[arolla.AnyQValue]]:
  """Creates a Koda extension type from the the given original class.

  This function is intended to be used as a class decorator. The decorated class
  serves as a schema for the new extension type.

  Internally, this function creates the following:
  -  A new `QType` for the extension type, which is a labeled `QType` on top of
    a TUPLE.
  - A `QValue` class for representing evaluated instances of the extension type.
  - An `ExprView` class for representing expressions that will evaluate to the
    extension type.

  It replaces the decorated class with a new class that acts as a factory. This
  factory's `__new__` method dispatches to either create an `Expr` or a `QValue`
  instance, depending on the types of the arguments provided.

  The fields of the dataclass are exposed as properties on both the `QValue` and
  `ExprView` classes. Any methods defined on the dataclass are also carried
  over.

  Note:
  - The decorated class must not have its own `__new__` method.
  - The type annotations on the fields of the dataclass are used to determine
    the schema of the underlying `DataSlice` (if relevant).
  - All fields must have type annotations.
  - Supported annotations include `SchemaItem`, `DataSlice`, `DataBag`,
    `JaggedShape`, and other extension types.

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

  if not isinstance(unsafe_override, bool):
    raise TypeError(
        f'expected {unsafe_override=} to be a bool - did you mean to write'
        ' `@extension_type()` instead of `@extension_type`?'
    )

  def impl(original_class: type[Any]) -> type[arolla.AnyQValue]:
    class_meta = _get_class_meta(original_class)

    # QType definitions.
    named_tuple_qtype = arolla.make_namedtuple_qtype(**class_meta.field_qtypes)
    tuple_qtype = arolla.abc.invoke_op(
        'qtype.decay_derived_qtype', (named_tuple_qtype,)
    )
    extension_qtype = M.derived_qtype.get_labeled_qtype(
        tuple_qtype, original_class.__name__
    ).qvalue

    # QValue construction.
    qvalue_class_attrs = {}
    for name in class_meta.signature.parameters:
      qvalue_class_attrs[name] = property(
          lambda self, k=name: _sidecast_qvalue(self, named_tuple_qtype)[k]
      )
    qvalue_class_attrs |= class_meta.methods
    qvalue_class = type(
        f'{original_class.__name__}QValue', (arolla.QValue,), qvalue_class_attrs
    )

    arolla.abc.register_qvalue_specialization(extension_qtype, qvalue_class)

    # ExprView construction.
    expr_view_class_attrs = {}
    for name in class_meta.signature.parameters:
      expr_view_class_attrs[name] = property(
          lambda self, k=name: M.namedtuple.get_field(
              _sidecast_expr(self, named_tuple_qtype), k
          )
      )
    expr_view_class_attrs |= class_meta.methods

    expr_view_class = type(
        f'{original_class.__name__}ExprView',
        (view.KodaView,),
        expr_view_class_attrs,
    )
    arolla.abc.set_expr_view_for_qtype(extension_qtype, expr_view_class)

    def dispatching_new(cls, *args, **kwargs):  # pylint: disable=unused-argument
      bound_args = class_meta.signature.bind(*args, **kwargs)
      bound_args.apply_defaults()
      field_values = bound_args.arguments

      if arolla.Expr in (type(v) for v in field_values.values()):
        field_values = {
            k: _cast_input_expr(v, class_meta.field_annotations[k])
            for k, v in field_values.items()
        }
        return M.annotation.qtype(
            _sidecast_expr(
                arolla.abc.lookup_operator('kd.namedtuple')(**field_values),
                extension_qtype,
            ),
            extension_qtype,
        )
      else:
        field_values = {
            k: _cast_input_qvalue(v, class_meta.field_annotations[k])
            for k, v in field_values.items()
        }
        return _sidecast_qvalue(
            arolla.namedtuple(**field_values), extension_qtype
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
