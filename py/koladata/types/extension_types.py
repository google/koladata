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
from arolla.objects import objects
from koladata.expr import view
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import extension_type_registry
from koladata.types import jagged_shape
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_item


M = arolla.M | derived_qtype.M | objects.M

_UPCAST_EXPR = M.derived_qtype.upcast(M.qtype.qtype_of(arolla.L.x), arolla.L.x)


@functools.lru_cache
def _get_downcast_expr(qtype):
  return M.derived_qtype.downcast(qtype, arolla.L.x)


@dataclasses.dataclass(frozen=True)
class _ClassMeta:
  signature: inspect.Signature
  methods: Mapping[str, Callable[..., Any]]
  field_annotations: Mapping[str, Any]
  field_qtypes: Mapping[str, arolla.QType]


def _safe_issubclass(subcls: Any, cls: type[Any]) -> bool:
  """Safe version of `issubclass` allowing `subcls` to be Any."""
  return isinstance(subcls, type) and issubclass(subcls, cls)


def _get_class_meta(original_class: type[Any]) -> _ClassMeta:
  """Returns meta information about the given class."""
  data_class = dataclasses.dataclass()(original_class)
  fields = {f.name: f for f in dataclasses.fields(data_class)}
  methods = {}
  for attr in dir(data_class):
    if attr in fields:
      continue
    method = getattr(data_class, attr)
    if not callable(method):
      continue
    methods[attr] = method

  sig = inspect.signature(data_class, eval_str=True)
  field_annotations = {}
  field_qtypes = {}
  for param in sig.parameters.values():
    if isinstance(param.annotation, schema_item.SchemaItem):
      field_qtypes[param.name] = qtypes.DATA_SLICE
    elif isinstance(param.annotation, arolla.QType):
      field_qtypes[param.name] = param.annotation
    elif _safe_issubclass(param.annotation, data_slice.DataSlice):
      field_qtypes[param.name] = qtypes.DATA_SLICE
    elif _safe_issubclass(param.annotation, data_bag.DataBag):
      field_qtypes[param.name] = qtypes.DATA_BAG
    elif _safe_issubclass(param.annotation, jagged_shape.JaggedShape):
      field_qtypes[param.name] = qtypes.JAGGED_SHAPE
    elif extension_type_registry.is_koda_extension_type(param.annotation):
      field_qtypes[param.name] = extension_type_registry.get_extension_qtype(
          param.annotation
      )
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


def _cast_input_qvalue(value: Any, annotation: Any) -> arolla.AnyQValue:
  if isinstance(annotation, schema_item.SchemaItem):
    return arolla.abc.aux_eval_op('kd.schema.cast_to_narrow', value, annotation)
  else:
    return py_boxing.as_qvalue(value)


def _cast_input_expr(value: arolla.Expr, annotation: Any) -> arolla.Expr:
  if isinstance(annotation, schema_item.SchemaItem):
    return arolla.abc.aux_bind_op('kd.schema.cast_to_narrow', value, annotation)
  else:
    return py_boxing.as_qvalue_or_expr(value)


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
    extension_qtype = M.derived_qtype.get_labeled_qtype(
        extension_type_registry.BASE_QTYPE, original_class.__name__
    ).qvalue

    # QValue construction.
    qvalue_class_attrs = {}
    for name, qtype in class_meta.field_qtypes.items():
      qvalue_class_attrs[name] = property(
          lambda self, k=name, qtype=qtype: arolla.eval(
              _UPCAST_EXPR, x=self
          ).get_attr(k, qtype)
      )
    qvalue_class_attrs |= class_meta.methods
    qvalue_class = type(
        f'{original_class.__name__}QValue', (arolla.QValue,), qvalue_class_attrs
    )

    arolla.abc.register_qvalue_specialization(extension_qtype, qvalue_class)

    # ExprView construction.
    expr_view_class_attrs = {}
    for name, qtype in class_meta.field_qtypes.items():
      expr_view_class_attrs[name] = property(
          lambda self, k=name, qtype=qtype: M.objects.get_object_attr(
              M.derived_qtype.upcast(M.qtype.qtype_of(self), self), k, qtype
          )
      )
    expr_view_methods = {
        name: method
        for name, method in class_meta.methods.items()
        if arolla.abc.is_allowed_expr_view_member_name(name)
    }
    expr_view_class_attrs |= expr_view_methods

    expr_view_class = type(
        f'{original_class.__name__}ExprView',
        (view.BaseKodaView,),
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
        obj = M.objects.make_object(None, **field_values)
        ext = M.derived_qtype.downcast(extension_qtype, obj)
        return M.annotation.qtype(ext, extension_qtype)
      else:
        field_values = {
            k: _cast_input_qvalue(v, class_meta.field_annotations[k])
            for k, v in field_values.items()
        }
        return arolla.eval(
            _get_downcast_expr(extension_qtype),
            x=objects.Object(**field_values),
        )

    cls_p = inspect.Parameter('cls', inspect.Parameter.POSITIONAL_OR_KEYWORD)
    dispatching_new.__signature__ = class_meta.signature.replace(
        parameters=[cls_p] + list(class_meta.signature.parameters.values()),
        return_annotation=inspect.Parameter.empty,
    )

    original_class.__new__ = dispatching_new
    extension_type_registry.register_extension_type(
        original_class, extension_qtype, unsafe_override=unsafe_override
    )

    return original_class

  return impl
