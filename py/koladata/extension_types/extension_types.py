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
import types
from typing import Any, Callable, Mapping, Self, Sequence
from arolla import arolla
from arolla.derived_qtype import derived_qtype
from arolla.objects import objects
from koladata.expr import view
from koladata.functor import functor_factories
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import extension_type_registry
from koladata.types import jagged_shape
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants
from koladata.types import schema_item


M = arolla.M | derived_qtype.M | objects.M

_VIRTUAL_METHOD_ATTR = '_kd_extension_type_virtual_method'
_OVERRIDE_METHOD_ATTR = '_kd_extension_type_override_method'


def virtual():
  """Marks the method as virtual, allowing it to be overridden."""

  def impl(fn: Callable[..., Any]) -> Callable[..., Any]:
    setattr(fn, _VIRTUAL_METHOD_ATTR, True)
    return fn

  return impl


def override():
  """Marks the method as overriding a virtual method."""

  def impl(fn: Callable[..., Any]) -> Callable[..., Any]:
    setattr(fn, _OVERRIDE_METHOD_ATTR, True)
    return fn

  return impl


def _make_functor_attr_name(method_name: str) -> str:
  return f'_functor_impl__{method_name}'


@dataclasses.dataclass(frozen=True)
class _VirtualMethod:
  # Callable that should be traced into a functor and stored as data.
  functor_impl: Callable[..., Any]
  # Method that should be attached to the ExprView and QValue specialization.
  method: Callable[..., Any]


def _make_virtual_method(
    method_name: str, method: Callable[..., Any], self_annotation: type[Any]
) -> _VirtualMethod:
  """Returns a virtual method as a pair of callables to be traced.

  The `functor_impl` is the calllable that should be traced into a functor and
  stored as data. In order to support returning "self" from a virtual method
  that may be called from a parent, the result is upcasted to Object if
  `self_annotation` is an extension type.

  The `method` is the method that should be attached to the ExprView and QValue
  specialization. This looks up the stored functor as data and calls it with
  a relevant `return_type_as`. If the result is an extension type, it also
  downcasts the results to the specified extension type.

  Args:
    method_name: The name of the method.
    method: The method to trace.
    self_annotation: The type of `self`.
  """
  sig = inspect.signature(method, eval_str=True)
  self_qtype = extension_type_registry.get_extension_qtype(self_annotation)
  output_qtype = None
  return_type_as = None
  if sig.return_annotation is not inspect.Parameter.empty:
    if sig.return_annotation == Self:
      return_type_as = py_boxing.get_dummy_qvalue(self_annotation)
    else:
      return_type_as = py_boxing.get_dummy_qvalue(sig.return_annotation)
    if extension_type_registry.is_koda_extension(return_type_as):
      output_qtype = return_type_as.qtype
      return_type_as = arolla.abc.aux_eval_op(
          'kd.extension_types.unwrap', return_type_as
      )
    # We fall back to whatever the user provided.
    if return_type_as is None and isinstance(
        sig.return_annotation, arolla.QValue
    ):
      return_type_as = sig.return_annotation

  # NOTE: We do not trace here, since we have yet to register an expr view.
  @functools.wraps(method)
  def functor_impl(self, *args, **kwargs):
    # Cast to self in order to downcast from a parent type.
    self_casted = arolla.abc.aux_bind_op(
        'kd.extension_types.dynamic_cast', self, self_qtype
    )
    res = method(self_casted, *args, **kwargs)
    if output_qtype is not None:
      return arolla.abc.aux_bind_op('kd.extension_types.unwrap', res)
    else:
      return res

  @functools.wraps(method)
  def method_impl(self, *args, **kwargs):
    fn = getattr(self, _make_functor_attr_name(method_name))
    if return_type_as is not None and 'return_type_as' in kwargs:
      raise ValueError('return_type_as is already set through annotations')
    return_type_as_ = kwargs.pop('return_type_as', return_type_as)
    res = fn(self, *args, **kwargs, return_type_as=return_type_as_)
    if output_qtype is not None:
      if isinstance(res, arolla.Expr):
        return arolla.abc.aux_bind_op(
            'kd.extension_types.wrap', res, output_qtype
        )
      else:
        return extension_type_registry.wrap(res, output_qtype)
    else:
      return res

  return _VirtualMethod(functor_impl=functor_impl, method=method_impl)


@dataclasses.dataclass(frozen=True)
class _ClassMeta:
  name: str
  fully_qualified_name: str
  signature: inspect.Signature
  non_virtual_methods: Mapping[str, Callable[..., Any]]
  virtual_methods: Mapping[str, Callable[..., Any]]
  field_annotations: Mapping[str, Any]
  field_qtypes: Mapping[str, arolla.QType]
  original_attributes: Sequence[str]
  original_class: type[Any]


def _safe_issubclass(subcls: Any, cls: type[Any]) -> bool:
  """Safe version of `issubclass` allowing `subcls` to be Any."""
  return isinstance(subcls, type) and issubclass(subcls, cls)


def _assert_allowed_virtual_tag(original_class: type[Any], attr: str):
  method = getattr(original_class, attr)
  for cls in original_class.__mro__[1:]:
    cls_attr = getattr(cls, attr, None)
    if (
        cls_attr is not None
        and cls_attr is not method
        and isinstance(cls_attr, types.FunctionType)
    ):
      raise AssertionError(
          f'redefinition of an existing method {attr} for the class'
          f' {original_class} is not allowed for the @virtual annotation'
      )


def _assert_allowed_override_tag(original_class: type[Any], attr: str):
  for cls in original_class.__mro__[1:]:
    if hasattr(cls, attr) and hasattr(getattr(cls, attr), _VIRTUAL_METHOD_ATTR):
      return
  raise AssertionError(
      f'the @override annotation on class {original_class} requires a'
      f' @virtual annotation on an ancestor for the method {attr}'
  )


def _assert_allowed_no_tag(original_class: type[Any], attr: str):
  for cls in original_class.__mro__[1:]:
    if hasattr(cls, attr) and hasattr(getattr(cls, attr), _VIRTUAL_METHOD_ATTR):
      raise AssertionError(
          f'missing @override annotation on class {original_class} for the'
          f' @virtual method {attr} defined on an ancestor class'
      )


def _get_signature(original_cls: type[Any]) -> inspect.Signature:
  """Creates a signature for the original_cls init fn."""
  params = {}
  # Reversed to maintain the original field order and to allow children to
  # overwite parents.
  for cls in reversed(original_cls.__mro__):
    cls_annotations = inspect.get_annotations(cls, eval_str=True)
    for name, annotation in cls_annotations.items():
      default = getattr(cls, name, inspect.Parameter.empty)
      param = inspect.Parameter(
          name,
          inspect.Parameter.POSITIONAL_OR_KEYWORD,
          default=default,
          annotation=annotation,
      )
      params[name] = param  # Allow children to overwrite parents.
  return inspect.Signature(list(params.values()))


def _get_class_meta(original_class: type[Any]) -> _ClassMeta:
  """Returns meta information about the given class."""
  sig = _get_signature(original_class)
  attributes = set(sig.parameters)
  non_virtual_methods, virtual_methods = {}, {}
  for attr in dir(original_class):
    if attr in attributes:
      continue
    method = getattr(original_class, attr)
    # Avoid builtin types - only add those defined by the user.
    if not isinstance(method, (types.FunctionType, types.MethodType)):
      continue
    if hasattr(method, _VIRTUAL_METHOD_ATTR):
      _assert_allowed_virtual_tag(original_class, attr)
      virtual_methods[attr] = method
    elif hasattr(method, _OVERRIDE_METHOD_ATTR):
      _assert_allowed_override_tag(original_class, attr)
      virtual_methods[attr] = method
    else:
      _assert_allowed_no_tag(original_class, attr)
      non_virtual_methods[attr] = method

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

  for name in virtual_methods:
    field_qtypes[_make_functor_attr_name(name)] = qtypes.DATA_SLICE
    field_annotations[_make_functor_attr_name(name)] = schema_constants.OBJECT

  return _ClassMeta(
      name=original_class.__name__,
      fully_qualified_name=(
          original_class.__module__ + '.' + original_class.__qualname__
      ),
      signature=sig,
      non_virtual_methods=non_virtual_methods,
      virtual_methods=virtual_methods,
      field_annotations=field_annotations,
      field_qtypes=field_qtypes,
      original_attributes=list(sig.parameters),
      original_class=original_class,
  )


def _cast_input_qvalue(value: Any, annotation: Any) -> arolla.AnyQValue:
  if isinstance(annotation, schema_item.SchemaItem):
    return arolla.abc.aux_eval_op('kd.schema.cast_to_narrow', value, annotation)
  elif extension_type_registry.is_koda_extension_type(annotation):
    # TODO: Restrict to only support _upcasting_ by encoding the
    # hierarchy into a derived QType chain, making this safe.
    return extension_type_registry.dynamic_cast(
        value, extension_type_registry.get_extension_qtype(annotation)
    )
  else:
    return py_boxing.as_qvalue(value)


def _cast_input_expr(value: arolla.Expr, annotation: Any) -> arolla.Expr:
  """Returns an expr casting `value` to the provided `annotation`."""
  if isinstance(annotation, schema_item.SchemaItem):
    return arolla.abc.aux_bind_op('kd.schema.cast_to_narrow', value, annotation)
  elif extension_type_registry.is_koda_extension_type(annotation):
    # TODO: Restrict to only support _upcasting_ by encoding the
    # hierarchy into a derived QType chain, making this safe.
    return arolla.abc.aux_bind_op(
        'kd.extension_types.dynamic_cast',
        value,
        extension_type_registry.get_extension_qtype(annotation),
    )
  else:
    return py_boxing.as_qvalue_or_expr(value)


def _cast_input(value: Any, annotation: Any) -> arolla.AnyQValue | arolla.Expr:
  if isinstance(value, arolla.Expr):
    return _cast_input_expr(value, annotation)
  else:
    return _cast_input_qvalue(value, annotation)


def _raise_new(cls, *args, **kwargs):
  del args, kwargs
  raise NotImplementedError(
      f'{repr(cls)} is not ready to be initialized - direct extension type'
      ' construction from inside of a @virtual method is not supported -'
      ' please use `self.with_attrs` instead'
  )


def _with_attrs_expr(
    ext: Any, attrs: Mapping[str, Any], field_annotations: Mapping[str, Any]
) -> arolla.Expr:
  attrs = {
      k: _cast_input_expr(v, field_annotations[k]) for k, v in attrs.items()
  }
  return arolla.abc.aux_bind_op('kd.extension_types.with_attrs', ext, **attrs)


def _with_attrs_qvalue(
    ext: Any, attrs: Mapping[str, Any], field_annotations: Mapping[str, Any]
) -> arolla.AnyQValue:
  attrs = {
      k: _cast_input_qvalue(v, field_annotations[k]) for k, v in attrs.items()
  }
  return arolla.abc.aux_eval_op('kd.extension_types.with_attrs', ext, **attrs)


def _with_attrs(
    ext: Any, attrs: Mapping[str, Any], field_annotations: Mapping[str, Any]
) -> arolla.Expr | arolla.AnyQValue:
  if isinstance(ext, arolla.Expr) or any(
      isinstance(attr, arolla.Expr) for attr in attrs.values()
  ):
    return _with_attrs_expr(ext, attrs, field_annotations)
  else:
    return _with_attrs_qvalue(ext, attrs, field_annotations)


def _get_default_arolla_repr_fn(
    class_name: str, attrs: Sequence[str]
) -> Callable[[Any], arolla.abc.ReprToken]:
  """Returns a QValue repr function used if not overridden by the user."""
  sorted_attrs = sorted(attrs)

  def repr_fn(value) -> arolla.abc.ReprToken:
    """Returns a string representation of the extension type."""

    def get_attr(attr: str) -> str:
      try:
        return repr(getattr(value, attr))
      except ValueError:
        return '<unknown>'

    res = arolla.abc.ReprToken()
    attrs_str = ', '.join(f'{attr}={get_attr(attr)}' for attr in sorted_attrs)
    res.text = f'{class_name}({attrs_str})'
    return res

  return repr_fn


def get_annotations(cls: type[Any]) -> dict[str, Any]:
  """Returns the annotations for the provided extension type class."""
  # Check that it's registered
  _ = extension_type_registry.get_extension_qtype(cls)
  cls_meta = _get_class_meta(cls)
  return {
      attr: cls_meta.field_annotations[attr]
      for attr in cls_meta.original_attributes
  }


def _make_qvalue_class(
    class_meta: _ClassMeta, virtual_methods: Mapping[str, _VirtualMethod]
) -> type[arolla.AnyQValue]:
  """Creates a qvalue specialization class."""
  qvalue_class_attrs = {}
  for name, qtype in class_meta.field_qtypes.items():
    qvalue_class_attrs[name] = property(
        lambda self, k=name, qtype=qtype: extension_type_registry.get_attr(
            self, k, qtype
        )
    )
  # Methods.
  qvalue_class_attrs |= class_meta.non_virtual_methods
  for name, virtual_method in virtual_methods.items():
    qvalue_class_attrs[name] = virtual_method.method
  if 'with_attrs' not in qvalue_class_attrs:
    qvalue_class_attrs['with_attrs'] = lambda self, **attrs: _with_attrs(
        self, attrs, class_meta.field_annotations
    )
  return type(
      f'{class_meta.name}_QValue',
      # We inherit from `original_class` mainly to support
      # `isinstance(my_extension, MyExtensionType)`. All user-defined attributes
      # and methods are handled by `qvalue_class_attrs`.
      (arolla.QValue, class_meta.original_class),
      qvalue_class_attrs,
  )


def _make_expr_view_class(
    class_meta: _ClassMeta, virtual_methods: Mapping[str, _VirtualMethod]
) -> type[arolla.AnyQValue]:
  """Creates an expr view class."""
  expr_view_class_attrs = {}
  for name, qtype in class_meta.field_qtypes.items():
    expr_view_class_attrs[name] = property(
        lambda self, k=name, qtype=qtype: arolla.abc.aux_bind_op(
            'kd.extension_types.get_attr', self, k, qtype
        )
    )
  expr_view_methods = {
      name: method
      for name, method in class_meta.non_virtual_methods.items()
      if arolla.abc.is_allowed_expr_view_member_name(name)
  }
  expr_view_class_attrs |= expr_view_methods
  for name, virtual_method in virtual_methods.items():
    expr_view_class_attrs[name] = virtual_method.method
  if 'with_attrs' not in expr_view_class_attrs:
    expr_view_class_attrs['with_attrs'] = (
        lambda self, **attrs: _with_attrs_expr(
            self, attrs, class_meta.field_annotations
        )
    )
  return type(
      f'{class_meta.name}_ExprView', (view.BaseKodaView,), expr_view_class_attrs
  )


def _make_functor_prototype(
    virtual_methods: Mapping[str, _VirtualMethod],
) -> objects.Object | None:
  """Returns an arolla::Object containing all the virtual methods or None."""
  if not virtual_methods:
    return None
  functors = {
      _make_functor_attr_name(name): functor_factories.trace_py_fn(
          vm.functor_impl
      )
      for name, vm in virtual_methods.items()
  }
  # Create a single Object containing all the methods. This will be used as
  # a prototype / base object.
  return objects.Object(**functors)


def _make_dispatch_fn(
    class_meta: _ClassMeta,
    extension_qtype: arolla.QType,
    functors_obj: objects.Object | None,
) -> Callable[..., arolla.AnyQValue | arolla.Expr]:
  """Constructs a <QValue, ExprView>-dispatching function."""

  all_methods = set(class_meta.non_virtual_methods) | set(
      class_meta.virtual_methods
  )
  if '_extension_arg_boxing' in class_meta.non_virtual_methods:
    boxing_fn = class_meta.non_virtual_methods['_extension_arg_boxing']
  else:
    boxing_fn = _cast_input

  def dispatching_new(cls, *args, **kwargs):  # pylint: disable=unused-argument
    bound_args = class_meta.signature.bind(*args, **kwargs)
    bound_args.apply_defaults()
    attrs = {
        k: boxing_fn(v, class_meta.field_annotations[k])
        for k, v in bound_args.arguments.items()
    }
    if arolla.Expr in (type(v) for v in attrs.values()):
      prototype = arolla.unspecified() if functors_obj is None else functors_obj
      value = arolla.abc.aux_bind_op(
          'kd.extension_types.make', extension_qtype, prototype, **attrs
      )
    else:
      value = extension_type_registry.make(
          extension_qtype, functors_obj, **attrs
      )
    if '_extension_post_init' in all_methods:
      value = value._extension_post_init()  # pylint: disable=protected-access
      if value is None:
        raise ValueError('_extension_post_init must return an instance')
    return value

  cls_p = inspect.Parameter('cls', inspect.Parameter.POSITIONAL_OR_KEYWORD)
  # The annotation is stripped to avoid polluting documentation and
  # auto-completion with potentially large schemas.
  parameters = [cls_p] + [
      p.replace(annotation=inspect.Parameter.empty)
      for p in class_meta.signature.parameters.values()
  ]
  dispatching_new.__signature__ = class_meta.signature.replace(
      parameters=parameters, return_annotation=inspect.Parameter.empty
  )
  return dispatching_new


def extension_type(
    unsafe_override=False,
) -> Callable[[type[Any]], type[arolla.AnyQValue]]:
  """Creates a Koda extension type from the given original class.

  This function is intended to be used as a class decorator. The decorated class
  serves as a schema for the new extension type.

  Internally, this function creates the following:
  -  A new `QType` for the extension type, which is a labeled `QType` on top of
    an arolla::Object.
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
  - The decorated class must not have its own `__new__` method - it will be
    ignored.
  - The decorated class must not have its own `__init__` method - it will be
    ignored.
  - The type annotations on the fields of the dataclass are used to determine
    the schema of the underlying `DataSlice` (if relevant).
  - All fields must have type annotations.
  - Supported annotations include `SchemaItem`, `DataSlice`, `DataBag`,
    `JaggedShape`, and other extension types. Additionally, any QType can be
    used as an annotation.
  - The `with_attrs` method is automatically added if not already present,
    allowing for attributes to be dynamically updated.
  - If the class implements the `_extension_arg_boxing(self, value, annotation)`
    classmethod, it will be called on all input arguments (including defaults)
    passed to `MyExtension(...)`. The classmethod should return an arolla QValue
    or an Expression. If the class does not implement such a method, a default
    method will be used.
  - If the class implements the `_extension_post_init(self)` method, it will be
    called as the final step of instantiating the extension through
    `MyExtension(...)`. The method should take `self`, do the necessary post
    processing, and then return the (potentially modified) `self`. As with other
    methods, it's required to be traceable in order to function in a tracing
    context.

  Example:
    @extension_type()
    class MyPoint:
      x: kd.FLOAT32
      y: kd.FLOAT32

      def norm(self):
        return (self.x**2 + self.y**2)**0.5

    # Creates a QValue instance of MyPoint.
    p1 = MyPoint(x=1.0, y=2.0)

  Extension type inheritance is supported through Python inheritance. Passing an
  extension type argument to a functor will automatically upcast / downcast the
  argument to the correct extension type based on the argument annotation. To
  support calling a child class's methods after upcasting, the parent method
  must be annotated with @kd.extension_types.virtual() and the child method
  must be annotated with @kd.extension_types.override(). Internally, this traces
  the methods into Functors. Virtual methods _require_ proper return
  annotations (and if relevant, input argument annotations).

  Example:
    @kd.extension_type(unsafe_override=True)
    class A:
      x: kd.INT32

      def fn(self, y):  # Normal method.
        return self.x + y

      @kd.extension_types.virtual()
      def virt_fn(self, y):  # Virtual method.
        return self.x * y

    @kd.extension_type(unsafe_override=True)
    class B(A):  # Inherits from A.
      y: kd.FLOAT32

      def fn(self, y):
        return self.x + self.y + y

      @kd.extension_types.override()
      def virt_fn(self, y):
        return self.x * self.y * y

    @kd.fn
    def call_a_fn(a: A):  # Automatically casts to A.
      return a.fn(4)      # Calls non-virtual method.

    @kd.fn
    def call_a_virt_fn(a: A):  # Automatically casts to A.
      return a.virt_fn(4)      # Calls virtual method.

    b = B(2, 3)
    # -> 6. `fn` is _not_ marked as virtual, so the parent method is invoked.
    call_a_fn(b)
    # -> 24.0. `virt_fn` is marked as virtual, so the child method is invoked.
    call_a_virt_fn(b)

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

  def impl(original_class: type[Any]) -> type[Any]:
    class_meta = _get_class_meta(original_class)

    # QType definitions.
    extension_qtype = M.derived_qtype.get_labeled_qtype(
        extension_type_registry.BASE_QTYPE, class_meta.fully_qualified_name
    ).qvalue
    extension_type_registry.register_extension_type(
        original_class, extension_qtype, unsafe_override=unsafe_override
    )

    # Virtual methods construction.
    virtual_methods = {
        name: _make_virtual_method(name, method, original_class)
        for name, method in class_meta.virtual_methods.items()
    }

    # QValue construction.
    qvalue_class = _make_qvalue_class(class_meta, virtual_methods)
    arolla.abc.register_qvalue_specialization(extension_qtype, qvalue_class)
    derived_qtype.register_labeled_qtype_repr_fn(
        class_meta.fully_qualified_name,
        _get_default_arolla_repr_fn(
            class_meta.name, class_meta.original_attributes
        ),
        # At this point, we know that the extension type is either not
        # registered or we should overwrite it.
        override=True,
    )

    # ExprView construction.
    expr_view_class = _make_expr_view_class(class_meta, virtual_methods)
    arolla.abc.set_expr_view_for_qtype(extension_qtype, expr_view_class)

    # Trace functors. To prevent hard to understand errors when doing
    # MyExtensionType(1, 2) inside of virtual methods, we attach a raising
    # __new__ instead.
    original_class.__new__ = _raise_new
    functors_obj = _make_functor_prototype(virtual_methods)

    # Attach the dispatching functor.
    original_class.__new__ = _make_dispatch_fn(
        class_meta, extension_qtype, functors_obj
    )
    return original_class

  return impl
