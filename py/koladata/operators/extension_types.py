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

"""Extension type operators."""

from arolla import arolla
from arolla.derived_qtype import derived_qtype
from arolla.objects import objects
from koladata.expr import view
from koladata.operators import arolla_bridge
from koladata.operators import optools
from koladata.types import schema_constants


M = arolla.M | derived_qtype.M | objects.M
P = arolla.P
INT64 = schema_constants.INT64
constraints = arolla.optools.constraints

_NULL_OBJ = objects.Object(_is_null_marker=arolla.present())


@optools.as_lambda_operator(
    'kd.extension_types._wrap',
    qtype_constraints=[
        constraints.expect_qtype_in(P.obj, [objects.OBJECT]),
        constraints.expect_qtype(P.qtype),
    ],
)
def _wrap(obj, qtype):
  """Wraps the `obj` into an extension type with the provided `qtype`."""
  return M.derived_qtype.downcast(qtype, obj)


# Lambda operator without qtype_constraints to support `M.annotation.qtype` to
# propagate even `obj` is symbolic.
@optools.add_to_registry(
    view=None, via_cc_operator_package=True
)  # Provided by the QType.
@optools.as_lambda_operator('kd.extension_types.wrap')
def wrap(obj, qtype):
  return M.annotation.qtype(_wrap(obj, qtype), qtype)


@optools.add_to_registry(view=view.BaseKodaView, via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.extension_types.unwrap',
    qtype_constraints=[(
        (M.qtype.decay_derived_qtype(P.ext) == objects.OBJECT)
        & (P.ext != objects.OBJECT),
        (
            'expected an extension type qtype, got'
            f' {constraints.name_type_msg(P.ext)}'
        ),
    )],
)
def unwrap(ext):
  """Unwraps the extension type `ext` into an arolla::Object."""
  return M.derived_qtype.upcast(M.qtype.qtype_of(ext), ext)


@optools.add_to_registry(
    view=None, via_cc_operator_package=True
)  # Provided by the QType.
@optools.as_lambda_operator('kd.extension_types.dynamic_cast')
def dynamic_cast(ext, qtype):
  """Up-, down-, and side-casts `value` to `qtype`."""
  return wrap(unwrap(ext), qtype)


@optools.add_to_registry(
    view=None, via_cc_operator_package=True
)  # Provided by the QType.
@optools.as_lambda_operator('kd.extension_types.make')
def make(qtype, prototype=arolla.unspecified(), /, **attrs):
  """Returns an extension type of the given `qtype` with the given `attrs`.

  Args:
    qtype: the output qtype of the extension type.
    prototype: parent object (arolla.Object).
    **attrs: attributes of the extension type.
  """
  attrs = arolla.optools.fix_trace_kwargs(attrs)
  obj = M.objects.make_object(prototype, attrs)
  return wrap(obj, qtype)


@optools.add_to_registry(
    view=None, via_cc_operator_package=True
)  # Provided by the QType.
@optools.as_lambda_operator('kd.extension_types.with_attrs')
def with_attrs(ext, /, **attrs):
  """Returns `ext` containing the given `attrs`."""
  attrs = arolla.optools.fix_trace_kwargs(attrs)
  # NOTE: Using Arolla operators for performance reasons. The following snippet
  # adds ~50ns to the runtime, while using Koda primitives adds ~1us.
  ext = M.core.with_assertion(
      ext,
      ~M.objects.has_object_attr(unwrap(ext), '_is_null_marker'),
      'expected a non-null extension type',
  )
  return arolla.abc.bind_op(make, M.qtype.qtype_of(ext), unwrap(ext), attrs)


# Consider asserting that `ext` is not null. Note that this adds ~50ns overhead
# compared to the existing total time of ~80ns.
@optools.add_to_registry(
    view=None, via_cc_operator_package=True
)  # Provided by the QType.
@optools.as_lambda_operator('kd.extension_types.get_attr')
def get_attr(ext, attr, qtype):
  """Returns the attribute of `ext` with name `attr` and type `qtype`."""
  attr = arolla_bridge.to_arolla_text(attr)
  return M.objects.get_object_attr(unwrap(ext), attr, qtype)


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator('kd.extension_types.has_attr')
def has_attr(ext, attr):
  """Returns present iff `attr` is an attribute of `ext`."""
  attr = arolla_bridge.to_arolla_text(attr)
  return arolla_bridge.to_data_slice(
      M.objects.has_object_attr(unwrap(ext), attr)
  )


@optools.add_to_registry(
    view=None, via_cc_operator_package=True
)  # Provided by the QType.
@optools.as_lambda_operator('kd.extension_types.make_null')
def make_null(qtype):
  """Returns a null instance of an extension type.

  WARNING: Not to be confused with the standard `kd.extension_types.make`!

  A null instance of an extension type has no attributes and calling `getattr`
  or `with_attrs` on it will raise an error.

  Args:
    qtype: QType of the extension type.
  """
  return wrap(_NULL_OBJ, qtype)


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator('kd.extension_types.is_null')
def is_null(ext):
  """Returns present iff `ext` is null."""
  return has_attr(ext, '_is_null_marker')


@optools.add_to_registry(view=view.BaseKodaView, via_cc_operator_package=True)
@optools.as_lambda_operator('kd.extension_types.get_attr_qtype')
def get_attr_qtype(ext, attr):
  """Returns the qtype of the `attr`, or NOTHING if the `attr` is missing."""
  attr = arolla_bridge.to_arolla_text(attr)
  return M.objects.get_object_attr_qtype(unwrap(ext), attr)
