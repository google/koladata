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
from koladata.operators import optools
from koladata.types import schema_constants


M = arolla.M | derived_qtype.M
P = arolla.P
INT64 = schema_constants.INT64
constraints = arolla.optools.constraints


# NOTE(b/417369165): Consider implementing in C++ to support validating that
# `qtype` is a labeled qtype.
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
@optools.add_to_registry(view=None)  # Provided by the QType.
@optools.as_lambda_operator('kd.extension_types.wrap')
def wrap(obj, qtype):
  # TODO: Remove in favor of supporting QType inference directly in
  # M.derived_qtype.downcast.
  return M.annotation.qtype(_wrap(obj, qtype), qtype)


@optools.add_to_registry(view=view.BaseKodaView)
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


@optools.add_to_registry(view=None)  # Provided by the QType.
@optools.as_lambda_operator('kd.extension_types.dynamic_cast')
def dynamic_cast(ext, qtype):
  """Up-, down-, and side-casts `value` to `qtype`."""
  return wrap(unwrap(ext), qtype)
