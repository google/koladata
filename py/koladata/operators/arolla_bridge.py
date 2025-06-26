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

"""Operator(s) for evaluating normal Arolla expressions on DataSlice(s)."""

from arolla import arolla
from arolla.derived_qtype import derived_qtype
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import qtypes

M = arolla.M | derived_qtype.M
P = arolla.P
constraints = arolla.optools.constraints

# NOTE: Implemented in C++ to allow bind-time literal evaluation.
to_arolla_int64 = arolla.abc.lookup_operator('koda_internal.to_arolla_int64')
to_arolla_text = arolla.abc.lookup_operator('koda_internal.to_arolla_text')


# Implemented here to avoid a dependency cycle between jagged_shape and here.
@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.shapes._reshape',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_jagged_shape(P.shape),
    ],
)
def _reshape(x, shape):  # pylint: disable=unused-argument
  """Returns a DataSlice with the provided shape."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.to_arolla_float64',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=arolla.FLOAT64,
)
def to_arolla_float64(x):  # pylint: disable=unused-argument
  """Returns `x` converted into an arolla float64 value.

  Note that `x` must adhere to the following requirements:
  * `rank = 0`.
  * Have one of the following schemas: NONE, FLOAT32, FLOAT64, OBJECT.
  * Have a present value with type FLOAT32 or FLOAT64.

  In all other cases, an exception is raised.

  Args:
    x: A DataItem to be converted into an arolla float64 value.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.to_arolla_boolean',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=arolla.BOOLEAN,
)
def to_arolla_boolean(x):  # pylint: disable=unused-argument
  """Returns `x` converted into an arolla boolean value.

  Note that `x` must adhere to the following requirements:
  * `rank = 0`.
  * Have one of the following schemas: NONE, BOOLEAN, OBJECT.
  * Have a present value with type BOOLEAN.

  In all other cases, an exception is raised.

  Args:
    x: A DataItem to be converted into an arolla boolean value.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.to_arolla_optional_unit',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=arolla.OPTIONAL_UNIT,
)
def to_arolla_optional_unit(x):  # pylint: disable=unused-argument
  """Returns `x` converted into an optional arolla unit value.

  Note that `x` must adhere to the following requirements:
  * `rank = 0`.
  * Have one of the following schemas: NONE, MASK, OBJECT.
  * Have a present value with type UNIT, or be missing.

  In all other cases, an exception is raised.

  Args:
    x: A DataItem to be converted into an optional arolla unit value.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.to_arolla_dense_array_int64',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=arolla.DENSE_ARRAY_INT64,
)
def to_arolla_dense_array_int64(x):  # pylint: disable=unused-argument
  """Returns `x` converted into a DENSE_ARRAY_INT64 value.

  Note that `x` must adhere to the following requirements:
  * Have any rank, as it will be flattened before conversion.
  * Have one of the following schemas: NONE, INT32, INT64, OBJECT.
  * Have values of a single type - either INT32 or INT64. Missing values of
    unknown type are treated as missing INT64 values.

  In all other cases, an exception is raised.

  Args:
    x: A DataSlice to be converted into a DENSE_ARRAY_INT64 value.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.to_arolla_dense_array_unit',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=arolla.DENSE_ARRAY_UNIT,
)
def to_arolla_dense_array_unit(x):  # pylint: disable=unused-argument
  """Returns `x` converted into a DENSE_ARRAY_UNIT value.

  Note that `x` must adhere to the following requirements:
  * Have any rank, as it will be flattened before conversion.
  * Have one of the following schemas: NONE, MASK, OBJECT.
  * Have values of a single type - UNIT. Missing values of unknown type are
    treated as missing UNIT values.

  In all other cases, an exception is raised.

  Args:
    x: A DataSlice to be converted into a DENSE_ARRAY_UNIT value.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.to_arolla_dense_array_text',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=arolla.DENSE_ARRAY_TEXT,
)
def to_arolla_dense_array_text(x):  # pylint: disable=unused-argument
  """Returns `x` converted into a DENSE_ARRAY_TEXT value.

  Note that `x` must adhere to the following requirements:
  * Have any rank, as it will be flattened before conversion.
  * Have one of the following schemas: NONE, STRING, OBJECT.
  * Have values of a single type - STRING. Missing values of unknown type are
    treated as missing STRING values.

  In all other cases, an exception is raised.

  Args:
    x: A DataSlice to be converted into a DENSE_ARRAY_TEXT value.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal._to_data_slice',
    qtype_constraints=[
        (
            M.qtype.is_scalar_qtype(P.x)
            | M.qtype.is_optional_qtype(P.x)
            | M.qtype.is_dense_array_qtype(P.x),
            (
                'expected scalar, optional or dense_array, got'
                f' {constraints.name_type_msg(P.x)}'
            ),
        ),
        constraints.expect_scalar_qtype_in(
            P.x,
            (
                arolla.INT32,
                arolla.INT64,
                arolla.FLOAT32,
                arolla.FLOAT64,
                arolla.BOOLEAN,
                arolla.UNIT,
                arolla.TEXT,
                arolla.BYTES,
            ),
        ),
    ],
)
def _to_data_slice(x):  # pylint: disable=unused-argument
  """Converts `x` to a DataSlice."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator('koda_internal.to_data_slice')
def to_data_slice(x):
  """Converts `x` to a DataSlice."""
  to_dense_array_or_scalar = arolla.types.DispatchOperator(
      'x',
      array_case=arolla.types.DispatchCase(
          M.array.as_dense_array(P.x), condition=M.qtype.is_array_qtype(P.x)
      ),
      default=P.x,
  )
  to_slice = arolla.types.DispatchOperator(
      'x',
      passthrough_case=arolla.types.DispatchCase(
          P.x, condition=P.x == qtypes.DATA_SLICE
      ),
      default=_to_data_slice(to_dense_array_or_scalar(P.x)),
  )
  return to_slice(x)


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.from_arolla_jagged_shape',
    qtype_constraints=[
        qtype_utils.expect_arolla_jagged_shape(P.shape),
    ],
)
def from_arolla_jagged_shape(shape):
  """Returns a Koda JaggedShape from an Arolla JaggedShape."""
  return M.derived_qtype.downcast(qtypes.JAGGED_SHAPE, shape)


@optools.add_to_registry(view=None)
@optools.as_lambda_operator(
    'koda_internal.to_arolla_jagged_shape',
    qtype_constraints=[
        qtype_utils.expect_jagged_shape(P.shape),
    ],
)
def to_arolla_jagged_shape(shape):
  """Returns an Arolla JaggedShape from a Koda JaggedShape."""
  return M.derived_qtype.upcast(qtypes.JAGGED_SHAPE, shape)
