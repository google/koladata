# Copyright 2024 Google LLC
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

"""Assertion Koda operators."""

from arolla import arolla
from koladata.operators import arolla_bridge
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import py_boxing
from koladata.types import qtypes


P = arolla.P
M = arolla.M
constraints = arolla.optools.constraints


@optools.add_to_registry()
@arolla.optools.as_lambda_operator(  # Default Arolla boxing (for message).
    'kde.assertion.with_assertion',
    qtype_constraints=[
        (
            (P.condition == arolla.UNIT)
            | (P.condition == arolla.OPTIONAL_UNIT)
            | (P.condition == qtypes.DATA_SLICE),
            (
                'expected a unit scalar, unit optional, or a DataSlice, got'
                f' {constraints.name_type_msg(P.condition)}'
            ),
        ),
        constraints.expect_scalar_text(P.message),
    ],
)
def with_assertion(x, condition, message):
  """Returns `x` if `condition` is present, else raises error `message`.

  Example:
    x = kd.slice(1)
    y = kd.slice(2)
    kde.assertion.with_assertion(x, x < y, 'x must be less than y') -> x.
    kde.assertion.with_assertion(x, x > y, 'x must be greater than y') -> error.

  Args:
    x: The value to return if `condition` is present.
    condition: A unit scalar, unit optional, or DataItem holding a mask.
    message: The error message to raise if `condition` is not present.
  """
  condition = arolla.types.DispatchOperator(
      'condition',
      data_slice_case=arolla.types.DispatchCase(
          arolla_bridge.to_arolla_optional_unit(P.condition),
          condition=P.condition == qtypes.DATA_SLICE,
      ),
      default=P.condition,
  )(condition)
  return M.core.with_assertion(x, condition, message)


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.assertion.assert_ds_has_primitives_of',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.ds),
        qtype_utils.expect_data_slice(P.primitive_schema),
        constraints.expect_scalar_text(P.message),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    aux_policy=py_boxing.DEFAULT_AROLLA_POLICY,  # Default Arolla boxing
)
def assert_ds_has_primitives_of(ds, primitive_schema, message):  # pylint: disable=unused-argument
  """Returns `ds` if it matches `primitive_schema`, or raises an exception.

  It raises an exception if:
    1) `ds`'s schema is not primitive_schema, OBJECT or ANY
    2) `ds` has present items and not all of them match `primitive_schema`

  The following examples will pass:
    assert_ds_has_primitives_of(kd.present, kd.MASK, '')
    assert_ds_has_primitives_of(kd.slice([kd.present, kd.missing]), kd.MASK, '')
    assert_ds_has_primitives_of(kd.slice(None, schema=kd.OBJECT), kd.MASK, '')
    assert_ds_has_primitives_of(kd.slice(None, schema=kd.ANY), kd.MASK, '')
    assert_ds_has_primitives_of(kd.slice([], schema=kd.OBJECT), kd.MASK, '')
    assert_ds_has_primitives_of(kd.slice([], schema=kd.ANY), kd.MASK, '')

  The following examples will fail:
    assert_ds_has_primitives_of(1, kd.MASK, '')
    assert_ds_has_primitives_of(kd.slice([kd.present, 1]), kd.MASK, '')
    assert_ds_has_primitives_of(kd.slice(1, schema=kd.OBJECT), kd.MASK, '')
    assert_ds_has_primitives_of(kd.slice(1, schema=kd.ANY), kd.MASK, '')

  Args:
    ds: DataSlice to assert the dtype of.
    primitive_schema: The expected primitive schema.
    message: The error message to raise if the primitive schemas do not match.

  Returns:
    `ds` if the primitive schemas match.
  """
  raise NotImplementedError('implemented in the backend')
