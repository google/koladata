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

"""Assertion Koda operators."""

from arolla import arolla as _arolla
from koladata.operators import arolla_bridge as _arolla_bridge
from koladata.operators import optools as _optools
from koladata.operators import qtype_utils as _qtype_utils
from koladata.types import qtypes as _qtypes


_P = _arolla.P
_M = _arolla.M
_constraints = _arolla.optools.constraints


@_optools.add_to_registry(via_cc_operator_package=True)
@_optools.as_backend_operator(
    'kd.assertion._with_assertion',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.condition),
        _qtype_utils.expect_data_slice(_P.message_or_fn),
    ],
    qtype_inference_expr=_P.x,
)
def _with_assertion(x, condition, message_or_fn, args):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(via_cc_operator_package=True)
@_optools.as_lambda_operator(
    'kd.assertion.with_assertion',
    qtype_constraints=[
        (
            (_P.condition == _arolla.UNIT)
            | (_P.condition == _arolla.OPTIONAL_UNIT)
            | (_P.condition == _qtypes.DATA_SLICE),
            (
                'expected a unit scalar, unit optional, or a DataSlice, got'
                f' {_constraints.name_type_msg(_P.condition)}'
            ),
        ),
        (
            (_P.message_or_fn == _arolla.TEXT)
            | (_P.message_or_fn == _qtypes.DATA_SLICE),
            (
                'expected TEXT or DATA_SLICE, got'
                f' {_constraints.name_type_msg(_P.message_or_fn)}'
            ),
        ),
    ],
)
def with_assertion(x, condition, message_or_fn, *args):
  """Returns `x` if `condition` is present, else raises error `message_or_fn`.

  `message_or_fn` should either be a STRING message or a functor taking the
  provided `*args` and creating an error message from it. If `message_or_fn` is
  a STRING, the `*args` should be omitted. If `message_or_fn` is a functor, it
  will only be invoked if `condition` is `missing`.

  Example:
    x = kd.slice(1)
    y = kd.slice(2)
    kd.assertion.with_assertion(x, x < y, 'x must be less than y') # -> x.
    kd.assertion.with_assertion(
        x, x > y, 'x must be greater than y'
    ) # -> error: 'x must be greater than y'.
    kd.assertion.with_assertion(
        x, x > y, lambda: 'x must be greater than y'
    ) # -> error: 'x must be greater than y'.
    kd.assertion.with_assertion(
        x,
        x > y,
        lambda x, y: kd.format('x={x} must be greater than y={y}', x=x, y=y),
        x,
        y,
    ) # -> error: 'x=1 must be greater than y=2'.

  Args:
    x: The value to return if `condition` is present.
    condition: A unit scalar, unit optional, or DataItem holding a mask.
    message_or_fn: The error message to raise if `condition` is not present, or
      a functor producing such an error message.
    *args: Auxiliary data to be passed to the `message_or_fn` functor.
  """
  args = _arolla.optools.fix_trace_args(args)
  # Note: consider optimizing the x == UNIT case.
  return _with_assertion(
      x,
      _arolla_bridge.to_data_slice(condition),
      _arolla_bridge.to_data_slice(message_or_fn),
      args,
  )


@_optools.add_to_registry(via_cc_operator_package=True)
@_optools.as_backend_operator(
    'kd.assertion.assert_primitive',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.arg_name),
        _qtype_utils.expect_data_slice(_P.ds),
        _qtype_utils.expect_data_slice(_P.primitive_schema),
    ],
)
def assert_primitive(arg_name, ds, primitive_schema):  # pylint: disable=unused-argument
  """Returns `ds` if its data is implicitly castable to `primitive_schema`.

  It raises an exception if:
    1) `ds`'s schema is not primitive_schema (including NONE) or OBJECT
    2) `ds` has present items and not all of them are castable to
       `primitive_schema`

  The following examples will pass:
    assert_primitive('x', kd.present, kd.MASK)
    assert_primitive('x', kd.slice([kd.present, kd.missing]), kd.MASK)
    assert_primitive('x', kd.slice(None, schema=kd.OBJECT), kd.MASK)
    assert_primitive('x', kd.slice([], schema=kd.OBJECT), kd.MASK)
    assert_primitive('x', kd.slice([1, 3.14], schema=kd.OBJECT), kd.FLOAT32)
    assert_primitive('x', kd.slice([1, 2]), kd.FLOAT32)

  The following examples will fail:
    assert_primitive('x', 1, kd.MASK)
    assert_primitive('x', kd.slice([kd.present, 1]), kd.MASK)
    assert_primitive('x', kd.slice(1, schema=kd.OBJECT), kd.MASK)

  Args:
    arg_name: The name of `ds`.
    ds: DataSlice to assert the dtype of.
    primitive_schema: The expected primitive schema.
  """
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(via_cc_operator_package=True)
@_optools.as_backend_operator(
    'kd.assertion.assert_present_scalar',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.arg_name),
        _qtype_utils.expect_data_slice(_P.ds),
        _qtype_utils.expect_data_slice(_P.primitive_schema),
    ],
)
def assert_present_scalar(arg_name, ds, primitive_schema):  # pylint: disable=unused-argument
  """Returns the present scalar `ds` if it's implicitly castable to `primitive_schema`.

  It raises an exception if:
    1) `ds`'s schema is not primitive_schema (including NONE) or OBJECT
    2) `ds` is not a scalar
    3) `ds` is not present
    4) `ds` is not castable to `primitive_schema`

  The following examples will pass:
    assert_present_scalar('x', kd.present, kd.MASK)
    assert_present_scalar('x', 1, kd.INT32)
    assert_present_scalar('x', 1, kd.FLOAT64)

  The following examples will fail:
    assert_primitive('x', kd.missing, kd.MASK)
    assert_primitive('x', kd.slice([kd.present]), kd.MASK)
    assert_primitive('x', kd.present, kd.INT32)

  Args:
    arg_name: The name of `ds`.
    ds: DataSlice to assert the dtype, presence and rank of.
    primitive_schema: The expected primitive schema.
  """
  raise NotImplementedError('implemented in the backend')
