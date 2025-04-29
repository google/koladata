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

import re
from absl.testing import absltest
from arolla import arolla
from koladata.expr import py_expr_eval_py_ext
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.operators import koda_internal_parallel
from koladata.operators import qtype_utils
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import iterable_qvalue
from koladata.types import jagged_shape
from koladata.types import qtypes as _  # pylint: disable=unused-import


class KodaQTypesTest(absltest.TestCase):

  def test_expect_data_slice(self):

    @arolla.optools.as_lambda_operator(
        'op1.name',
        qtype_constraints=[qtype_utils.expect_data_slice(arolla.P.x)],
    )
    def _op(x):
      return x

    with self.subTest('success'):
      _op(data_slice.DataSlice.from_vals(arolla.dense_array([1])))

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError, 'expected DATA_SLICE, got x: INT32'
      ):
        _op(4)

  def test_expect_data_bag(self):

    @arolla.optools.as_lambda_operator(
        'op2.name',
        qtype_constraints=[qtype_utils.expect_data_bag(arolla.P.x)],
    )
    def _op(x):
      return x

    with self.subTest('success'):
      _op(data_bag.DataBag.empty())

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError, 'expected DATA_BAG, got x: INT32'
      ):
        _op(4)

  def test_expect_jagged_shape(self):

    @arolla.optools.as_lambda_operator(
        'op3.name',
        qtype_constraints=[qtype_utils.expect_jagged_shape(arolla.P.x)],
    )
    def _op(x):
      return x

    with self.subTest('success'):
      _op(jagged_shape.create_shape())

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError, 'expected JAGGED_SHAPE, got x: INT32'
      ):
        _op(4)

  def test_expect_jagged_shape_or_unspecified(self):

    @arolla.optools.as_lambda_operator(
        'op5.name',
        qtype_constraints=[
            qtype_utils.expect_jagged_shape_or_unspecified(arolla.P.x)
        ],
    )
    def _op(x):
      return x

    with self.subTest('success'):
      _op(jagged_shape.create_shape())
      _op(arolla.unspecified())

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError, 'expected JAGGED_SHAPE or UNSPECIFIED, got x: INT32'
      ):
        _op(4)

  def test_expect_data_slice_args(self):
    @arolla.optools.as_lambda_operator(
        'op4.name',
        qtype_constraints=[qtype_utils.expect_data_slice_args(arolla.P.x)],
    )
    def _op(*x):
      return x

    with self.subTest('success'):
      _op()
      _op(data_slice.DataSlice.from_vals(1))
      _op(data_slice.DataSlice.from_vals(1), data_slice.DataSlice.from_vals(2))

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError,
          'expected all arguments to be DATA_SLICE, got x:'
          ' tuple<DATA_SLICE,INT32>',
      ):
        _op(data_slice.DataSlice.from_vals(1), arolla.int32(1))

  def test_expect_data_slice_kwargs(self):
    @arolla.optools.as_lambda_operator(
        'op4.name',
        qtype_constraints=[qtype_utils.expect_data_slice_kwargs(arolla.P.x)],
    )
    def _op(x):
      return x

    with self.subTest('success'):
      _op(arolla.namedtuple())
      _op(arolla.namedtuple(a=data_slice.DataSlice.from_vals(1)))
      _op(
          arolla.namedtuple(
              a=data_slice.DataSlice.from_vals(1),
              b=data_slice.DataSlice.from_vals(2),
          )
      )

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError,
          'expected all arguments to be DATA_SLICE, got x:'
          ' namedtuple<a=DATA_SLICE,b=INT32>',
      ):
        _op(
            arolla.namedtuple(
                a=data_slice.DataSlice.from_vals(1), b=arolla.int32(1)
            )
        )

  def test_expect_non_deterministic(self):
    @arolla.optools.as_lambda_operator(
        'op4.name',
        qtype_constraints=[
            qtype_utils.expect_non_deterministic(arolla.P.non_deterministic)
        ],
    )
    def _op(non_deterministic):
      del non_deterministic  # unused
      return 123

    with self.subTest('success'):
      _op(py_expr_eval_py_ext.eval_expr(arolla.abc.bind_op(
          'koda_internal.non_deterministic',
          arolla.L[py_expr_eval_py_ext.NON_DETERMINISTIC_TOKEN_LEAF_KEY],
          arolla.literal(arolla.int64(0))
      )))

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError,
          'expected NON_DETERMINISTIC_TOKEN, got non_deterministic: DATA_SLICE',
      ):
        _op(data_slice.DataSlice.from_vals(123))

  def test_expect_iterable(self):

    @arolla.optools.as_lambda_operator(
        'op1.name',
        qtype_constraints=[qtype_utils.expect_iterable(arolla.P.x)],
    )
    def _op(x):
      return x

    with self.subTest('success'):
      _op(iterable_qvalue.Iterable(1, 2, 3))

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError, 'expected an iterable type, got x: INT32'
      ):
        _op(4)

    with self.subTest('failure on unspecified'):
      with self.assertRaisesRegex(
          ValueError, 'expected an iterable type, got x: UNSPECIFIED'
      ):
        _op(arolla.unspecified())

    with self.subTest('failure on sequence'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape('expected an iterable type, got x: SEQUENCE[INT32]'),
      ):
        _op(arolla.types.Sequence(1, 2, 3))

  def test_expect_iterable_or_unspecified(self):

    @arolla.optools.as_lambda_operator(
        'op1.name',
        qtype_constraints=[
            qtype_utils.expect_iterable_or_unspecified(arolla.P.x)
        ],
    )
    def _op(x):
      return x

    with self.subTest('success'):
      _op(iterable_qvalue.Iterable(1, 2, 3))
      _op(arolla.unspecified())

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError, 'expected an iterable type or unspecified, got x: INT32'
      ):
        _op(4)

    with self.subTest('failure on sequence'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              'expected an iterable type or unspecified, got x: SEQUENCE[INT32]'
          ),
      ):
        _op(arolla.types.Sequence(1, 2, 3))

  def test_expect_namedtuple(self):
    @arolla.optools.as_lambda_operator(
        'op4.name',
        qtype_constraints=[qtype_utils.expect_namedtuple(arolla.P.x)],
    )
    def _op(x):
      return x

    with self.subTest('success'):
      _op(arolla.namedtuple())
      _op(arolla.namedtuple(a=data_slice.DataSlice.from_vals(1)))
      _op(
          arolla.namedtuple(
              a=data_slice.DataSlice.from_vals(1),
              b=arolla.int32(1),
          )
      )

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError,
          'expected a namedtuple, got x: tuple<DATA_SLICE>',
      ):
        _op(arolla.tuple(data_slice.DataSlice.from_vals(1)))

  def test_expect_executor(self):
    @arolla.optools.as_lambda_operator(
        'op4.name',
        qtype_constraints=[qtype_utils.expect_executor(arolla.P.x)],
    )
    def _op(x):
      return x

    with self.subTest('success'):
      _op(koda_internal_parallel.get_eager_executor())

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError,
          'expected an executor, got x: DATA_SLICE',
      ):
        _op(data_slice.DataSlice.from_vals(1))

    # Make sure we can serialize operators using expect_executor.
    _ = arolla.s11n.dumps(_op)

  def test_expect_future(self):
    @arolla.optools.as_lambda_operator(
        'op4.name',
        qtype_constraints=[qtype_utils.expect_future(arolla.P.x)],
    )
    def _op(x):
      return x

    with self.subTest('success'):
      _op(koda_internal_parallel.as_future(data_slice.DataSlice.from_vals(1)))

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError,
          'expected a future, got x: DATA_SLICE',
      ):
        _op(data_slice.DataSlice.from_vals(1))

    # Make sure we can serialize operators using expect_future.
    _ = arolla.s11n.dumps(_op)

  def test_expect_stream(self):
    @arolla.optools.as_lambda_operator(
        'op4.name',
        qtype_constraints=[qtype_utils.expect_stream(arolla.P.x)],
    )
    def _op(x):
      return x

    stream_qtype = arolla.eval(
        koda_internal_parallel.get_stream_qtype(arolla.INT32)
    )

    with self.subTest('success'):
      _op(arolla.M.annotation.qtype(arolla.L.x, stream_qtype))

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError,
          'expected a stream, got x: DATA_SLICE',
      ):
        _op(data_slice.DataSlice.from_vals(1))

    # Make sure we can serialize operators using expect_stream.
    _ = arolla.s11n.dumps(_op)


if __name__ == '__main__':
  absltest.main()
