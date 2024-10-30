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

from absl.testing import absltest
from arolla import arolla
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.operators import qtype_utils
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import jagged_shape


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

  def test_expect_data_slice_named_tuple(self):
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

  def test_expect_accepts_hidden_seed(self):
    @arolla.optools.as_lambda_operator(
        'op4.name',
        qtype_constraints=[qtype_utils.expect_accepts_hidden_seed()],
    )
    def _op(hidden_seed):
      del hidden_seed  # unused
      return 123

    with self.subTest('success'):
      _op(arolla.int64(123))

    with self.subTest('failure'):
      with self.assertRaisesRegex(
          ValueError,
          'expected hidden_seed to be INT64, got hidden_seed: DATA_SLICE',
      ):
        _op(data_slice.DataSlice.from_vals(123))


if __name__ == '__main__':
  absltest.main()
