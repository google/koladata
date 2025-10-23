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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators import slices
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

I = input_container.InputContainer('I')

kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals

DATA_SLICE = qtypes.DATA_SLICE

QTYPES = [
    (arolla.make_tuple_qtype(DATA_SLICE), DATA_SLICE),
    (arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE), DATA_SLICE),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
    ),
    (arolla.make_tuple_qtype(DATA_SLICE), DATA_SLICE, DATA_SLICE),
    (arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE), DATA_SLICE, DATA_SLICE),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
        DATA_SLICE,
    ),
    # etc.
]


class SlicesStackTest(parameterized.TestCase):

  @parameterized.parameters(
      # single input
      (
          (ds([[[0]]]),),
          0,
          ds([[[[0]]]]),
      ),
      (
          (ds([[[0]]]),),
          1,
          ds([[[[0]]]]),
      ),
      (
          (ds([[[0]]]),),
          2,
          ds([[[[0]]]]),
      ),
      (
          (ds([[[0]]]),),
          3,
          ds([[[[0]]]]),
      ),
      # rank 0
      (
          (
              ds(0),
              ds(1),
              ds(2),
          ),
          0,
          ds([0, 1, 2]),
      ),
      # rank 0, mixed dtypes
      (
          (
              ds(None),
              ds('a'),
              ds(2),
          ),
          0,
          ds([None, 'a', 2]),
      ),
      # rank 1, ndim = 0
      (
          (
              ds([1, 2, 3]),
              ds([4, 5, 6]),
          ),
          0,
          ds([[1, 4], [2, 5], [3, 6]]),
      ),
      # rank 1, ndim = 1
      (
          (
              ds([1, 2, 3]),
              ds([4, 5]),
          ),
          1,
          ds([[1, 2, 3], [4, 5]]),
      ),
      # rank 3, ndim = 3
      (
          (
              ds([[[1, 2], [3]], [[4]]]),
              ds([[[5, 6], [7]], [[8]]]),
          ),
          3,
          ds([[[[1, 2], [3]], [[4]]], [[[5, 6], [7]], [[8]]]]),
      ),
  )
  def test_eval(self, args, ndim, expected):
    result = kd.slices.stack(*args, ndim=ndim)
    testing.assert_equal(result, expected)

  def test_default_output(self):
    # NOTE(b/390562645): Tests the default output of kd.stack.
    result = slices._concat_or_stack(I.x, I.y).eval(x=ds(True), y=ds(1))
    testing.assert_equal(result, ds([]))

  @parameterized.parameters(
      # ndim < 0
      (
          (
              ds([[0]]),
              ds([[1]]),
          ),
          -1,
          'invalid ndim=-1 for rank=2 stack',
      ),
      # ndim > rank
      (
          (
              ds([[0]]),
              ds([[1]]),
          ),
          3,
          'invalid ndim=3 for rank=2 stack',
      ),
      # mismatched rank
      (
          (
              ds(0),
              ds([1]),
          ),
          1,
          'all concat/stack args must have the same rank, got 0 and 1',
      ),
  )
  def test_invalid_ndim(self, args, ndim, expected_regex):
    with self.assertRaisesRegex(ValueError, expected_regex):
      kd.slices.stack(*args, ndim=ndim)

  def test_same_databag(self):
    db = data_bag.DataBag.empty_mutable()
    a = db.obj(x=1)
    b = db.obj(x=2)
    result = kd.slices.stack(a, b)
    self.assertEqual(result.get_bag().fingerprint, db.fingerprint)

  def test_multiple_databag(self):
    db1 = data_bag.DataBag.empty_mutable()
    a = db1.obj(x=1)
    db2 = data_bag.DataBag.empty_mutable()
    b = db2.obj(x=2)
    result = kd.slices.stack(a, b)
    self.assertNotEqual(result.get_bag().fingerprint, db1.fingerprint)
    self.assertNotEqual(result.get_bag().fingerprint, db2.fingerprint)
    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_equal(
        result, ds([a.no_bag(), b.no_bag()]).with_bag(result.get_bag())
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.slices.stack,
        QTYPES,
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES
        + (
            arolla.make_tuple_qtype(),
            arolla.make_tuple_qtype(DATA_SLICE),
            arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
            arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.stack(I.x, I.y, I.z)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.stack, kde.stack))


if __name__ == '__main__':
  absltest.main()
