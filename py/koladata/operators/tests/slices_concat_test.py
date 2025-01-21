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
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators import slices
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

M = arolla.M
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


# Signature of the underlying operator. Because of the FULL_SIGNATURE binding
# policy, this is not 1:1 with the Python operator signature, which is actually
# `concat(arg: DATA_SLICE, *args: DATA_SLICE, ndim: DATA_SLICE)`.
QTYPES = frozenset([
    (arolla.make_tuple_qtype(DATA_SLICE), DATA_SLICE),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
    ),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
    ),
    (arolla.make_tuple_qtype(DATA_SLICE), DATA_SLICE, DATA_SLICE),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
        DATA_SLICE,
    ),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
        DATA_SLICE,
    ),
    # etc.
])


class SlicesConcatImplTest(parameterized.TestCase):

  @parameterized.parameters(
      # single input
      ((ds([[[0]]]),), 1, ds([[[0]]])),
      ((ds([[[0]]]),), 2, ds([[[0]]])),
      ((ds([[[0]]]),), 3, ds([[[0]]])),
      # rank 1 concat
      (
          (
              ds([1, 2, 3]),
              ds([4, 5]),
              ds([6, 7, 8, 9]),
          ),
          1,
          ds([1, 2, 3, 4, 5, 6, 7, 8, 9]),
      ),
      # rank 1 concat, mixed dtypes
      (
          (
              ds([1, 2, 'a']),
              ds(['b', 3]),
              ds([4, 5, None, 6]),
          ),
          1,
          ds([1, 2, 'a', 'b', 3, 4, 5, None, 6]),
      ),
      (
          (
              ds([b'a', b'b', b'c']),
              ds([1, 2]),
              ds(['a', 'b', 'c', 'd']),
          ),
          1,
          ds([b'a', b'b', b'c', 1, 2, 'a', 'b', 'c', 'd']),
      ),
      (
          (
              ds([1, 2, 3], schema_constants.INT32),
              ds([4, 5, 6, 7], schema_constants.INT64),
          ),
          1,
          ds([1, 2, 3, 4, 5, 6, 7], schema_constants.INT64),
      ),
      (
          (ds([1, 2, 3]), ds([4.1, 5.2, 6.3])),
          1,
          ds([1, 2, 3, 4.1, 5.2, 6.3]),
      ),
      # rank 3 concat, ndim = 1
      (
          (
              ds([[[1, 2], [3]], [[4]]]),
              ds([[[5, 6], [7]], [[8]]]),
          ),
          1,
          ds([[[1, 2, 5, 6], [3, 7]], [[4, 8]]]),
      ),
      # rank 3 concat, ndim = 2
      (
          (
              ds([[[1, 2], [3]], [[4]]]),
              ds([[[5, 6], [7]], [[8]]]),
          ),
          2,
          ds([[[1, 2], [3], [5, 6], [7]], [[4], [8]]]),
      ),
      # rank 3 concat, ndim = 3
      (
          (
              ds([[[1, 2], [3]], [[4]]]),
              ds([[[5, 6], [7]], [[8]]]),
          ),
          3,
          ds([[[1, 2], [3]], [[4]], [[5, 6], [7]], [[8]]]),
      ),
  )
  def test_eval(self, args, ndim, expected):
    result = expr_eval.eval(kde.slices.concat(*args, ndim=ndim))
    testing.assert_equal(result, expected)

  def test_default_output(self):
    # NOTE(b/390562645): Tests the default output of kd.concat.
    result = expr_eval.eval(slices._concat_or_stack(ds(False), ds(1)))
    testing.assert_equal(result, ds([]))

  @parameterized.parameters(
      # rank = 0
      (
          (
              ds(0),
              ds(1),
          ),
          0,
          (
              'concatentation of DataItems (rank=0) is not supported - use'
              ' stack instead'
          ),
      ),
      # ndim < 1
      (
          (
              ds([[0]]),
              ds([[1]]),
          ),
          0,
          'invalid ndim=0 for rank=2 concat',
      ),
      (
          (
              ds([[0]]),
              ds([[1]]),
          ),
          -1,
          'invalid ndim=-1 for rank=2 concat',
      ),
      # ndim > rank
      (
          (
              ds([[0]]),
              ds([[1]]),
          ),
          3,
          'invalid ndim=3 for rank=2 concat',
      ),
      # mismatched rank
      (
          (
              ds([0]),
              ds([1]),
              ds([[2]]),
          ),
          1,
          'all concat/stack args must have the same rank, got 1 and 2',
      ),
      # mismatched shape prefix
      (
          (
              ds([[0], [1]]),
              ds([[2]]),
          ),
          1,
          (
              'concat/stack requires all inputs to have the same shape prefix'
              ' before the concatenation dimension'
          ),
      ),
  )
  def test_invalid_rank_or_ndim(self, args, ndim, expected_regex):
    with self.assertRaisesRegex(ValueError, re.escape(expected_regex)):
      expr_eval.eval(kde.slices.concat(*args, ndim=ndim))

  def test_same_databag(self):
    db = data_bag.DataBag.empty()
    a = ds([db.obj(x=1)])
    b = ds([db.obj(x=2)])
    result = expr_eval.eval(kde.slices.concat(a, b))
    self.assertEqual(result.get_bag().fingerprint, db.fingerprint)

  def test_multiple_databag(self):
    db1 = data_bag.DataBag.empty()
    a = db1.obj(x=1)
    a_slice = ds([a])
    db2 = data_bag.DataBag.empty()
    b = db2.obj(x=2)
    b_slice = ds([b])
    result = expr_eval.eval(kde.slices.concat(a_slice, b_slice))
    self.assertNotEqual(result.get_bag().fingerprint, db1.fingerprint)
    self.assertNotEqual(result.get_bag().fingerprint, db2.fingerprint)
    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_equal(
        result, ds([a.no_bag(), b.no_bag()]).with_bag(result.get_bag())
    )

  def test_qtype_signatures(self):
    signature = arolla.abc.get_operator_signature(kde.slices.concat)
    self.assertLen(signature.parameters, 2)
    self.assertEqual(signature.parameters[0].name, 'args')
    self.assertEqual(signature.parameters[1].name, 'ndim')

    arolla.testing.assert_qtype_signatures(
        kde.slices.concat,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES
        + (
            arolla.make_tuple_qtype(),
            arolla.make_tuple_qtype(DATA_SLICE),
            arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
            arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.concat(I.x, I.y, I.z)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.concat, kde.concat))

  def test_repr(self):
    self.assertEqual(
        repr(kde.slices.concat(I.x, I.y, I.z, ndim=3)),
        'kd.slices.concat(I.x, I.y, I.z, ndim=DataItem(3, schema: INT32))',
    )


if __name__ == '__main__':
  absltest.main()
