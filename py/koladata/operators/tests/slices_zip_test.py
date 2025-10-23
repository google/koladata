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
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE

QTYPES = [
    (arolla.make_tuple_qtype(DATA_SLICE), DATA_SLICE),
    (arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE), DATA_SLICE),
    (
        arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE, DATA_SLICE),
        DATA_SLICE,
    ),
    # etc.
]


class SlicesZipTest(parameterized.TestCase):

  @parameterized.parameters(
      # single input
      (
          (ds([[[0]]]),),
          ds([[[[0]]]]),
      ),
      # rank 0
      (
          (
              ds(0),
              ds(1),
              ds(2),
          ),
          ds([0, 1, 2]),
      ),
      # rank 0, mixed dtypes
      (
          (
              ds(None),
              ds('a'),
              ds(2),
          ),
          ds([None, 'a', 2]),
      ),
      # rank 1
      (
          (
              ds([1, 2, 3]),
              ds([4, 5, 6]),
          ),
          ds([[1, 4], [2, 5], [3, 6]]),
      ),
      # requires alignment
      (
          (
              ds(1),
              ds([2, 3, 4]),
          ),
          ds([[1, 2], [1, 3], [1, 4]]),
      ),
  )
  def test_eval(self, args, expected):
    result = kd.slices.zip(*args)
    testing.assert_equal(result, expected)

  def test_same_databag(self):
    db = data_bag.DataBag.empty_mutable()
    a = db.obj(x=1)
    b = db.obj(x=2)
    result = kd.slices.zip(a, b)
    self.assertEqual(result.get_bag().fingerprint, db.fingerprint)

  def test_multiple_databag(self):
    db1 = data_bag.DataBag.empty_mutable()
    a = db1.obj(x=1)
    db2 = data_bag.DataBag.empty_mutable()
    b = db2.obj(x=2)
    result = kd.slices.zip(a, b)
    self.assertNotEqual(result.get_bag().fingerprint, db1.fingerprint)
    self.assertNotEqual(result.get_bag().fingerprint, db2.fingerprint)
    self.assertFalse(result.get_bag().is_mutable())
    testing.assert_equal(
        result, ds([a.no_bag(), b.no_bag()]).with_bag(result.get_bag())
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.slices.zip,
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
    self.assertTrue(view.has_koda_view(kde.slices.zip(I.x, I.y, I.z)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.zip, kde.zip))

  def test_repr(self):
    self.assertEqual(
        repr(kde.slices.zip(I.x, I.y, I.z)), 'kd.slices.zip(I.x, I.y, I.z)'
    )


if __name__ == '__main__':
  absltest.main()
