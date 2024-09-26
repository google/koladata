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

"""Tests for kde.logical.cond."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class LogicalCondTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          # condition, yes, no, expected
          ds(
              [arolla.present(), None, arolla.present(), None, arolla.present()]
          ),
          ds([1, None, None, 3, None]),
          ds([4, 5, 6, 7, None]),
          ds([1, 5, None, 7, None]),
      ),
      # Mixed types
      (
          ds(
              [arolla.present(), None, arolla.present(), None, arolla.present()]
          ),
          ds([1, None, None, 3, None]),
          ds(['a', 'b', None, 'c', None]),
          ds([1, 'b', None, 'c', None]),
      ),
      # Scalars
      (ds(arolla.present()), ds(1), ds(2), ds(1)),
      (ds(None, schema_constants.MASK), ds('a'), ds('b'), ds('b')),
      # Auto broadcasting
      (ds(arolla.present()), ds([1, 2, 3]), ds([4, 5, 6]), ds([1, 2, 3])),
      (
          ds([arolla.present(), arolla.missing(), arolla.present()]),
          ds(1),
          ds('a'),
          ds([1, 'a', 1]),
      ),
      (
          ds([arolla.present(), arolla.missing(), arolla.present()]),
          ds([[1, 2, 3], [4, 6], [7, 8]]),
          ds('a'),
          ds([[1, 2, 3], ['a', 'a'], [7, 8]]),
      ),
  )
  def test_eval(self, condition, yes, no, expected):
    testing.assert_equal(
        expr_eval.eval(kde.logical.cond(condition, yes, no)), expected
    )

  def test_merging(self):
    mask = ds([arolla.present(), None])
    x = data_bag.DataBag.empty().new(a=ds([1, 1]))
    x.get_schema().a = schema_constants.OBJECT
    y = (
        data_bag.DataBag.empty()
        .new(x=ds([1, 1]))
        .with_schema(x.get_schema().no_db())
    )
    y.set_attr(
        'a', ds(['abc', 'xyz'], schema_constants.OBJECT), update_schema=True
    )
    self.assertNotEqual(x.db.fingerprint, y.db.fingerprint)
    testing.assert_equivalent(
        expr_eval.eval(kde.logical.cond(mask, x, y)).a,
        ds([1, 'xyz']).with_db(x.db).with_fallback(y.db),
    )

  def test_incompatible_schema_error(self):
    x = ds([1, None])
    y = data_bag.DataBag.empty().new()
    with self.assertRaisesRegex(
        exceptions.KodaError, 'cannot find a common schema for provided schemas'
    ):
      expr_eval.eval(kde.logical.cond(ds(arolla.present()), x, y))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.logical.cond,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.logical.cond(I.x, I.y, I.z)),
        'kde.logical.cond(I.x, I.y, I.z)',
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.logical.cond(I.x, I.y, I.z)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.logical.cond, kde.cond))


if __name__ == '__main__':
  absltest.main()
