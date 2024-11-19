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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
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

  @parameterized.parameters(
      (
          ds([1, None, 2, 3, None]),
          ds(
              [arolla.present(), None, arolla.present(), None, arolla.present()]
          ),
          ds([1, None, 2, None, None]),
      ),
      # Mixed types.
      (
          ds(['a', 1, None, 1.5, 'b', 2.5, 2]),
          ds([
              None,
              arolla.present(),
              arolla.present(),
              None,
              arolla.present(),
              arolla.present(),
              arolla.present(),
          ]),
          ds([None, 1, None, None, 'b', 2.5, 2]),
      ),
      # Scalar input, scalar output.
      (
          ds(1, schema_constants.INT64),
          ds(arolla.present()),
          ds(1, schema_constants.INT64),
      ),
      (
          1,
          ds(arolla.missing()),
          ds(arolla.missing(), schema_constants.INT32),
      ),
      (
          ds(None, schema_constants.MASK),
          ds(arolla.present()),
          ds(None, schema_constants.MASK),
      ),
      (
          ds(None),
          ds(arolla.missing()),
          ds(None),
      ),
      # Auto-broadcasting.
      (
          ds([1, 2, 3]),
          ds(arolla.present()),
          ds([1, 2, 3]),
      ),
      (
          ds([1, 2, 3]),
          ds(arolla.missing()),
          ds([None, None, None], schema_constants.INT32),
      ),
      (
          1,
          ds([arolla.present(), arolla.missing(), arolla.present()]),
          ds([1, None, 1]),
      ),
      # Multi-dimensional.
      (
          ds([[1, 2], [4, 5, 6], [7, 8]]),
          ds([arolla.missing(), arolla.present(), arolla.present()]),
          ds([[None, None], [4, 5, 6], [7, 8]]),
      ),
      # Mixed types.
      (
          ds([[1, 2], ['a', 'b', 'c'], [arolla.present()]]),
          ds([
              [arolla.present(), arolla.missing()],
              [arolla.present(), arolla.present(), arolla.missing()],
              [arolla.missing()],
          ]),
          ds([[1, None], ['a', 'b', None], [None]]),
      ),
  )
  def test_binary_eval(self, values, mask, expected):
    result = expr_eval.eval(kde.logical.cond(mask, values))
    testing.assert_equal(result, expected)

  def test_merging(self):
    mask = ds([arolla.present(), None])
    x = data_bag.DataBag.empty().new(a=ds([1, 1]))
    x.get_schema().a = schema_constants.OBJECT
    y = (
        data_bag.DataBag.empty()
        .new(x=ds([1, 1]))
        .with_schema(x.get_schema().no_bag())
    )
    y.set_attr('a', ds(['abc', 'xyz'], schema_constants.OBJECT))
    self.assertNotEqual(x.get_bag().fingerprint, y.get_bag().fingerprint)
    testing.assert_equivalent(
        expr_eval.eval(kde.logical.cond(mask, x, y)).a,
        ds([1, 'xyz']).with_bag(x.get_bag()).enriched(y.get_bag()),
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
    self.assertTrue(view.has_koda_view(kde.logical.cond(I.x, I.y, I.z)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.logical.cond, kde.cond))


if __name__ == '__main__':
  absltest.main()
