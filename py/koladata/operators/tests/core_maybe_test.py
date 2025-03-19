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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

eager = eager_op_utils.operators_container('kd')
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreGetAttrTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.db = data_bag.DataBag.empty()
    self.entity = eager.new(a=ds([1, 2, 3]), b=ds(['a', None, 'c']))
    self.object = eager.obj(a=ds([1, 2, 3]), b=ds(['a', None, 'c']))

  @parameterized.parameters(
      (kde.maybe(I.x, 'a'), ds([1, 2, 3])),
      (
          kde.maybe(I.x, 'c'),
          ds([None, None, None], schema_constants.NONE),
      ),
      (kde.maybe(I.x, 'b'), ds(['a', None, 'c'])),
      (
          # Filter self.x
          kde.maybe(
              kde.apply_mask(
                  I.x, ds([None, arolla.present(), arolla.present()])
              ),
              'b',
          ),
          ds([None, None, 'c']),
      ),
      (
          # Filter self.x completely.
          kde.maybe(
              kde.apply_mask(I.x, ds([None, arolla.present(), None])), 'b'
          ),
          ds([None, None, None], schema_constants.STRING),
      ),
  )
  def test_eval(self, expr, expected):
    testing.assert_equal(
        expr_eval.eval(expr, x=self.entity),
        expected.with_bag(self.entity.get_bag()),
    )
    testing.assert_equal(
        expr_eval.eval(expr, x=self.object),
        expected.with_bag(self.object.get_bag()),
    )

  @parameterized.parameters(
      (kde.maybe(I.x, ds(['a', 'd', 'a'])), ds([1, None, 3])),
      (kde.maybe(I.x, ds(['a', 'd', 'b'])), ds([1, None, 'c'])),
      (kde.maybe(I.x, ds(['a', 'd', None])), ds([1, None, None])),
      (
          kde.maybe(I.x, ds(['d', 'b', None])),
          ds([None, None, None], schema_constants.STRING),
      ),
      (
          kde.maybe(I.x, ds(['b', 'a', 'c'])),
          ds(['a', 2, None], schema_constants.OBJECT),
      ),
      (kde.maybe(I.x, ds([None, None, None])), ds([None, None, None])),
  )
  def test_eval_with_attr_name_slice(self, expr, expected):
    testing.assert_equal(
        expr_eval.eval(expr, x=self.entity),
        expected.with_bag(self.entity.get_bag()),
    )
    testing.assert_equal(
        expr_eval.eval(expr, x=self.object),
        expected.with_bag(self.object.get_bag()),
    )

  @parameterized.parameters(
      (kde.maybe(I.x, ds(['a', 'd', 'a'])), ds([1, None, 1])),
      (kde.maybe(I.x, ds(['a', 'd', 'c'])), ds([1, None, None])),
      (kde.maybe(I.x, ds(['a', 'd', None])), ds([1, None, None])),
      (kde.maybe(I.x, ds(['d', 'b', None])), ds([None, 'a', None])),
      (kde.maybe(I.x, ds([None, None, None])), ds([None, None, None])),
  )
  def test_eval_with_attr_name_slice_and_obj_item(self, expr, expected):
    testing.assert_equal(
        expr_eval.eval(expr, x=self.entity.L[0]),
        expected.with_bag(self.entity.get_bag()),
    )
    testing.assert_equal(
        expr_eval.eval(expr, x=self.object.L[0]),
        expected.with_bag(self.object.get_bag()),
    )

  def test_attr_name_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'argument `attr_name` must be an item holding STRING, got an item of'
        ' INT32',
    ):
      eager.core.maybe(self.entity, 42)

  def test_schema_conflict(self):
    with self.assertRaisesRegex(
        ValueError,
        'cannot find a common schema',
    ):
      expr_eval.eval(
          kde.core.maybe(
              ds([
                  eager.new(a=eager.new(y=1), b=1),
                  eager.new(a=eager.new(y=2), b=2),
              ]),
              ds(['a', 'b']),
          )
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.maybe,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.maybe(I.x, 'a')),
        "kd.core.maybe(I.x, DataItem('a', schema: STRING))",
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.maybe(I.x, 'a')))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.maybe, kde.maybe))


if __name__ == '__main__':
  absltest.main()
