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
from koladata.expr import py_expr_eval_py_ext
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
eval_op = py_expr_eval_py_ext.eval_op
bag = data_bag.DataBag.empty


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreGetAttrTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.entity = eager.new(a=ds([1, 2, 3]), b=ds(['a', None, 'c']))
    self.object = eager.obj(a=ds([1, 2, 3]), b=ds(['a', None, 'c']))

  @parameterized.parameters(
      (kde.get_attr(I.x, 'a'), ds([1, 2, 3])),
      (kde.get_attr(I.x, 'a', None), ds([1, 2, 3])),
      (
          kde.get_attr(I.x, 'c', None),
          ds([None, None, None], schema_constants.NONE),
      ),
      (kde.get_attr(I.x, 'b', '42'), ds(['a', '42', 'c'])),
      (kde.get_attr(I.x, 'b', 42), ds(['a', 42, 'c'], schema_constants.OBJECT)),
      (kde.get_attr(I.x, 'b'), ds(['a', None, 'c'])),
      (
          # Filter self.x
          kde.get_attr(
              kde.apply_mask(
                  I.x, ds([None, arolla.present(), arolla.present()])
              ),
              'b',
          ),
          ds([None, None, 'c']),
      ),
      (
          # Filter self.x completely.
          kde.get_attr(
              kde.apply_mask(I.x, ds([None, arolla.present(), None])), 'b'
          ),
          ds([None, None, None], schema_constants.STRING),
      ),
      (
          # Filter self.x completely.
          kde.get_attr(
              kde.apply_mask(I.x, ds([None, arolla.present(), None])), 'b', 42
          ),
          ds([None, 42, None], schema_constants.OBJECT),
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

  def test_same_bag(self):
    db = data_bag.DataBag.empty()
    entity = db.new(a=ds([1, 2, 3]), b=ds(['a', None, 'c']))
    default = db.new(a=42).with_schema(entity.get_schema())
    entity = db.new(e=entity & ds([arolla.present(), None, None]))
    result = eager.get_attr(entity, 'e', default)
    testing.assert_equal(result.get_bag(), db)
    testing.assert_equal(result.a, ds([1, 42, 42]).with_bag(entity.get_bag()))

  def test_missing(self):
    entity = bag().new(a=1, b=2)
    with self.assertRaisesRegex(ValueError, r'the attribute \'c\' is missing'):
      eager.core.get_attr(entity, 'c')

  def test_attr_name_error(self):
    entity = bag().new(a=1, b=2)
    with self.assertRaisesRegex(
        ValueError,
        'argument `attr_name` must be an item holding STRING, got an item of'
        ' INT32',
    ):
      eager.core.get_attr(entity, 42)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.get_attr,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(repr(kde.core.get_attr(I.x, 'a')), 'I.x.a')
    self.assertEqual(
        repr(kde.core.get_attr(I.x, 'a', None)),
        "kd.core.get_attr(I.x, DataItem('a', schema: STRING), "
        'DataItem(None, schema: NONE))',
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.get_attr(I.x, 'a')))
    self.assertTrue(view.has_koda_view(kde.core.get_attr(I.x, 'a', 42)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.get_attr, kde.get_attr))


if __name__ == '__main__':
  absltest.main()
