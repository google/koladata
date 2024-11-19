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


present = arolla.present()
missing = arolla.missing()

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreHasAttrTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.db = data_bag.DataBag.empty()
    self.entity = self.db.new(a=ds([1, None]), b=ds([None, None]))
    self.object = self.db.obj(a=ds([1, None]), b=ds([None, None]))

  @parameterized.parameters(
      (kde.core.has_attr(I.x, 'a'), ds([present, missing])),
      (kde.core.has_attr(I.x, 'b'), ds([missing, missing])),
      (kde.core.has_attr(I.x, 'c'), ds([missing, missing])),
  )
  def test_eval(self, expr, expected):
    testing.assert_equal(expr_eval.eval(expr, x=self.entity), expected)
    testing.assert_equal(expr_eval.eval(expr, x=self.object), expected)

  def test_has_attr_does_not_fail_when_no_common_schema_for_attr(self):
    db = data_bag.DataBag.empty()
    a = ds(1)
    b = db.new(foo='bar')
    c = ds(3.14)

    object_1 = db.obj(a=a, b=b, c=c)
    object_2 = db.obj(a=a, b=b, d=c)
    object_3 = db.obj(b=a, a=b, c=c)
    mixed_objects = ds([object_1, object_2, object_3]).with_bag(db)

    testing.assert_equal(
        expr_eval.eval(kde.core.has_attr(mixed_objects, 'a')),
        ds([present, present, present]),
    )
    # Although "a" is present in all objects, getting it might still fail.
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r'cannot find a common schema for provided schemas',
    ):
      expr_eval.eval(kde.core.get_attr(mixed_objects, 'a'))
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r'cannot find a common schema for provided schemas',
    ):
      expr_eval.eval(kde.core.maybe(mixed_objects, 'a'))

    # Make sure that it behaves as expected when there is a common schema.
    # "c" is not present in all objects, but it has a common schema.
    testing.assert_equal(
        expr_eval.eval(kde.core.has_attr(mixed_objects, 'c')),
        ds([present, missing, present]),
    )
    with self.assertRaisesRegex(
        ValueError,
        "the attribute 'c' is missing",
    ):
      expr_eval.eval(kde.core.get_attr(mixed_objects, 'c'))
    testing.assert_equal(
        expr_eval.eval(kde.core.maybe(mixed_objects, 'c').no_bag()),
        ds([3.14, None, 3.14]),
    )

  def test_has_attr_of_schema(self):
    s = kde.schema.new_schema(x=schema_constants.INT32)
    testing.assert_equal(expr_eval.eval(kde.core.has_attr(s, 'x')), ds(present))

  def test_attr_name_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'requires `attr_name` to be DataItem holding string, got '
        r'DataItem\(42, schema: INT32\)',
    ):
      expr_eval.eval(kde.core.has_attr(self.entity, 42))

    with self.assertRaisesRegex(
        ValueError,
        r'cannot fetch attributes without a DataBag: a; during evaluation of'
        r' operator kde.core._get_attr_with_default',
    ):
      expr_eval.eval(kde.core.has_attr(43, 'a'))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.has_attr,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.has_attr(I.x, 'a')),
        "kde.core.has_attr(I.x, DataItem('a', schema: STRING))",
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.has_attr(I.x, 'a')))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.has_attr, kde.has_attr))

if __name__ == '__main__':
  absltest.main()
