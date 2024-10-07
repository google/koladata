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
from koladata.operators import kde_operators
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
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE),
])


class CoreDeepCloneTest(parameterized.TestCase):

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_obj(self, pass_schema):
    db = data_bag.DataBag.empty()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    if pass_schema:
      result = expr_eval.eval(kde.deep_clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.deep_clone(o))

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_db(), o.no_db())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.b.no_db(), o.b.no_db())
    testing.assert_equal(result.c.no_db(), o.c.no_db())
    testing.assert_equal(result.b.a.no_db(), o.b.a.no_db())
    testing.assert_equal(result.self.no_db(), result.no_db())
    testing.assert_equal(
        result.get_schema().no_db(), schema_constants.OBJECT
    )
    testing.assert_equal(
        result.b.a.get_schema().no_db(), o.b.a.get_schema().no_db()
    )

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_obj_list(self, pass_schema):
    db = data_bag.DataBag.empty()
    b_slice = db.new(a=ds([1, None, 2]))
    a_slice = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o = db.list(a_slice)
    if pass_schema:
      result = expr_eval.eval(kde.deep_clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.deep_clone(o))

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_db(), o.no_db())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result[:].b.no_db(), o[:].b.no_db())
    testing.assert_equal(result[:].c.no_db(), o[:].c.no_db())
    testing.assert_equal(result[:].b.a.no_db(), o[:].b.a.no_db())
    self.assertTrue(result.get_schema().is_list_schema())
    testing.assert_equal(
        result[:].b.a.get_schema().no_db(),
        o[:].b.a.get_schema().no_db(),
    )

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_obj_dict(self, pass_schema):
    db = data_bag.DataBag.empty()
    b_slice = db.new(a=ds([1, None, 2]))
    values = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    keys = ds([0, 1, 2])
    o = db.dict(keys, values)
    if pass_schema:
      result = expr_eval.eval(kde.deep_clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.deep_clone(o))

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_db(), o.no_db())
    result_values = result[keys]
    self.assertSetEqual(
        set(result.get_keys().internal_as_py()), set(keys.internal_as_py())
    )
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(
          result_values.no_db(),
          values.no_db(),
      )
    testing.assert_equal(
        result_values.c.no_db(),
        values.c.no_db(),
    )
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(
          result_values.b.no_db(),
          values.b.no_db(),
      )
    testing.assert_equal(
        result_values.b.a.no_db(),
        values.b.a.no_db(),
    )
    self.assertTrue(result.get_schema().is_dict_schema())

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_entity(self, pass_schema):
    db = data_bag.DataBag.empty()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = expr_eval.eval(kde.deep_clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.deep_clone(o))

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_db(), o.no_db())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.b.no_db(), o.b.no_db())
    testing.assert_equal(result.c.no_db(), o.c.no_db())
    testing.assert_equal(result.b.a.no_db(), o.b.a.no_db())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(
          result.get_schema().no_db(), o.get_schema().no_db()
      )
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(
          result.b.get_schema().no_db(), o.b.get_schema().no_db()
      )
    testing.assert_equal(
        result.b.a.get_schema().no_db(), o.b.a.get_schema().no_db()
    )

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_clone_only_reachable(self, pass_schema):
    db = data_bag.DataBag.empty()
    fb = data_bag.DataBag.empty()
    a_slice = db.new(b=ds([1, None, 2]), c=ds(['foo', 'bar', 'baz']))
    _ = fb.new(a=a_slice.no_db(), c=ds([1, None, 2]))
    o = db.new(a=a_slice)
    merged_db = o.with_fallback(fb).db.merge_fallbacks()
    o = o.with_db(merged_db)
    if pass_schema:
      result = expr_eval.eval(kde.deep_clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.deep_clone(o))

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.db, o.db)

    expected_db = data_bag.DataBag.empty()
    result.get_schema().with_db(expected_db).set_attr(
        'a', result.get_schema().a.no_db()
    )
    result.get_schema().with_db(expected_db).a.set_attr(
        'b', schema_constants.INT32
    )
    result.get_schema().with_db(expected_db).a.set_attr(
        'c', schema_constants.TEXT
    )
    result.with_db(expected_db).set_attr('a', result.a.no_db())
    result.a.with_db(expected_db).set_attr('b', result.a.b.no_db())
    result.a.with_db(expected_db).set_attr('c', result.a.c.no_db())
    self.assertTrue(result.db._exactly_equal(expected_db))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.deep_clone,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.deep_clone(I.x)))


if __name__ == '__main__':
  absltest.main()
