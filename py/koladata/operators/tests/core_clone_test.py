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

"""Tests for kde.core.clone."""

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


class CoreCloneTest(parameterized.TestCase):

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_obj(self, pass_schema):
    db = data_bag.DataBag.empty()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = expr_eval.eval(kde.clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.clone(o))

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.with_db(None), o.with_db(None))
    testing.assert_equal(result.b.with_db(None), o.b.with_db(None))
    testing.assert_equal(result.c.with_db(None), o.c.with_db(None))
    testing.assert_equal(result.b.a.with_db(None), o.b.a.with_db(None))
    testing.assert_equal(
        result.get_schema().with_db(None), schema_constants.OBJECT
    )
    testing.assert_equal(
        result.b.get_schema().with_db(None), o.b.get_schema().with_db(None)
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
      result = expr_eval.eval(kde.clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.clone(o))

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.with_db(None), o.with_db(None))
    testing.assert_equal(result[:].b.with_db(None), o[:].b.with_db(None))
    testing.assert_equal(result[:].c.with_db(None), o[:].c.with_db(None))
    testing.assert_equal(result[:].b.a.with_db(None), o[:].b.a.with_db(None))
    self.assertTrue(result.get_schema().is_list_schema())
    testing.assert_equal(
        result[:].b.get_schema().with_db(None),
        o[:].b.get_schema().with_db(None),
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
      result = expr_eval.eval(kde.clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.clone(o))

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.with_db(None), o.with_db(None))
    result_values = result[keys]
    self.assertSetEqual(set(result.get_keys().to_py()), set(keys.to_py()))
    testing.assert_equal(
        result_values.with_db(None),
        values.with_db(None),
    )
    testing.assert_equal(
        result_values.c.with_db(None),
        values.c.with_db(None),
    )
    testing.assert_equal(
        result_values.b.with_db(None),
        values.b.with_db(None),
    )
    testing.assert_equal(
        result_values.b.a.with_db(None),
        values.b.a.with_db(None),
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
      result = expr_eval.eval(kde.clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.clone(o))

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.with_db(None), o.with_db(None))
    testing.assert_equal(result.b.with_db(None), o.b.with_db(None))
    testing.assert_equal(result.c.with_db(None), o.c.with_db(None))
    testing.assert_equal(result.b.a.with_db(None), o.b.a.with_db(None))
    testing.assert_equal(
        result.get_schema().with_db(None), o.get_schema().with_db(None)
    )
    testing.assert_equal(
        result.b.get_schema().with_db(None), o.b.get_schema().with_db(None)
    )

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_clone_only_reachable(self, pass_schema):
    db = data_bag.DataBag.empty()
    fb = data_bag.DataBag.empty()
    a_slice = db.new(b=ds([1, None, 2]), c=ds(['foo', 'bar', 'baz']))
    _ = fb.new(a=a_slice.with_db(None), c=ds([1, None, 2]))
    o = db.new(a=a_slice)
    merged_db = o.with_fallback(fb).db.merge_fallbacks()
    o = o.with_db(merged_db)
    if pass_schema:
      result = expr_eval.eval(kde.clone(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.clone(o))

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.db, o.db)

    expected_db = data_bag.DataBag.empty()
    result.get_schema().with_db(expected_db).set_attr(
        'a', o.get_schema().a.with_db(None)
    )
    result.get_schema().with_db(expected_db).a.set_attr(
        'b', schema_constants.INT32
    )
    result.get_schema().with_db(expected_db).a.set_attr(
        'c', schema_constants.TEXT
    )
    result.with_db(expected_db).set_attr('a', o.a.with_db(None))
    result.a.with_db(expected_db).set_attr('b', o.a.b.with_db(None))
    result.a.with_db(expected_db).set_attr('c', o.a.c.with_db(None))
    self.assertTrue(result.db._exactly_equal(expected_db))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.clone,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.clone(I.x)))


if __name__ == '__main__':
  absltest.main()
