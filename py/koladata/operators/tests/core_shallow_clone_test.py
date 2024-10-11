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

"""Tests for kde.core.shallow_clone."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE),
])


class CoreShallowCloneTest(parameterized.TestCase):

  def test_objects(self):
    db = data_bag.DataBag.empty()
    y = db.obj(x=42)
    x = db.obj(y=y)
    result = expr_eval.eval(kde.shallow_clone(x))
    testing.assert_equal(result.y.no_db(), y.no_db())
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape('object schema is missing for the DataItem')
    ):
      _ = result.y.x

  def test_entities_simple(self):
    db = data_bag.DataBag.empty()
    y = db.new(x=42)
    x = db.new(y=y)
    result = expr_eval.eval(kde.shallow_clone(x))
    testing.assert_equal(result.y.no_db(), y.no_db())
    with self.assertRaisesRegex(ValueError, r'the attribute \'x\' is missing'):
      _ = result.y.x

  def test_entities(self):
    db = data_bag.DataBag.empty()
    y = db.new(x=42)
    x = db.new(y=y)
    result = expr_eval.eval(kde.shallow_clone(x))
    testing.assert_equal(result.y.no_db(), y.no_db())

  @parameterized.product(
      noise_positioned_in_front=[True, False],
      pass_schema=[True, False],
  )
  def test_fallback(self, noise_positioned_in_front, pass_schema):
    db = data_bag.DataBag.empty()
    a_slice = db.obj(b=[1, None, 2], c=['foo', 'bar', 'baz'])
    b_list = db.list(db.new(u=ds([[1, 2], [], [3]]), v=ds([[4, 5], [], [6]])))
    c_dict = db.dict({'a': 1, 'b': 2})
    o = db.new(
        a=a_slice,
        b=b_list,
        c=c_dict,
    )
    # TODO: test no_follow, uu, uuid
    fb = data_bag.DataBag.empty()
    o.a.with_db(fb).set_attr(
        '__schema__', o.a.get_attr('__schema__').no_db()
    )
    o.a.with_db(fb).set_attr('d', ds([1, 2, 3]))
    fb_noise = data_bag.DataBag.empty()
    noise = fb_noise.obj(a=[1, 2, 3])
    if noise_positioned_in_front:
      o_fb = o.with_db(noise.with_fallback(db).with_fallback(fb).db)
    else:
      o_fb = o.with_fallback(fb).with_fallback(fb_noise)

    if pass_schema:
      result = expr_eval.eval(kde.shallow_clone(o_fb, o_fb.get_schema()))
    else:
      result = expr_eval.eval(kde.shallow_clone(o_fb))

    testing.assert_equal(result.a.no_db(), o_fb.a.no_db())
    testing.assert_equal(result.b.no_db(), o_fb.b.no_db())
    testing.assert_equal(result.c.no_db(), o_fb.c.no_db())
    testing.assert_equal(
        result.get_schema().a.no_db(), o_fb.get_schema().a.no_db()
    )
    testing.assert_equal(
        result.get_schema().b.no_db(), o_fb.get_schema().b.no_db()
    )
    testing.assert_equal(
        result.get_schema().c.no_db(), o_fb.get_schema().c.no_db()
    )
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_db(), o_fb.no_db())
    testing.assert_equal(
        result.get_schema().no_db(), o_fb.get_schema().no_db()
    )

    expected_db = data_bag.DataBag.empty()
    result.get_schema().with_db(expected_db).set_attr(
        'a', o_fb.get_schema().a.no_db()
    )
    result.get_schema().with_db(expected_db).set_attr(
        'b', o_fb.get_schema().b.no_db()
    )
    result.get_schema().with_db(expected_db).set_attr(
        'c', o_fb.get_schema().c.no_db()
    )
    result.with_db(expected_db).set_attr('a', o_fb.a.no_db())
    result.with_db(expected_db).set_attr('b', o_fb.b.no_db())
    result.with_db(expected_db).set_attr('c', o_fb.c.no_db())
    self.assertTrue(result.db._exactly_equal(expected_db))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.shallow_clone,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.shallow_clone(I.x)))


if __name__ == '__main__':
  absltest.main()
