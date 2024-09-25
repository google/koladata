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

"""Tests for kde.core.extract."""

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

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE),
])


class CoreExtractTest(parameterized.TestCase):

  @parameterized.product(
      noise_positioned_in_front=[True, False],
      pass_schema=[True, False],
  )
  def test_fallback(self, noise_positioned_in_front, pass_schema):
    db = data_bag.DataBag.empty()
    a_slice = db.obj(b=ds([1, None, 2]), c=ds(['foo', 'bar', 'baz']))
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
        '__schema__', o.a.get_attr('__schema__').with_db(None)
    )
    o.a.with_db(fb).set_attr('d', ds([1, 2, 3]))
    fb_noise = data_bag.DataBag.empty()
    noise = fb_noise.obj(a=[1, 2, 3])
    if noise_positioned_in_front:
      o_fb = o.with_db(noise.with_fallback(db).with_fallback(fb).db)
    else:
      o_fb = o.with_fallback(fb).with_fallback(fb_noise)

    if pass_schema:
      result = expr_eval.eval(kde.extract(o_fb, o_fb.get_schema()))
    else:
      result = expr_eval.eval(kde.extract(o_fb))

    expected_db = o.with_fallback(fb).db.merge_fallbacks()
    testing.assert_equivalent(result.db, expected_db)

  @parameterized.parameters(
      (True,),
      (False,),
  )
  def test_eval_with_schema_partial(self, noise_positioned_in_front):
    db = data_bag.DataBag.empty()
    a_slice = db.obj(b=ds([1, None, 2]), c=ds(['foo', 'bar', 'baz']))
    b_list = db.list(db.new(u=ds([[1, 2], [], [3]]), v=ds([[4, 5], [], [6]])))
    o = db.new(
        a=a_slice,
        b=b_list,
    )
    expected_db = db.fork()
    o.a.set_attr('d', ds([1, 2, 3]))
    a_schema = (
        data_bag.DataBag.empty()
        .new(b=ds([1, 2]), c=ds(['a', 'b']))
        .get_schema()
    )
    schema_db = data_bag.DataBag.empty()
    schema = o.get_schema().with_db(schema_db)
    schema.a = a_schema.with_db(None)
    schema.a.b = a_schema.b.with_db(None)
    schema.a.c = a_schema.c.with_db(None)
    schema.b = o.b.get_schema().with_db(None)
    schema.b.set_attr(
        '__items__', o.b.get_schema().get_attr('__items__').with_db(None)
    )
    schema.b.get_attr('__items__').u = (
        o.b.get_schema().get_attr('__items__').u.with_db(None)
    )
    schema.b.get_attr('__items__').v = (
        o.b.get_schema().get_attr('__items__').v.with_db(None)
    )
    fb_noise = data_bag.DataBag.empty()
    noise = fb_noise.obj(a=[1, 2, 3])
    if noise_positioned_in_front:
      o_fb = o.with_fallback(fb_noise)
    else:
      o_fb = o.with_db(noise.with_fallback(db).db)

    result = expr_eval.eval(kde.extract(o_fb, schema))

    expected_db = (
        schema.with_fallback(expected_db).db.merge_fallbacks()
    )
    del (
        o.a.with_db(expected_db).get_attr('__schema__').b,
        o.a.with_db(expected_db).get_attr('__schema__').c,
    )
    self.assertEqual(result.a.get_attr('__schema__').get_present_count(), 0)
    result.a.set_attr('__schema__', o.a.get_attr('__schema__').with_db(None))
    testing.assert_equivalent(result.db, expected_db)

  def test_eval_nofollow(self):
    db = data_bag.DataBag.empty()
    a_slice = db.obj(b=ds([1, None, 2]), c=ds(['foo', 'bar', 'baz']))
    b_list = db.list(db.new(u=ds([[1, 2], [], [3]]), v=ds([[4, 5], [], [6]])))
    o = db.new(
        a=a_slice,
        b=b_list,
    )
    fb = data_bag.DataBag.empty()
    o.a.with_db(fb).set_attr(
        '__schema__', o.a.get_attr('__schema__').with_db(None)
    )
    fb_d = expr_eval.eval(kde.nofollow(fb.new(x=ds([1, 2, None]))))
    o.a.with_db(fb).get_attr('__schema__').set_attr(
        'd', fb_d.get_schema().with_db(None)
    )
    o.a.with_db(fb).set_attr('d', fb_d)
    o_fb = o.with_fallback(fb)

    result = expr_eval.eval(kde.extract(o_fb))

    self.assertFalse(result.db._exactly_equal(db))
    o.a.set_attr('d', fb_d.with_db(None))
    o.a.get_attr('__schema__').set_attr('d', fb_d.get_schema().with_db(None))
    testing.assert_equivalent(result.db, db)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.extract,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.extract(I.x)))


if __name__ == '__main__':
  absltest.main()
