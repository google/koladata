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

"""Tests for kde.core.extract."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
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
    b_list = db.implode(
        db.new(u=ds([[1, 2], [], [3]]), v=ds([[4, 5], [], [6]]))
    )
    c_dict = db.dict({'a': 1, 'b': 2})
    o = db.new(
        a=a_slice,
        b=b_list,
        c=c_dict,
    )
    # TODO: test no_follow, uu, uuid
    fb = data_bag.DataBag.empty()
    o.a.with_bag(fb).set_attr('__schema__', o.a.get_attr('__schema__').no_bag())
    o.a.with_bag(fb).set_attr('d', ds([1, 2, 3]))
    fb_noise = data_bag.DataBag.empty()
    noise = fb_noise.obj(a=[1, 2, 3])
    if noise_positioned_in_front:
      o_fb = o.with_bag(noise.enriched(db, fb).get_bag())
    else:
      o_fb = o.enriched(fb, fb_noise)

    if pass_schema:
      result = expr_eval.eval(kde.extract(o_fb, o_fb.get_schema()))
    else:
      result = expr_eval.eval(kde.extract(o_fb))

    self.assertFalse(result.get_bag().is_mutable())
    expected_bag = o.enriched(fb).get_bag().merge_fallbacks()
    testing.assert_equivalent(result.get_bag(), expected_bag)

  @parameterized.parameters(
      (True,),
      (False,),
  )
  def test_eval_with_schema_partial(self, noise_positioned_in_front):
    db = data_bag.DataBag.empty()
    b_list = db.implode(
        db.new(u=ds([[1, 2], [], [3]]), v=ds([[4, 5], [], [6]]))
    )
    expected_bag = db.fork()
    a_slice = db.obj(b=ds([1, None, 2]), c=ds(['foo', 'bar', 'baz']))
    o = db.new(
        a=a_slice,
        b=b_list,
    )
    o.a.set_attr('d', ds([1, 2, 3]))
    a_schema = (
        data_bag.DataBag.empty()
        .new(b=ds([1, 2]), c=ds(['a', 'b']))
        .get_schema()
    )
    schema_bag = data_bag.DataBag.empty()
    schema = o.get_schema().with_bag(schema_bag)
    schema.a = a_schema.no_bag()
    schema.a.b = a_schema.b.no_bag()
    schema.a.c = a_schema.c.no_bag()
    schema.b = o.b.get_schema().no_bag()
    schema.b.set_attr(
        '__items__', o.b.get_schema().get_attr('__items__').no_bag()
    )
    schema.b.get_attr('__items__').u = (
        o.b.get_schema().get_attr('__items__').u.no_bag()
    )
    schema.b.get_attr('__items__').v = (
        o.b.get_schema().get_attr('__items__').v.no_bag()
    )
    a_slice_exp = expected_bag.new(
        b=ds([1, None, 2]),
        c=ds(['foo', 'bar', 'baz']),
        itemid=a_slice.get_itemid(),
        schema=a_schema.no_bag())
    o_exp = expected_bag.new(
        a=a_slice_exp,
        b=b_list,
        itemid=o.get_itemid(),
        schema=schema.no_bag())
    del o_exp
    fb_noise = data_bag.DataBag.empty()
    noise = fb_noise.obj(a=[1, 2, 3])
    if noise_positioned_in_front:
      o_fb = o.enriched(fb_noise)
    else:
      o_fb = o.with_bag(noise.enriched(db).get_bag())

    result = expr_eval.eval(kde.extract(o_fb, schema))

    self.assertFalse(result.get_bag().is_mutable())
    self.assertEqual(
        result.a.with_schema(schema_constants.OBJECT)
        .get_attr('__schema__')
        .get_present_count(),
        0,
    )
    testing.assert_equivalent(result.get_bag(), expected_bag)

  def test_eval_nofollow(self):
    db = data_bag.DataBag.empty()
    a_slice = db.obj(b=ds([1, None, 2]), c=ds(['foo', 'bar', 'baz']))
    b_list = db.implode(
        db.new(u=ds([[1, 2], [], [3]]), v=ds([[4, 5], [], [6]]))
    )
    o = db.new(
        a=a_slice,
        b=b_list,
    )
    fb = data_bag.DataBag.empty()
    o.a.with_bag(fb).set_attr('__schema__', o.a.get_attr('__schema__').no_bag())
    fb_d = expr_eval.eval(kde.nofollow(fb.new(x=ds([1, 2, None]))))
    o.a.with_bag(fb).get_attr('__schema__').set_attr(
        'd', fb_d.get_schema().no_bag()
    )
    o.a.with_bag(fb).set_attr('d', fb_d)
    o_fb = o.enriched(fb)

    result = expr_eval.eval(kde.extract(o_fb))

    self.assertFalse(result.get_bag().is_mutable())
    self.assertFalse(result.get_bag()._exactly_equal(db))
    o.a.set_attr('d', fb_d.no_bag())
    o.a.get_attr('__schema__').set_attr('d', fb_d.get_schema().no_bag())
    testing.assert_equivalent(result.get_bag(), db)

  def test_eval_lists(self):
    db = data_bag.DataBag.empty()
    tmp_db = data_bag.DataBag.empty()
    fb = data_bag.DataBag.empty()
    list_slice = ds([
        db.list([], item_schema=schema_constants.INT32),
        tmp_db.list([5, 6]).no_bag(),
        db.list([7, 8]),
    ])
    del list_slice.S[2][:]
    fb_lists = list_slice.with_bag(fb).with_schema(list_slice.get_schema())
    fb_update = kde.list_append_update(fb_lists, 1)
    enriched_lists = kde.enriched(list_slice, fb_update)
    enriched_list_sizes = expr_eval.eval(kde.list_size(enriched_lists))
    testing.assert_equal(
        enriched_list_sizes.no_bag(),
        ds([0, 1, 0], schema=schema_constants.INT64).no_bag(),
    )
    extracted_and_enriched_lists = kde.enriched(
        kde.extract(list_slice), fb_update
    )
    extracted_and_enriched_list_sizes = expr_eval.eval(
        kde.list_size(extracted_and_enriched_lists)
    )
    testing.assert_equal(
        extracted_and_enriched_list_sizes.no_bag(),
        ds([0, 1, 0], schema=schema_constants.INT64).no_bag(),
    )

  def test_invalid_object_dtype_schema(self):
    db = data_bag.DataBag.empty()
    o = db.obj(x=1)
    o.set_attr('__schema__', schema_constants.INT32)
    with self.assertRaisesRegex(
        ValueError,
        'unsupported schema found during extract/clone',
    ):
      expr_eval.eval(kde.extract(o))

  def test_mixed_objects_and_schemas(self):
    db = data_bag.DataBag.empty()
    schema = db.new_schema(x=schema_constants.INT32).with_schema(
        schema_constants.OBJECT
    )
    schema.set_attr('__schema__', schema_constants.SCHEMA)
    o = kde.stack(db.obj(x=1), schema)
    with self.assertRaisesRegex(
        ValueError,
        'unsupported schema found during extract/clone',
    ):
      expr_eval.eval(kde.extract(o))

  def test_primitives_mismatch(self):
    db = data_bag.DataBag.empty()
    o = db.obj(x=ds([1, 2, 3]))
    schema = db.new_schema(x=schema_constants.STRING)
    with self.assertRaisesRegex(
        ValueError,
        "during extract/clone, while processing the attribute 'x',"
        " got a slice with primitive type STRING while the"
        " actual content has type INT32",
    ):
      expr_eval.eval(kde.extract(o, schema))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.extract,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.extract(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.extract, kde.extract))


if __name__ == '__main__':
  absltest.main()
