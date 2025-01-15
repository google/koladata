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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals


class IdsDeepUuidTest(parameterized.TestCase):

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_obj(self, pass_schema):
    db = data_bag.DataBag.empty()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = expr_eval.eval(kde.deep_uuid(o, o.get_schema()))
    else:
      result = expr_eval.eval(kde.deep_uuid(o))
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(
          expr_eval.eval(kde.at(result, 0)), expr_eval.eval(kde.at(result, 1))
      )
    odb = data_bag.DataBag.empty()
    o2 = odb.obj(b=odb.new(a=1), c='foo')
    result2 = expr_eval.eval(kde.deep_uuid(o2))
    testing.assert_equal(result2, expr_eval.eval(kde.at(result, 0)))

  def test_with_seed(self):
    x = bag().obj(y=bag().obj(a=1), z=bag().list([2, 3]))
    res_no_seed = expr_eval.eval(kde.ids.deep_uuid(x))
    res_with_seed = expr_eval.eval(kde.ids.deep_uuid(x, seed='seed'))
    res_with_seed2 = expr_eval.eval(kde.ids.deep_uuid(x, seed='seed2'))
    self.assertNotEqual(res_no_seed, res_with_seed)
    self.assertNotEqual(res_with_seed2, res_with_seed)

  def test_no_bag_object(self):
    x = bag().obj(x=1, y=2).no_bag()
    with self.assertRaisesRegex(
        ValueError,
        'expected to have a schema in __schema__ attribute',
    ):
      expr_eval.eval(kde.ids.deep_uuid(x))

  def test_no_bag_entity(self):
    x = bag().new(x=1, y=2).no_bag()
    with self.assertRaisesRegex(
        ValueError,
        'cannot compute deep_uuid of entity slice without a DataBag',
    ):
      expr_eval.eval(kde.ids.deep_uuid(x))

  def test_no_bag_objects_only_primitives(self):
    x = ds([1, None, 'foo']).no_bag()
    testing.assert_equal(x.get_schema(), schema_constants.OBJECT)
    res_1 = expr_eval.eval(kde.ids.deep_uuid(x))
    res_2 = expr_eval.eval(kde.ids.deep_uuid(x))
    testing.assert_equal(res_1, res_2)

  def test_no_bag_primitives(self):
    x = ds([1, None, 3]).no_bag()
    testing.assert_equal(x.get_schema(), schema_constants.INT32)
    res_1 = expr_eval.eval(kde.ids.deep_uuid(x))
    res_2 = expr_eval.eval(kde.ids.deep_uuid(x))
    testing.assert_equal(res_1, res_2)

  def test_seed_slice(self):
    x = bag().obj(x=ds([1, 2]), y=ds([3, 4]))
    with self.assertRaisesRegex(
        ValueError, r'seed can only be 0-rank schema slice, got: rank\(1\)'
    ):
      expr_eval.eval(kde.ids.deep_uuid(x, seed=ds(['a', 'b'])))

  def test_with_schema_and_seed(self):
    s = bag().new_schema(x=schema_constants.INT32)
    x = bag().obj(x=42, y='abc')
    _ = expr_eval.eval(kde.ids.deep_uuid(x, schema=s, seed='seed'))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.deep_uuid(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.ids.deep_uuid, kde.deep_uuid))

  def test_repr(self):
    self.assertEqual(
        repr(kde.ids.deep_uuid(I.x, schema=I.schema, seed=I.y)),
        'kd.ids.deep_uuid(I.x, I.schema, seed=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
