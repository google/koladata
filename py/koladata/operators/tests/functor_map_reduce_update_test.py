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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functor import boxing as _
from koladata.functor import functions
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde
kdf = functions.functor


def _bag_fn(f):
  """Helper to create a functor that returns a DataBag."""
  return kdf.fn(f, use_tracing=False, return_type_as=data_bag.DataBag)


class MapReduceUpdateTest(parameterized.TestCase):

  def test_simple(self):
    fn = _bag_fn(lambda x: user_facing_kd.attrs(x, y=x.a + 1))  # pyrefly: ignore[missing-attribute]
    x = fns.new(a=ds([1, 2, 3]))
    update_bag = kd.functor.map_reduce_update(fn, x=x)
    self.assertIsInstance(update_bag, data_bag.DataBag)
    x_updated = x.updated(update_bag)
    testing.assert_equal(x_updated.y.no_bag(), ds([2, 3, 4]))

  def test_lambda(self):
    x = fns.new(a=ds([1, 2]))
    update_bag = kd.functor.map_reduce_update(
        lambda x: user_facing_kd.attrs(x, y=x.a * 2), x=x  # pyrefly: ignore[missing-attribute]
    )
    self.assertIsInstance(update_bag, data_bag.DataBag)
    x_updated = x.updated(update_bag)
    testing.assert_equal(x_updated.y.no_bag(), ds([2, 4]))

  def test_item(self):
    fn = _bag_fn(lambda x: user_facing_kd.attrs(x, y=x.a + 10))  # pyrefly: ignore[missing-attribute]
    x = fns.new(a=5)
    update_bag = kd.functor.map_reduce_update(fn, x=x)
    self.assertIsInstance(update_bag, data_bag.DataBag)
    x_updated = x.updated(update_bag)
    testing.assert_equal(x_updated.y.no_bag(), ds(15))

  def test_include_missing(self):
    fn = _bag_fn(lambda x: user_facing_kd.attrs(x, y=1))  # pyrefly: ignore[missing-attribute]
    x = fns.new(a=ds([1, None]))
    update_bag = kd.functor.map_reduce_update(fn, x=x, include_missing=False)
    x_updated = x.updated(update_bag)
    # Only the non-missing item gets updated.
    testing.assert_equal(x_updated.S[0].y.no_bag(), ds(1))

  def test_missing_functor(self):
    fn = ds(None)
    x = fns.new(a=1)
    update_bag = kd.functor.map_reduce_update(fn, x=x)
    # Missing functor produces a null DataBag.
    self.assertEqual(repr(update_bag), 'DataBag(null)')

  def test_empty_input(self):
    fn = _bag_fn(lambda x: user_facing_kd.attrs(x, y=x.a + 1))  # pyrefly: ignore[missing-attribute]
    x = fns.new(a=ds([], schema=user_facing_kd.INT32))
    update_bag = kd.functor.map_reduce_update(fn, x=x)
    # Empty input produces a null DataBag.
    self.assertEqual(repr(update_bag), 'DataBag(null)')

  def test_different_shapes(self):
    fn1 = _bag_fn(lambda x: user_facing_kd.attrs(x, y=x.a + 1))  # pyrefly: ignore[missing-attribute]
    fn2 = _bag_fn(lambda x: user_facing_kd.attrs(x, y=x.a * 10))  # pyrefly: ignore[missing-attribute]
    x = fns.new(a=ds([3, 5]))
    update_bag = kd.functor.map_reduce_update(ds([fn1, fn2]), x=x)
    x_updated = x.updated(update_bag)
    testing.assert_equal(x_updated.y.no_bag(), ds([4, 50]))

  def test_return_non_databag_error(self):
    # map_reduce_update requires functor to return DataBag, not DataSlice.
    fn = kdf.fn(I.x + 1)  # pyrefly: ignore[unsupported-operation]
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the functor is expected to return a DataBag, but the result has'
            ' type `DATA_SLICE` instead',
        ),
    ):
      _ = kd.functor.map_reduce_update(fn, x=x)

  def test_incompatible_shapes(self):
    fn = _bag_fn(lambda x: user_facing_kd.attrs(x, y=1))  # pyrefly: ignore[missing-attribute]
    fn = ds([fn, fn])
    x = fns.new(a=ds([1, 2, 3]))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'shapes are not compatible: JaggedShape(2) vs JaggedShape(3)'
        ),
    ):
      _ = kd.functor.map_reduce_update(fn, x=x)

  def test_non_functor_input_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a functor DATA_SLICE, got fn: INT32')
    ):
      kde.functor.map_reduce_update(arolla.int32(1))  # pyrefly: ignore[missing-attribute]

  def test_multiple_kwargs(self):
    fn = _bag_fn(lambda x, y: user_facing_kd.attrs(x, z=x.a + y))  # pyrefly: ignore[missing-attribute]
    x = fns.new(a=ds([1, 2]))
    y = ds([10, 20])
    update_bag = kd.functor.map_reduce_update(fn, x=x, y=y)
    x_updated = x.updated(update_bag)
    testing.assert_equal(x_updated.z.no_bag(), ds([11, 22]))

  def test_nested_update(self):
    child = fns.new(val=ds([10, 20]))
    x = fns.new(child=child)
    # A functor can return an update to no longer reachable attributes.
    fn = _bag_fn(
        lambda x: user_facing_kd.bags.updated(  # pyrefly: ignore[missing-attribute]
            user_facing_kd.attrs(x, child=None),  # pyrefly: ignore[missing-attribute]
            user_facing_kd.attrs(x.child, extra=x.child.val + 100),  # pyrefly: ignore[missing-attribute]
        )
    )
    update_bag = kd.functor.map_reduce_update(fn, x=x)

    testing.assert_equivalent(
        x.updated(update_bag).child,
        ds([None, None]),
        schemas_equality=False,
    )
    testing.assert_equivalent(x.child.updated(update_bag).extra, ds([110, 120]))

  def test_merge_conflict(self):
    # When different items in the slice set the same attribute on the same
    # object to different values, the merge should raise an error.
    child = fns.new(val=42)
    x = fns.new(child=ds([child, child]))
    fn = _bag_fn(
        lambda x: user_facing_kd.attrs(x.child, extra=x.child.val + x.idx)  # pyrefly: ignore[missing-attribute]
    )
    x = x.with_attrs(idx=ds([1, 2]))
    with self.assertRaisesRegex(
        ValueError,
        "The cause is the values of attribute 'extra' are different",
    ):
      _ = kd.functor.map_reduce_update(fn, x=x)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.functor.map_reduce_update(I.fn, I.x))  # pyrefly: ignore[missing-attribute]
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.functor.map_reduce_update, kde.map_reduce_update  # pyrefly: ignore[missing-attribute]
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.functor.map_reduce_update(I.fn, x=I.x, y=I.y)),  # pyrefly: ignore[missing-attribute]
        'kd.functor.map_reduce_update(I.fn,'
        ' include_missing=DataItem(False, schema: BOOLEAN),'
        ' x=I.x, y=I.y)',
    )


if __name__ == '__main__':
  absltest.main()
