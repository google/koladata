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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd
from koladata import kd_ext

I = kd.I
kde = kd_ext.lazy
ds = kd.slice
get_attr_or_item = kd_ext.contrib.get_attr_or_item  # pyrefly: ignore[missing-attribute]


class GetAttrOrItemTest(parameterized.TestCase):

  def test_explicit_entity(self):
    e = kd.new(x=ds([1, 2]), y=ds(['a', 'b']))
    kd.testing.assert_equivalent(get_attr_or_item(e, 'x'), ds([1, 2]))
    kd.testing.assert_equivalent(
        get_attr_or_item(e, kd.obj('y')), ds(['a', 'b'])
    )

    nested = kd.new(inner=e)
    res = get_attr_or_item(nested, 'inner')
    kd.testing.assert_equivalent(res, e, ids_equality=True)
    kd.testing.assert_equal(res.get_bag(), nested.get_bag())

    with self.assertRaisesRegex(ValueError, "the attribute 'z' is missing"):
      get_attr_or_item(e, 'z')

    with self.assertRaisesRegex(
        ValueError, 'argument `key` must be an item holding STRING'
    ):
      get_attr_or_item(e, 0)
    with self.assertRaisesRegex(
        ValueError, 'argument `key` must be an item holding STRING'
    ):
      get_attr_or_item(e, ds(None))

  def test_explicit_dict(self):
    d_str = kd.dict({'a': 10, 'b': 20})
    kd.testing.assert_equivalent(get_attr_or_item(d_str, 'a'), ds(10))
    kd.testing.assert_equivalent(
        get_attr_or_item(d_str, 'missing'), ds(None, schema=kd.INT32)
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(d_str, ds(None)), ds(None, schema=kd.INT32)
    )

    d_int64 = kd.dict({1: 'x', 2: 'y'}, key_schema=kd.INT64)
    kd.testing.assert_equivalent(
        get_attr_or_item(d_int64, kd.int32(1)), ds('x')
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(d_int64, kd.int64(1)), ds('x')
    )

    d_int32 = kd.dict({1: 'x', 2: 'y'}, key_schema=kd.INT32)
    kd.testing.assert_equivalent(get_attr_or_item(d_int32, 2), ds('y'))
    with self.assertRaisesRegex(ValueError, 'incompatible'):
      get_attr_or_item(d_int32, kd.int64(2))

    with self.assertRaises(ValueError):
      get_attr_or_item(d_str, 1)

    with self.assertRaises(ValueError):
      get_attr_or_item(d_int32, 'a')

  def test_explicit_list(self):
    l = kd.list([10, 20, 30])
    kd.testing.assert_equivalent(get_attr_or_item(l, 1), ds(20))
    kd.testing.assert_equivalent(get_attr_or_item(l, -1), ds(30))
    kd.testing.assert_equivalent(get_attr_or_item(l, kd.obj(0)), ds(10))
    kd.testing.assert_equivalent(
        get_attr_or_item(l, 10), ds(None, schema=kd.INT32)
    )
    kd.testing.assert_equivalent(
        get_attr_or_item(l, ds(None)), ds(None, schema=kd.INT32)
    )

    l_2d = ds([kd.list([1, 2]), kd.list([3])])
    kd.testing.assert_equivalent(get_attr_or_item(l_2d, 1), ds([2, None]))

    with self.assertRaisesRegex(
        ValueError,
        'cannot get items from list\\(s\\): expected indices to be integers',
    ):
      get_attr_or_item(l, 'a')

  def test_none_schema(self):
    kd.testing.assert_equal(get_attr_or_item(ds(None), 'x'), ds(None))
    kd.testing.assert_equal(
        get_attr_or_item(ds([None, None]), 0), ds([None, None])
    )

  def test_errors(self):
    # Non-scalar key.
    with self.assertRaisesRegex(
        ValueError, 'argument `key` must be an item, got a slice of rank 1 > 0'
    ):
      get_attr_or_item(kd.obj(x=1), ds(['x']))

    # Primitive ds.
    with self.assertRaisesRegex(
        ValueError,
        'get_attr_or_item requires DataSlice to have entity, dict, list, or'
        ' OBJECT schema',
    ):
      get_attr_or_item(ds(42), 'x')

    # Schema ds.
    with self.assertRaisesRegex(
        ValueError,
        'get_attr_or_item requires DataSlice to have entity, dict, list, or'
        ' OBJECT schema',
    ):
      get_attr_or_item(kd.INT32, 'x')

    # Missing DataBag.
    with self.assertRaisesRegex(ValueError, 'cannot lookup without a DataBag'):
      get_attr_or_item(kd.new(x=1).no_bag(), 'x')
    # Missing DataBag with all items missing.
    with self.assertRaisesRegex(ValueError, 'cannot lookup without a DataBag'):
      get_attr_or_item((kd.new(x=ds([1, 2])) & kd.missing).no_bag(), 'x')

  def test_view_and_repr(self):
    expr = kde.contrib.get_attr_or_item(I.ds, I.key)
    self.assertTrue(kd.is_expr(expr))
    self.assertEqual(repr(expr), 'kd_ext.contrib.get_attr_or_item(I.ds, I.key)')

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.contrib.get_attr_or_item,
        (  # pyrefly: ignore[bad-argument-type]
            (
                kd.qtypes.DATA_SLICE,
                kd.qtypes.DATA_SLICE,
                kd.qtypes.DATA_SLICE,
            ),
        ),
        possible_qtypes=kd.testing.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
    )


if __name__ == '__main__':
  absltest.main()
