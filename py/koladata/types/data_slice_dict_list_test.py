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
from koladata.functions import functions as fns
from koladata.functor import boxing as _
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import dict_item as _
from koladata.types import jagged_shape
from koladata.types import list_item as _
from koladata.types import schema_constants


kde = kde_operators.kde
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals


class DataSliceDictListTest(parameterized.TestCase):

  def test_dict_slice(self):
    db = bag()
    single_dict = db.dict()
    single_dict[1] = 7
    single_dict['abc'] = 3.14
    many_dicts = db.dict_shaped(jagged_shape.create_shape([3]))
    many_dicts['self'] = many_dicts
    keys345 = ds([3, 4, 5], schema_constants.INT32)
    values678 = ds([6, 7, 8], schema_constants.INT32)
    many_dicts[keys345] = values678

    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      many_dicts[ds([['a', 'b'], ['c']])] = 42

    testing.assert_equal(single_dict.get_shape(), jagged_shape.create_shape())
    testing.assert_equal(many_dicts.get_shape(), jagged_shape.create_shape([3]))

    testing.assert_dicts_keys_equal(
        single_dict, ds(['abc', 1], schema_constants.OBJECT)
    )
    testing.assert_dicts_values_equal(
        single_dict,
        ds([3.14, 7], schema_constants.OBJECT),
    )
    testing.assert_dicts_keys_equal(
        many_dicts,
        ds([[3, 'self'], [4, 'self'], [5, 'self']], schema_constants.OBJECT),
    )
    testing.assert_dicts_values_equal(
        many_dicts,
        ds([
            [6, many_dicts.S[0].with_schema(schema_constants.OBJECT)],
            [7, many_dicts.S[1].with_schema(schema_constants.OBJECT)],
            [8, many_dicts.S[2].with_schema(schema_constants.OBJECT)],
        ]),
    )

    testing.assert_equal(
        single_dict[1], ds(7, schema_constants.OBJECT).with_bag(db)
    )
    testing.assert_equal(
        single_dict[2], ds(None, schema_constants.OBJECT).with_bag(db)
    )
    testing.assert_allclose(
        single_dict['abc'],
        ds(3.14, schema_constants.OBJECT).with_bag(db),
    )

    testing.assert_equal(
        many_dicts[keys345],
        values678.with_schema(schema_constants.OBJECT).with_bag(db),
    )
    testing.assert_equal(
        many_dicts['self'], many_dicts.with_schema(schema_constants.OBJECT)
    )

    del many_dicts[4]
    del single_dict['abc']

    testing.assert_dicts_keys_equal(
        single_dict, ds([1, 'abc'], schema_constants.OBJECT)
    )
    testing.assert_dicts_values_equal(
        single_dict,
        ds([7, None], schema_constants.OBJECT),
    )
    testing.assert_dicts_keys_equal(
        many_dicts,
        ds(
            [[3, 4, 'self'], ['self', 4], [4, 5, 'self']],
            schema_constants.OBJECT,
        ),
    )
    testing.assert_dicts_values_equal(
        many_dicts,
        ds([
            [6, many_dicts.S[0].with_schema(schema_constants.OBJECT), None],
            [many_dicts.S[1].with_schema(schema_constants.OBJECT), None],
            [8, many_dicts.S[2].with_schema(schema_constants.OBJECT), None],
        ]),
    )

    single_dict[keys345] = values678
    testing.assert_dicts_keys_equal(
        single_dict, ds([1, 3, 4, 5, 'abc'], schema_constants.OBJECT)
    )
    testing.assert_dicts_values_equal(
        single_dict,
        ds([7, 6, 7, 8, None], schema_constants.OBJECT),
    )
    testing.assert_equal(
        single_dict[keys345],
        values678.with_schema(schema_constants.OBJECT).with_bag(db),
    )

    keys = ds([[1, 2], [3, 4], [5, 6]], schema_constants.INT32)
    many_dicts[keys] = 7
    testing.assert_equal(
        many_dicts[keys],
        ds([[7, 7], [7, 7], [7, 7]], schema_constants.OBJECT).with_bag(db),
    )

    single_dict.clear()
    many_dicts.clear()
    testing.assert_dicts_keys_equal(
        single_dict, ds(['abc', 4, 3, 1, 5], schema_constants.OBJECT)
    )
    testing.assert_dicts_values_equal(
        single_dict,
        ds([None] * 5, schema_constants.OBJECT),
    )
    testing.assert_dicts_keys_equal(
        many_dicts,
        ds(
            [['self', 1, 4, 2, 3], [4, 'self', 3], [6, 4, 'self', 5]],
            schema_constants.OBJECT,
        ),
    )
    testing.assert_dicts_values_equal(
        many_dicts,
        ds([[None] * 5, [None] * 3, [None] * 4], schema_constants.OBJECT),
    )

  def test_dict_objects_del_key_values(self):
    db = bag()
    d1 = db.dict({'a': 42, 'b': 37}).embed_schema()
    d2 = db.dict({'a': 53, 'c': 12}).with_schema(schema_constants.OBJECT)
    d = ds([d1, d2])

    with self.assertRaisesRegex(
        ValueError, re.escape('object schema(s) are missing')
    ):
      _ = d['a']

    with self.assertRaisesRegex(
        ValueError, re.escape('object schema(s) are missing')
    ):
      d['a'] = 101

    with self.assertRaisesRegex(
        ValueError, re.escape('object schema(s) are missing')
    ):
      del d['a']

  def test_dict_ops_errors(self):
    db = bag()
    non_dicts = db.new_shaped(jagged_shape.create_shape([3]), x=1)
    with self.assertRaisesRegex(ValueError, 'object with unsupported type'):
      non_dicts[set()] = 'b'  # pyrefly: ignore[unsupported-operation]
    with self.assertRaisesRegex(ValueError, 'object with unsupported type'):
      _ = non_dicts[set()]  # pyrefly: ignore[bad-index]
    with self.assertRaisesRegex(ValueError, 'object with unsupported type'):
      non_dicts['a'] = ValueError
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Expected either a list or a dict slice, got ENTITY(x=INT32)'
        ),
    ):
      non_dicts['a'] = 'b'
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Expected either a list or a dict slice, got ENTITY(x=INT32)'
        ),
    ):
      _ = non_dicts['a']
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'must have the same or less number of dimensions as dct (or keys ' +
            'if larger), got max(dct.get_ndim(), keys.get_ndim()): 0 < ' +
            'values.get_ndim(): 1'
        )
    ):
      db.dict()[1] = ds([1, 2, 3])

    o1 = db.obj(x=1)
    o2 = db.obj(db.dict({1: 2}))
    s = ds([o1, o2]).fork_bag()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'dict(s) expected, got an OBJECT DataSlice with the first non-dict'
            ' schema at ds.flatten().S[0] IMPLICIT_ENTITY(x=INT32)'
        ),
    ):
      s['a'] = 1

  def test_getitem_misuse_errors(self):
    db = bag()
    obj_item = db.obj(a=1)
    with self.assertRaisesRegex(
        ValueError,
        r'dict\(s\) expected, got IMPLICIT_ENTITY\(a=INT32\)\. If you meant to'
        r' index items in the DataSlice, consider using \.S\[indices\]',
    ):
      _ = obj_item[0]

    obj_slice = ds([db.obj(a=1), db.obj(a=2)])
    with self.assertRaisesRegex(
        ValueError,
        r'dict\(s\) expected, got an OBJECT DataSlice with the first non-dict'
        r' schema at ds.flatten\(\)\.S\[1\].*If you meant to index items in the'
        r' DataSlice, consider using \.S\[indices\]',
    ):
      _ = obj_slice[0]

    with self.assertRaisesRegex(
        ValueError,
        r'dict\(s\) expected, got an OBJECT DataSlice with the first non-dict'
        r' schema at ds.flatten\(\)\.S\[1\].*If you meant to select items in'
        r' the DataSlice, consider using \.select\(mask\)',
    ):
      _ = obj_slice[obj_slice.a > 1]

    list_slice = ds([db.list([1, 2]), db.list([3, 4])])
    mask = obj_slice.a == 1
    with self.assertRaisesRegex(
        ValueError,
        r'cannot get items from list\(s\): expected indices to be integers\. '
        r'If you meant to select items in the DataSlice, consider using'
        r' \.select\(mask\)',
    ):
      _ = list_slice[mask]

  def test_clear_errors(self):
    db = bag()
    d_itemid = db.dict({1: 2}).get_itemid()
    with self.assertRaisesRegex(
        ValueError,
        'cannot clear slice of schema: ITEMID, expected a list or dict',
    ):
      d_itemid.clear()

    l_itemid = db.list([1, 2, 3]).get_itemid()
    with self.assertRaisesRegex(
        ValueError,
        'cannot clear slice of schema: ITEMID, expected a list or dict',
    ):
      l_itemid.clear()

    x = ds([1, 2, 3]).with_bag(db)
    with self.assertRaisesRegex(
        ValueError,
        'cannot clear slice of schema: INT32, expected a list or dict',
    ):
      x.clear()

    d_obj = db.dict({1: 2})
    d_obj.clear()  # no error here
    l_obj = db.list([1, 2])
    l_obj.clear()  # no error here

  def test_dict_op_schema_errors(self):
    db = bag()
    db2 = bag()
    d = db.dict({'a': 1, 'b': 2})
    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for keys is incompatible.

Expected schema for keys: STRING
Assigned schema for keys: INT32"""),
    ):
      _ = d[1]

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for values is incompatible.

Expected schema for values: INT32
Assigned schema for values: STRING"""),
    ):
      d['a'] = 'a'

    d = db.obj(d)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for values is incompatible.

Expected schema for values: INT32
Assigned schema for values: STRING"""),
    ):
      d['a'] = 'a'

    d2 = db.dict(db.new(x=ds([1, 2]), y=ds([3, 4])), ds(1))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for keys is incompatible.

Expected schema for keys: ENTITY(x=INT32, y=INT32)
Assigned schema for keys: ENTITY(x=FLOAT32, y=INT32)"""),
    ):
      _ = d2[db.new(x=ds(3.0), y=ds(5))]

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for keys is incompatible.

Expected schema for keys: ENTITY(x=INT32, y=INT32)
Assigned schema for keys: ENTITY(x=STRING)"""),
    ):
      _ = d2[db2.new(x=ds('a'))]

    e = db.new(x=1)
    d3 = db.dict(e, db.new(x='a'))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for values is incompatible.

Expected schema for values: ENTITY(x=STRING)
Assigned schema for values: ENTITY(y=FLOAT32)"""),
    ):
      d3[e] = db.new(y=1.0)

  def test_dict_size(self):
    db = bag()
    d = ds([db.dict({1: 2}), db.dict({3: 4, 5: 6})])
    testing.assert_equal(d.dict_size(), ds([1, 2], schema_constants.INT64))
    testing.assert_equal(d.S[0].dict_size(), ds(1, schema_constants.INT64))
    testing.assert_equal(d.S[1].dict_size(), ds(2, schema_constants.INT64))

  # More comprehensive tests are in dicts_with_dict_update_test.py.
  def test_with_dict_update(self):
    x1 = bag().dict(ds([1, 2]), ds([3, 4])).freeze_bag()
    testing.assert_equivalent(
        x1.with_dict_update(fns.dict({1: 5, 3: 6})),
        bag().dict(ds([1, 2, 3]), ds([5, 4, 6])),
    )

  # More comprehensive tests are in lists_appended_list_test.py
  def test_with_list_append_update(self):
    x = ds([fns.list([1, 2]), fns.list([3, 4])])
    append = ds([5, 6])
    result = x.with_list_append_update(append)

    testing.assert_equivalent(
        result,
        ds([fns.list([1, 2, 5]), fns.list([3, 4, 6])]),
    )

  def test_list_slice(self):
    db = bag()
    indices210 = ds([2, 1, 0])

    single_list = db.list()
    many_lists = db.list_shaped(jagged_shape.create_shape([3]))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'must have the same or less number of dimensions as lst (or ' +
            'indices if larger), got max(lst.get_ndim(), ' +
            'indices.get_ndim()): 0 < items.get_ndim(): 1'
        )
    ):
      single_list[1] = ds([1, 2, 3])

    with self.assertRaisesRegex(
        ValueError,
        'slice with 1 dimensions, while 2 dimensions are required',
    ):
      many_lists[:] = ds([1, 2, 3])

    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      many_lists[:] = ds([[1, 2, 3], [4, 5, 6]])

    single_list[:] = ds([1, 2, 3])
    many_lists[ds(None)] = ds([1, 2, 3])
    testing.assert_equal(
        many_lists[:], ds([[], [], []], schema_constants.OBJECT).with_bag(db)
    )
    many_lists[:] = ds([[1, 2, 3], [4, 5, 6], [7, 8, 9]])

    single_list[1] = 'x'
    many_lists[indices210] = ds(['a', 'b', 'c'])

    testing.assert_equal(
        single_list[-1], ds(3, schema_constants.OBJECT).with_bag(db)
    )
    testing.assert_equal(single_list[indices210], ds([3, 'x', 1]).with_bag(db))
    testing.assert_equal(many_lists[-1], ds(['a', 6, 9]).with_bag(db))
    testing.assert_equal(
        many_lists[indices210],
        ds(['a', 'b', 'c'], schema_constants.OBJECT).with_bag(db),
    )

    testing.assert_equal(single_list[:], ds([1, 'x', 3]).with_bag(db))
    testing.assert_equal(single_list[1:], ds(['x', 3]).with_bag(db))
    testing.assert_equal(
        many_lists[:], ds([[1, 2, 'a'], [4, 'b', 6], ['c', 8, 9]]).with_bag(db)
    )
    testing.assert_equal(
        many_lists[:-1], ds([[1, 2], [4, 'b'], ['c', 8]]).with_bag(db)
    )

    single_list.append(ds([5, 7]))
    many_lists.append(ds([10, 20, 30]))
    testing.assert_equal(single_list[:], ds([1, 'x', 3, 5, 7]).with_bag(db))
    testing.assert_equal(
        many_lists[:],
        ds([[1, 2, 'a', 10], [4, 'b', 6, 20], ['c', 8, 9, 30]]).with_bag(db),
    )

    single_list.clear()
    many_lists.clear()
    testing.assert_equal(
        single_list[:], ds([], schema_constants.OBJECT).with_bag(db)
    )
    testing.assert_equal(
        many_lists[:], ds([[], [], []], schema_constants.OBJECT).with_bag(db)
    )

    lst = db.list([db.obj(a=1), db.obj(a=2)])
    testing.assert_equal(lst[:].a, ds([1, 2]).with_bag(db))

    with self.assertRaisesRegex(
        ValueError, 'only supported for Koda List DataItem'
    ):
      many_lists.append([4, 5, 6])
    with self.assertRaisesRegex(
        ValueError, 'only supported for Koda List DataItem'
    ):
      many_lists[:] = [[1, 2], [3], [4, 5]]

  def test_list_assign_none(self):
    db = bag()
    single_list = db.list(item_schema=schema_constants.INT32)
    many_lists = db.list_shaped(jagged_shape.create_shape([3]))

    single_list[:] = ds([1, 2, 3])
    single_list[1] = None
    testing.assert_equal(single_list[:], ds([1, None, 3]).with_bag(db))

    single_list[1:] = None
    testing.assert_equal(single_list[:], ds([1, None, None]).with_bag(db))

    single_list[:] = None
    testing.assert_equal(
        single_list[:],
        ds([None, None, None]).with_schema(schema_constants.INT32).with_bag(db),
    )

    many_lists[:] = ds([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    many_lists[1] = None
    many_lists[ds([0, 0, 2])] = ds(['a', 'b', None])
    testing.assert_equal(
        many_lists[:],
        ds([['a', None, 3], ['b', None, 6], [7, None, None]]).with_bag(db),
    )

    many_lists[1:] = None
    testing.assert_equal(
        many_lists[:],
        ds([['a', None, None], ['b', None, None], [7, None, None]]).with_bag(
            db
        ),
    )

  def test_del_list_items(self):
    db = bag()

    single_list = db.list(item_schema=schema_constants.INT32)
    many_lists = db.list_shaped(
        jagged_shape.create_shape([3]), item_schema=schema_constants.INT32
    )

    single_list[:] = ds([1, 2, 3])
    del single_list[1]
    del single_list[-2]
    testing.assert_equal(single_list[:], ds([3]).with_bag(db))

    many_lists[:] = ds([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    del many_lists[ds([-2, -1, 0])]
    testing.assert_equal(
        many_lists[:], ds([[1, 3], [4, 5], [8, 9]]).with_bag(db)
    )

    del many_lists[-1]
    testing.assert_equal(many_lists[:], ds([[1], [4], [8]]).with_bag(db))

    single_list[:] = ds([1, 2, 3, 4])
    del single_list[1:3]
    testing.assert_equal(single_list[:], ds([1, 4]).with_bag(db))
    del single_list[-1]
    testing.assert_equal(single_list[:], ds([1]).with_bag(db))

    many_lists.internal_as_py()[1].append(5)
    del many_lists[-1:]
    testing.assert_equal(many_lists[:], ds([[], [4], []]).with_bag(db))

    many_lists[:] = ds([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    del many_lists[ds([2, 1, -1])]
    testing.assert_equal(
        many_lists[:], ds([[1, 2], [4, 6], [7, 8]]).with_bag(db)
    )

    del many_lists[-2]
    testing.assert_equal(many_lists[:], ds([[2], [6], [8]]).with_bag(db))

    many_lists[:] = ds([[1, 2, 3], [4], [5, 6]])
    del many_lists[ds([[None, None], [None], [None]])]
    testing.assert_equal(
        many_lists[:], ds([[1, 2, 3], [4], [5, 6]]).with_bag(db)
    )

    many_lists = db.list_shaped(jagged_shape.create_shape([3]))
    many_lists[:] = ds([[1, 2, 3, 'a'], [4, 5, 6, 'b'], [7, 8, 9, 'c']])
    del many_lists[2]
    testing.assert_equal(
        many_lists[:], ds([[1, 2, 'a'], [4, 5, 'b'], [7, 8, 'c']]).with_bag(db)
    )
    del many_lists[2:]
    testing.assert_equal(
        many_lists[:],
        ds([[1, 2], [4, 5], [7, 8]], schema_constants.OBJECT).with_bag(db),
    )

    with self.assertRaisesRegex(
        ValueError, 'cannot remove items of a list without a DataBag'
    ):
      del db.list([1, 2, 3]).no_bag()[0]

    with self.assertRaisesRegex(
        ValueError, 'cannot remove items of a list without a DataBag'
    ):
      del db.list([1, 2, 3]).no_bag()[0:1]

  def test_list_objects_del_items(self):
    db = bag()
    l1 = db.list([1, 2, 3]).embed_schema()
    l2 = db.list([4, 5]).with_schema(schema_constants.OBJECT)
    l = ds([l1, l2])

    with self.assertRaisesRegex(
        ValueError, re.escape('object schema(s) are missing')
    ):
      _ = l[0]
    with self.assertRaisesRegex(
        ValueError, re.escape('object schema(s) are missing')
    ):
      _ = l[0:2]

    with self.assertRaisesRegex(
        ValueError, re.escape('object schema(s) are missing')
    ):
      l[0] = 42
    with self.assertRaisesRegex(
        ValueError, re.escape('object schema(s) are missing')
    ):
      l[0:2] = ds([[42], [12, 15]])

    with self.assertRaisesRegex(
        ValueError, re.escape('object schema(s) are missing')
    ):
      del l[0]
    with self.assertRaisesRegex(
        ValueError, re.escape('object schema(s) are missing')
    ):
      del l[0:2]

  def test_list_pop(self):
    l = kde.implode(ds([[1, 2, 3], [4, 5]])).eval().fork_bag()
    testing.assert_equal(l.pop(1), ds([2, 5]).with_bag(l.get_bag()))
    testing.assert_equal(l.pop(ds([1, -1])), ds([3, 4]).with_bag(l.get_bag()))
    testing.assert_equal(l[:], ds([[1], []]).with_bag(l.get_bag()))

    testing.assert_equal(l.pop(), ds([1, None]).with_bag(l.get_bag()))

    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to INT64'
    ):
      l.pop('a')

    with self.assertRaisesRegex(ValueError, 'object with unsupported type'):
      l.pop(bag())

  @parameterized.parameters(
      # Varying stop.
      (slice(None), ds([1, 2, 3])),
      (slice(ds(None)), ds([1, 2, 3])),
      (slice(ds(None, schema_constants.INT32)), ds([1, 2, 3])),
      (slice(arolla.optional_int64(None)), ds([1, 2, 3])),
      (slice(2), ds([1, 2])),
      (slice(ds(2)), ds([1, 2])),
      (slice(ds(2, schema_constants.OBJECT)), ds([1, 2])),
      (slice(arolla.int64(2)), ds([1, 2])),
      # Varying start.
      (slice(None, None), ds([1, 2, 3])),
      (slice(ds(None), None), ds([1, 2, 3])),
      (slice(ds(None, schema_constants.INT32), None), ds([1, 2, 3])),
      (slice(arolla.optional_int64(None), None), ds([1, 2, 3])),
      (slice(1, None), ds([2, 3])),
      (slice(ds(1), None), ds([2, 3])),
      (slice(ds(1, schema_constants.OBJECT), None), ds([2, 3])),
      (slice(arolla.int64(1), None), ds([2, 3])),
      # Varying step (only None or 1 are allowed).
      (slice(None, None, None), ds([1, 2, 3])),
      (slice(None, None, ds(None)), ds([1, 2, 3])),
      (slice(None, None, 1), ds([1, 2, 3])),
      (slice(None, None, ds(1)), ds([1, 2, 3])),
      (slice(None, None, arolla.int32(1)), ds([1, 2, 3])),
  )
  def test_list_subscript_with_slice_variations(self, slice_, expected):
    # Tests that different slice variations are supported, as long as they can
    # be considered ints.
    list_ = bag().list([1, 2, 3])
    testing.assert_equal(list_[slice_].no_bag(), expected)

  @parameterized.parameters(
      (
          slice([1, 2]),
          (
              "unsupported type: list; during unpacking of the 'stop' slice"
              ' argument'
          ),
      ),
      (
          slice('foo'),
          (
              'unsupported narrowing cast to INT64 for the given STRING'
              " DataSlice; during unpacking of the 'stop' slice argument"
          ),
      ),
      (
          slice([1, 2], None),
          (
              "unsupported type: list; during unpacking of the 'start' slice"
              ' argument'
          ),
      ),
      (
          slice('foo', None),
          (
              'unsupported narrowing cast to INT64 for the given STRING'
              " DataSlice; during unpacking of the 'start' slice argument"
          ),
      ),
  )
  def test_list_subscript_with_slice_error(self, slice_, expected_error_msg):
    list_ = bag().list([1, 2, 3])
    with self.assertRaisesRegex(ValueError, re.escape(expected_error_msg)):
      _ = list_[slice_]

  @parameterized.parameters(
      # Varying stop.
      (slice(None), ds([-1, -1, -1])),
      (slice(ds(None)), ds([-1, -1, -1])),
      (slice(ds(None, schema_constants.INT32)), ds([-1, -1, -1])),
      (slice(arolla.optional_int64(None)), ds([-1, -1, -1])),
      (slice(2), ds([-1, -1, 3])),
      (slice(ds(2)), ds([-1, -1, 3])),
      (slice(ds(2, schema_constants.OBJECT)), ds([-1, -1, 3])),
      (slice(arolla.int64(2)), ds([-1, -1, 3])),
      # Varying start.
      (slice(None, None), ds([-1, -1, -1])),
      (slice(ds(None), None), ds([-1, -1, -1])),
      (slice(ds(None, schema_constants.INT32), None), ds([-1, -1, -1])),
      (slice(arolla.optional_int64(None), None), ds([-1, -1, -1])),
      (slice(1, None), ds([1, -1, -1])),
      (slice(ds(1), None), ds([1, -1, -1])),
      (slice(ds(1, schema_constants.OBJECT), None), ds([1, -1, -1])),
      (slice(arolla.int64(1), None), ds([1, -1, -1])),
      # Varying step (only None or 1 are allowed).
      (slice(None, None, None), ds([-1, -1, -1])),
      (slice(None, None, ds(None)), ds([-1, -1, -1])),
      (slice(None, None, 1), ds([-1, -1, -1])),
      (slice(None, None, ds(1)), ds([-1, -1, -1])),
      (slice(None, None, arolla.int32(1)), ds([-1, -1, -1])),
  )
  def test_list_ass_subscript_with_slice_variations(self, slice_, expected):
    # Tests that different slice variations are supported, as long as they can
    # be considered ints.
    list_ = bag().list([1, 2, 3])
    list_[slice_] = -1
    testing.assert_equal(list_[:].no_bag(), expected)

  @parameterized.parameters(
      (
          slice([1, 2]),
          (
              "unsupported type: list; during unpacking of the 'stop' slice"
              ' argument'
          ),
      ),
      (
          slice('foo'),
          (
              'unsupported narrowing cast to INT64 for the given STRING'
              " DataSlice; during unpacking of the 'stop' slice argument"
          ),
      ),
      (
          slice([1, 2], None),
          (
              "unsupported type: list; during unpacking of the 'start' slice"
              ' argument'
          ),
      ),
      (
          slice('foo', None),
          (
              'unsupported narrowing cast to INT64 for the given STRING'
              " DataSlice; during unpacking of the 'start' slice argument"
          ),
      ),
  )
  def test_list_ass_subscript_with_slice_error(
      self, slice_, expected_error_msg
  ):
    list_ = bag().list([1, 2, 3])
    with self.assertRaisesRegex(ValueError, re.escape(expected_error_msg)):
      list_[slice_] = -1

  def test_list_op_schema_error(self):
    db = bag()
    l = db.list([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the schema for list items is incompatible.\n\n'
            'Expected schema for list items: INT32\n'
            'Assigned schema for list items: STRING'
        ),
    ):
      l[:] = ds(['el', 'psy', 'congroo'])

    l = db.obj(l)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the schema for list items is incompatible.\n\n'
            'Expected schema for list items: INT32\n'
            'Assigned schema for list items: STRING'
        ),
    ):
      l[:] = ds(['el', 'psy', 'congroo'])

    l = db.list([db.new(x=1)])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for list items is incompatible.

Expected schema for list items: ENTITY(x=INT32)
Assigned schema for list items: ENTITY(y=INT32)"""),
    ):
      l[0] = db.new(y=1)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for list items is incompatible.

Expected schema for list items: ENTITY(x=INT32)
Assigned schema for list items: ENTITY(y=INT32)"""),
    ):
      l[0] = bag().new(y=1)

    l2 = db.list([db.new(x=1)])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(r"""the schema for list items is incompatible.

Expected schema for list items: ENTITY(x=INT32)
Assigned schema for list items: ENTITY(a=STRING)"""),
    ):
      l2[0] = bag().new(a='x')

  def test_list_op_error(self):
    db = bag()
    l = db.new(x=1).fork_bag()
    with self.assertRaisesRegex(
        ValueError,
        re.escape('list(s) expected, got ENTITY(x=INT32)'),
    ):
      l.append(1)

    o1 = db.obj(x=1)
    o2 = db.obj(db.list([1]))
    s = ds([o1, o2]).fork_bag()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'list(s) expected, got an OBJECT DataSlice with the first non-list'
            ' schema at ds.flatten().S[0] IMPLICIT_ENTITY(x=INT32)'
        ),
    ):
      s.append(1)

  def test_list_size(self):
    db = bag()
    l = db.list([[1, 2, 3], [4, 5]])
    testing.assert_equal(l.list_size(), ds(2, schema_constants.INT64))
    testing.assert_equal(l[:].list_size(), ds([3, 2], schema_constants.INT64))

  def test_is_list(self):
    db = bag()
    x = ds([db.list([1, 2]), db.list([3, 4])])
    self.assertTrue(x.is_list())
    self.assertTrue(db.obj(x).is_list())
    self.assertFalse(ds([db.obj(db.list()), db.obj(db.dict())]).is_list())
    x = ds([db.dict({1: 2}), db.dict({3: 4})])
    self.assertFalse(x.is_list())
    self.assertFalse(db.obj(x).is_list())
    x = ds([1.0, 2.0])
    self.assertFalse(x.is_list())
    self.assertFalse(db.obj(x).is_list())
    x = ds([db.list([1, 2]).embed_schema(), 1.0])
    self.assertFalse(x.is_list())
    self.assertFalse(db.obj(x).is_list())

  def test_is_dict(self):
    db = bag()
    x = ds([db.dict({1: 2}), db.dict({3: 4})])
    self.assertTrue(x.is_dict())
    self.assertTrue(db.obj(x).is_dict())
    self.assertFalse(ds([db.obj(db.list()), db.obj(db.dict())]).is_dict())
    x = ds([db.list([1, 2]), db.list([3, 4])])
    self.assertFalse(x.is_dict())
    self.assertFalse(db.obj(x).is_dict())
    x = ds([1.0, 2.0])
    self.assertFalse(x.is_dict())
    self.assertFalse(db.obj(x).is_dict())
    x = ds([db.dict({1: 2}).embed_schema(), 1.0])
    self.assertFalse(x.is_dict())
    self.assertFalse(db.obj(x).is_dict())

  def test_is_entity(self):
    db = bag()
    x = db.new(a=ds([1, 2]))
    self.assertTrue(x.is_entity())
    self.assertTrue(db.obj(x).is_entity())
    self.assertFalse(ds([db.obj(a=1), db.obj(db.dict())]).is_entity())
    x = ds([db.dict({1: 2}), db.dict({3: 4})])
    self.assertFalse(x.is_entity())
    self.assertFalse(db.obj(x).is_entity())
    x = ds([1.0, 2.0])
    self.assertFalse(x.is_entity())
    self.assertFalse(db.obj(x).is_entity())
    x = ds([db.obj(a=1), 1.0])
    self.assertFalse(x.is_entity())
    self.assertFalse(db.obj(x).is_entity())

  def test_is_schema(self):
    db = bag()
    entity_schema = db.new_schema(a=schema_constants.INT32)
    list_schema = db.list_schema(schema_constants.INT32)
    dict_schema = db.dict_schema(
        schema_constants.STRING, schema_constants.INT32
    )
    named_schema = db.named_schema('foo', a=schema_constants.INT32)

    self.assertTrue(entity_schema.is_struct_schema())
    self.assertTrue(list_schema.is_struct_schema())
    self.assertTrue(dict_schema.is_struct_schema())
    self.assertFalse(ds([1.0, 2.0]).get_schema().is_struct_schema())

    self.assertTrue(entity_schema.is_entity_schema())
    self.assertFalse(list_schema.is_entity_schema())
    self.assertFalse(dict_schema.is_entity_schema())

    self.assertFalse(entity_schema.is_list_schema())
    self.assertTrue(list_schema.is_list_schema())
    self.assertFalse(dict_schema.is_list_schema())

    self.assertFalse(entity_schema.is_dict_schema())
    self.assertFalse(list_schema.is_dict_schema())
    self.assertTrue(dict_schema.is_dict_schema())

    self.assertTrue(named_schema.is_named_schema())
    self.assertFalse(entity_schema.is_named_schema())
    self.assertFalse(list_schema.is_named_schema())
    self.assertFalse(dict_schema.is_named_schema())
    self.assertFalse(named_schema.no_bag().is_named_schema())

    testing.assert_equal(named_schema.get_schema_name(), ds('foo'))
    testing.assert_equal(
        named_schema.no_bag().get_schema_name(),
        ds(None, schema_constants.STRING),
    )
    testing.assert_equal(
        entity_schema.get_schema_name(), ds(None, schema_constants.STRING)
    )
    testing.assert_equal(
        list_schema.get_schema_name(), ds(None, schema_constants.STRING)
    )
    testing.assert_equal(
        dict_schema.get_schema_name(), ds(None, schema_constants.STRING)
    )

  def test_empty_subscript_method_slice(self):
    db = bag()
    testing.assert_equal(ds(None).with_bag(db)[:], ds([]).with_bag(db))
    testing.assert_equal(
        ds([None, None]).with_bag(db)[:],
        ds([[], []]).with_bag(db),
    )

  def test_empty_subscript_method_slice_dict(self):
    db = bag()

    testing.assert_unordered_equal(
        db.dict(ds([1, 2]), ds([3, 4]))[:], ds([3, 4]).with_bag(db)
    )

    ds(None, schema_constants.OBJECT).with_bag(db)[:] = ds([42])
    (db.list() & ds(None))[:] = ds([42])

    testing.assert_equal(
        (db.dict() & ds(None))[:], ds([], schema_constants.OBJECT).with_bag(db)
    )
    testing.assert_equal(
        db.dict()[:], ds([], schema_constants.OBJECT).with_bag(db)
    )
    testing.assert_equal(
        db.dict_shaped(jagged_shape.create_shape([3]))[:],
        ds([[], [], []], schema_constants.OBJECT).with_bag(db),
    )

    with self.assertRaisesRegex(
        ValueError,
        re.escape('slice with start or stop is not supported for dictionaries'),
    ):
      _ = db.dict(ds(['a', 'b']), ds([3, 4]))[1:2]

    with self.assertRaisesRegex(
        ValueError,
        re.escape('slice with start or stop is not supported for dictionaries'),
    ):
      _ = db.dict(ds(['a', 'b']), ds([3, 4]))[:1]

    with self.assertRaisesRegex(
        ValueError,
        re.escape('slice with start or stop is not supported for dictionaries'),
    ):
      _ = db.dict(ds(['a', 'b']), ds([3, 4]))[1:]

  def test_empty_subscript_method_int(self):
    db = bag()
    testing.assert_equal(
        ds(None, schema_constants.OBJECT).with_bag(db)[0], ds(None).with_bag(db)
    )
    testing.assert_equal(
        ds([None, None], schema_constants.OBJECT).with_bag(db)[0],
        ds([None, None]).with_bag(db),
    )
    testing.assert_equal(
        (db.obj(db.dict()) & ds(None))[0], ds(None).with_bag(db)
    )

    ds(None, schema_constants.OBJECT).with_bag(db)[0] = 42
    (db.list() & ds(None))[0] = 42
    (db.dict() & ds(None))['abc'] = 42

  def test_empty_entity_subscript(self):
    db = bag()
    testing.assert_equal(
        (db.list([1, 2, 3]) & ds(None))[0],
        ds(None, schema_constants.INT32).with_bag(db),
    )
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                'unsupported narrowing cast to INT64 for the given STRING'
                ' DataSlice'
            )
        ),
    ) as cm:
      _ = (db.list() & ds(None))['abc']
    self.assertRegex(
        str(cm.exception),
        re.escape(
            'cannot get items from list(s): expected indices to be integers'
        ),
    )

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                """unsupported narrowing cast to INT64 for the given STRING DataSlice"""
            )
        ),
    ) as cm:
      (db.list() & ds(None))['abc'] = 42
    self.assertRegex(
        str(cm.exception),
        re.escape(
            'cannot set items from list(s): expected indices to be integers'
        ),
    )

    testing.assert_equal(
        (db.dict({'a': 42}) & ds(None))['a'],
        ds(None, schema_constants.INT32).with_bag(db),
    )
    with self.assertRaisesRegex(
        ValueError, 'the schema for keys is incompatible'
    ):
      _ = (db.dict({'a': 42}) & ds(None))[42]

  def test_list_subscript_key_error(self):
    lst = bag().list([1, 2, 3])
    testing.assert_equal(lst[1].no_bag(), ds(2))
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      _ = lst[1, 2]  # pyrefly: ignore[bad-index]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      _ = lst[[1, 2]]  # pyrefly: ignore[bad-index]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      lst[1, 2] = 42  # pyrefly: ignore[unsupported-operation]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      lst[[1, 2]] = 42  # pyrefly: ignore[unsupported-operation]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      del lst[1, 2]  # pyrefly: ignore[unsupported-operation]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      del lst[[1, 2]]  # pyrefly: ignore[unsupported-operation]
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                'unsupported narrowing cast to INT64 for the given STRING'
                ' DataSlice'
            )
        ),
    ):
      del lst['a']

  def test_dict_subscript_key_error(self):
    dct = bag().dict({'a': 42})
    testing.assert_equal(dct['a'].no_bag(), ds(42))
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      _ = dct['a', 'b']  # pyrefly: ignore[bad-index]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      _ = dct[['a', 'b']]  # pyrefly: ignore[bad-index]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      dct['a', 'b'] = 42  # pyrefly: ignore[unsupported-operation]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      dct[['a', 'b']] = 42  # pyrefly: ignore[unsupported-operation]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      del dct['a', 'b']  # pyrefly: ignore[unsupported-operation]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      del dct[['a', 'b']]  # pyrefly: ignore[unsupported-operation]


if __name__ == '__main__':
  absltest.main()
