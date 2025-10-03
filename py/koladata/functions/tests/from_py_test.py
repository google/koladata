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

import dataclasses
import enum
import gc
import re
import sys
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


@dataclasses.dataclass
class NestedKlass:
  x: str
  def __hash__(self):
    return hash(self.x)


@dataclasses.dataclass
class TestKlass:
  a: int
  b: NestedKlass
  c: bytes
  x: str = 'x'


@dataclasses.dataclass
class TestKlassInternals:
  a: int
  b: float

_VERSION_PARAMS = [('v1', fns.from_py), ('v2', fns._from_py_v2)]


def _zip_with_version_params(input_objects):
  test_cases = zip(input_objects, _VERSION_PARAMS)
  for (case_name, input_obj), (version, from_py_fn) in test_cases:
    yield (f'{case_name}_{version}', (input_obj, from_py_fn))


class FromPyTest(parameterized.TestCase):

  # More detailed tests for conversions to Koda OBJECT are located in
  # obj_test.py.
  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_object(self, from_py_fn):
    obj = from_py_fn({'a': {'b': [1, 2, 3]}})
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(obj, ds(['a'], schema_constants.OBJECT))
    values = obj['a']
    testing.assert_equal(values.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(values, ds(['b'], schema_constants.OBJECT))
    nested_values = values['b']
    testing.assert_equal(
        nested_values.get_schema().no_bag(), schema_constants.OBJECT
    )
    testing.assert_equal(
        nested_values[:],
        ds([1, 2, 3], schema_constants.OBJECT).with_bag(obj.get_bag()),
    )

    ref = fns.obj().ref()
    testing.assert_equal(from_py_fn([ref], from_dim=1), ds([ref]))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_same_bag(self, from_py_fn):
    db = fns.mutable_bag()
    o1 = db.obj()
    o2 = db.obj()

    res = from_py_fn(o1)
    testing.assert_equal(res.get_bag(), db)

    res = from_py_fn([[o1, o2], [42]], from_dim=2)
    self.assertTrue(res.get_bag().is_mutable())
    testing.assert_equal(
        res, ds([[o1, o2], [42]], schema_constants.OBJECT).with_bag(db)
    )

    # The result databag is not the same in case of entity schema.
    l1 = db.list()
    l2 = db.list()
    res = from_py_fn([[l1, l2], [o1, o2]], from_dim=2)
    testing.assert_equivalent(
        res, ds([[l1, l2], [o1, o2]], schema_constants.OBJECT)
    )

    res = from_py_fn([[o1, o2], [l1, l2], [42]], from_dim=2)
    testing.assert_equivalent(
        res,
        ds([[o1, o2], [l1, l2], [42]], schema_constants.OBJECT),
    )

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_does_not_borrow_data_from_input_db(self, from_py_fn):
    db = fns.mutable_bag()
    e1 = db.new(a=42, schema='S')
    e2 = db.new(a=12, schema='S')
    lst = [e1, e2]
    res = from_py_fn(lst)
    self.assertNotEqual(e1.get_bag().fingerprint, res.get_bag().fingerprint)
    self.assertFalse(res.get_bag().is_mutable())

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_can_use_frozen_input_bag(self, from_py_fn):
    db = fns.mutable_bag()
    e = db.new(a=12, schema='S').freeze_bag()
    lst = [e]
    res = from_py_fn(lst)
    testing.assert_equal(res[:].a.no_bag(), ds([12], schema_constants.INT32))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_different_bags(self, from_py_fn):
    o1 = fns.obj()  # bag 1
    o2 = fns.list()  # bag 2

    res = from_py_fn([[o1, o2], [42]], from_dim=2)
    testing.assert_equal(
        res.no_bag(),
        ds([[o1, o2], [42]], schema_constants.OBJECT).no_bag(),
    )

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_list(self, from_py_fn):
    l = from_py_fn([1, 2, 3])
    testing.assert_equal(l[:].no_bag(), ds([1, 2, 3], schema_constants.OBJECT))
    self.assertFalse(l.get_bag().is_mutable())

    l = from_py_fn([1, 3.14])
    testing.assert_equal(l[:].no_bag(), ds([1, 3.14], schema_constants.OBJECT))

    l = from_py_fn([[1, 2, 3], 4], from_dim=0)
    self.assertEqual(l.get_ndim(), 0)
    testing.assert_equal(
        l[:].S[0][:].no_bag(), ds([1, 2, 3], schema_constants.OBJECT)
    )
    testing.assert_equal(
        l[:].S[1], ds(4, schema_constants.OBJECT).with_bag(l.get_bag())
    )
    self.assertFalse(l.get_bag().is_mutable())

    l = from_py_fn([[1, 2, 3], 4], from_dim=1)
    testing.assert_equal(
        l.S[0][:].no_bag(), ds([1, 2, 3], schema_constants.OBJECT)
    )
    testing.assert_equal(
        l.S[1], ds(4, schema_constants.OBJECT).with_bag(l.get_bag())
    )
    self.assertFalse(l.get_bag().is_mutable())

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_list_with_none(self, from_py_fn):
    l = from_py_fn([None, [1, 2, 3], [4, 5]])
    testing.assert_equal(l[:].S[0].no_bag(), ds(None, schema_constants.OBJECT))
    testing.assert_equal(
        l[:].S[1][:].no_bag(), ds([1, 2, 3], schema_constants.OBJECT)
    )
    testing.assert_equal(
        l[:].S[2][:].no_bag(), ds([4, 5], schema_constants.OBJECT)
    )
    self.assertFalse(l.get_bag().is_mutable())

    l = from_py_fn([[1, 2, 3], [4, 5], None])
    testing.assert_equal(l[:].S[2].no_bag(), ds(None, schema_constants.OBJECT))
    testing.assert_equal(
        l[:].S[0][:].no_bag(), ds([1, 2, 3], schema_constants.OBJECT)
    )
    testing.assert_equal(
        l[:].S[1][:].no_bag(), ds([4, 5], schema_constants.OBJECT)
    )
    self.assertFalse(l.get_bag().is_mutable())

  # More detailed tests for conversions to Koda Entities for Lists are located
  # in new_test.py.
  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_list_with_schema(self, from_py_fn):
    # Python list items can be various Python / Koda objects that are normalized
    # to Koda Items.
    l = from_py_fn(
        [1, 2, 3], schema=kde.list_schema(schema_constants.FLOAT32).eval()
    )
    testing.assert_allclose(l[:].no_bag(), ds([1.0, 2.0, 3.0]))

    l = from_py_fn(
        [[1, 2], [ds(42, schema_constants.INT64)]],
        schema=kde.list_schema(
            kde.list_schema(schema_constants.FLOAT64)
        ).eval(),
    )
    testing.assert_allclose(
        l[:][:].no_bag(), ds([[1.0, 2.0], [42.0]], schema_constants.FLOAT64)
    )

    l = from_py_fn(
        [1, 3.14], schema=kde.list_schema(schema_constants.OBJECT).eval()
    )
    testing.assert_equal(l[:].no_bag(), ds([1, 3.14], schema_constants.OBJECT))

    l = from_py_fn(
        [{'a': 2, 'b': 4}, {'c': 6, 'd': 8}],
        schema=kde.list_schema(
            kde.dict_schema(schema_constants.STRING, schema_constants.INT32)
        ).eval(),
    )
    testing.assert_equal(
        l[:]['a'].no_bag(), ds([2, None], schema_constants.INT32)
    )
    testing.assert_equal(
        l[:]['b'].no_bag(), ds([4, None], schema_constants.INT32)
    )
    testing.assert_equal(
        l[:]['c'].no_bag(), ds([None, 6], schema_constants.INT32)
    )
    testing.assert_equal(
        l[:]['d'].no_bag(), ds([None, 8], schema_constants.INT32)
    )

  def test_list_schema_mismatch(self):
    with self.assertRaisesRegex(ValueError, 'schema mismatch'):
      fns._from_py_v2(
          [[1], 2, 3],
          schema=kde.list_schema(schema_constants.INT32).eval(),
          from_dim=1,
      )

  def test_dict_schema_mismatch(self):
    with self.assertRaisesRegex(ValueError, 'schema mismatch'):
      fns._from_py_v2(
          [{'a': 1}, 2, 3],
          schema=kde.dict_schema(
              schema_constants.INT32, schema_constants.INT32
          ).eval(),
          from_dim=1,
      )

  @parameterized.named_parameters(
      _zip_with_version_params((
          ('list', [1, 2, 3]),
          ('dict', {'a': 2, 'b': 4}),
          ('list of dicts', [{'a': 2, 'b': 4}, {'c': 6, 'd': 8}]),
          ('empty_tuple', ()),
          ('tuple', (1, 2, 3)),
          ('obj', dataclasses.make_dataclass('Obj', [('x', int)])(x=123)),
      ))
  )
  def test_same_objects_converted_to_different_items(self, test_case):
    input_obj, from_py_fn = test_case
    d = from_py_fn({'x': input_obj, 'y': input_obj})
    self.assertNotEqual(d['x'].get_itemid(), d['y'].get_itemid())
    self.assertEqual(d['x'].to_py(), d['y'].to_py())
    l = from_py_fn([input_obj, input_obj])
    self.assertNotEqual(l[0].get_itemid(), l[1].get_itemid())
    self.assertEqual(l[0].to_py(), l[1].to_py())
    o = from_py_fn({'x': input_obj, 'y': input_obj}, dict_as_obj=True)
    self.assertNotEqual(o.x.get_itemid(), o.y.get_itemid())
    self.assertEqual(o.x.to_py(), o.y.to_py())

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_with_object_schema(self, from_py_fn):
    # Python dictionary keys and values can be various Python / Koda objects
    # that are normalized to Koda Items.
    d = from_py_fn(
        {ds('a'): [1, 2], 'b': [42]},
        schema=schema_constants.OBJECT,
    )
    testing.assert_dicts_keys_equal(d, ds(['a', 'b'], schema_constants.OBJECT))
    testing.assert_equal(
        d[ds(['a', 'b'])][:].no_bag(),
        ds([[1, 2], [42]], schema_constants.OBJECT),
    )

    d = from_py_fn(
        {ds('a'): {2: 3}, 'b': 3.14},
        schema=kde.dict_schema(
            schema_constants.OBJECT, schema_constants.OBJECT
        ).eval(),
    )
    testing.assert_dicts_keys_equal(d, ds(['a', 'b'], schema_constants.OBJECT))
    testing.assert_dicts_keys_equal(
        d[ds(['a'])], ds([[2]], schema_constants.OBJECT)
    )
    testing.assert_equal(
        d[ds(['a'])][ds([2])].no_bag(), ds([3], schema_constants.OBJECT)
    )
    testing.assert_equal(
        d[ds(['b'])].no_bag(), ds([3.14], schema_constants.OBJECT)
    )

  # More detailed tests for conversions to Koda Entities for Dicts are located
  # in new_test.py.
  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_with_schema(self, from_py_fn):
    # Python dictionary keys and values can be various Python / Koda objects
    # that are normalized to Koda Items.
    d = from_py_fn(
        {ds('a'): [1, 2], 'b': [42]},
        schema=kde.dict_schema(
            schema_constants.STRING, kde.list_schema(schema_constants.INT32)
        ).eval(),
    )
    testing.assert_dicts_keys_equal(d, ds(['a', 'b']))
    testing.assert_equal(d[ds(['a', 'b'])][:].no_bag(), ds([[1, 2], [42]]))

    d = from_py_fn(
        {ds('a'): 1, 'b': 3.14},
        schema=kde.dict_schema(
            schema_constants.STRING, schema_constants.OBJECT
        ).eval(),
    )
    testing.assert_dicts_keys_equal(d, ds(['a', 'b']))
    testing.assert_equal(
        d[ds(['a', 'b'])].no_bag(), ds([1, 3.14], schema_constants.OBJECT)
    )

  def test_dict_as_obj_with_dict_schema(self):
    # Python dictionary keys and values can be various Python / Koda objects
    # that are normalized to Koda Items.
    d = fns._from_py_v2(
        {ds('a'): [1, 2], 'b': [42]},
        schema=kde.dict_schema(
            schema_constants.STRING, kde.list_schema(schema_constants.INT32)
        ).eval(),
        dict_as_obj=True,
    )
    testing.assert_dicts_keys_equal(d, ds(['a', 'b']))
    testing.assert_equal(d[ds(['a', 'b'])][:].no_bag(), ds([[1, 2], [42]]))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_primitive(self, from_py_fn):
    item = from_py_fn(42)
    testing.assert_equal(item, ds(42))
    self.assertFalse(item.has_bag())

    item = from_py_fn(42, schema=schema_constants.FLOAT32)
    testing.assert_equal(item, ds(42.))
    self.assertFalse(item.has_bag())

    item = from_py_fn(42, schema=schema_constants.OBJECT)
    testing.assert_equal(item.no_bag(), ds(42, schema_constants.OBJECT))
    self.assertTrue(item.has_bag())
    self.assertFalse(item.get_bag().is_mutable())

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_primitive_float64(self, from_py_fn):
    item = from_py_fn(0.1, schema=schema_constants.FLOAT64)
    testing.assert_equal(item, fns.float64(0.1))

    float_list = from_py_fn(
        [0.1], schema=kde.list_schema(schema_constants.FLOAT64).eval()
    )
    testing.assert_equal(float_list[:].no_bag(), fns.float64([0.1]))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_string_t0_float64_fails(self, from_py_fn):
    with self.assertRaisesRegex(ValueError, 'the schema is incompatible'):
      _ = from_py_fn('3.14', schema=schema_constants.FLOAT64)

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_primitive_int_enum(self, from_py_fn):
    class MyIntEnum(enum.IntEnum):
      A = 1
      B = 2
      C = 2**33

    testing.assert_equal(from_py_fn(MyIntEnum.A), ds(1))
    testing.assert_equal(from_py_fn(MyIntEnum.B), ds(2))
    testing.assert_equal(from_py_fn(MyIntEnum.C), ds(2**33))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_primitive_str_enum(self, from_py_fn):
    class MyStrEnum(enum.StrEnum):
      A = 'a'
      B = 'b'

    testing.assert_equal(from_py_fn(MyStrEnum.A), ds('a'))
    testing.assert_equal(from_py_fn(MyStrEnum.B), ds('b'))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_primitive_casting_error(self, from_py_fn):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("""the schema is incompatible:
expected schema: BYTES
assigned schema: MASK"""),
    ):
      from_py_fn(b'xyz', schema=schema_constants.MASK)

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_primitive_down_casting_error(self, from_py_fn):
    with self.assertRaisesRegex(
        ValueError, re.escape('''the schema is incompatible:
expected schema: FLOAT32
assigned schema: INT32''')
    ):
      from_py_fn(3.14, schema=schema_constants.INT32)
    with self.assertRaisesRegex(
        ValueError,
        re.escape("""the schema is incompatible:
expected schema: FLOAT32
assigned schema: INT32"""),
    ):
      from_py_fn([1, 3.14], from_dim=1, schema=schema_constants.INT32)

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_primitives_common_schema(self, from_py_fn):
    res = from_py_fn([1, 3.14], from_dim=1)
    testing.assert_equal(res, ds([1.0, 3.14]))
    self.assertFalse(res.has_bag())

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_primitives_object(self, from_py_fn):
    res = from_py_fn([1, 3.14], from_dim=1, schema=schema_constants.OBJECT)
    testing.assert_equal(res.no_bag(), ds([1, 3.14], schema_constants.OBJECT))
    self.assertTrue(res.has_bag())
    self.assertFalse(res.get_bag().is_mutable())

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_empty_object(self, from_py_fn):
    res = from_py_fn(None, schema=schema_constants.OBJECT)
    testing.assert_equal(res.no_bag(), ds(None, schema_constants.OBJECT))
    self.assertTrue(res.has_bag())
    self.assertFalse(res.get_bag().is_mutable())

    res = from_py_fn([], from_dim=1, schema=schema_constants.OBJECT)
    testing.assert_equal(res.no_bag(), ds([], schema_constants.OBJECT))
    self.assertTrue(res.has_bag())
    self.assertFalse(res.get_bag().is_mutable())

    res = from_py_fn([None, None], from_dim=1, schema=schema_constants.OBJECT)
    testing.assert_equal(
        res.no_bag(), ds([None, None], schema_constants.OBJECT)
    )
    self.assertTrue(res.has_bag())
    self.assertFalse(res.get_bag().is_mutable())

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_list_from_dim(self, from_py_fn):
    input_list = [[1, 2.0], [3, 4]]

    l0 = from_py_fn(input_list, from_dim=0)
    self.assertEqual(l0.get_ndim(), 0)
    testing.assert_equal(
        l0[:][:],
        ds([[1, 2.0], [3, 4]], schema_constants.OBJECT).with_bag(l0.get_bag()),
    )

    l1 = from_py_fn(input_list, from_dim=1)
    self.assertEqual(l1.get_ndim(), 1)
    testing.assert_equal(
        l1[:],
        ds([[1, 2.0], [3, 4]], schema_constants.OBJECT).with_bag(l1.get_bag()),
    )

    l2 = from_py_fn(input_list, from_dim=2)
    self.assertEqual(l2.get_ndim(), 2)
    testing.assert_equal(l2, ds([[1.0, 2.0], [3.0, 4.0]]))

    l3 = from_py_fn([1, 2], from_dim=1)
    testing.assert_equal(l3, ds([1, 2]))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_empty_from_dim(self, from_py_fn):
    l0 = from_py_fn([], from_dim=0)
    testing.assert_equal(
        l0[:], ds([], schema_constants.OBJECT).with_bag(l0.get_bag())
    )

    l1 = from_py_fn([], from_dim=1)
    testing.assert_equal(l1, ds([]).with_bag(l1.get_bag()))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_empty_list_of_lists_with_schema(self, from_py_fn):
    schema1 = kde.list_schema(schema_constants.FLOAT64).eval()
    schema2 = kde.list_schema(schema1).eval()
    l0 = from_py_fn([], schema=schema2)
    testing.assert_equal(
        l0[:],
        ds([], schema1).with_bag(l0.get_bag()),
    )

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_empty_dict_of_dicts_with_schema(self, from_py_fn):
    schema1 = kde.dict_schema(
        key_schema=schema_constants.STRING,
        value_schema=schema_constants.FLOAT64,
    ).eval()
    schema2 = kde.dict_schema(
        key_schema=schema_constants.STRING, value_schema=schema1
    ).eval()
    d = from_py_fn({}, schema=schema2)
    testing.assert_equal(
        d[:],
        ds([], schema1).with_bag(d.get_bag()),
    )

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_list_from_dim_with_schema(self, from_py_fn):
    input_list = [[1, 2.0], [3, 4]]

    l0 = from_py_fn(
        input_list,
        schema=kde.list_schema(
            kde.list_schema(schema_constants.FLOAT64)
        ).eval(),
        from_dim=0,
    )
    self.assertEqual(l0.get_ndim(), 0)
    testing.assert_equal(
        l0[:][:].no_bag(), ds([[1, 2], [3, 4]], schema_constants.FLOAT64)
    )

    l0_object = from_py_fn(
        input_list,
        schema=schema_constants.OBJECT,
        from_dim=0,
    )
    self.assertEqual(l0_object.get_ndim(), 0)

    testing.assert_equal(
        l0_object[:][:].no_bag(),
        ds([[1, 2.0], [3, 4]], schema_constants.OBJECT),
    )

    l1 = from_py_fn(
        input_list,
        schema=kde.list_schema(schema_constants.FLOAT32).eval(),
        from_dim=1,
    )
    self.assertEqual(l1.get_ndim(), 1)
    testing.assert_equal(
        l1[:].no_bag(), ds([[1, 2], [3, 4]], schema_constants.FLOAT32)
    )

    l2 = from_py_fn(input_list, schema=schema_constants.FLOAT64, from_dim=2)
    self.assertEqual(l2.get_ndim(), 2)
    testing.assert_equal(l2, ds([[1, 2], [3, 4]], schema_constants.FLOAT64))

    l3 = from_py_fn(input_list, schema=schema_constants.OBJECT, from_dim=2)
    self.assertEqual(l3.get_ndim(), 2)
    testing.assert_equal(
        l3.no_bag(), ds([[1, 2.0], [3, 4]], schema_constants.OBJECT)
    )

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_from_dim(self, from_py_fn):
    input_dict = [{ds('a'): [1, 2], 'b': [42]}, {ds('c'): [3, 4], 'd': [34]}]

    d0 = from_py_fn(input_dict, from_dim=0)
    inner_slice = d0[:]
    testing.assert_dicts_keys_equal(
        inner_slice, ds([['a', 'b'], ['c', 'd']], schema_constants.OBJECT)
    )
    testing.assert_equal(
        inner_slice[ds([['a', 'b'], ['c', 'd']])][:].no_bag(),
        ds([[[1, 2], [42]], [[3, 4], [34]]], schema_constants.OBJECT),
    )

    d1 = from_py_fn(input_dict, from_dim=1)
    testing.assert_dicts_keys_equal(
        d1, ds([['a', 'b'], ['c', 'd']], schema_constants.OBJECT)
    )
    testing.assert_equal(
        d1[ds([['a', 'b'], ['c', 'd']])][:].no_bag(),
        ds([[[1, 2], [42]], [[3, 4], [34]]], schema_constants.OBJECT),
    )

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_empty_dict(self, from_py_fn):
    d0 = from_py_fn({})
    testing.assert_dicts_keys_equal(d0, ds([], schema_constants.OBJECT))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_from_dim_error(self, from_py_fn):
    input_list = [[1, 2, 3], 4]

    with self.assertRaisesRegex(
        ValueError,
        'input has to be a valid nested list. non-lists and lists cannot be'
        ' mixed in a level',
    ):
      _ = from_py_fn(input_list, from_dim=2)

    with self.assertRaisesRegex(
        ValueError,
        'could not traverse the nested list of depth 1 up to the level 12',
    ):
      _ = from_py_fn([1, 3.14], from_dim=12)

    with self.assertRaisesRegex(
        ValueError,
        'could not traverse the nested list of depth 1 up to the level 2',
    ):
      _ = from_py_fn([], from_dim=2)

    schema = kde.schema.new_schema(
        a=schema_constants.STRING, b=kde.list_schema(schema_constants.INT32)
    ).eval()
    with self.assertRaisesRegex(
        ValueError,
        'could not traverse the nested list of depth 1 up to the level 2',
    ):
      _ = from_py_fn([], from_dim=2, schema=schema)

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_none(self, from_py_fn):
    item = from_py_fn(None)
    testing.assert_equal(item, ds(None))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_none_with_schema(self, from_py_fn):
    item = from_py_fn(None, schema=schema_constants.FLOAT32)
    testing.assert_equal(item, ds(None, schema_constants.FLOAT32))

    schema = kde.schema.new_schema(
        a=schema_constants.STRING, b=kde.list_schema(schema_constants.INT32)
    ).eval()
    item = from_py_fn(None, schema=schema)
    testing.assert_equivalent(item.get_schema(), schema)
    testing.assert_equal(item.no_bag(), ds(None).with_schema(schema.no_bag()))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_empty_slice(self, from_py_fn):
    res = from_py_fn([], from_dim=1, schema=schema_constants.FLOAT32)
    testing.assert_equal(res.no_bag(), ds([], schema_constants.FLOAT32))
    schema = kde.schema.new_schema(
        a=schema_constants.STRING, b=kde.list_schema(schema_constants.INT32)
    ).eval()
    res = from_py_fn([], from_dim=1, schema=schema)
    testing.assert_equal(res.no_bag(), ds([], schema.no_bag()))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_obj_reference(self, from_py_fn):
    obj = fns.obj()
    item = from_py_fn(obj.ref())
    testing.assert_equal(item, obj.no_bag())

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_entity_reference(self, from_py_fn):
    entity = fns.new(x=42).fork_bag()
    item = from_py_fn(entity.ref())
    self.assertTrue(item.has_bag())
    testing.assert_equal(
        item.get_attr('__schema__').no_bag(), entity.get_schema().no_bag()
    )
    testing.assert_equal(
        item.with_schema(entity.get_schema().no_bag()).no_bag(), entity.no_bag()
    )

    item = from_py_fn(entity.ref(), schema=entity.get_schema())
    self.assertTrue(item.get_bag().is_mutable())
    # NOTE: Schema bag is unchanged and treated similar to other inputs.
    testing.assert_equal(item, entity)

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_if_schema_provided(self, from_py_fn):
    schema = kde.named_schema('foo', a=schema_constants.INT32).eval()
    d = from_py_fn({'a': 2}, schema=schema)
    self.assertFalse(d.is_dict())
    testing.assert_equal(d.get_schema(), schema.with_bag(d.get_bag()))
    testing.assert_equal(d.a, ds(2).with_bag(d.get_bag()))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_if_schema_provided_with_nested_object(self, from_py_fn):
    schema = kde.named_schema(
        'foo', a=schema_constants.INT32, b=schema_constants.OBJECT
    ).eval()
    d = from_py_fn({'a': 2, 'b': {'x': 'abc'}}, schema=schema)
    self.assertFalse(d.is_dict())
    testing.assert_equal(d.get_schema(), schema.with_bag(d.get_bag()))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_obj_in_list_if_schema_provided(self, from_py_fn):
    @dataclasses.dataclass
    class TestClass:
      a: int

    with self.subTest('list of dicts as objects'):
      schema = kde.list_schema(
          kde.named_schema('foo', a=schema_constants.INT32)
      ).eval()
      d = from_py_fn([{'a': 2}, {'a': 3}], schema=schema, dict_as_obj=True)
      self.assertFalse(d.is_dict())
      testing.assert_equal(d.get_schema(), schema.with_bag(d.get_bag()))
      testing.assert_equal(d[:].a, ds([2, 3]).with_bag(d.get_bag()))

    with self.subTest('list of data items'):
      db = fns.mutable_bag()
      entity_schema = db.named_schema('foo', a=schema_constants.INT32)
      ds1 = db.new(a=2, schema=entity_schema)
      ds2 = db.new(a=3, schema=entity_schema)
      schema = db.list_schema(entity_schema)
      d = fns.from_py([ds1, ds2], schema=schema)
      self.assertFalse(d.is_dict())
      testing.assert_equal(d.get_schema(), schema.with_bag(d.get_bag()))
      testing.assert_equal(d[:].a, ds([2, 3]).with_bag(d.get_bag()))

    with self.subTest('list of dataclasses'):
      ds1 = TestClass(a=2)
      ds2 = TestClass(a=3)
      schema = kde.list_schema(
          kde.named_schema('foo', a=schema_constants.INT32)
      ).eval()
      d = fns.from_py([ds1, ds2], schema=schema)
      self.assertFalse(d.is_dict())
      testing.assert_equal(d.get_schema(), schema.with_bag(d.get_bag()))
      testing.assert_equal(d[:].a, ds([2, 3]).with_bag(d.get_bag()))

    with self.subTest('entities mixed with dataclasses'):
      entity_schema = kde.named_schema('foo', a=schema_constants.INT32).eval()
      schema = kde.list_schema(entity_schema).eval()
      entity = fns.new(a=2, schema=entity_schema)
      obj = TestClass(a=3)
      x = fns.from_py([obj, entity], schema=schema, dict_as_obj=True)
      self.assertFalse(x.is_dict())
      testing.assert_equal(x.get_schema(), schema.with_bag(x.get_bag()))
      testing.assert_equal(x[:].a, ds([3, 2]).with_bag(x.get_bag()))

      x = fns.from_py([entity, obj], schema=schema, dict_as_obj=True)
      self.assertFalse(x.is_dict())
      testing.assert_equal(x.get_schema(), schema.with_bag(x.get_bag()))
      testing.assert_equal(x[:].a, ds([2, 3]).with_bag(x.get_bag()))

    with self.subTest('entities mixed with dicts'):
      entity_schema = kde.named_schema('foo', a=schema_constants.INT32).eval()
      schema = kde.list_schema(entity_schema).eval()
      entity = fns.new(a=2, schema=entity_schema)
      d = {'a': 3}
      x = fns.from_py([d, entity], schema=schema, dict_as_obj=True)
      self.assertFalse(x.is_dict())
      testing.assert_equal(x.get_schema(), schema.with_bag(x.get_bag()))
      testing.assert_equal(x[:].a, ds([3, 2]).with_bag(x.get_bag()))

    with self.subTest(
        'fails when entities with a different schema are mixed with dicts or'
        ' dataclasses'
    ):
      d = {'a': 3}
      obj = TestClass(a=4)

      schema = kde.list_schema(
          kde.named_schema('foo', a=schema_constants.INT32)
      ).eval()

      with self.assertRaisesRegex(
          ValueError,
          'cannot find a common schema',
      ):
        _ = fns.from_py([obj, fns.new(a=2)], schema=schema, dict_as_obj=True)

      with self.assertRaisesRegex(ValueError, 'cannot find a common schema'):
        _ = fns.from_py([fns.new(a=2), d], schema=schema, dict_as_obj=True)

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_object(self, from_py_fn):
    obj = from_py_fn(
        {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')},
        dict_as_obj=True,
    )
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    self.assertCountEqual(fns.dir(obj), ['a', 'b', 'c'])
    testing.assert_equal(obj.a.no_bag(), ds(42))
    b = obj.b
    testing.assert_equal(b.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(b.x.no_bag(), ds('abc'))
    testing.assert_equal(obj.c.no_bag(), ds(b'xyz'))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_object_different_keys_values(self, from_py_fn):
    obj = from_py_fn(
        [
            {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')},
            {'b': {'x': 'abc'}, 'c': ds(b'xyz')},
        ],
        dict_as_obj=True,
    )
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    first_obj = obj[:].S[0]
    self.assertCountEqual(fns.dir(first_obj), ['a', 'b', 'c'])
    testing.assert_equal(first_obj.a.no_bag(), ds(42))
    b = first_obj.b
    testing.assert_equal(b.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(b.x.no_bag(), ds('abc'))
    testing.assert_equal(first_obj.c.no_bag(), ds(b'xyz'))

    second_obj = obj[:].S[1]
    self.assertCountEqual(fns.dir(second_obj), ['b', 'c'])
    b = second_obj.b
    testing.assert_equal(b.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(b.x.no_bag(), ds('abc'))
    testing.assert_equal(second_obj.c.no_bag(), ds(b'xyz'))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_entity_with_schema(self, from_py_fn):
    schema = kde.schema.new_schema(
        a=schema_constants.FLOAT32,
        b=kde.schema.new_schema(x=schema_constants.STRING),
        c=schema_constants.BYTES,
    ).eval()
    entity = from_py_fn(
        {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')},
        dict_as_obj=True,
        schema=schema,
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(fns.dir(entity), ['a', 'b', 'c'])
    testing.assert_equal(entity.a.no_bag(), ds(42.0))
    b = entity.b
    testing.assert_equal(b.get_schema().no_bag(), schema.b.no_bag())
    testing.assert_equal(b.x.no_bag(), ds('abc'))
    testing.assert_equal(entity.c.no_bag(), ds(b'xyz'))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_entity_with_schema_and_different_keys_values(
      self, from_py_fn
  ):
    entity_schema = kde.schema.new_schema(
        a=schema_constants.FLOAT32,
        b=kde.schema.new_schema(x=schema_constants.STRING),
        c=schema_constants.BYTES,
    ).eval()
    schema = kde.schema.list_schema(entity_schema).eval()
    entity = from_py_fn(
        [
            {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')},
            {'b': {'x': 'abc'}, 'c': ds(b'xyz')},
        ],
        dict_as_obj=True,
        schema=schema,
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    first_entity = entity[:].S[0]
    self.assertCountEqual(fns.dir(first_entity), ['a', 'b', 'c'])
    testing.assert_equal(first_entity.a.no_bag(), ds(42.0))
    b = first_entity.b
    testing.assert_equal(b.get_schema().no_bag(), entity_schema.b.no_bag())
    testing.assert_equal(b.x.no_bag(), ds('abc'))
    testing.assert_equal(first_entity.c.no_bag(), ds(b'xyz'))

    second_entity = entity[:].S[1]
    self.assertCountEqual(fns.dir(second_entity), ['a', 'b', 'c'])
    testing.assert_equal(
        second_entity.a.no_bag(), ds(None, schema_constants.FLOAT32)
    )
    b = second_entity.b
    testing.assert_equal(b.get_schema().no_bag(), entity_schema.b.no_bag())
    testing.assert_equal(b.x.no_bag(), ds('abc'))
    testing.assert_equal(second_entity.c.no_bag(), ds(b'xyz'))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_entity_with_nested_object(self, from_py_fn):
    schema = kde.schema.new_schema(
        a=schema_constants.INT64,
        b=schema_constants.OBJECT,
        c=schema_constants.BYTES,
    ).eval()
    entity = from_py_fn(
        {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')},
        dict_as_obj=True,
        schema=schema,
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(fns.dir(entity), ['a', 'b', 'c'])
    testing.assert_equal(entity.a.no_bag(), ds(42, schema_constants.INT64))
    obj_b = entity.b
    testing.assert_equal(obj_b.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(obj_b.x.no_bag(), ds('abc'))
    testing.assert_equal(entity.c.no_bag(), ds(b'xyz'))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_entity_incomplete_schema(self, from_py_fn):
    schema = kde.schema.new_schema(b=schema_constants.OBJECT).eval()
    entity = from_py_fn(
        {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')},
        dict_as_obj=True,
        schema=schema,
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(fns.dir(entity), ['b'])
    testing.assert_equal(
        entity.b.get_schema().no_bag(), schema_constants.OBJECT
    )
    testing.assert_equal(entity.b.x.no_bag(), ds('abc'))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_entity_empty_schema(self, from_py_fn):
    schema = kde.schema.new_schema().eval()
    entity = from_py_fn(
        {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')},
        dict_as_obj=True,
        schema=schema,
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(fns.dir(entity), [])

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_bag_adoption(self, from_py_fn):
    obj_b = from_py_fn({'x': 'abc'}, dict_as_obj=True)
    obj = from_py_fn({'a': 42, 'b': obj_b}, dict_as_obj=True)
    testing.assert_equal(obj.b.x.no_bag(), ds('abc'))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_entity_incompatible_schema(self, from_py_fn):
    schema = kde.schema.new_schema(
        a=schema_constants.INT64,
        b=kde.schema.new_schema(x=schema_constants.FLOAT32),
        c=schema_constants.FLOAT32,
    ).eval()
    error_msg = "schema for attribute 'x' is incompatible"
    with self.assertRaisesRegex(ValueError, error_msg):
      from_py_fn(
          {'a': 42, 'b': {'x': 'abc'}, 'c': ds(b'xyz')},
          dict_as_obj=True,
          schema=schema,
      )

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_dict_key_is_data_item(self, from_py_fn):
    # Object.
    obj = from_py_fn({ds('a'): 42}, dict_as_obj=True)
    self.assertCountEqual(fns.dir(obj), ['a'])
    testing.assert_equal(obj.a.no_bag(), ds(42))
    # Entity - non STRING schema with STRING item.
    entity = from_py_fn(
        {ds('a', schema_constants.OBJECT): 42},
        dict_as_obj=True,
        schema=kde.schema.new_schema(a=schema_constants.INT32).eval(),
    )
    self.assertCountEqual(fns.dir(entity), ['a'])
    testing.assert_equal(entity.a.no_bag(), ds(42))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_non_unicode_key(self, from_py_fn):
    with self.assertRaisesRegex(
        ValueError,
        'dict_as_obj requires keys to be valid unicode objects, got bytes'
    ):
      from_py_fn({b'xyz': 42}, dict_as_obj=True)

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dict_as_obj_non_text_data_item(self, from_py_fn):
    with self.assertRaisesRegex(TypeError, 'unhashable type'):
      from_py_fn({ds(['abc']): 42}, dict_as_obj=True)
    with self.assertRaisesRegex(
        ValueError, "dict keys cannot be non-STRING DataItems, got b'abc'"
    ):
      from_py_fn({ds(b'abc'): 42}, dict_as_obj=True)

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_incompatible_schema(self, from_py_fn):
    entity = fns.new(x=1)
    schema = kde.schema.new_schema(x=schema_constants.INT32).eval()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            '''the schema is incompatible:
expected schema: ENTITY(x=INT32)
assigned schema: ENTITY(x=INT32)'''
        ),
    ):
      from_py_fn(entity, schema=schema)

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dataclasses(self, from_py_fn):
    obj = from_py_fn(TestKlass(42, NestedKlass('abc'), b'xyz'))
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    self.assertCountEqual(fns.dir(obj), ['a', 'b', 'c', 'x'])
    testing.assert_equal(obj.a.no_bag(), ds(42))
    b = obj.b
    testing.assert_equal(b.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(b.x.no_bag(), ds('abc'))
    testing.assert_equal(obj.c.no_bag(), ds(b'xyz'))
    self.assertFalse(b.is_dict())

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dataclasses_with_schema(self, from_py_fn):

    @dataclasses.dataclass
    class TestClass:
      a: float
      b: NestedKlass
      c: bytes
      d: list[int]

    list_schema1 = kde.list_schema(schema_constants.INT32).eval()
    list_schema2 = kde.list_schema(list_schema1).eval()

    schema = kde.schema.new_schema(
        a=schema_constants.FLOAT32,
        b=kde.schema.new_schema(x=schema_constants.STRING),
        c=schema_constants.BYTES,
        d=list_schema2,
    ).eval()
    entity = from_py_fn(
        TestClass(42, NestedKlass('abc'), b'xyz', []), schema=schema
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(fns.dir(entity), ['a', 'b', 'c', 'd'])
    testing.assert_equal(entity.a.no_bag(), ds(42.0))
    b = entity.b
    testing.assert_equal(b.get_schema().no_bag(), schema.b.no_bag())
    testing.assert_equal(b.x.no_bag(), ds('abc'))
    testing.assert_equal(entity.c.no_bag(), ds(b'xyz'))
    testing.assert_equal(
        entity.d[:], ds([], list_schema1.with_bag(entity.get_bag()))
    )

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dataclasses_with_incomplete_schema(self, from_py_fn):
    schema = kde.schema.new_schema(
        a=schema_constants.FLOAT32,
    ).eval()
    entity = from_py_fn(
        TestKlass(42, NestedKlass('abc'), b'xyz'), schema=schema
    )
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(fns.dir(entity), ['a'])
    testing.assert_equal(entity.a.no_bag(), ds(42.0))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dataclasses_with_incomplete_schema_wrong_type(self, from_py_fn):
    schema = kde.schema.new_schema(
        a=schema_constants.FLOAT32,
    ).eval()
    with self.assertRaisesRegex(
        ValueError,
        'schema is incompatible',
    ):
      _ = from_py_fn(3.14, schema=schema)

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_list_of_dataclasses(self, from_py_fn):
    obj = from_py_fn([NestedKlass('a'), NestedKlass('b')])
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    nested = obj[:]
    testing.assert_equal(nested.S[0].x.no_bag(), ds('a'))
    testing.assert_equal(nested.S[1].x.no_bag(), ds('b'))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dataclass_with_list(self, from_py_fn):
    @dataclasses.dataclass
    class Test:
      l: list[int]

    obj = from_py_fn(Test([1, 2, 3]))
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        obj.l[:].no_bag(), ds([1, 2, 3], schema_constants.OBJECT)
    )

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dataclass_with_koda_obj(self, from_py_fn):
    @dataclasses.dataclass
    class Test:
      koda: data_slice.DataSlice

    schema = kde.schema.new_schema(
        koda=kde.schema.new_schema(x=schema_constants.INT32)
    ).eval()
    entity = from_py_fn(Test(fns.new(x=1, schema=schema.koda)), schema=schema)
    testing.assert_equal(entity.get_schema().no_bag(), schema.no_bag())
    self.assertCountEqual(fns.dir(entity), ['koda'])
    koda = entity.koda
    testing.assert_equal(koda.get_schema().no_bag(), schema.koda.no_bag())
    testing.assert_equal(koda.x.no_bag(), ds(1))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dataclasses_prevent_memory_leak(self, from_py_fn):
    gc.collect()
    base_count = sys.getrefcount(42)
    for _ in range(10):
      from_py_fn(TestKlassInternals(42, 3.14))
    gc.collect()
    self.assertEqual(base_count, sys.getrefcount(42))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dataclasses_errors(self, from_py_fn):
    with mock.patch.object(dataclasses, 'fields', return_value=[1, 2]):
      with self.assertRaisesRegex(
          AttributeError, "'int' object has no attribute 'name'"
      ):
        from_py_fn(TestKlassInternals(42, 3.14))
    with mock.patch.object(dataclasses, 'fields') as fields_mock:
      fields_mock.side_effect = ValueError('fields')
      with self.assertRaisesRegex(ValueError, 'fields'):
        from_py_fn(TestKlassInternals(42, 3.14))
    with mock.patch.object(dataclasses, 'fields', return_value=(1, 2)):
      with self.assertRaisesRegex(AttributeError, 'has no attribute \'name\''):
        from_py_fn(TestKlassInternals(42, 3.14))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dataclasses_field_attribute_error(self, from_py_fn):
    class TestField:

      def __init__(self, val):
        self._val = val

      @property
      def name(self):
        return 'non_existent'

    with mock.patch.object(
        dataclasses, 'fields', return_value=(TestField(42), TestField(3.14))
    ):
      with self.assertRaisesRegex(
          AttributeError, 'has no attribute \'non_existent\''
      ):
        from_py_fn(TestKlassInternals(42, 3.14))

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_dataclasses_field_invalid_name_error(self, from_py_fn):
    class TestField:

      def __init__(self, val):
        self._val = val

      @property
      def name(self):
        return b'non_existent'

    with mock.patch.object(
        dataclasses, 'fields', return_value=(TestField(42), TestField(3.14))
    ):
      with self.assertRaisesRegex(
          TypeError, "attribute name must be string, not 'bytes'"
      ):
        from_py_fn(TestKlassInternals(42, 3.14))

  def test_item_id(self):
    with self.subTest('list'):
      l1 = fns.from_py([1, 2, 3], itemid=kde.uuid_for_list('1').eval())
      l2 = fns.from_py([1, 2, 3], itemid=kde.uuid_for_list('1').eval())
      testing.assert_equivalent(l1, l2)
      testing.assert_equal(
          l1.no_bag().get_itemid(), kde.uuid_for_list('1').eval()
      )

      l3 = fns.from_py(
          [[1, 2], [3]],
          itemid=kde.uuid_for_list('1').eval(),
      )
      l4 = fns.from_py(
          [[1, 2], [3]],
          itemid=kde.uuid_for_list('1').eval(),
      )
      testing.assert_equivalent(l3, l4)
      testing.assert_equal(
          l3.no_bag().get_itemid(), kde.uuid_for_list('1').eval()
      )
      self.assertNotEqual(l3[:].S[0].fingerprint, l3[:].S[1].fingerprint)

      l5 = fns.from_py(
          [{'a': 1, 'b': 2}, {'c': 3, 'd': 4}],
          itemid=kde.uuid_for_list(a=ds('1')).eval(),
      )
      l6 = fns.from_py(
          [{'a': 1, 'b': 2}, {'c': 3, 'd': 4}],
          itemid=kde.uuid_for_list(a=ds('1')).eval(),
      )
      testing.assert_equivalent(l5, l6)
      testing.assert_equal(
          l5.no_bag().get_itemid(), kde.uuid_for_list(a=ds('1')).eval()
      )
      self.assertNotEqual(l5[:].S[0].fingerprint, l5[:].S[1].fingerprint)

    with self.subTest('dict'):
      d1 = fns.from_py({'a': 1, 'b': 2}, itemid=kde.uuid_for_dict('1').eval())
      d2 = fns.from_py({'a': 1, 'b': 2}, itemid=kde.uuid_for_dict('1').eval())
      testing.assert_equivalent(d1, d2)
      testing.assert_equal(
          d1.no_bag().get_itemid(), kde.uuid_for_dict('1').eval()
      )

      d3 = fns.from_py(
          [{'a': 1, 'b': 2}, [1, 2, 3]], itemid=kde.uuid_for_list('1').eval()
      )
      d4 = fns.from_py(
          [{'a': 1, 'b': 2}, [1, 2, 3]], itemid=kde.uuid_for_list('1').eval()
      )
      testing.assert_equivalent(d3, d4)
      testing.assert_equal(
          d3[1][:].no_bag(), ds([1, 2, 3], schema_constants.OBJECT)
      )
      testing.assert_equal(
          d3.no_bag().get_itemid(), kde.uuid_for_list('1').eval()
      )

      d5 = fns.from_py(
          {'a': [1, 2, 3], 'b': {'x': 'abc'}},
          itemid=kde.uuid_for_dict('1').eval(),
      )
      d6 = fns.from_py(
          {'a': [1, 2, 3], 'b': {'x': 'abc'}},
          itemid=kde.uuid_for_dict('1').eval(),
      )
      testing.assert_equivalent(d5, d6)
      testing.assert_equal(
          d5['a'][:].no_bag(), ds([1, 2, 3], schema_constants.OBJECT)
      )
      testing.assert_equal(
          d5['b']['x'].no_bag(), ds('abc', schema_constants.OBJECT)
      )
      testing.assert_equal(
          d5.no_bag().get_itemid(), kde.uuid_for_dict('1').eval()
      )

    with self.subTest('obj'):
      o1 = fns.from_py(
          TestKlass(a=42, b=NestedKlass('abc'), c=b'xyz', x='123'),
          itemid=kde.uuid('1').eval(),
      )
      o2 = fns.from_py(
          TestKlass(a=42, b=NestedKlass('abc'), c=b'xyz', x='123'),
          itemid=kde.uuid('1').eval(),
      )
      testing.assert_equivalent(o1, o2)
      self.assertNotEqual(o1.x.fingerprint, o1.b.x.fingerprint)
      testing.assert_equal(o1.no_bag().get_itemid(), kde.uuid('1').eval())

    with self.subTest('dict_as_obj'):
      o1 = fns.from_py(
          {'a': 1, 'b': 2}, dict_as_obj=True, itemid=kde.uuid('1').eval()
      )
      o2 = fns.from_py(
          {'a': 1, 'b': 2}, dict_as_obj=True, itemid=kde.uuid('1').eval()
      )
      testing.assert_equivalent(o1, o2)
      self.assertNotEqual(o1.a.fingerprint, o1.b.fingerprint)
      testing.assert_equal(o1.no_bag().get_itemid(), kde.uuid('1').eval())

      o3 = fns.from_py(
          [{'a': 1, 'b': 2}, [1, 2, 3]],
          dict_as_obj=True,
          itemid=kde.uuid_for_list('1').eval(),
      )
      o4 = fns.from_py(
          [{'a': 1, 'b': 2}, [1, 2, 3]],
          dict_as_obj=True,
          itemid=kde.uuid_for_list('1').eval(),
      )
      testing.assert_equivalent(o3, o4)
      self.assertNotEqual(o3[:].S[0].fingerprint, o3[:].S[1].fingerprint)

      testing.assert_equal(
          o3[1][:].no_bag(), ds([1, 2, 3], schema_constants.OBJECT)
      )
      testing.assert_equal(
          o3.no_bag().get_itemid(), kde.uuid_for_list('1').eval()
      )

    with self.subTest('nested obj'):
      o1 = fns.from_py(
          {'a': 1, 'b': {'a': 'abc'}},
          dict_as_obj=True,
          itemid=kde.uuid('1').eval(),
      )
      o2 = fns.from_py(
          {'a': 1, 'b': {'a': 'abc'}},
          dict_as_obj=True,
          itemid=kde.uuid('1').eval(),
      )
      testing.assert_equivalent(o1, o2)
      self.assertNotEqual(o1.a.fingerprint, o1.b.a.fingerprint)
      testing.assert_equal(o1.no_bag().get_itemid(), kde.uuid('1').eval())
      self.assertFalse(o1.b.is_dict())

    with self.subTest('attr_name child itemid'):
      parent_itemid = kde.uuid('1').eval()
      child_itemid = kde.uuid(
          '__from_py_child__', parent=parent_itemid, attr_name='a'
      ).eval()
      obj = fns.from_py(
          {'a': {'b': '1'}},
          schema=kde.uu_schema(
              a=kde.uu_schema(b=schema_constants.STRING)
          ).eval(),
          itemid=parent_itemid,
      )
      testing.assert_equal(obj.no_bag().get_itemid(), parent_itemid)
      testing.assert_equal(obj.a.no_bag().get_itemid(), child_itemid)

    with self.subTest('dict_value_index child itemid'):
      parent_itemid = kde.uuid('1').eval()
      child_itemid = kde.uuid(
          '__from_py_child__', parent=parent_itemid, attr_name='a'
      ).eval()
      child_dict_itemid = kde.uuid_for_dict(
          '__from_py_child__', parent=child_itemid, attr_name='b'
      ).eval()

      child_list_itemid = kde.uuid_for_list(
          '__from_py_child__',
          parent=child_dict_itemid,
          dict_value_index=ds([0, 1], schema_constants.INT64),
      ).eval()
      obj = fns.from_py(
          {'a': {'b': {'1': [1, 2, 3], '2': [4, 5]}}},
          schema=kde.uu_schema(
              a=kde.uu_schema(
                  b=kde.dict_schema(
                      schema_constants.STRING,
                      kde.list_schema(schema_constants.INT32),
                  )
              )
          ).eval(),
          itemid=parent_itemid,
      )
      testing.assert_equal(obj.no_bag().get_itemid(), parent_itemid)
      testing.assert_equal(obj.a.no_bag().get_itemid(), child_itemid)
      testing.assert_equal(
          obj.a.b[ds(['1', '2'])].no_bag().get_itemid(), child_list_itemid
      )

    with self.subTest('dict_key_index child itemid'):
      parent_itemid = kde.uuid_for_dict('1').eval()

      child_keys_itemid = kde.uuid(
          '__from_py_child__',
          parent=parent_itemid,
          dict_key_index=ds([0, 1], schema_constants.INT64),
      ).eval()
      key1 = NestedKlass('1')
      key2 = NestedKlass('2')
      obj = fns.from_py(
          {key1: [1, 2, 3], key2: [4, 5]},
          itemid=parent_itemid,
      )
      testing.assert_equal(obj.no_bag().get_itemid(), parent_itemid)
      testing.assert_dicts_keys_equal(
          obj, child_keys_itemid.with_schema(schema_constants.OBJECT)
      )

    with self.subTest('list_item_index child itemid'):
      parent_itemid = kde.uuid_for_list('1').eval()
      child_list_itemid = kde.uuid_for_list(
          '__from_py_child__',
          parent=parent_itemid,
          list_item_index=ds([0, 1], schema_constants.INT64),
      ).eval()
      obj = fns.from_py(
          [[1, 2, 3], [4, 5, 6]],
          itemid=parent_itemid,
          from_dim=0,
      )
      testing.assert_equal(obj.no_bag().get_itemid(), parent_itemid)
      testing.assert_equal(
          obj[:].no_bag().get_itemid(), child_list_itemid
      )

    with self.subTest('list_with_from_dim'):
      l1 = fns.from_py(
          [[1, 2], [3]],
          itemid=kde.uuid_for_list(a=ds(['1', '2'])).eval(),
          from_dim=1,
      )
      l2 = fns.from_py(
          [[1, 2], [3]],
          itemid=kde.uuid_for_list(a=ds(['1', '2'])).eval(),
          from_dim=1,
      )
      testing.assert_equivalent(l1, l2)
      testing.assert_equal(
          l1.no_bag().get_itemid(), kde.uuid_for_list(a=ds(['1', '2'])).eval()
      )

    with self.subTest('dict_with_from_dim'):
      d1 = fns.from_py(
          [{'a': 1, 'b': 2}, {'c': 3, 'd': 4}],
          itemid=kde.uuid_for_dict(a=ds(['1', '2'])).eval(),
          from_dim=1,
      )
      d2 = fns.from_py(
          [{'a': 1, 'b': 2}, {'c': 3, 'd': 4}],
          itemid=kde.uuid_for_dict(a=ds(['1', '2'])).eval(),
          from_dim=1,
      )
      testing.assert_equivalent(d1, d2)
      testing.assert_equal(
          d1.no_bag().get_itemid(), kde.uuid_for_dict(a=ds(['1', '2'])).eval()
      )

    with self.subTest('itemid caching'):
      d = {'a': 42}
      l1 = fns.from_py([d, d], itemid=kde.uuid_for_list('1').eval())
      self.assertNotEqual(l1[:].S[0].fingerprint, l1[:].S[1].fingerprint)

      d1 = fns.from_py({'a': d, 'b': d}, itemid=kde.uuid_for_dict('1').eval())
      self.assertNotEqual(d1['a'].fingerprint, d1['b'].fingerprint)

  def test_item_id_errors(self):
    with self.assertRaisesRegex(
        ValueError, '`itemid` expected ITEMID schema, got INT32'
    ):
      _ = fns.from_py([1, 2], itemid=ds(42))
    with self.assertRaisesRegex(
        ValueError,
        'ItemId for DataSlice size=1 does not match the input list size=2 when'
        ' from_dim=1',
    ):
      _ = fns.from_py(
          [[1, 2], [3]],
          itemid=kde.uuid_for_list(a=ds(['1'])).eval(),
          from_dim=1,
      )

    with self.assertRaisesRegex(
        ValueError,
        'ItemId for DataSlice must be a DataSlice of non-zero rank if'
        ' from_dim > 0',
    ):
      _ = fns.from_py(
          [[1, 2], [3]],
          itemid=kde.uuid_for_list(a=ds('1')).eval(),
          from_dim=1,
      )
    with self.assertRaisesRegex(
        ValueError, 'itemid argument to list creation, requires List ItemIds'
    ):
      _ = fns.from_py(
          [[1, 2], [3]],
          itemid=kde.uuid_for_dict(a=ds('1')).eval(),
      )

    with self.assertRaisesRegex(
        ValueError, 'itemid argument to dict creation, requires Dict ItemIds'
    ):
      _ = fns.from_py(
          {'a': 1, 'b': 2},
          itemid=kde.uuid(a=ds('1')).eval(),
      )

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_deep_dict_with_repetitions(self, from_py_fn):
    py_d = {'abc': 42, 'def': 64}
    schema = kde.dict_schema(
        schema_constants.STRING, schema_constants.INT32
    ).eval()
    for _ in range(2):
      py_d = {12: py_d, 42: py_d}
      schema = kde.dict_schema(schema_constants.INT32, schema).eval()

    with self.subTest('no schema'):
      d = from_py_fn(py_d)
      testing.assert_dicts_keys_equal(d, ds([12, 42], schema_constants.OBJECT))
      d1 = d[12]
      d2 = d[42]
      testing.assert_dicts_keys_equal(d1, ds([12, 42], schema_constants.OBJECT))
      testing.assert_dicts_keys_equal(d2, ds([12, 42], schema_constants.OBJECT))
      d11 = d1[12]
      d12 = d1[12]
      d21 = d2[42]
      d22 = d2[42]
      testing.assert_dicts_keys_equal(
          d11, ds(['abc', 'def'], schema_constants.OBJECT)
      )
      testing.assert_dicts_keys_equal(
          d12, ds(['abc', 'def'], schema_constants.OBJECT)
      )
      testing.assert_dicts_keys_equal(
          d21, ds(['abc', 'def'], schema_constants.OBJECT)
      )
      testing.assert_dicts_keys_equal(
          d22, ds(['abc', 'def'], schema_constants.OBJECT)
      )

    with self.subTest('with schema'):
      d = from_py_fn(py_d, schema=schema)
      testing.assert_dicts_keys_equal(d, ds([12, 42]))
      d1 = d[12]
      d2 = d[42]
      testing.assert_dicts_keys_equal(d1, ds([12, 42]))
      testing.assert_dicts_keys_equal(d2, ds([12, 42]))
      d11 = d1[12]
      d12 = d1[12]
      d21 = d2[42]
      d22 = d2[42]
      testing.assert_dicts_keys_equal(d11, ds(['abc', 'def']))
      testing.assert_dicts_keys_equal(d12, ds(['abc', 'def']))
      testing.assert_dicts_keys_equal(d21, ds(['abc', 'def']))
      testing.assert_dicts_keys_equal(d22, ds(['abc', 'def']))

  def test_deep_dict_recursive_error(self):
    py_d = {'a': 42}
    schema = kde.dict_schema(
        schema_constants.STRING, schema_constants.OBJECT
    ).eval()
    bottom_d = py_d
    for i in range(3):
      py_d = {f'd{i}': py_d}
      schema = kde.dict_schema(schema_constants.STRING, schema).eval()
    level_1_d = py_d
    py_d = {'top': level_1_d}
    bottom_d['cycle'] = level_1_d

    # TODO: handle recursion in v2.
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.from_py(py_d)
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.from_py(py_d, schema=schema)

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_deep_list_with_repetitions(self, from_py_fn):
    py_l = [1, 2, 3]
    schema = kde.list_schema(schema_constants.INT32).eval()
    for _ in range(2):
      py_l = [py_l, py_l]
      schema = kde.list_schema(schema).eval()

    testing.assert_equal(
        from_py_fn(py_l)[:][:][:].no_bag(),
        ds(
            [[[1, 2, 3], [1, 2, 3]], [[1, 2, 3], [1, 2, 3]]],
            schema_constants.OBJECT,
        ),
    )

    testing.assert_equal(
        from_py_fn(py_l, schema=schema)[:][:][:].no_bag(),
        ds([[[1, 2, 3], [1, 2, 3]], [[1, 2, 3], [1, 2, 3]]]),
    )

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_deep_list_recursive_error(self, from_py_fn):
    py_l = [1, 2, 3]
    schema = kde.list_schema(schema_constants.INT32).eval()
    bottom_l = py_l
    for _ in range(3):
      py_l = [py_l, py_l]
      schema = kde.list_schema(schema).eval()
    level_1_l = py_l
    py_l = [level_1_l]
    bottom_l.append(level_1_l)
    # TODO: handle recursion in v2.
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.from_py(py_l)
    error_msg = (
        'recursive .* cannot be converted'
        if from_py_fn == fns.from_py
        else 'object with unsupported type: list'
    )
    with self.assertRaisesRegex(ValueError, error_msg):
      from_py_fn(py_l, schema=schema)

  def test_deep_object_repetitions(self):
    py_d = {'abc': 42}
    schema = kde.uu_schema(abc=schema_constants.INT32).eval()
    for _ in range(2):
      py_d = {'x': py_d, 'y': py_d}
      schema = kde.uu_schema(x=schema, y=schema).eval()

    obj = fns.from_py(py_d, dict_as_obj=True)
    testing.assert_equal(obj.x.x.abc.no_bag(), ds(42))
    testing.assert_equal(obj.x.y.abc.no_bag(), ds(42))
    testing.assert_equal(obj.y.x.abc.no_bag(), ds(42))
    testing.assert_equal(obj.y.y.abc.no_bag(), ds(42))

    entity = fns.from_py(py_d, dict_as_obj=True, schema=schema)
    testing.assert_equal(entity.x.x.abc.no_bag(), ds(42))
    testing.assert_equal(entity.x.y.abc.no_bag(), ds(42))
    testing.assert_equal(entity.y.x.abc.no_bag(), ds(42))
    testing.assert_equal(entity.y.y.abc.no_bag(), ds(42))

  def test_deep_object_recursive_error(self):
    py_d = {'a': 42}
    schema = kde.uu_schema(
        a=schema_constants.INT32, cycle=schema_constants.OBJECT
    ).eval()
    bottom_d = py_d
    for i in range(3):
      py_d = {f'd{i}': py_d}
      schema = kde.uu_schema(**{f'd{i}': schema}).eval()
    level_1_d = py_d
    py_d = {'top': level_1_d}
    schema = kde.uu_schema(top=schema).eval()
    bottom_d['cycle'] = level_1_d
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.from_py(py_d, dict_as_obj=True)
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.from_py(py_d, dict_as_obj=True, schema=schema)

  def test_deep_itemid_recursive_error(self):
    py_l = [1, 2, 3]
    schema = kde.list_schema(schema_constants.INT32).eval()
    bottom_l = py_l
    for _ in range(3):
      py_l = [py_l, py_l]
      schema = kde.list_schema(schema).eval()
    level_1_l = py_l
    py_l = [level_1_l]
    bottom_l.append(level_1_l)
    with self.assertRaisesRegex(ValueError, 'recursive .* cannot be converted'):
      fns.from_py(py_l, itemid=kde.uuid_for_list('list').eval())

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_no_recursion_detected(self, from_py_fn):
    with self.subTest('list'):
      py_l = [1, 2, 3]
      py_l2 = [py_l, [py_l, [py_l, py_l]]]
      _ = from_py_fn(py_l2)
    with self.subTest('dict'):
      py_d = {'a': 1, 'b': 2}
      py_d2 = {'a': py_d, 'b': py_d}
      _ = from_py_fn(py_d2)
    with self.subTest('object'):
      py_d = {'a': 1, 'b': 2}
      py_d2 = {'a': {'b': py_d}, 'b': {'a': py_d}}
      _ = fns.from_py(py_d2, dict_as_obj=True)

    with self.subTest('different levels'):
      x = [1]
      _ = from_py_fn(
          [x, [x], x],
          schema=kde.list_schema(
              kde.list_schema(schema_constants.OBJECT)
          ).eval(),
      )
      _ = from_py_fn(
          [(), [()]],
          schema=kde.list_schema(
              kde.list_schema(schema_constants.OBJECT)
          ).eval(),
      )

  def test_alias(self):
    obj = fns.from_pytree({'a': 42})
    testing.assert_equal(obj.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_dicts_keys_equal(obj, ds(['a'], schema_constants.OBJECT))
    values = obj['a']
    testing.assert_equal(values.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        values, ds(42, schema_constants.OBJECT).with_bag(values.get_bag())
    )

  @parameterized.named_parameters(_VERSION_PARAMS)
  def test_arg_errors_schema(self, from_py_fn):
    with self.assertRaisesRegex(
        TypeError, 'expecting schema to be a DataSlice, got int'
    ):
      from_py_fn([1, 2], schema=42)
    with self.assertRaisesRegex(
        ValueError, r'schema\'s schema must be SCHEMA, got: INT32'
    ):
      from_py_fn([1, 2], schema=ds(42))
    with self.assertRaisesRegex(
        ValueError, r'schema\'s schema must be SCHEMA, got: INT32'
    ):
      from_py_fn([1, 2], schema=ds([42]))

    with self.assertRaisesRegex(
        ValueError, r'schema mismatch: expected an object schema here'
    ):
      fns._from_py_v2([1, 2], schema=schema_constants.SCHEMA)

  def test_arg_errors(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting dict_as_obj to be a bool, got int'
    ):
      fns.from_py([1, 2], dict_as_obj=42)  # pytype: disable=wrong-arg-types

    with self.assertRaisesRegex(
        TypeError, 'expecting itemid to be a DataSlice, got int'
    ):
      fns.from_py(  # pytype: disable=wrong-arg-types
          [1, 2],
          dict_as_obj=False,
          itemid=42,
          schema=kde.schema.new_schema().eval(),
          from_dim=0,
      )

    with self.assertRaisesRegex(
        TypeError, 'expecting from_dim to be an int, got str'
    ):
      fns.from_py([1, 2], from_dim='abc')  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
