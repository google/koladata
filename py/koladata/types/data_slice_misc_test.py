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

import inspect
import io
from unittest import mock
import warnings

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import input_container
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import signature_test_utils
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import dict_item as _
from koladata.types import jagged_shape
from koladata.types import list_item as _
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals

INT64 = schema_constants.INT64

SRC_PIN = optools.SRC_PIN


@tracing_decorator.TraceAsFnDecorator()
def add_one(x):
  """Adds one to the input."""
  return x + 1


class DataSliceMethodsTest(parameterized.TestCase):

  def test_add_method(self):
    self.assertFalse(hasattr(data_slice.DataSlice, 'foo'))

    @data_slice.add_method(data_slice.DataSlice, 'foo')
    def foo(self):
      """Converts DataSlice to Python list."""
      return self.internal_as_py()

    self.assertTrue(hasattr(data_slice.DataSlice, 'foo'))

    x = ds([1, 2, 3])
    self.assertEqual(x.foo(), [1, 2, 3])

    with self.assertRaisesRegex(TypeError, 'method name must be a string'):
      data_slice.add_method(data_slice.DataSlice, b'foo')(lambda x: x)  # pyrefly: ignore[bad-argument-type]

    with self.assertRaisesRegex(
        AttributeError, r'object attribute \'foo\' is read-only'
    ):
      x.foo = 42

  def test_add_method_to_subclass(self):

    class SubDataSlice(data_slice.DataSlice):
      pass

    self.assertFalse(hasattr(data_slice.DataSlice, 'bar'))
    self.assertFalse(hasattr(SubDataSlice, 'bar'))

    @data_slice.add_method(SubDataSlice, 'bar')
    def bar(self):
      del self
      pass

    self.assertFalse(hasattr(data_slice.DataSlice, 'bar'))
    self.assertTrue(hasattr(SubDataSlice, 'bar'))

  def test_add_method_to_superclass(self):

    class SubDataSlice(data_slice.DataSlice):
      pass

    self.assertFalse(hasattr(data_slice.DataSlice, 'superbar'))
    self.assertFalse(hasattr(SubDataSlice, 'superbar'))

    @data_slice.add_method(data_slice.DataSlice, 'superbar')
    def superbar(self):
      del self
      pass

    self.assertTrue(hasattr(data_slice.DataSlice, 'superbar'))
    self.assertTrue(hasattr(SubDataSlice, 'superbar'))
    delattr(data_slice.DataSlice, 'superbar')

  def test_subclass(self):

    x = bag().obj()
    x.some_method = 42
    testing.assert_equal(x.some_method, ds(42).with_bag(x.get_bag()))

    @data_slice.register_reserved_class_method_names
    class SubDataSlice(data_slice.DataSlice):

      def some_method(self):
        pass

    self.assertFalse(hasattr(data_slice.DataSlice, 'some_method'))
    self.assertTrue(hasattr(SubDataSlice, 'some_method'))

    with self.assertRaisesRegex(
        AttributeError, r'has no attribute \'some_method\''
    ):
      _ = x.some_method
    with self.assertRaisesRegex(
        AttributeError, r'has no attribute \'some_method\''
    ):
      x.some_method = 42

  def test_subclass_error(self):

    with self.assertRaises(AssertionError):

      @data_slice.register_reserved_class_method_names
      class NonDataSliceSubType:
        pass

      del NonDataSliceSubType

  @parameterized.named_parameters(
      *signature_test_utils.generate_method_function_signature_compatibility_cases(
          ds([1, 2, 3]),
          user_facing_kd,
          skip_methods={
              'S',  # Has different meanings between method and function.
              'implode',  # method lacks db= argument for consistency with view
              'new',  # method offers much simpler and restrictive interface
              'strict_new',  # method offers much simpler and
                             # restrictive interface
          },
          skip_params=[
              ('with_bag', 0),  # bag is positional-only in C++
              ('with_schema', 0),  # schema is positional-only in C++
              ('set_schema', 0),  # schema is positional-only in C++
              ('get_attr', 0),  # attr_name is positional-only in C++
              ('get_attr', 1),  # default is None instead of unspecified
              ('set_attr', 0),  # attr_name is positional-only in C++
              ('set_attr', 1),  # value is positional-only in C++
          ],
      )
  )
  def test_consistent_signatures(self, *args, **kwargs):
    signature_test_utils.check_method_function_signature_compatibility(
        self, *args, **kwargs
    )

  def test_get_reserved_attrs(self):
    # Assert that get_reserved_attrs() is a superset of the DataSlice methods
    # without leading underscore. It also contains registered methods from its
    # subclasses
    self.assertEmpty(
        set([
            attr
            for attr in dir(data_slice.DataSlice)
            if not attr.startswith('_')
        ])
        - data_slice.get_reserved_attrs()
    )

    @data_slice.register_reserved_class_method_names
    class SubDataSlice(data_slice.DataSlice):  # pylint: disable=unused-variable

      def new_method(self):
        pass

    self.assertIn('new_method', data_slice.get_reserved_attrs())


class DataSliceMergingTest(parameterized.TestCase):

  def test_set_get_attr(self):
    db = bag()
    x = db.new(abc=ds([42]))
    db2 = bag()
    x2 = db2.new(qwe=ds([57]))

    x.get_schema().xyz = x2.get_schema()
    x.xyz = x2
    testing.assert_equal(x.abc, ds([42]).with_bag(db))
    testing.assert_equal(
        x.abc.get_schema(), schema_constants.INT32.with_bag(db)
    )
    testing.assert_equal(x.xyz.qwe, ds([57]).with_bag(db))
    testing.assert_equal(
        x.xyz.qwe.get_schema(), schema_constants.INT32.with_bag(db)
    )

  def test_set_get_dict_single(self):
    db = bag()
    dct = db.dict()
    dct['a'] = 7
    db2 = bag()
    dct2 = db2.dict()
    dct2['b'] = 5
    dct['obj'] = dct2

    testing.assert_equal(dct['a'], ds(7, schema_constants.OBJECT).with_bag(db))
    testing.assert_equal(
        dct['obj']['b'], ds(5, schema_constants.OBJECT).with_bag(db)
    )

    ds([dct.embed_schema(), dct['obj']])['c'] = ds([db2.obj(a=1), db2.obj(a=2)])
    testing.assert_equal(dct['c'].a, ds(1).with_bag(db))
    testing.assert_equal(dct['obj']['c'].a, ds(2).with_bag(db))

  def test_dict_keys_bag_merging(self):
    obj1 = bag().obj(a=7)
    obj2 = bag().obj(a=3)
    dct = bag().dict()
    dct[obj1] = 4
    dct[obj2] = 5
    testing.assert_dicts_keys_equal(
        dct, ds([obj1, obj2], schema_constants.OBJECT)
    )

  def test_set_get_dict_slice(self):
    db = bag()
    keys_ds = db.dict_shaped(jagged_shape.create_shape([2]))
    keys_ds['abc_key'] = 1
    values_ds = db.new(abc_value=ds(['v', 'w']))

    with self.assertRaisesRegex(
        ValueError, 'only supported for Koda List DataItem'
    ):
      keys_ds[ds(['a', 'b'])] = [4, 5]

    with self.assertRaisesRegex(
        ValueError, 'only supported for Koda Dict DataItem'
    ):
      keys_ds[ds(['a', 'b'])] = {'a': 4}

    with self.subTest('only keys have db'):
      db2 = bag()
      dct = db2.dict()
      dct[keys_ds] = 1
      testing.assert_equal(
          dct[keys_ds], ds([1, 1], schema_constants.OBJECT).with_bag(db2)
      )
      testing.assert_equal(
          keys_ds.with_bag(db2)['abc_key'],
          ds([1, 1], schema_constants.OBJECT).with_bag(db2),
      )
      del db2

    with self.subTest('only values have db'):
      db2 = bag()
      dct = db2.dict()
      keys = ds(['a', 'b'])
      dct[keys] = values_ds
      testing.assert_equal(dct[keys].abc_value, ds(['v', 'w']).with_bag(db2))
      del db2

    with self.subTest('keys and values have the same db'):
      db2 = bag()
      dct = db2.dict()
      dct[keys_ds] = values_ds
      testing.assert_equal(
          keys_ds.with_bag(db2)['abc_key'],
          ds([1, 1], schema_constants.OBJECT).with_bag(db2),
      )
      testing.assert_equal(dct[keys_ds].abc_value, ds(['v', 'w']).with_bag(db2))
      del db2

    with self.subTest('keys and values have different db'):
      db3 = bag()
      values = db3.new(abc_value=ds(['Y', 'Z']))
      db2 = bag()
      dct = db2.dict()
      dct[keys_ds] = values
      testing.assert_equal(
          keys_ds.with_bag(db2)['abc_key'],
          ds([1, 1], schema_constants.OBJECT).with_bag(db2),
      )
      testing.assert_equal(dct[keys_ds].abc_value, ds(['Y', 'Z']).with_bag(db2))
      del db2

  def test_set_get_list_single(self):
    db = bag()
    lst = db.list()
    lst.append(7)
    db2 = bag()
    lst2 = db2.list()
    lst2.append(5)
    lst[0] = lst2

    testing.assert_equal(lst[0][0], ds(5, schema_constants.OBJECT).with_bag(db))

  def test_append_list_single(self):
    db = bag()
    lst = db.list(item_schema=schema_constants.OBJECT)
    lst2 = bag().list([5])
    lst3 = bag().list([6])
    lst.append(lst2)
    lst.append(ds([lst3]))

    testing.assert_equal(lst[0][0], ds(5).with_bag(db))
    testing.assert_equal(lst[1][0], ds(6).with_bag(db))

  def test_replace_in_list_single(self):
    db = bag()
    lst = db.list()
    lst.append(7)
    db2 = bag()
    lst2 = db2.list([5])
    lst2_ext = db2.list([lst2])
    lst[:] = lst2_ext[:]
    lst[1:] = ds([
        bag().obj(a=1),
        bag().obj(a=2),
    ])
    testing.assert_equal(lst[0][0], ds(5, schema_constants.INT32).with_bag(db))
    testing.assert_equal(
        lst[1:].a, ds([1, 2], schema_constants.INT32).with_bag(db)
    )

  def test_get_set_schema(self):
    db = bag()
    obj = db.new(a=1)
    db2 = bag()
    obj2 = obj.with_bag(db2)
    obj2.get_schema().x = obj.get_schema()

    testing.assert_equal(
        obj2.get_schema().x.a, schema_constants.INT32.with_bag(obj2.get_bag())
    )


class DataSliceFallbackTest(parameterized.TestCase):

  @parameterized.parameters(
      (None,), (ds([1, 2, 3]),), (bag().new(x=1),)
  )
  def test_errors(self, db):
    with self.assertRaisesRegex(
        ValueError, 'expected all arguments to be DATA_BAG'
    ):
      ds([1, 2, 3]).enriched(db)

  def test_immutable(self):
    db = bag()
    x = db.new(q=1)
    x.get_schema().w = schema_constants.INT32
    x_fb = x.freeze_bag().enriched(db)
    with self.assertRaisesRegex(ValueError, 'immutable'):
      x_fb.get_schema().w = schema_constants.INT32
      x_fb.w = x_fb.q + 1

  def test_immutable_with_merging(self):
    db = bag()
    x = db.new(q=1)
    x.get_schema().w = schema_constants.INT32
    x_fb = x.freeze_bag().enriched(db)
    db2 = bag()
    x2 = db2.new(q=1)
    with self.assertRaisesRegex(ValueError, 'immutable'):
      x_fb.get_schema().w = schema_constants.INT32
      x_fb.w = x2.q + 1

  def test_get_attr_no_original_bag(self):
    db = bag()
    x = db.new(abc=ds([3.14, None]))
    x = x.with_bag(None)
    x = x.enriched(db)
    testing.assert_allclose(x.abc, ds([3.14, None]).with_bag(x.get_bag()))

  def test_get_attr(self):
    db = bag()
    x = db.new(abc=ds([3.14, None]))
    x.get_schema().xyz = schema_constants.FLOAT64
    x.S[0].xyz = ds(2.71, schema_constants.FLOAT64)
    # Note: x.S[1].abc is REMOVED, x.S[1].xyz is UNSET

    fb_bag = bag()
    fb_x = x.with_bag(fb_bag)
    fb_x.get_schema().abc = schema_constants.FLOAT32
    fb_x.abc = ds([None, 2.71])
    fb_x.get_schema().xyz = schema_constants.FLOAT64
    fb_x.xyz = ds([None, 14.5])

    merged_x = x.freeze_bag().enriched(fb_bag)

    testing.assert_allclose(
        merged_x.abc, ds([3.14, None]).with_bag(merged_x.get_bag())
    )
    testing.assert_allclose(
        merged_x.xyz,
        ds([2.71, 14.5], schema_constants.FLOAT64).with_bag(merged_x.get_bag()),
    )

    # update new DataBag
    new_bag = bag()
    new_x = x.with_bag(new_bag)
    new_x.xyz = ds([None, 3.14], schema_constants.FLOAT64)
    # Note: new_x.S[0].xyz is REMOVED
    merged_x = new_x.freeze_bag().enriched(db, fb_bag)
    testing.assert_allclose(
        merged_x.xyz,
        ds([None, 3.14], schema_constants.FLOAT64)
        .with_bag(merged_x.get_bag()),
    )
    testing.assert_allclose(
        x.xyz, ds([2.71, None], schema_constants.FLOAT64).with_bag(x.get_bag())
    )

    # update original DataBag
    x.xyz = ds([1.61, None], schema_constants.FLOAT64)
    testing.assert_allclose(
        merged_x.xyz,
        ds([None, 3.14], schema_constants.FLOAT64)
        .with_bag(merged_x.get_bag()),
    )

  def test_get_attr_mixed_type(self):
    db = bag()
    x = db.new(abc=ds([314, None]))
    x.S[0].xyz = ds(315, schema_constants.OBJECT)
    # Note: x.S[1].abc is REMOVED, x.S[1].xyz is UNSET

    fb_bag = bag()
    fb_x = x.with_bag(fb_bag)
    fb_x.abc = ds([None, '2.71'])
    fb_x.xyz = ds([None, '3.17'])

    merged_x = x.freeze_bag().enriched(fb_bag)

    testing.assert_equal(
        merged_x.abc,
        ds([314, None]).with_bag(merged_x.get_bag()),
    )
    testing.assert_equal(
        merged_x.xyz,
        ds([315, '3.17']).with_bag(merged_x.get_bag()),
    )

  def test_get_attr_all_removed(self):
    for size in range(10):
      with self.subTest(f'size={size}'):
        db = bag()
        x = db.new(abc=ds([None] * size, schema_constants.INT32))

        fb_bag = bag()
        fb_x = x.with_bag(fb_bag)
        fb_x.abc = ds(list(range(size)), schema_constants.INT32)

        merged_x = x.freeze_bag().enriched(fb_bag)

        testing.assert_equal(
            merged_x.abc,
            ds([None] * size, schema_constants.INT32).with_bag(
                merged_x.get_bag()
            ),
        )

  def test_dict(self):
    db = bag()
    x = db.dict_shaped(jagged_shape.create_shape([2]))
    x['abc'] = ds([3.14, None])
    x['xyz'] = ds([2.71, None])

    fb_bag = bag()
    fb_x = x.with_bag(fb_bag)
    x.get_schema().with_bag(fb_bag).set_attr(
        '__keys__', x.get_schema().get_attr('__keys__')
    )
    x.get_schema().with_bag(fb_bag).set_attr(
        '__values__', x.get_schema().get_attr('__values__')
    )
    fb_x['abc'] = ds([None, 2.71])
    fb_x['qwe'] = ds([None, 'pi'])
    fb_x['asd'] = ds(['e', None])

    merged_x = x.freeze_bag().enriched(fb_bag)

    testing.assert_dicts_keys_equal(
        merged_x,
        ds([['abc', 'xyz', 'asd', 'qwe']] * 2, schema_constants.OBJECT),
    )
    testing.assert_dicts_values_equal(
        merged_x,
        ds(
            [[3.14, 2.71, 'e', None], [None, None, None, 'pi']],
            schema_constants.OBJECT,
        ),
    )
    testing.assert_allclose(
        merged_x['abc'],
        ds([3.14, None], schema_constants.OBJECT).with_bag(merged_x.get_bag()),
    )
    testing.assert_allclose(
        merged_x['xyz'],
        ds([2.71, None], schema_constants.OBJECT).with_bag(merged_x.get_bag()),
    )

    new_bag = bag()
    merged_x = merged_x.with_bag(new_bag)
    merged_x.get_schema().with_bag(new_bag).set_attr(
        '__keys__', x.get_schema().get_attr('__keys__')
    )
    merged_x.get_schema().with_bag(new_bag).set_attr(
        '__values__', x.get_schema().get_attr('__values__')
    )
    merged_x['xyz'] = ds([None, 3.14])
    merged_x = merged_x.freeze_bag().enriched(db, fb_bag)
    testing.assert_allclose(
        merged_x['xyz'],
        ds([None, 3.14], schema_constants.OBJECT).with_bag(merged_x.get_bag()),
    )

  def test_deep_fallbacks(self):
    cnt = 100
    dbs = [bag() for _ in range(cnt)]
    dct = dbs[0].dict()
    dct_schema = dct.get_schema()
    obj = dbs[0].new(q=1)
    merged_bag = bag()
    for i, db in enumerate(dbs):
      dct_schema.with_bag(db).set_attr(
          '__keys__', dct_schema.get_attr('__keys__')
      )
      dct_schema.with_bag(db).set_attr(
          '__values__', dct_schema.get_attr('__values__')
      )
      dct.with_bag(db)[f'd{i}'] = i
      setattr(obj.get_schema().with_bag(db), f'a{i}', schema_constants.INT32)
      setattr(obj.with_bag(db), f'a{i}', -i)
      merged_bag = dct.with_bag(merged_bag).freeze_bag().enriched(db).get_bag()

    dct = dct.with_bag(merged_bag)
    testing.assert_dicts_keys_equal(
        dct, ds([f'd{i}' for i in range(cnt)], schema_constants.OBJECT)
    )
    testing.assert_dicts_values_equal(
        dct,
        ds([i for i in range(cnt)], schema_constants.OBJECT),
    )
    obj = obj.with_bag(merged_bag)
    for i in range(cnt):
      testing.assert_equal(
          dct[f'd{i}'], ds(i, schema_constants.OBJECT).with_bag(dct.get_bag())
      )
      testing.assert_equal(
          getattr(obj, f'a{i}'), ds(-i).with_bag(obj.get_bag())
      )

  def test_disabled_data_item_magic_methods(self):
    with self.assertRaisesRegex(
        TypeError, '__bool__ disabled for koladata.types.data_slice.DataSlice'
    ):
      bool(ds([arolla.unit()]))

  def test_get_present_count(self):
    testing.assert_equal(ds(57).get_present_count(), ds(1, INT64))
    testing.assert_equal(ds(None).get_present_count(), ds(0, INT64))
    testing.assert_equal(
        ds([3.14, None, 57.0]).get_present_count(), ds(2, INT64)
    )

  def test_get_size(self):
    self.assertEqual(ds(57).get_size(), ds(1, INT64))
    self.assertEqual(ds(None).get_size(), ds(1, INT64))
    self.assertEqual(ds([3.14, None, 57.0]).get_size(), ds(3, INT64))
    self.assertEqual(ds([[1, 2], [3, None], [None]]).get_size(), ds(5, INT64))

  def test_neg(self):
    self.assertEqual(-ds(5), ds(-5, INT64))

  def test_pos(self):
    self.assertEqual(+ds(-5), ds(-5, INT64))

  def test_get_ndim(self):
    testing.assert_equal(ds(57).get_ndim(), ds(0, INT64))
    testing.assert_equal(ds([1, 2, 3]).get_ndim(), ds(1, INT64))
    testing.assert_equal(ds([[1, 2], [3, 4, 5]]).get_ndim(), ds(2, INT64))
    testing.assert_equal(ds([[[[[]]]]]).get_ndim(), ds(5, INT64))

  # More comprehensive tests are in the core_stub_test.py.
  def test_stub(self):
    db1 = bag()
    x = db1.new(x=1, y=2)
    x_stub = x.stub()
    testing.assert_equal(x_stub.no_bag(), x.no_bag())
    testing.assert_equal(x_stub.get_schema().no_bag(), x.get_schema().no_bag())

  def test_default_value_boxing(self):
    # There are a few existing DataSlice methods with DataItem default values.
    # We chose with_attrs() arbitrarily. If it will be ever refactored, please
    # replace it with another method.
    sig = inspect.signature(data_slice.DataSlice.with_attrs)
    overwrite_schema_param = sig.parameters['overwrite_schema']
    self.assertIsInstance(overwrite_schema_param.default, data_item.DataItem)

  # More comprehensive tests are in the core_with_attrs_test.py.
  def test_with_attrs(self):
    obj1 = bag().obj(x=1, y=2)
    obj2 = obj1.freeze_bag().with_attrs(x=3, z=4)
    testing.assert_equal(obj2.x.no_bag(), ds(3))
    testing.assert_equal(obj2.y.no_bag(), ds(2))
    testing.assert_equal(obj2.z.no_bag(), ds(4))

  # More comprehensive tests are in the core_strict_with_attrs_test.py.
  def test_strict_with_attrs(self):
    e1 = bag().new(x=1, y=2)
    e2 = e1.freeze_bag().strict_with_attrs(x=3, y=4)
    testing.assert_equal(e2.x.no_bag(), ds(3))
    testing.assert_equal(e2.y.no_bag(), ds(4))

    obj1 = bag().obj(x=1, y=2)
    with self.assertRaisesRegex(
        ValueError, 'x must have an Entity schema, actual schema: OBJECT'
    ):
      obj1.strict_with_attrs(x=3, y=4)

  # More comprehensive tests are in the core_with_attr_test.py.
  def test_with_attr(self):
    obj1 = bag().obj(x=1, y=2)
    obj2 = obj1.freeze_bag().with_attr('x', 3).with_attr('z', 4)
    testing.assert_equal(obj2.x.no_bag(), ds(3))
    testing.assert_equal(obj2.y.no_bag(), ds(2))
    testing.assert_equal(obj2.z.no_bag(), ds(4))

  # More comprehensive tests are in the test_core_subslice.py.
  @parameterized.parameters(
      # x.ndim=1
      (ds([1, 2, 3]), [ds(1)], ds(2)),
      (ds([1, 2, 3]), [ds([1, 0])], ds([2, 1])),
      (ds([1, 2, 3]), [slice(0, 2)], ds([1, 2])),
      (ds([1, 2, 3]), [slice(None, 2)], ds([1, 2])),
      (ds([1, 2, 3]), [...], ds([1, 2, 3])),
      # x.ndim=2
      (ds([[1, 2], [3], [4, 5, 6]]), [ds(0), ds(-1)], ds(2)),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          [slice(1, 3), slice(1, -1)],
          ds([[], [5]]),
      ),
      (ds([[1, 2], [3], [4, 5, 6]]), [..., ds(0), ds(-1)], ds(2)),
      (ds([[1, 2], [3], [4, 5, 6]]), [..., slice(1, 3)], ds([[2], [], [5, 6]])),
      # Mixed types
      (
          ds([[1, 'a'], [3], [4, 'b', 6]]),
          [..., ds(1)],
          ds(['a', None, 'b'], schema_constants.OBJECT),
      ),
      # Out-of-bound indices
      (ds([[1, 2], [3], [4, 5, 6]]), [..., ds(2)], ds([None, None, 6])),
  )
  def test_subslice(self, x, slices, expected):
    testing.assert_equal(x.S[*slices], expected)

  # More comprehensive tests are in the core_take_test.py.
  @parameterized.parameters(
      # 1D DataSlice 'x'
      (ds([1, 2, 3, 4]), ds(1), ds(2)),
      (
          ds([1, 2, 3, 4]),
          ds(None, schema_constants.INT32),
          ds(None, schema_constants.INT32),
      ),
      (ds([1, 2, 3, 4]), ds([1, 3]), ds([2, 4])),
      (ds([1, 2, 3, 4]), ds([1, None]), ds([2, None])),
      (ds([1, 2, 3, 4]), ds([[1], [3]]), ds([[2], [4]])),
      (ds([1, 2, 3, 4]), ds([[1], [None]]), ds([[2], [None]])),
      # 2D DataSlice 'x'
      (ds([[1, 2], [3, 4]]), ds(1), ds([2, 4])),
      (ds([[1, 2], [3, 4]]), ds([1, 3]), ds([2, None])),
      (ds([[1, 2], [3, 4]]), ds([[1], [3]]), ds([[2], [None]])),
      # Negative indices
      (ds([[1, 2], [3, 4]]), ds(-1), ds([2, 4])),
      (ds([[1, 2], [3, 4]]), ds([-1, -2]), ds([2, 3])),
  )
  def test_take(self, x, indices, expected):
    testing.assert_equal(x.take(indices), expected)

  @parameterized.parameters(
      (ds([1, 2, 3]), 1),
      (ds([[1, 2], [3]]), 1),
      (ds([[1, 2], [3]]), 2),
      (ds([[[1], [2]], [[3]]]), 1),
      (ds([[[1], [2]], [[3]]]), 2),
      (ds([[[1], [2]], [[3]]]), 3),
  )
  def test_implode_explode(self, x, ndim):
    imploded = x.implode(ndim=ndim)
    self.assertEqual(imploded.get_ndim(), x.get_ndim() - ndim)
    testing.assert_equal(imploded.explode(ndim).no_bag(), x)

  def test_implode_itemid(self):
    itemid = kde.allocation.new_listid_shaped_as(ds([1, 2])).eval()
    x = ds([[1, 2], [3]])
    imploded = x.implode(1, itemid=itemid)
    testing.assert_equal(imploded.get_itemid().no_bag(), itemid)
    testing.assert_equal(imploded[:].no_bag(), x)

  def test_is_empty(self):
    self.assertTrue(ds(None).is_empty())
    self.assertTrue(ds([]).is_empty())
    self.assertTrue(ds([None]).is_empty())
    self.assertTrue(ds([None, None]).is_empty())
    self.assertFalse(ds(1).is_empty())
    self.assertFalse(ds([1]).is_empty())
    self.assertFalse(ds([1, None]).is_empty())


class DataSliceSlicingTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds([1, 2, 3]), 0, ds(1)),
      (ds([1, 2, 3]), 1, ds(2)),
      (ds([1, 2, 3]), 2, ds(3)),
      (ds([1, 2, 3]), 3, ds(None, schema=schema_constants.INT32)),
      (ds([1, 2, 3]), -1, ds(3)),
      (ds([1, 2, 3]), slice(None), ds([1, 2, 3])),
      (ds([1, 2, 3]), slice(None, 2), ds([1, 2])),
      (ds([[1, 2], [3], [4, 5, 6]]), 0, ds([1, 3, 4])),
      (ds([[1, 2], [3], [4, 5, 6]]), 1, ds([2, None, 5])),
      (ds([[1, 2], [3], [4, 5, 6]]), 2, ds([None, None, 6])),
      (ds([[1, 2], [3], [4, 5, 6]]), slice(1, 2), ds([[2], [], [5]])),
      (ds([[1, 2], [3], [4, 5, 6]]), slice(1, None), ds([[2], [], [5, 6]])),
      # multi-dim indexing/slicing
      (ds([[1, 2], [3], [4, 5, 6]]), (0, -1), ds(2)),
      (ds([[1, 2], [3], [4, 5, 6]]), (1, 0), ds(3)),
      (
          ds([[1, 2], [3], [4, 5, 6]]),
          (slice(1, 3), slice(1, -1)),
          ds([[], [5]]),
      ),
      (ds([[1, 2], [3], [4, 5, 6]]), (..., 1), ds([2, None, 5])),
      (ds([[1, 2], [3], [4, 5, 6]]), (0, ...), ds([1, 2])),
  )
  def test_get_item(self, x, i, expected):
    testing.assert_equal(x.S[i], expected)

  def test_len_disabled(self):
    with self.assertRaisesRegex(
        ValueError,
        'length is not well defined for .S; did you mean to use .L?',
    ):
      _ = len(ds([1, 2, 3]).S)  # pyrefly: ignore[bad-argument-type]

  def test_iter_disabled(self):
    with self.assertRaisesRegex(
        ValueError,
        'iteration is not well defined over .S; did you mean to use .L?',
    ):
      for _ in ds([1, 2, 3]).S:
        pass


class DataSliceListSlicingTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds([1, 2, 3]), 0, ds(1)),
      (ds([1, 2, 3]), 1, ds(2)),
      (ds([1, 2, 3]), 2, ds(3)),
      (ds([1, 2, 3]), 3, ds(None, schema=schema_constants.INT32)),
      (ds([1, 2, 3]), -1, ds(3)),
      (ds([1, 2, 3]), slice(None), ds([1, 2, 3])),
      (ds([1, 2, 3]), slice(None, 2), ds([1, 2])),
      (ds([[1, 2], [3], [4, 5, 6]]), 0, ds([1, 2])),
      (ds([[1, 2], [3], [4, 5, 6]]), 1, ds([3])),
      (ds([[1, 2], [3], [4, 5, 6]]), 2, ds([4, 5, 6])),
      (ds([[1, 2], [3], [4, 5, 6]]), slice(1, 2), ds([[3]])),
      (ds([[1, 2], [3], [4, 5, 6]]), slice(1, None), ds([[3], [4, 5, 6]])),
  )
  def test_get_item(self, x, i, expected):
    testing.assert_equal(x.L[i], expected)

  @parameterized.parameters(
      (ds([]), 0),
      (ds([1]), 1),
      (ds([1, 2, 3]), 3),
      (ds([[1, 2], [3], [4, 5, 6]]), 3),
  )
  def test_len(self, x, expected):
    self.assertLen(x.L, expected)

  def test_iter(self):
    d = ds([1, 2, 3])
    for idx, el in enumerate(d.L):
      self.assertEqual(el, d.L[idx])

  def test_is_mutable(self):
    x = ds(None)
    self.assertFalse(x.is_mutable())
    x = x.with_bag(bag())
    self.assertTrue(x.is_mutable())
    x = x.freeze_bag()
    self.assertFalse(x.is_mutable())

  def test_get_sizes(self):
    testing.assert_equal(
        ds([[[1, 2]], [[3, 4], [5]]]).get_sizes(),
        ds([[2], [1, 2], [2, 2, 1]], schema=schema_constants.INT64)
    )

  def test_data_slice_docstrings(self):
    def has_docstring(method):
      return (hasattr(method, 'getdoc') and method.getdoc()) or method.__doc__

    public_methods = [
        m for m in dir(data_slice.DataSlice) if not m.startswith('_')
    ]
    for method_name in public_methods:
      method = getattr(data_slice.DataSlice, method_name)
      self.assertTrue(
          has_docstring(method),
          f'DataSlice method {method_name} has no docstring.',
      )

  def test_docstring_from_non_existent_operator_fails(self):
    @data_slice.add_method(
        data_slice.DataSlice, 'test_method', docstring_from='non-existent'
    )
    def _test_method(self):
      return self.internal_as_py()

    with self.assertRaisesRegex(LookupError, 'unknown operator: non-existent'):
      _ = _test_method.getdoc()

    # Remove the method to avoid breaking docstring tests.
    delattr(data_slice.DataSlice, 'test_method')

  def test_display(self):
    with mock.patch('sys.stdout', new_callable=io.StringIO) as mock_stdout:
      with mock.patch.object(warnings, 'warn') as mock_warn:
        _ = ds([1, 2]).display()
        self.assertEqual(mock_stdout.getvalue(), repr(ds([1, 2])) + '\n')
        _ = ds([1, 2]).display()  # to make sure importing is tried only once.
        mock_warn.assert_called_once()

  @parameterized.named_parameters(
      (
          'simple',
          lambda x: x,
          [
              'DataItem(Functor'
              ' DataSliceListSlicingTest.<lambda>[x](returns=I.x), schema:'
              ' OBJECT'
          ],
      ),
      (
          'two_args',
          lambda x, y: x + y,
          [
              (
                  'DataItem(Functor DataSliceListSlicingTest.<lambda>[x,'
                  f' y](returns=(I.x + I.y){SRC_PIN}), schema: OBJECT'
              ),
          ],
      ),
      (
          'named_functor',
          add_one,
          [
              'DataItem(Functor add_one[x](',
              "__doc__='Adds one to the input.'",
              f'returns=(I.x + DataItem(1, schema: INT32)){SRC_PIN}',
          ],
      ),
      (
          'default_argument',
          lambda x=1: x,
          [
              (
                  'DataItem(Functor'
                  ' DataSliceListSlicingTest.<lambda>[x=1](returns=I.x)'
              ),
          ],
      ),
      (
          'varargs',
          lambda x, *unused_args: x,
          [
              (
                  'DataItem(Functor DataSliceListSlicingTest.<lambda>[x,'
                  ' *unused_args](returns=I.x), schema: OBJECT'
              ),
          ],
      ),
      (
          'varkwargs',
          lambda x, **unused_kwargs: x,
          [
              (
                  'DataItem(Functor DataSliceListSlicingTest.<lambda>[x,'
                  ' **unused_kwargs](returns=I.x), schema: OBJECT'
              ),
          ],
      ),
      (
          'nested_sub_functor',
          lambda x: add_one(x),  # pylint: disable=unnecessary-lambda
          [
              'DataItem(Functor DataSliceListSlicingTest.<lambda>[x](',
              f'_add_one_result=V.add_one(I.x){SRC_PIN}',
              'add_one=Functor add_one[x](',
              "__doc__='Adds one to the input.'",
              f'returns=(I.x + DataItem(1, schema: INT32)){SRC_PIN}',
              'returns=V._add_one_result',
          ],
      ),
      (
          'default_argument_sub_functor',
          lambda x, f=(lambda x: x + 1): f(x),
          [
              (
                  'DataItem(Functor DataSliceListSlicingTest.<lambda>[x,'
                  ' f=Functor DataSliceListSlicingTest.<lambda>[x](returns=(I.x'
                  f' + DataItem(1, schema: INT32)){SRC_PIN}'
              ),
              'returns=I.f(I.x)'
          ],
      ),
      (
          'positional_only_arg',
          lambda x, /, y: x + y,
          [
              (
                  'DataItem(Functor DataSliceListSlicingTest.<lambda>[x, /,'
                  f' y](returns=(I.x + I.y){SRC_PIN}), schema: OBJECT'
              ),
          ],
      ),
      (
          'all_positional_only_args',
          lambda x, y, /: x + y,
          [
              (
                  'DataItem(Functor DataSliceListSlicingTest.<lambda>[x, y,'
                  f' /](returns=(I.x + I.y){SRC_PIN}), schema: OBJECT'
              ),
          ],
      ),
      (
          'keyword_only_arg',
          lambda x, *, y: x + y,
          [
              (
                  'DataItem(Functor DataSliceListSlicingTest.<lambda>[x, *,'
                  f' y](returns=(I.x + I.y){SRC_PIN}), schema: OBJECT'
              ),
          ],
      ),
      (
          'all_keyword_only_args',
          lambda *, x, y: x + y,
          [
              (
                  'DataItem(Functor DataSliceListSlicingTest.<lambda>[*, x,'
                  f' y](returns=(I.x + I.y){SRC_PIN}), schema: OBJECT'
              ),
          ],
      ),
  )
  def test_functor_repr(self, f, expected_substrings):
    # We only test that certain substrings are present in the repr. This way, we
    # avoid testing non-deterministic things like attribute order or bag ids.
    fn = functor_factories.fn(f)
    fn_repr = repr(fn)
    for line in expected_substrings:
      self.assertIn(line, fn_repr)

  def test_repr_error(self):
    self.assertRegex(
        repr(bag().obj(x=1).with_bag(bag())),
        r'(?s)^DataItem\(INVALID_ARGUMENT: object schema is missing .*, schema:'
        r' OBJECT, bag_id: .*\)$',
    )

  def test_colab_has_safe_repr(self):
    self.assertFalse(hasattr(data_slice.DataSlice, '_COLAB_HAS_SAFE_REPR'))

  def test_dict_get_expr_item(self):
    db = bag()
    d = db.dict({1: 2}).freeze_bag()

    testing.assert_equal(
        d[I.x].eval(x=1),  # pyrefly: ignore[bad-index]
        ds(2).with_bag(d.get_bag())
    )

  def test_list_get_expr_item(self):
    db = bag()
    l = db.list([1, 2]).freeze_bag()

    testing.assert_equal(
        l[I.x].eval(x=1),  # pyrefly: ignore[bad-index]
        ds(2).with_bag(l.get_bag())
    )


if __name__ == '__main__':
  absltest.main()
