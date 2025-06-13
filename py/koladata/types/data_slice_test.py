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

import gc
import inspect
import io
import re
import sys
from unittest import mock
import warnings

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd as user_facing_kd

from koladata.expr import input_container
from koladata.functions import functions as fns
from koladata.functions.tests import test_pb2
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.operators import kde_operators
from koladata.testing import signature_test_utils
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import dict_item as _
from koladata.types import jagged_shape
from koladata.types import list_item as _
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals

INT64 = schema_constants.INT64

present = mask_constants.present
missing = mask_constants.missing


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
      data_slice.add_method(data_slice.DataSlice, b'foo')(lambda x: x)  # pytype: disable=wrong-arg-types

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


class DataSliceTest(parameterized.TestCase):

  def test_ref_count(self):
    gc.collect()
    diff_count = 10
    base_count = sys.getrefcount(data_slice.DataSlice)
    slices = []
    for _ in range(diff_count):
      slices.append(ds([1]))  # Adding DataSlice(s)

    self.assertEqual(
        sys.getrefcount(data_slice.DataSlice), base_count + diff_count
    )

    # NOTE: ds() invokes `PyDataSlice_Type()` C Python function multiple times
    # and this verifies there are no leaking references.
    del slices
    gc.collect()
    self.assertEqual(sys.getrefcount(data_slice.DataSlice), base_count)

    # NOTE: with_schema() invokes `PyDataSlice_Type()` C Python function and
    # this verifies there are no leaking references.
    ds(1).with_schema(schema_constants.OBJECT)
    self.assertEqual(sys.getrefcount(data_slice.DataSlice), base_count)

    items = []
    for _ in range(diff_count):
      items.append(ds(1))  # Adding DataItem(s)
    self.assertEqual(sys.getrefcount(data_slice.DataSlice), base_count)

    res = ds([1]) + ds([2])
    self.assertEqual(sys.getrefcount(data_slice.DataSlice), base_count + 1)
    del res

    _ = ds([1, 2, 3]).S[0]
    self.assertEqual(sys.getrefcount(data_slice.DataSlice), base_count)

  def test_qvalue(self):
    self.assertTrue(issubclass(data_slice.DataSlice, arolla.QValue))
    x = ds([1, 2, 3])
    self.assertIsInstance(x, arolla.QValue)

  def test_fingerprint(self):
    x1 = ds(arolla.dense_array_int64([None, None]))
    x2 = ds([None, None], schema_constants.INT64)
    self.assertEqual(x1.fingerprint, x2.fingerprint)
    x3 = x2.with_schema(schema_constants.INT32)
    self.assertNotEqual(x1.fingerprint, x3.fingerprint)
    x4 = x2.with_schema(schema_constants.STRING)
    self.assertNotEqual(x1.fingerprint, x4.fingerprint)

    db = bag()
    self.assertNotEqual(x1.with_bag(db).fingerprint, x2.fingerprint)
    self.assertEqual(x1.with_bag(db).fingerprint, x2.with_bag(db).fingerprint)

    shape = jagged_shape.create_shape([2], [1, 1])
    self.assertNotEqual(x1.reshape(shape).fingerprint, x2.fingerprint)
    self.assertEqual(
        x1.reshape(shape).fingerprint, x2.reshape(shape).fingerprint
    )

    self.assertEqual(ds([42, 'abc']).fingerprint, ds([42, 'abc']).fingerprint)
    self.assertNotEqual(
        ds([42, 'abc']).fingerprint,
        ds([42, b'abc']).fingerprint,
    )

    self.assertNotEqual(ds([1]).fingerprint, ds(1).fingerprint)

  def test_unspecified(self):
    testing.assert_equal(data_slice.unspecified(), data_slice.unspecified())
    with self.assertRaises(AssertionError):
      testing.assert_equal(ds(42), data_slice.unspecified())
    with self.assertRaises(AssertionError):
      testing.assert_equal(
          data_slice.unspecified().with_bag(bag()), data_slice.unspecified()
      )

  @parameterized.named_parameters(
      (
          'data_item',
          ds(12),
          'DataItem(12, schema: INT32)',
      ),
      (
          'int32',
          ds([1, 2]),
          'DataSlice([1, 2], schema: INT32, shape: JaggedShape(2))',
      ),
      (
          'int64',
          ds([1, 2], schema_constants.INT64),
          'DataSlice([1, 2], schema: INT64, shape: JaggedShape(2))',
      ),
      (
          'int64_as_object',
          bag().obj(ds([1, 2], schema_constants.INT64)).no_bag(),
          (
              'DataSlice([int64{1}, int64{2}], schema: OBJECT, shape:'
              ' JaggedShape(2))'
          ),
      ),
      (
          'float32',
          ds([1.0, 1.5]),
          'DataSlice([1.0, 1.5], schema: FLOAT32, shape: JaggedShape(2))',
      ),
      (
          'float64',
          ds([1.0, 1.5], schema_constants.FLOAT64),
          'DataSlice([1.0, 1.5], schema: FLOAT64, shape: JaggedShape(2))',
      ),
      (
          'float32_as_object',
          bag().obj(ds([1.0, 1.5], schema_constants.FLOAT32)).no_bag(),
          'DataSlice([1.0, 1.5], schema: OBJECT, shape: JaggedShape(2))',
      ),
      (
          'float64_as_object',
          bag().obj(ds([1.0, 1.5], schema_constants.FLOAT64)).no_bag(),
          (
              'DataSlice([float64{1.0}, float64{1.5}], schema: OBJECT, shape:'
              ' JaggedShape(2))'
          ),
      ),
      (
          'boolean',
          ds([True, False]),
          'DataSlice([True, False], schema: BOOLEAN, shape: JaggedShape(2))',
      ),
      (
          'missing mask DataItem',
          mask_constants.missing,
          'DataItem(missing, schema: MASK)',
      ),
      (
          'present mask DataItem',
          mask_constants.present,
          'DataItem(present, schema: MASK)',
      ),
      (
          'mask DataSlice',
          ds([mask_constants.present, mask_constants.missing]),
          'DataSlice([present, missing], schema: MASK, shape: JaggedShape(2))',
      ),
      (
          'mask DataSlice with OBJECT schema',
          ds([mask_constants.present, mask_constants.missing]).with_schema(
              schema_constants.OBJECT
          ),
          'DataSlice([present, None], schema: OBJECT, shape: JaggedShape(2))',
      ),
      (
          'text',
          ds('a'),
          "DataItem('a', schema: STRING)",
      ),
      (
          'text list',
          ds(['a', 'b']),
          "DataSlice(['a', 'b'], schema: STRING, shape: JaggedShape(2))",
      ),
      (
          'bytes',
          ds([b'a', b'b']),
          "DataSlice([b'a', b'b'], schema: BYTES, shape: JaggedShape(2))",
      ),
      (
          'int32_with_object',
          ds([1, 2]).with_schema(schema_constants.OBJECT),
          'DataSlice([1, 2], schema: OBJECT, shape: JaggedShape(2))',
      ),
      (
          'mixed_data',
          ds([1, 'abc', True, 1.0, arolla.int64(1), arolla.float64(1.0)]),
          (
              "DataSlice([1, 'abc', True, 1.0, int64{1}, float64{1.0}], schema:"
              ' OBJECT, shape: JaggedShape(6))'
          ),
      ),
      (
          'int32_with_none',
          ds([1, None]),
          'DataSlice([1, None], schema: INT32, shape: JaggedShape(2))',
      ),
      (
          'empty',
          ds([], schema_constants.INT64),
          'DataSlice([], schema: INT64, shape: JaggedShape(0))',
      ),
      (
          'empty_int64_internal',
          ds(arolla.dense_array_int64([])),
          'DataSlice([], schema: INT64, shape: JaggedShape(0))',
      ),
      (
          'multidim',
          ds([[[1], [2]], [[3], [4], [5]]]),
          (
              'DataSlice([[[1], [2]], [[3], [4], [5]]], schema: INT32, '
              'shape: JaggedShape(2, [2, 3], 1))'
          ),
      ),
  )
  def test_debug_repr(self, x, expected_repr):
    self.assertEqual(x._debug_repr(), expected_repr)

  def test_debug_large_repr(self):
    db = bag()
    x = db.new(x=ds([[x for x in range(5)] for y in range(4)]))
    self.assertRegex(
        x._debug_repr(),
        r'''DataSlice\(\[
  \[
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
  \],
  \[
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
  \],
  \[
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
  \],
  \[
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
    Entity:\$[0-9a-zA-Z]{22},
  \],
\], schema: ENTITY\(x=INT32\), shape: JaggedShape\(4, 5\), bag_id: \$[0-9a-f]{4}\)''',
    )
    o = db.obj(x=ds([[x for x in range(5)] for y in range(4)]))
    self.assertRegex(
        o._debug_repr(),
        r'''DataSlice\(\[
  \[
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
  \],
  \[
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
  \],
  \[
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
  \],
  \[
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
    Obj:\$[0-9a-zA-Z]{22},
  \],
\], schema: OBJECT, shape: JaggedShape\(4, 5\), bag_id: \$[0-9a-f]{4}\)''',
    )

  def test_debug_repr_with_bag(self):
    db = bag()
    x = ds([1, 2]).with_bag(db)
    bag_id = '$' + str(db.fingerprint)[-4:]
    self.assertEqual(
        x._debug_repr(),
        'DataSlice([1, 2], schema: INT32, shape: JaggedShape(2), '
        f'bag_id: {bag_id})',
    )

  @parameterized.named_parameters(
      (
          'data_item',
          ds(12),
          'DataItem(12, schema: INT32)',
          '12',
      ),
      (
          'int32',
          ds([1, 2]),
          'DataSlice([1, 2], schema: INT32, ndims: 1, size: 2)',
          '[1, 2]',
      ),
      (
          'int64',
          ds([1, 2], schema_constants.INT64),
          'DataSlice([1, 2], schema: INT64, ndims: 1, size: 2)',
          '[1, 2]',
      ),
      (
          'int64_as_object',
          bag().obj(ds([1, 2], schema_constants.INT64)).no_bag(),
          'DataSlice([int64{1}, int64{2}], schema: OBJECT, ndims: 1, size: 2)',
          '[int64{1}, int64{2}]',
      ),
      (
          'float32',
          ds([1.0, 1.5]),
          'DataSlice([1.0, 1.5], schema: FLOAT32, ndims: 1, size: 2)',
          '[1.0, 1.5]',
      ),
      (
          'float64',
          ds([1.0, 1.5], schema_constants.FLOAT64),
          'DataSlice([1.0, 1.5], schema: FLOAT64, ndims: 1, size: 2)',
          '[1.0, 1.5]',
      ),
      (
          'float32_as_object',
          bag().obj(ds([1.0, 1.5], schema_constants.FLOAT32)).no_bag(),
          'DataSlice([1.0, 1.5], schema: OBJECT, ndims: 1, size: 2)',
          '[1.0, 1.5]',
      ),
      (
          'float64_as_object',
          bag().obj(ds([1.0, 1.5], schema_constants.FLOAT64)).no_bag(),
          (
              'DataSlice([float64{1.0}, float64{1.5}], schema: OBJECT, ndims:'
              ' 1, size: 2)'
          ),
          '[float64{1.0}, float64{1.5}]',
      ),
      (
          'boolean',
          ds([True, False]),
          'DataSlice([True, False], schema: BOOLEAN, ndims: 1, size: 2)',
          '[True, False]',
      ),
      (
          'missing mask DataItem',
          mask_constants.missing,
          'DataItem(missing, schema: MASK)',
          'missing',
      ),
      (
          'present mask DataItem',
          mask_constants.present,
          'DataItem(present, schema: MASK)',
          'present',
      ),
      (
          'mask DataSlice',
          ds([mask_constants.present, mask_constants.missing]),
          'DataSlice([present, missing], schema: MASK, ndims: 1, size: 2)',
          '[present, missing]',
      ),
      (
          'mask DataSlice with OBJECT schema',
          ds([mask_constants.present, mask_constants.missing]).with_schema(
              schema_constants.OBJECT
          ),
          'DataSlice([present, None], schema: OBJECT, ndims: 1, size: 2)',
          '[present, None]',
      ),
      (
          'text',
          ds('a'),
          "DataItem('a', schema: STRING)",
          'a',
      ),
      (
          'text list',
          ds(['a', 'b']),
          "DataSlice(['a', 'b'], schema: STRING, ndims: 1, size: 2)",
          "['a', 'b']",
      ),
      (
          'bytes',
          ds([b'a', b'b']),
          "DataSlice([b'a', b'b'], schema: BYTES, ndims: 1, size: 2)",
          "[b'a', b'b']",
      ),
      (
          'int32_with_object',
          ds([1, 2]).with_schema(schema_constants.OBJECT),
          'DataSlice([1, 2], schema: OBJECT, ndims: 1, size: 2)',
          '[1, 2]',
      ),
      (
          'mixed_data',
          ds([1, 'abc', True, 1.0, arolla.int64(1), arolla.float64(1.0)]),
          (
              "DataSlice([1, 'abc', True, 1.0, int64{1}, float64{1.0}], schema:"
              ' OBJECT, ndims: 1, size: 6)'
          ),
          "[1, 'abc', True, 1.0, int64{1}, float64{1.0}]",
      ),
      (
          'int32_with_none',
          ds([1, None]),
          'DataSlice([1, None], schema: INT32, ndims: 1, size: 2)',
          '[1, None]',
      ),
      (
          'empty',
          ds([], schema_constants.INT64),
          'DataSlice([], schema: INT64, ndims: 1, size: 0)',
          '[]',
      ),
      (
          'empty_int64_internal',
          ds(arolla.dense_array_int64([])),
          'DataSlice([], schema: INT64, ndims: 1, size: 0)',
          '[]',
      ),
      (
          'multidim',
          ds([[[1], [2]], [[3], [4], [5]]]),
          (
              'DataSlice([[[1], [2]], [[3], [4], [5]]], schema: INT32, ndims:'
              ' 3, size: 5)'
          ),
          '[[[1], [2]], [[3], [4], [5]]]',
      ),
      (
          'large_string',
          ds(['a' * 1000]),
          'DataSlice([\n'
          f"  '{'a' * 128}'...'{'a' * 128}',\n"
          '], schema: STRING, ndims: 1, size: 1)',
          f"[\n  '{'a' * 1000}',\n]",  # No truncation.
      ),
      (
          'large_bytestring',
          ds([b'a' * 1000]),
          'DataSlice([\n'
          f"  b'{'a' * 128}'...'{'a' * 128}',\n"
          '], schema: BYTES, ndims: 1, size: 1)',
          f"[\n  b'{'a' * 1000}',\n]",  # No truncation.
      ),
  )
  def test_repr_and_str_no_bag(self, x, expected_repr, expected_str):
    self.assertEqual(repr(x), expected_repr)
    self.assertEqual(str(x), expected_str)

  def test_repr_entity_and_obj(self):
    db = bag()
    x = db.new(x=ds([1, 2, 3]))
    self.assertEqual(
        repr(x),
        (
            'DataSlice([Entity(x=1), Entity(x=2), Entity(x=3)], schema:'
            ' ENTITY(x=INT32), ndims: 1, size: 3)'
        ),
    )
    self.assertEqual(
        str(x),
        '[Entity(x=1), Entity(x=2), Entity(x=3)]',
    )

    y = db.new(x=ds([1, 2, 3]), schema='foo')
    self.assertEqual(
        repr(y),
        (
            'DataSlice([Entity(x=1), Entity(x=2), Entity(x=3)], schema:'
            ' foo(x=INT32), ndims: 1, size: 3)'
        ),
    )
    self.assertEqual(
        str(y),
        '[Entity(x=1), Entity(x=2), Entity(x=3)]',
    )

    z = db.obj(x=ds([1, 2, 3]))
    self.assertEqual(
        repr(z),
        'DataSlice([Obj(x=1), Obj(x=2), Obj(x=3)], schema: OBJECT, ndims: 1,'
        ' size: 3)',
    )
    self.assertEqual(
        str(z),
        '[Obj(x=1), Obj(x=2), Obj(x=3)]',
    )

  def test_repr_large_entity_and_obj(self):
    db = bag()
    x = db.new(x=ds([[x for x in range(5)] for y in range(4)]))
    self.assertEqual(
        repr(x),
        'DataSlice(attrs: [x], schema: ENTITY(x=INT32), ndims: 2, size: 20)',
    )

    y = db.obj(x=ds([[x for x in range(5)] for y in range(4)]))
    self.assertEqual(
        repr(y),
        'DataSlice(attrs: [x], schema: OBJECT, ndims: 2, size: 20)',
    )

  # Special case for itemid, since it includes a non-deterministic id.
  def test_str_repr_itemid_works(self):
    x = bag().list()
    self.assertRegex(str(x.get_itemid()), r'.*:.*')
    self.assertRegex(repr(x.get_itemid()), r'DataItem(.*:.*, schema: ITEMID)')

  # NOTE: DataSlice has custom __eq__ which works pointwise and returns another
  # DataSlice. So multi-dim DataSlices cannot be used as Python dict keys.
  def test_non_hashable(self):
    with self.assertRaisesRegex(TypeError, 'unhashable type'):
      hash(ds([1, 2, 3]))

  def test_get_bag(self):
    x = ds([1, 2, 3])
    self.assertIsNone(x.get_bag())

    db = bag()
    x = x.with_bag(db)
    self.assertIsNotNone(x.get_bag())

    self.assertIsNone(x.with_bag(None).get_bag())
    self.assertIsNone(x.no_bag().get_bag())

    x = db.new_shaped(jagged_shape.create_shape([1]))
    self.assertIsNotNone(x.get_bag())
    # NOTE: At the moment x.get_bag() is not db. If this is needed, we could
    # store the db PyObject* reference in PyDataSlice object. The underlying
    # DataBag that PyObject points to, is the same.
    self.assertIsNot(x.get_bag(), db)

    with self.assertRaisesRegex(
        TypeError, 'expecting db to be a DataBag, got list'
    ):
      x.with_bag([1, 2, 3])

    with self.assertRaisesRegex(
        TypeError, 'expecting db to be a DataBag, got DenseArray'
    ):
      x.with_bag(arolla.dense_array([1, 2, 3]))

  def test_get_attr_on_none(self):
    x = ds([None]).with_bag(bag())
    testing.assert_equal(x.x, ds([None]).with_bag(x.get_bag()))
    x = ds(None).with_bag(bag())
    testing.assert_equal(x.x, ds(None).with_bag(x.get_bag()))

  def test_get_attr_with_default_none(self):
    x = ds([None]).with_bag(bag())
    default = ds(42).with_bag(bag())
    testing.assert_equal(
        x.get_attr('x', default).no_bag(), ds([None], schema_constants.INT32)
    )

  def test_get_attr_with_default_extraction(self):
    # Regression test for b/408434629.
    db = bag()
    entities = db.new(x=ds([db.list([1, 2]), db.list([3, 4])]))
    updated_lists = (
        entities.x & ds([None, arolla.present()])
    ).with_list_append_update(8)
    filtered_entities = entities.with_attr(
        'x', entities.x & ds([arolla.present(), None])
    )
    result = filtered_entities.get_attr('x', updated_lists)
    testing.assert_equal(result[:].no_bag(), ds([[1, 2], [3, 4, 8]]))

  def test_get_keys_on_none(self):
    x = ds([None]).with_bag(bag())
    testing.assert_equal(
        x.get_keys(), ds([[]], schema_constants.NONE).with_bag(x.get_bag())
    )
    x = ds(None).with_bag(bag())
    testing.assert_equal(
        x.get_keys(), ds([], schema_constants.NONE).with_bag(x.get_bag())
    )

  # More comprehensive tests are in the core_get_values_test.py.
  def test_get_values(self):
    db = bag()
    d1 = db.dict({1: 2, 3: 4})
    d2 = db.dict({3: 5})
    d = ds([d1, None, d2])

    testing.assert_unordered_equal(
        d.get_values(),
        ds([[2, 4], [], [5]]).with_bag(db),
    )
    testing.assert_equal(d.get_values(), d[d.get_keys()])

    testing.assert_equal(
        d.get_values(ds([[3, 1], [1], [3]])),
        ds([[4, 2], [None], [5]]).with_bag(db),
    )

  def test_get_values_on_none(self):
    x = ds([None]).with_bag(bag())
    testing.assert_equal(
        x.get_values(), ds([[]], schema_constants.NONE).with_bag(x.get_bag())
    )
    x = ds(None).with_bag(bag())
    testing.assert_equal(
        x.get_values(), ds([], schema_constants.NONE).with_bag(x.get_bag())
    )

  def test_fork_bag(self):
    x = ds([1, 2, 3])

    with self.assertRaisesRegex(
        ValueError, 'fork_bag expects the DataSlice to have a DataBag attached'
    ):
      x.fork_bag()

    db = bag()
    x = x.with_bag(db)

    x1 = x.fork_bag()
    self.assertIsNot(x, x1)
    self.assertIsNot(x1.get_bag(), x.get_bag())
    self.assertIsNot(x1.get_bag(), db)
    self.assertTrue(x1.get_bag().is_mutable())

  def test_freeze_bag(self):
    x = ds([1, 2, 3])
    testing.assert_equal(x, x.freeze_bag())

    db = bag()
    x = x.with_bag(db)
    x1 = x.freeze_bag()
    self.assertIsNot(x, x1)
    self.assertIsNot(x1.get_bag(), x.get_bag())
    self.assertIsNot(x1.get_bag(), x1.get_bag())
    self.assertFalse(x1.get_bag().is_mutable())

  def test_freeze_on_db_with_fallbacks(self):
    fallback = bag()
    x = ds([1, 2, 3]).enriched(fallback)
    frozen_x = x.freeze_bag()

    self.assertTrue(fallback.is_mutable())
    self.assertFalse(x.get_bag().is_mutable())
    self.assertFalse(frozen_x.get_bag().is_mutable())
    # frozen_x should have a forked databag, because while x's bag was
    # immutable, the fallback was not.
    self.assertNotEqual(x.fingerprint, frozen_x.fingerprint)

  def test_with_merged_bag(self):
    db1 = bag()
    x = db1.new(a=1)
    db2 = bag()
    y = x.with_bag(db2)
    y.set_attr('a', 2)
    y.set_attr('b', 2)
    z = x.enriched(db2)

    new_z = z.with_merged_bag()
    self.assertIsNot(new_z.get_bag(), db1)
    self.assertIsNot(new_z.get_bag(), db2)
    self.assertIsNot(new_z.get_bag(), z.get_bag())
    self.assertFalse(new_z.get_bag().is_mutable())
    testing.assert_equal(new_z.a.no_bag(), ds(1))
    testing.assert_equal(new_z.b.no_bag(), ds(2))

  def test_enriched(self):
    db1 = bag()
    schema = db1.new_schema(a=schema_constants.INT32)
    x = db1.new(a=1, schema=schema)

    db2 = bag()
    x = x.with_bag(db2)

    x = x.enriched(db1)
    self.assertNotEqual(x.get_bag().fingerprint, db1.fingerprint)
    self.assertNotEqual(x.get_bag().fingerprint, db2.fingerprint)
    testing.assert_equivalent(x.a.no_bag(), ds(1).no_bag())

    x = x.with_bag(db2).enriched(db1, db1)
    self.assertNotEqual(x.get_bag().fingerprint, db1.fingerprint)
    self.assertNotEqual(x.get_bag().fingerprint, db2.fingerprint)
    testing.assert_equivalent(x.a.no_bag(), ds(1).no_bag())

  def test_updated(self):
    schema = kde.schema.new_schema(a=schema_constants.INT32).eval()

    db1 = bag()
    db1.merge_inplace(schema.get_bag())
    x = db1.new(a=1, schema=schema)

    db2 = bag()
    db2.merge_inplace(schema.get_bag())
    x.with_bag(db2).a = 2

    x = x.updated(db2)
    self.assertNotEqual(x.get_bag().fingerprint, db1.fingerprint)
    self.assertNotEqual(x.get_bag().fingerprint, db2.fingerprint)
    testing.assert_equivalent(x.a.no_bag(), ds(2).no_bag())

    x = x.with_bag(db1).updated(db2, db2)
    self.assertNotEqual(x.get_bag().fingerprint, db1.fingerprint)
    self.assertNotEqual(x.get_bag().fingerprint, db2.fingerprint)
    testing.assert_equivalent(x.a.no_bag(), ds(2).no_bag())

  def test_ref(self):
    x = ds([1, 2, 3])

    with self.assertRaisesRegex(
        ValueError, 'unsupported schema: INT32'
    ):
      x.ref()

    db = bag()
    x = db.obj(x=x)
    testing.assert_equal(x.ref(), x.with_bag(None))

  def test_reserved_ipython_method_names(self):
    db = bag()
    x = db.new(getdoc=1, trait_names=2, normal=3)
    self.assertEqual(x.normal, 3)
    with self.assertRaises(AttributeError):
      _ = x.getdoc
    with self.assertRaises(AttributeError):
      _ = x.trait_names

  def test_dir(self):
    db = bag()
    # No attrs (primitive with no bag).
    self.assertEqual(
        dir(ds([1, 2, 3])),
        sorted(dir(data_slice.DataSlice)),
    )
    # With attrs.
    self.assertEqual(
        dir(db.new(a=ds([1]), b=ds(['abc']))),
        sorted({'a', 'b'} | set(dir(data_slice.DataSlice))),
    )
    # With attrs DataItem (more methods than DataSlice)
    self.assertEqual(
        dir(db.new(a=1, b='abc')),
        sorted({'a', 'b'} | set(dir(data_item.DataItem))),
    )
    # No available attrs (no db).
    self.assertEqual(
        dir(db.new(a=ds([1]), b=ds(['abc'])).no_bag()),
        sorted(dir(data_slice.DataSlice)),
    )
    # No available attrs (no __schema__ attr on object).
    self.assertEqual(
        dir(db.new(a=ds([1])).with_schema(schema_constants.OBJECT)),
        sorted(dir(data_slice.DataSlice)),
    )
    # Intersection of attrs.
    self.assertEqual(
        dir(ds([db.obj(a=1, b='abc'), db.obj(a='def', c=123)])),
        sorted({'a'} | set(dir(data_slice.DataSlice))),
    )

  def test_dir_reserved_names(self):
    db = bag()
    x = db.new(_x=1, getdoc=2, reshape=3, y=4)
    setattr(x, '', 5)
    # Reserved names and names starting with `_` are _not_ included. Reshape is
    # included in dir(DataItem) since it's a method there, and `''` is included
    # since it's a valid attribute name even though it's only accessible via
    # getattr(x, '') and not via x.<smth>.
    self.assertEqual(dir(x), sorted({'y', ''} | set(dir(data_item.DataItem))))

  def test_get_attr_names_entity(self):
    db = bag()
    fb = bag()
    x = db.new(a=1, b='abc')
    self.assertEqual(x.get_attr_names(intersection=True), ['a', 'b'])
    self.assertEqual(ds([x]).get_attr_names(intersection=True), ['a', 'b'])
    x.with_bag(fb).set_attr('c', 42)
    self.assertEqual(
        x.with_bag(db).enriched(fb).get_attr_names(intersection=True),
        ['a', 'b', 'c'],
    )
    self.assertEqual(
        ds([x]).with_bag(db).enriched(fb).get_attr_names(intersection=True),
        ['a', 'b', 'c'],
    )
    with self.assertRaisesRegex(
        ValueError, 'cannot get available attributes without a DataBag'
    ):
      x.no_bag().get_attr_names(intersection=True)

  def test_get_attr_names_object(self):
    db = bag()
    x = db.obj(a=1, b='abc')
    self.assertEqual(x.get_attr_names(intersection=True), ['a', 'b'])
    self.assertEqual(ds([x]).get_attr_names(intersection=True), ['a', 'b'])
    # Returns either the intersection of attributes...
    self.assertEqual(
        ds([x, db.obj(a='def', c=123)]).get_attr_names(intersection=True), ['a']
    )
    # ... or the union of attributes.
    self.assertEqual(
        ds([x, db.obj(a='def', c=123)]).get_attr_names(intersection=False),
        ['a', 'b', 'c'],
    )
    with self.assertRaisesRegex(
        ValueError, 'cannot get available attributes without a DataBag'
    ):
      x.no_bag().get_attr_names(intersection=True)
    with self.assertRaisesRegex(
        ValueError, 'object schema is missing for the DataItem'
    ):
      db.new(a=1, b='abc').with_schema(schema_constants.OBJECT).get_attr_names(
          intersection=True
      )

  def test_get_attr_names_primitive(self):
    x = ds([1, 2, 3]).with_bag(bag())
    self.assertEqual(x.get_attr_names(intersection=True), [])

  def test_get_attr_names_schema(self):
    db = bag()
    self.assertEqual(
        schema_constants.INT32.with_bag(db).get_attr_names(intersection=True),
        [],
    )
    schema1 = db.new_schema(
        a=schema_constants.INT32, b=schema_constants.FLOAT32
    )
    schema2 = db.new_schema(
        a=schema_constants.INT32, c=schema_constants.FLOAT32
    )
    schemas = ds([schema1, schema2])
    # Returns either the intersection of attributes...
    self.assertEqual(schemas.get_attr_names(intersection=True), ['a'])
    # ... or the union of attributes.
    self.assertEqual(
        schemas.get_attr_names(intersection=False), ['a', 'b', 'c']
    )

  def test_get_attr_names_reserved_names(self):
    db = bag()
    x = db.new(_x=1, getdoc=2, reshape=3)
    # Reserved names and names starting with `_` _are_ included.
    self.assertEqual(
        x.get_attr_names(intersection=True), ['_x', 'getdoc', 'reshape']
    )

  def test_get_attr_names_call_errors(self):
    db = bag()
    x = db.new(x=1)
    with self.assertRaisesRegex(
        TypeError,
        re.escape(
            'get_attr_names() missing 1 required keyword-only argument:'
            " 'intersection'"
        ),
    ):
      x.get_attr_names()
    with self.assertRaisesRegex(
        TypeError,
        re.escape(
            'get_attr_names() expected bool for `intersection`, got: str'
        ),
    ):
      x.get_attr_names(intersection='foo')

  def test_internal_as_py(self):
    x = ds([[1, 2], [3], [4, 5]])
    self.assertEqual(x.internal_as_py(), [[1, 2], [3], [4, 5]])

  def test_to_proto_minimal(self):
    # NOTE: more tests for to_proto in
    # py/koladata/functions/tests/to_proto_test.py

    message = fns.new()._to_proto(test_pb2.EmptyMessage)  # pylint: disable=protected-access
    self.assertIsInstance(message, test_pb2.EmptyMessage)
    self.assertEqual(message, test_pb2.EmptyMessage())

    messages = ds([fns.new()])._to_proto(test_pb2.EmptyMessage)  # pylint: disable=protected-access
    self.assertIsInstance(messages, list)
    self.assertLen(messages, 1)
    self.assertIsInstance(messages[0], test_pb2.EmptyMessage)
    self.assertEqual(messages, [test_pb2.EmptyMessage()])

  def test_to_proto_errors(self):
    with self.assertRaisesRegex(
        ValueError, 'to_proto accepts exactly 1 arguments, got 0'
    ):
      _ = ds([])._to_proto()  # pylint: disable=protected-access

    with self.assertRaisesRegex(
        TypeError,
        re.escape(
            'to_proto expects message_class to be a proto class, got NoneType'
        ),
    ):
      _ = ds([])._to_proto(None)  # pylint: disable=protected-access

    with self.assertRaisesRegex(
        ValueError,
        re.escape('message cast from python to C++ failed, got type int'),
    ):
      _ = ds([])._to_proto(int)  # pylint: disable=protected-access

    with self.assertRaisesRegex(
        ValueError,
        re.escape('to_proto expects a DataSlice with ndim 0 or 1, got ndim=2'),
    ):
      _ = ds([[]])._to_proto(test_pb2.EmptyMessage)  # pylint: disable=protected-access

  def test_assignment_rhs_koda_iterables(self):
    db = bag()
    x = db.obj()
    # Text
    x.a = 'abc'
    self.assertEqual(x.a, 'abc')
    # Bytes
    x.b = b'abc'
    self.assertEqual(x.b, b'abc')
    x.a = []
    testing.assert_equal(
        x.a.get_schema().get_attr('__items__'),
        schema_constants.NONE.with_bag(db),
    )
    x.a = ()
    testing.assert_equal(
        x.a.get_schema().get_attr('__items__'),
        schema_constants.NONE.with_bag(db),
    )
    # Other iterables are not supported in boxing code.
    with self.assertRaisesRegex(ValueError, 'object with unsupported type'):
      x.a = set()
    with self.assertRaisesRegex(ValueError, 'object with unsupported type'):
      x.a = iter([1, 2, 3])

  def test_set_get_attr(self):
    db = bag()
    x = db.new(abc=ds([42]))
    x.get_schema().xyz = schema_constants.INT64
    x.xyz = ds([12], schema_constants.INT64)
    testing.assert_equal(x.abc, ds([42]).with_bag(db))
    testing.assert_equal(
        x.abc.get_schema(), schema_constants.INT32.with_bag(db)
    )
    testing.assert_equal(x.xyz, ds([12], schema_constants.INT64).with_bag(db))
    testing.assert_equal(
        x.xyz.get_schema(), schema_constants.INT64.with_bag(db)
    )

  def test_get_itemid(self):
    o = bag().obj(x=ds([1, 2, 3]))
    testing.assert_equal(o.get_itemid(), o.with_schema(schema_constants.ITEMID))
    with self.assertRaisesRegex(ValueError, 'unsupported schema: INT32'):
      ds([1, 2, 3]).get_itemid()
    with self.assertRaisesRegex(ValueError, 'cannot cast INT32 to ITEMID'):
      ds([1, 2, 3], schema_constants.OBJECT).get_itemid()

  def test_has_attr(self):
    db = bag()

    with self.subTest('entity item'):
      x = db.new(a=ds(42), b=ds(None))
      testing.assert_equal(x.has_attr('a'), ds(present))
      testing.assert_equal(x.has_attr('b'), ds(missing))
      testing.assert_equal(x.has_attr('c'), ds(missing))

    with self.subTest('entity slice'):
      x = db.new(a=ds([42]), b=ds([None]))
      testing.assert_equal(x.has_attr('a'), ds([present]))
      testing.assert_equal(x.has_attr('b'), ds([missing]))
      testing.assert_equal(x.has_attr('c'), ds([missing]))

    with self.subTest('obj item'):
      x = db.obj(a=ds(42), b=ds(None))
      testing.assert_equal(x.has_attr('a'), ds(present))
      testing.assert_equal(x.has_attr('b'), ds(missing))
      testing.assert_equal(x.has_attr('c'), ds(missing))

    with self.subTest('obj slice'):
      x = db.obj(a=ds([42]), b=ds([None]))
      testing.assert_equal(x.has_attr('a'), ds([present]))
      testing.assert_equal(x.has_attr('b'), ds([missing]))
      testing.assert_equal(x.has_attr('c'), ds([missing]))

  def test_set_get_attr_methods(self):
    db = bag()

    with self.subTest('entity'):
      x = db.new(abc=ds([42], schema_constants.INT64))
      testing.assert_equal(
          x.get_attr('abc'), ds([42], schema_constants.INT64).with_bag(db)
      )
      testing.assert_equal(
          x.get_attr('abc').get_schema(), schema_constants.INT64.with_bag(db)
      )
      # Missing
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(r"attribute 'xyz' is missing"),
      ):
        x.get_attr('xyz')
      testing.assert_equal(x.get_attr('xyz', None), ds([None]).with_bag(db))
      testing.assert_equal(x.get_attr('xyz', b'b'), ds([b'b']).with_bag(db))

      x.set_attr('xyz', ds(b'12'))

      with self.assertRaisesRegex(
          ValueError, r'schema for attribute \'xyz\' is incompatible'
      ):
        x.set_attr('xyz', ds([12]), overwrite_schema=False)

      x.set_attr('xyz', ds([12]), overwrite_schema=True)
      testing.assert_equal(x.get_attr('xyz'), ds([12]).with_bag(db))
      testing.assert_equal(
          x.get_attr('xyz').get_schema(), schema_constants.INT32.with_bag(db)
      )

    with self.subTest('object'):
      x = db.obj(abc=ds([42], schema_constants.INT64))
      testing.assert_equal(
          x.get_attr('abc'), ds([42], schema_constants.INT64).with_bag(db)
      )
      testing.assert_equal(
          x.get_attr('abc').get_schema(), schema_constants.INT64.with_bag(db)
      )

      for attr, val, overwrite_schema, res_schema in [
          ('xyz', ds([b'12']), True, schema_constants.BYTES),
          ('pqr', ds(['123']), False, schema_constants.STRING),
      ]:
        x.set_attr(attr, val, overwrite_schema=overwrite_schema)
        testing.assert_equal(x.get_attr(attr), val.with_bag(db))
        testing.assert_equal(
            x.get_attr(attr).get_schema(), res_schema.with_bag(db)
        )
        testing.assert_equal(
            x.get_attr('__schema__').get_attr(attr),
            ds([res_schema]).with_bag(db),
        )

    with self.subTest('objects with explicit schema'):
      x = db.obj(abc=ds([42, 12]))
      e_schema = db.new(abc=ds(42, schema_constants.INT64)).get_schema()
      x.set_attr('__schema__', e_schema)
      testing.assert_equal(
          x.get_attr('abc'),
          ds([42, 12], schema_constants.INT64).with_bag(db)
      )
      self.assertEqual(x.get_attr('abc').internal_as_py(), [42, 12])
      testing.assert_equal(
          x.get_attr('abc').get_schema(), schema_constants.INT64.with_bag(db)
      )

      x.set_attr(
          'abc',
          # Casting INT32 -> INT64 is allowed and done automatically.
          ds([1, 2], schema_constants.INT32),
      )
      testing.assert_equal(
          x.get_attr('abc'), ds([1, 2], schema_constants.INT64).with_bag(db)
      )
      testing.assert_equal(
          x.get_attr('abc').get_schema(), schema_constants.INT64.with_bag(db)
      )

      with self.assertRaisesRegex(
          ValueError,
          r'the schema for attribute \'abc\' is incompatible',
      ):
        x.set_attr('abc', ds([b'x', b'y']), overwrite_schema=False)
      # Overwrite with overwriting schema.
      x.set_attr('abc', ds([b'x', b'y']), overwrite_schema=True)
      testing.assert_equal(x.get_attr('abc'), ds([b'x', b'y']).with_bag(db))
      testing.assert_equal(
          x.get_attr('abc').get_schema(), schema_constants.BYTES.with_bag(db)
      )
      testing.assert_equal(
          x.get_attr('__schema__').get_attr('abc'),
          ds([schema_constants.BYTES, schema_constants.BYTES]).with_bag(db),
      )

    with self.subTest('errors'):
      x = db.new(abc=ds([42], schema_constants.INT64))
      with self.assertRaisesRegex(
          TypeError, 'expecting attr_name to be a DataSlice'
      ):
        x.set_attr(b'invalid_attr', 1)
      with self.assertRaises(ValueError):
        x.set_attr('invalid__val', ValueError)
      with self.assertRaisesRegex(TypeError, 'expected bool'):
        x.set_attr('invalid__overwrite_schema_type', 1, overwrite_schema=42)
      with self.assertRaisesRegex(
          TypeError, 'accepts 2 to 3 positional arguments'
      ):
        x.set_attr('invalid__overwrite_schema_type', 1, None, 42)

  def test_get_attr_ds_attr_name(self):
    db = bag()
    x = db.obj(a=ds([1, 2]), b=ds([3, 4], schema_constants.INT32))
    with self.subTest('smoke_test'):
      x1 = x.get_attr(ds(['a', 'b']))
      testing.assert_equal(x1.no_bag(), ds([1, 4]))
    with self.subTest('with_default'):
      x1 = x.get_attr(ds(['a', 'c']), None)
      testing.assert_equal(x1.no_bag(), ds([1, None]))
    with self.subTest('invalid_attr_name'):
      with self.assertRaisesRegex(
          TypeError, 'expecting attr_name to be a DataSlice'
      ):
        x.get_attr(db)
    with self.subTest('py_list_attr_name'):
      with self.assertRaisesRegex(
          TypeError, 'expecting attr_name to be a DataSlice'
      ):
        x.get_attr(['a', 'b'])

  def test_set_attr_ds_attr_name(self):
    db = bag()
    x = db.obj()
    with self.subTest('smoke_test'):
      x.set_attr(ds(['a', 'b']), ds([123, 456], schema_constants.INT32))
      testing.assert_equal(x.get_attr('a'), ds(123).with_bag(db))
      testing.assert_equal(x.get_attr('b'), ds(456).with_bag(db))
      testing.assert_equal(
          x.get_attr('__schema__').get_attr('a'),
          ds(schema_constants.INT32).with_bag(db),
      )
      testing.assert_equal(
          x.get_attr('__schema__').get_attr('b'),
          ds(schema_constants.INT32).with_bag(db),
      )

    with self.subTest('schema_narrowing'):
      x.set_attr(
          ds(['a', 'b'], schema_constants.OBJECT),
          ds([123, 456], schema_constants.INT32),
      )
      testing.assert_equal(x.get_attr('a'), ds(123).with_bag(db))
      testing.assert_equal(x.get_attr('b'), ds(456).with_bag(db))

    with self.subTest('set_list'):
      x.set_attr(ds(['a']), ds([db.list([1, 2])]), overwrite_schema=True)
      testing.assert_nested_lists_equal(x.get_attr('a'), db.list([1, 2]))

    with self.subTest('set_attr_on_schema_slice'):
      schema_slice = ds([
          db.new_schema(a=schema_constants.INT32),
          db.new_schema(b=schema_constants.STRING),
      ])
      schema_slice.set_attr(ds(['a', 'b']), schema_constants.FLOAT32)
      testing.assert_equal(
          schema_slice.get_attr('a', None),
          ds([schema_constants.FLOAT32, None]).with_bag(db),
      )
      testing.assert_equal(
          schema_slice.get_attr('b', None),
          ds([None, schema_constants.FLOAT32]).with_bag(db),
      )

    with self.subTest('set_attr_on_schema_attr_slice'):
      schema_item = db.new_schema(
          a=schema_constants.INT32, b=schema_constants.STRING
      )
      schema_item.set_attr(
          ds(['a', 'b']),
          ds([schema_constants.FLOAT32, schema_constants.FLOAT64]),
      )
      testing.assert_equal(
          schema_item.get_attr('a'), schema_constants.FLOAT32.with_bag(db)
      )
      testing.assert_equal(
          schema_item.get_attr('b'), schema_constants.FLOAT64.with_bag(db)
      )

  def test_set_attr_ds_attr_name_errors(self):
    db = bag()
    x = db.obj()

    with self.subTest('str_attr_name'):
      with self.assertRaisesRegex(ValueError, 'must be a slice of STRING'):
        x.set_attr(ds([1, 2]), ds([123, 456]))

  def test_set_attr_incompatible_schema(self):
    db = bag()
    db2 = bag()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            """the schema for attribute 'x' is incompatible.

Expected schema for 'x': ENTITY(c=INT32)
Assigned schema for 'x': ENTITY(b=STRING)

To fix this, explicitly override schema of 'x' in the original schema by passing overwrite_schema=True."""
        ),
    ):
      db.new(x=db.new(c=1)).x = db2.new(b='a')

    o = db.new(x='a').embed_schema()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            """the schema for attribute 'x' is incompatible.

Expected schema for 'x': STRING
Assigned schema for 'x': INT32

To fix this, explicitly override schema of 'x' in the Object schema by passing overwrite_schema=True."""
        ),
    ):
      o.x = 1

    o1 = db.new(x=1).embed_schema()
    o2 = db.new(x=1.0).embed_schema()
    o = ds([o1, o2])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            """the schema for attribute 'x' is incompatible.

Expected schema for 'x': FLOAT32
Assigned schema for 'x': INT32

To fix this, explicitly override schema of 'x' in the Object schema by passing overwrite_schema=True."""
        ),
    ):
      o.x = 1

  def test_set_get_attr_empty_attr_name(self):
    db = bag()
    x = db.new()
    setattr(x.get_schema(), '', schema_constants.INT32)
    setattr(x, '', 1)
    testing.assert_equal(getattr(x, ''), ds(1).with_bag(db))

  def test_set_attr_auto_broadcasting(self):
    db = bag()
    x = db.new_shaped(jagged_shape.create_shape([3]))
    x.get_schema().xyz = schema_constants.INT32
    x.xyz = ds(12)
    testing.assert_equal(x.xyz, ds([12, 12, 12]).with_bag(db))

    x_abc = ds([12, 12])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            f'DataSlice with shape={x_abc.get_shape()} cannot be expanded to'
            f' shape={x.get_shape()}'
        ),
    ):
      x.abc = x_abc

  def test_set_get_attr_empty_entity(self):
    x = bag().new(a=1) & ds(None)
    testing.assert_equal(
        x.a, ds(None, schema_constants.INT32).with_bag(x.get_bag())
    )
    x = bag().new_shaped(jagged_shape.create_shape([2]), a=1) & ds(None)
    testing.assert_equal(
        x.a, ds([None, None], schema_constants.INT32).with_bag(x.get_bag())
    )

  def test_set_get_attr_empty_object(self):
    x = bag().obj(a=1) & ds(None)
    testing.assert_equal(x.a, ds(None).with_bag(x.get_bag()))
    x = bag().obj_shaped(jagged_shape.create_shape([2]), a=1) & ds(None)
    testing.assert_equal(x.a, ds([None, None]).with_bag(x.get_bag()))

  def test_get_attr_object_mixed_data_implicit_cast(self):
    db = bag()
    x = ds([db.obj(a=1), db.obj(a=2.0)])
    testing.assert_equal(x.a, ds([1.0, 2.0]).with_bag(db))

  def test_set_get_attr_object_missing_schema_attr(self):
    obj = bag().obj(a=1)
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'object schema is missing for the DataItem'
        ),
    ):
      _ = obj.with_bag(bag()).a
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'object schema is missing for the DataItem'
        ),
    ):
      obj.with_bag(bag()).a = 1
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            r'object schema is missing for the DataItem'
        ),
    ):
      del obj.with_bag(bag()).a

  def test_set_get_attr_slice_of_objects_missing_schema_attr(self):
    db = bag()
    obj_1 = db.obj(a=1)
    obj_2 = db.new(a=1).with_schema(schema_constants.OBJECT)
    obj = ds([obj_1, obj_2])
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('object schema(s) are missing')
        ),
    ):
      _ = obj.a
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('object schema(s) are missing')
        ),
    ):
      obj.a = 1
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('object schema(s) are missing')
        ),
    ):
      del obj.a

  def test_set_get_attr_object_wrong_schema_attr(self):
    obj = bag().obj(a=1)
    obj.set_attr('__schema__', schema_constants.INT32)
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'cannot get or set attributes on schema: INT32'
        ),
    ):
      _ = obj.a
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'cannot get or set attributes on schema: INT32'
        ),
    ):
      obj.a = 1
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            r'objects must have ObjectId\(s\) as __schema__ attribute, got'
            r' INT32'
        ),
    ):
      del obj.a

  def test_set_attr_merging(self):
    db1 = bag()
    db2 = bag()

    obj1 = db1.obj(a=1)
    obj2 = db1.obj(a=2)
    obj3 = db1.obj(a=3)
    root = db2.obj(b=ds([4, 5, 6]))
    root.c = ds([obj1, obj2, obj3])

    testing.assert_equal(root.c.a, ds([1, 2, 3]).with_bag(root.get_bag()))

  def test_set_get_attr_on_qvalue_properties(self):
    x = bag().obj()
    # qtype.
    x.set_attr('qtype', 42)
    testing.assert_equal(x.get_attr('qtype'), ds(42).with_bag(x.get_bag()))
    self.assertEqual(x.qtype, qtypes.DATA_SLICE)
    with self.assertRaisesRegex(
        AttributeError, r'attribute \'qtype\'.*is not writable'
    ):
      x.qtype = 42
    # fingerprint.
    x.set_attr('fingerprint', 42)
    testing.assert_equal(
        x.get_attr('fingerprint'), ds(42).with_bag(x.get_bag())
    )
    with self.assertRaisesRegex(
        AttributeError, r'attribute \'fingerprint\'.*is not writable'
    ):
      x.fingerprint = 42
    # DataSlice's specific property `db`.
    x.db = 42
    testing.assert_equal(x.db, ds(42).with_bag(x.get_bag()))

  def test_getattr_errors(self):
    x = ds([1, 2, 3])
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex("failed to get 'abc' attribute"),
    ):
      _ = x.abc
    with self.assertRaisesRegex(TypeError, r'attribute name must be string'):
      getattr(x, 12345)  # pytype: disable=wrong-arg-types

  def test_set_get_attr_implicit_schema_slice_error(self):
    # NOTE: Regression test for b/364826956.
    db = bag()
    obj = db.obj(a=db.obj(x=1, y=3.14))
    entity = db.new(a=db.new(x=1, y=3.14))
    entity.get_schema().a = obj.a.get_attr('__schema__')
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'DataSlice cannot have an implicit schema as its schema'
        ),
    ):
      _ = entity.a  # Has implicit schema.

  def test_set_attr_none(self):
    with self.subTest('entity'):
      e = bag().new(x=42)
      e.x = None
      testing.assert_equal(
          e.x.get_schema(), schema_constants.INT32.with_bag(e.get_bag())
      )

      db = bag()
      e = db.new(x=db.new())
      e.x = None
      testing.assert_equal(e.x.get_schema(), e.get_schema().x)

    with self.subTest('object'):
      o = bag().obj(x=42)
      o.x = None
      testing.assert_equal(
          o.x.get_schema(), schema_constants.NONE.with_bag(o.get_bag())
      )

    with self.subTest('incompatible schema'):
      with self.assertRaisesRegex(
          ValueError,
          r'the schema for attribute \'x\' is incompatible',
      ):
        db = bag()
        db.new(x=db.new()).x = ds(None, schema_constants.ITEMID)

    with self.subTest('schema'):
      with self.assertRaisesRegex(
          ValueError,
          'only schemas can be assigned as attributes of schemas, got: None',
      ):
        bag().new().get_schema().x = None

  def test_setattr_assignment_rhs_scalar(self):
    x = bag().obj(a=1)
    x.b = 4
    testing.assert_equal(x.b, ds(4).with_bag(x.get_bag()))

  def test_setattr_assignment_rhs_auto_packing_list(self):
    x = bag().obj(a=1)
    x.b = [1, 2, 3]
    testing.assert_equal(x.b[:], ds([1, 2, 3]).with_bag(x.get_bag()))
    testing.assert_equal(
        x.b.get_schema().get_attr('__items__'),
        schema_constants.INT32.with_bag(x.get_bag()),
    )

  def test_setattr_assignment_rhs_auto_packing_dicts(self):
    x = bag().obj(a=1)
    x.b = {'a': {42: 3.14}, 'b': {37: 2.0}}
    testing.assert_dicts_keys_equal(x.b, ds(['a', 'b']))
    testing.assert_allclose(
        x.b[ds(['a', 'b', 'a'])][42],
        ds([3.14, None, 3.14]).with_bag(x.get_bag()),
    )
    testing.assert_allclose(x.b['b'][37], ds(2.0).with_bag(x.get_bag()))
    self.assertEqual(
        x.b.get_schema().get_attr('__keys__'), schema_constants.STRING
    )
    self.assertEqual(
        x.b.get_schema().get_attr('__values__').get_attr('__keys__'),
        schema_constants.INT32,
    )
    self.assertEqual(
        x.b.get_schema().get_attr('__values__').get_attr('__values__'),
        schema_constants.FLOAT32,
    )

  def test_setattr_assignment_rhs_error(self):
    x = bag().obj(a=ds([1, 2, 3]))
    with self.assertRaisesRegex(
        ValueError, 'only supported for Koda List DataItem'
    ):
      x.b = [4, 5, 6]
    with self.assertRaisesRegex(
        ValueError, r'got DataSlice with shape JaggedShape\(2\)'
    ):
      x.b = [1, 2, ds([3, 4])]
    with self.assertRaisesRegex(
        ValueError, 'only supported for Koda Dict DataItem'
    ):
      x.b = {'abc': 42}

  def test_setattr_assignment_rhs_dict_error(self):
    x = bag().obj()
    with self.assertRaisesRegex(ValueError, 'unsupported type: Obj'):

      class Obj:
        pass

      x.b = {'a': Obj()}

    with self.assertRaisesRegex(
        ValueError, r'got DataSlice with shape JaggedShape\(3\)'
    ):
      x.b = {'a': ds([1, 2, 3])}

    with self.assertRaisesRegex(
        ValueError, r'got DataSlice with shape JaggedShape\(3\)'
    ):
      x.b = {'a': {42: ds([1, 2, 3])}}

  def test_set_multiple_attrs(self):
    x = bag().new(a=1, b='a')
    x.set_attrs(a=2, b='abc')
    testing.assert_equal(x.a, ds(2).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds('abc').with_bag(x.get_bag()))

    with self.assertRaisesRegex(
        ValueError, r'schema for attribute \'b\' is incompatible'
    ):
      x.set_attrs(a=2, b=b'abc')

    x.set_attrs(a=2, b=b'abc', overwrite_schema=True)
    testing.assert_equal(x.a, ds(2).with_bag(x.get_bag()))
    testing.assert_equal(x.b, ds(b'abc').with_bag(x.get_bag()))

  def test_set_multiple_attrs_with_merging(self):
    o = bag().obj(a=1)
    b = bag().new(x='abc', y=1234)
    o.set_attrs(b=b, d={'a': 42}, l=[1, 2, 3])

    testing.assert_equal(o.a, ds(1).with_bag(o.get_bag()))
    # Merged DataBag from another object / entity.
    testing.assert_equal(o.b.x, ds('abc').with_bag(o.get_bag()))
    testing.assert_equal(o.b.y, ds(1234).with_bag(o.get_bag()))
    # Merged DataBag from creating a DataBag during boxing of complex Python
    # values.
    testing.assert_dicts_equal(o.d, bag().dict({'a': 42}))
    testing.assert_equal(o.l[:], ds([1, 2, 3]).with_bag(o.get_bag()))

  def test_set_multiple_attrs_wrong_overwrite_schema_type(self):
    o = bag().obj()
    with self.assertRaisesRegex(
        TypeError, 'expected bool for overwrite_schema, got int'
    ):
      o.set_attrs(overwrite_schema=42)

  def test_del_attr(self):
    db = bag()

    with self.subTest('entity'):
      e = db.new(a=1, b=2)
      del e.a
      testing.assert_equal(e.a, ds(None, schema_constants.INT32).with_bag(db))
      testing.assert_equal(
          e.a.get_schema(), schema_constants.INT32.with_bag(db)
      )
      del e.get_schema().b
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              re.escape(
                  """the attribute 'b' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('b')"""
              )
          ),
      ):
        _ = e.b
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              re.escape(
                  """the attribute 'c' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('c')"""
              )
          ),
      ):
        del e.get_schema().c
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              re.escape(
                  """the attribute 'c' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('c')"""
              )
          ),
      ):
        del e.c

    with self.subTest('object'):
      o = db.obj(a=1, b=2)
      del o.a
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              re.escape(
                  """the attribute 'a' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('a')"""
              )
          ),
      ):
        _ = o.a
      del o.get_attr('__schema__').b
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              re.escape(
                  """the attribute 'b' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('b')"""
              )
          ),
      ):
        del o.b
      with self.assertRaisesWithPredicateMatch(
          ValueError,
          arolla.testing.any_cause_message_regex(
              re.escape(
                  """the attribute 'c' is missing on the schema.

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('c')"""
              )
          ),
      ):
        del o.c

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                """the attribute 'a' is missing for at least one object at ds.flatten().S[1]

If it is not a typo, perhaps ignore the schema when getting the attribute. For example, ds.maybe('a')"""
            )
        ),
    ):
      _ = ds([[db.obj(a=1), db.obj(b=2)]]).a

  def test_maybe_method(self):
    db = bag()
    obj = ds([db.obj(a=1), db.obj(x=42), db.obj(a=3)])
    testing.assert_equal(obj.maybe('a'), ds([1, None, 3]).with_bag(db))
    testing.assert_equal(
        obj.maybe(ds(['a', 'a', 'c'])), ds([1, None, None]).with_bag(db)
    )

  def test_internal_as_arolla_value(self):
    x = ds([1, 2, 3], schema_constants.FLOAT32)
    arolla.testing.assert_qvalue_allclose(
        x.internal_as_arolla_value(),
        arolla.dense_array([1.0, 2, 3], arolla.FLOAT32),
    )
    x = ds([1, 'abc', 3.14])
    with self.assertRaisesRegex(
        ValueError,
        'only DataSlices with primitive values of the same type can be '
        'converted to Arolla value, got: MIXED',
    ):
      x.internal_as_arolla_value()

  def test_internal_as_dense_array(self):
    x = ds([1, 2, 3], schema_constants.FLOAT32)
    arolla.testing.assert_qvalue_allclose(
        x.internal_as_dense_array(),
        arolla.dense_array([1.0, 2, 3], arolla.FLOAT32),
    )
    x = ds([1, 'abc', 3.14])
    with self.assertRaisesRegex(
        ValueError,
        'only DataSlices with primitive values of the same type can be '
        'converted to Arolla value, got: MIXED',
    ):
      x.internal_as_dense_array()

  @parameterized.parameters(
      (ds([1, 2, 3]), jagged_shape.create_shape([3])),
      (ds([[1, 2], [3]]), jagged_shape.create_shape([2], [2, 1])),
  )
  def test_get_shape(self, x, expected_shape):
    testing.assert_equal(x.get_shape(), expected_shape)

  @parameterized.parameters(
      (ds(1), jagged_shape.create_shape([1]), ds([1])),
      (ds(1), jagged_shape.create_shape([1], [1], [1]), ds([[[1]]])),
      (ds([1]), jagged_shape.create_shape(), ds(1)),
      (ds([[[1]]]), jagged_shape.create_shape([1]), ds([1])),
      (ds([[[1]]]), jagged_shape.create_shape(), ds(1)),
      (ds([[1, 2], [3]]), jagged_shape.create_shape([3]), ds([1, 2, 3])),
      (
          ds([1, 2, 3]),
          jagged_shape.create_shape([2], [2, 1]),
          ds([[1, 2], [3]]),
      ),
      (
          ds([1, 2, 3]),
          (2, ds([2, 1])),
          ds([[1, 2], [3]]),
      ),
      (
          ds([[1, 2], [3]]),
          (-1,),
          ds([1, 2, 3]),
      ),
  )
  def test_reshape(self, x, shape, expected_output):
    new_x = x.reshape(shape)
    testing.assert_equal(new_x, expected_output)

  def test_reshape_incompatible_shape_exception(self):
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        'shape size must be compatible with number of items: shape_size=2 !='
        ' items_size=3',
    ):
      x.reshape(jagged_shape.create_shape([2]))

  @parameterized.parameters(1, arolla.int32(1))
  def test_reshape_non_shape(self, non_shape):
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'expected a tuple'):
      x.reshape(non_shape)

  @parameterized.parameters(
      (ds([1, 2, 3]),),
      (ds([[1, 2], [3]]),),
      (ds([[[1], [2]], [[3]]]),),
      (ds([[[1], [2]], [[], [3]]]),),
  )
  def test_reshape_as(self, shape_from):
    x = ds(['a', 'b', 'c'])
    res = x.reshape_as(shape_from)
    testing.assert_equal(res.flatten(), x)
    testing.assert_equal(res.get_shape(), shape_from.get_shape())

  def test_reshape_as_errors(self):
    with self.assertRaisesRegex(
        ValueError, 'shape size must be compatible with number of items'
    ):
      ds(1).reshape_as(ds([1, 2]))
    with self.assertRaisesRegex(TypeError, '`shape_from` must be a DataSlice'):
      ds(1).reshape_as([])

  @parameterized.parameters(
      (ds(1), ds([1])),
      (ds([[1, 2], [3, 4]]), ds([1, 2, 3, 4])),
      (ds([[[1], [2]], [[3], [4]]]), 1, ds([[1, 2], [3, 4]])),
      (ds([[[1], [2]], [[3], [4]]]), -2, ds([[1, 2], [3, 4]])),
      (ds([[[1, 2], [3]], [[4, 5]]]), 0, 2, ds([[1, 2], [3], [4, 5]])),
  )
  def test_flatten(self, *inputs_and_expected):
    args, expected = inputs_and_expected[:-1], inputs_and_expected[-1]
    flattened = args[0].flatten(*args[1:])
    testing.assert_equal(flattened, expected)

  @parameterized.parameters(
      (ds(1), 2, ds([1, 1])),
      (ds([1, 2]), 2, ds([[1, 1], [2, 2]])),
      (ds([[1, 2], [3]]), ds([2, 3]), ds([[[1, 1], [2, 2]], [[3, 3, 3]]])),
      (ds([[1, 2], [3]]), ds([[0, 1], [2]]), ds([[[], [2]], [[3, 3]]])),
  )
  def test_repeat(self, x, sizes, expected):
    testing.assert_equal(x.repeat(sizes), expected)

  @parameterized.parameters(
      (
          ds([1, 2, 3]),
          ds([arolla.missing(), arolla.present(), arolla.missing()]),
          ds([2]),
      ),
      (
          ds([[1], [2], [3]]),
          ds([[arolla.missing()], [arolla.present()], [arolla.missing()]]),
          ds([[], [2], []]),
      ),
      (
          ds([[1], [None], [3]]),
          ds([[arolla.present()], [arolla.present()], [arolla.present()]]),
          ds([[1], [None], [3]]),
      ),
      (
          ds(['a', 1, None, 1.5]),
          ds([
              arolla.missing(),
              arolla.missing(),
              arolla.missing(),
              arolla.present(),
          ]),
          ds([1.5], schema_constants.OBJECT),
      ),
      # Test case for kd.present.
      (ds([1]), ds(arolla.present()), ds([1])),
      # Test case for kd.missing.
      (ds([1]), ds(arolla.missing()), ds([], schema_constants.INT32)),
  )
  def test_select(self, x, filter_input, expected_output):
    testing.assert_equal(x.select(filter_input), expected_output)

  def test_select_filter_error(self):
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.select: the schema of the `fltr` DataSlice should only'
            ' be OBJECT or MASK or can be evaluated to such DataSlice'
            ' (i.e. Python function or Koda Functor)'
        ),
    ):
      x.select(ds([1, 2, 3]))

    with self.subTest('Shape mismatch'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape('kd.slices.select: failed to broadcast `fltr` to `ds`'),
      ):
        x = ds([[1, 2], [None, None], [7, 8, 9]])
        y = ds([arolla.present(), arolla.present(), None, arolla.present()])
        x.select(y)

  @parameterized.parameters(
      (
          ds([[1, 2, None, 4], [None, None], [7, 8, 9]]),
          ds([[1, 2, 4], [], [7, 8, 9]]),
      ),
      (ds([1]), ds([1])),
      (ds([arolla.missing()]), ds([], schema_constants.MASK)),
  )
  def test_select_present(self, x, expected_output):
    testing.assert_equal(x.select_present(), expected_output)

  # More comprehensive tests are in the core_select_items_test.py.
  @parameterized.parameters(
      (
          bag().list([1, 2, 3]),
          ds([None, present, present]),
          ds([2, 3]),
      ),
      (
          bag().list([1, 2, 3]),
          functor_factories.expr_fn(I.self >= 2),
          ds([2, 3]),
      ),
      (
          ds([bag().list([1, 2, 3]), bag().list([2, 3, 4])]),
          ds([None, present]),
          ds([[], [2, 3, 4]]),
      ),
      (
          bag().list([1, 2, 3]),
          lambda x: x >= 2,
          ds([2, 3]),
      ),
  )
  def test_select_items(self, x, filter_input, expected_output):
    testing.assert_equal(x.select_items(filter_input).no_bag(), expected_output)

  # More comprehensive tests are in the core_select_keys_test.py.
  @parameterized.parameters(
      (
          ds([bag().dict({1: 1}), bag().dict({2: 2}), bag().dict({3: 3})]),
          ds([present, None, None]),
          ds([[1], [], []]),
      ),
      (
          ds([bag().dict({1: 1}), bag().dict({2: 2}), bag().dict({3: 3})]),
          functor_factories.expr_fn(I.self == 1),
          ds([[1], [], []]),
      ),
      (
          ds([[bag().dict({1: 1})], [bag().dict({2: 2}), bag().dict({3: 3})]]),
          ds([present, None]),
          ds([[[1]], [[], []]]),
      ),
      (
          bag().dict({1: 3, 2: 4, 3: 5}),
          lambda x: x == 2,
          ds([2]),
      ),
  )
  def test_select_keys(self, x, filter_input, expected_output):
    testing.assert_equal(x.select_keys(filter_input).no_bag(), expected_output)

  # More comprehensive tests are in the core_select_values_test.py.
  @parameterized.parameters(
      (
          ds([bag().dict({1: 1}), bag().dict({2: 2}), bag().dict({3: 3})]),
          ds([present, None, None]),
          ds([[1], [], []]),
      ),
      (
          ds([bag().dict({4: 1}), bag().dict({5: 2}), bag().dict({6: 3})]),
          functor_factories.expr_fn(I.self == 1),
          ds([[1], [], []]),
      ),
      (
          ds([[bag().dict({1: 1})], [bag().dict({2: 2}), bag().dict({3: 3})]]),
          ds([present, None]),
          ds([[[1]], [[], []]]),
      ),
      (
          bag().dict({3: 1, 4: 2, 5: 3}),
          lambda x: x == 2,
          ds([2]),
      ),
  )
  def test_select_values(self, x, filter_input, expected_output):
    testing.assert_equal(
        x.select_values(filter_input).no_bag(), expected_output
    )

  @parameterized.parameters(
      # ndim=0
      (ds(1), ds([1, 2, 3]), 0, ds([1, 1, 1])),
      (ds(1), ds([[1, 2], [3]]), 0, ds([[1, 1], [1]])),
      (ds([1, 2]), ds([[1, 2], [3]]), 0, ds([[1, 1], [2]])),
      # ndim=1
      (ds([1, 2]), ds([1, 2, 3]), 1, ds([[1, 2], [1, 2], [1, 2]])),
  )
  def test_expand_to(self, source, target, ndim, expected_output):
    testing.assert_equal(source.expand_to(target, ndim), expected_output)
    if ndim == 0:
      testing.assert_equal(source.expand_to(target), expected_output)

  @parameterized.parameters(
      ([1, 2, 3], None, schema_constants.INT32),
      (['a', 'b'], None, schema_constants.STRING),
      ([b'a', b'b', b'c'], None, schema_constants.BYTES),
      (['a', b'b', 34], None, schema_constants.OBJECT),
      ([1, 2, 3], schema_constants.INT64, schema_constants.INT64),
      ([1, 2, 3], schema_constants.FLOAT64, schema_constants.FLOAT64),
  )
  def test_get_schema(self, inputs, qtype, expected_schema):
    x = ds(inputs, qtype)
    testing.assert_equal(x.get_schema(), expected_schema)
    testing.assert_equal(x.get_schema().get_schema(), schema_constants.SCHEMA)

  def test_with_schema(self):
    db = bag()
    x = db.new(x=ds([1, 2, 3]), y='abc')
    testing.assert_equal(x.get_schema().x, schema_constants.INT32.with_bag(db))
    testing.assert_equal(x.get_schema().y, schema_constants.STRING.with_bag(db))

    with self.assertRaisesRegex(
        TypeError, 'expecting schema to be a DataSlice, '
                   'got koladata.types.data_bag.DataBag'
    ):
      x.with_schema(db)

    with self.assertRaisesRegex(ValueError, "schema's schema must be SCHEMA"):
      x.with_schema(x)

    schema = db.new(x=1, y='abc').get_schema()
    testing.assert_equal(x.with_schema(schema).get_schema(), schema)

    non_schema = db.new().with_schema(schema_constants.SCHEMA)
    with self.assertRaisesRegex(
        ValueError, 'schema must contain either a DType or valid schema ItemId'
    ):
      x.with_schema(non_schema)

    with self.assertRaisesRegex(
        ValueError, 'a non-schema item cannot be present in a schema DataSlice'
    ):
      ds(1).with_schema(schema_constants.SCHEMA)

    # NOTE: Works without deep schema verification.
    ds([1, 'abc']).with_schema(schema_constants.SCHEMA)

  def test_set_schema(self):
    db = bag()
    x = db.new(x=ds([1, 2, 3]))

    with self.assertRaisesRegex(
        TypeError, 'expecting schema to be a DataSlice, got '
                   'koladata.types.data_bag.DataBag'
    ):
      x.set_schema(db)

    with self.assertRaisesRegex(ValueError, "schema's schema must be SCHEMA"):
      x.set_schema(x)

    schema = db.new(x=1, y='abc').get_schema()
    testing.assert_equal(x.set_schema(schema).get_schema(), schema)

    db_2 = bag()
    schema = db_2.new(x=1, y='abc').get_schema()
    res_schema = x.set_schema(schema).get_schema()
    testing.assert_equal(res_schema, schema.with_bag(db))
    testing.assert_equal(res_schema.y, schema_constants.STRING.with_bag(db))

    non_schema = db.new().set_schema(schema_constants.SCHEMA)
    with self.assertRaisesRegex(
        ValueError, 'schema must contain either a DType or valid schema ItemId'
    ):
      x.set_schema(non_schema)

    with self.assertRaisesRegex(
        ValueError,
        'cannot set an Entity schema on a DataSlice without a DataBag.',
    ):
      ds(1).set_schema(schema)

    with self.assertRaisesRegex(
        ValueError, 'a non-schema item cannot be present in a schema DataSlice'
    ):
      ds(1).with_bag(db).set_schema(schema_constants.SCHEMA)

    # NOTE: Works without deep schema verification.
    ds([1, 'abc']).with_bag(db).set_schema(schema_constants.SCHEMA)

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
      non_dicts[set()] = 'b'
    with self.assertRaisesRegex(ValueError, 'object with unsupported type'):
      _ = non_dicts[set()]
    with self.assertRaisesRegex(ValueError, 'object with unsupported type'):
      non_dicts['a'] = ValueError
    with self.assertRaisesRegex(
        ValueError, re.escape('dict(s) expected, got ENTITY(x=INT32)')
    ):
      non_dicts['a'] = 'b'
    with self.assertRaisesRegex(
        ValueError, re.escape('dict(s) expected, got ENTITY(x=INT32)')
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
    x1 = bag().dict(ds([1, 2]), ds([3, 4]))
    testing.assert_dicts_equal(
        x1.with_dict_update(fns.dict({1: 5, 3: 6})),
        bag().dict(ds([1, 2, 3]), ds([5, 4, 6])),
    )

  # More comprehensive tests are in lists_appended_list_test.py
  def test_with_list_append_update(self):
    x = ds([fns.list([1, 2]), fns.list([3, 4])])
    append = ds([5, 6])
    result = x.with_list_append_update(append)

    testing.assert_nested_lists_equal(
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
      _ = lst[1, 2]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      _ = lst[[1, 2]]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      lst[1, 2] = 42
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      lst[[1, 2]] = 42
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      del lst[1, 2]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      del lst[[1, 2]]
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
      _ = dct['a', 'b']
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      _ = dct[['a', 'b']]
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      dct['a', 'b'] = 42
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      dct[['a', 'b']] = 42
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      del dct['a', 'b']
    with self.assertRaisesRegex(
        ValueError, 'passing a Python list/tuple.*is ambiguous'
    ):
      del dct[['a', 'b']]

  def test_magic_methods(self):
    x = ds([1, 2, 3])
    y = ds([4, 5, 6])
    z = ds([1, 2, None])
    mask = ds([arolla.present(), None, arolla.present()])
    with self.subTest('add'):
      testing.assert_equal(x + y, ds([5, 7, 9]))
      # With auto-boxing
      testing.assert_equal(x + 4, ds([5, 6, 7]))
      # __radd__ with auto-boxing
      testing.assert_equal(4 + x, ds([5, 6, 7]))
    with self.subTest('sub'):
      testing.assert_equal(x - y, ds([-3, -3, -3]))
      # With auto-boxing
      testing.assert_equal(x - 4, ds([-3, -2, -1]))
      # __rsub__ with auto-boxing
      testing.assert_equal(4 - x, ds([3, 2, 1]))
    with self.subTest('mul'):
      testing.assert_equal(x * y, ds([4, 10, 18]))
      # With auto-boxing
      testing.assert_equal(x * 4, ds([4, 8, 12]))
      # __rmul__ with auto-boxing
      testing.assert_equal(4 * x, ds([4, 8, 12]))
    with self.subTest('div'):
      testing.assert_equal(y / x, ds([4, 2.5, 2]))
      # With auto-boxing
      testing.assert_equal(z / 2, ds([0.5, 1, None]))
      # __rtruediv__ with auto-boxing
      testing.assert_equal(2 / z, ds([2.0, 1.0, None]))
    with self.subTest('floordiv'):
      testing.assert_equal(y // x, ds([4, 2, 2]))
      # With auto-boxing
      testing.assert_equal(z // 2, ds([0, 1, None]))
      # __rfloordiv__ with auto-boxing
      testing.assert_equal(2 // z, ds([2, 1, None]))
    with self.subTest('mod'):
      testing.assert_equal(y % x, ds([0, 1, 0]))
      # With auto-boxing
      testing.assert_equal(z % 2, ds([1, 0, None]))
      # __rmod__ with auto-boxing
      testing.assert_equal(2 % z, ds([0, 0, None]))
    with self.subTest('pow'):
      testing.assert_equal(x**z, ds([1.0, 4.0, None]))
      # With auto-boxing
      testing.assert_equal(z**2, ds([1.0, 4.0, None]))
      # __rpow__ with auto-boxing
      testing.assert_equal(2**z, ds([2.0, 4.0, None]))
    with self.subTest('and'):
      testing.assert_equal(x & mask, ds([1, None, 3]))
      # only __rand__ with auto-boxing
      testing.assert_equal(1 & mask, ds([1, None, 1]))
    with self.subTest('eq'):
      testing.assert_equal(
          x == z, ds([arolla.present(), arolla.present(), None])
      )
      # With auto-boxing
      testing.assert_equal(x == 2, ds([None, arolla.present(), None]))
      testing.assert_equal(  # pytype: disable=wrong-arg-types
          2 == x, ds([None, arolla.present(), None])
      )
    with self.subTest('ne'):
      testing.assert_equal(
          x != z, ds([None, None, None], schema_constants.MASK)
      )
      # With auto-boxing
      testing.assert_equal(
          x != 2, ds([arolla.present(), None, arolla.present()])
      )
      testing.assert_equal(  # pytype: disable=wrong-arg-types
          2 != x, ds([arolla.present(), None, arolla.present()])
      )
    with self.subTest('gt'):
      testing.assert_equal(
          y > z, ds([arolla.present(), arolla.present(), None])
      )
      # With auto-boxing
      testing.assert_equal(
          x > 1, ds([None, arolla.present(), arolla.present()])
      )
      testing.assert_equal(  # pytype: disable=wrong-arg-types
          2 > x, ds([arolla.present(), None, None])
      )
    with self.subTest('ge'):
      testing.assert_equal(
          y >= z, ds([arolla.present(), arolla.present(), None])
      )
      # With auto-boxing
      testing.assert_equal(
          x >= 2, ds([None, arolla.present(), arolla.present()])
      )
      testing.assert_equal(  # pytype: disable=wrong-arg-types
          1 >= x, ds([arolla.present(), None, None])
      )
    with self.subTest('lt'):
      testing.assert_equal(y < z, ds([None, None, None], schema_constants.MASK))
      # With auto-boxing
      testing.assert_equal(x < 2, ds([arolla.present(), None, None]))
      testing.assert_equal(  # pytype: disable=wrong-arg-types
          2 < x, ds([None, None, arolla.present()])
      )
    with self.subTest('le'):
      testing.assert_equal(
          y <= z, ds([None, None, None], schema_constants.MASK)
      )
      # With auto-boxing
      testing.assert_equal(
          x <= 2, ds([arolla.present(), arolla.present(), None])
      )
      testing.assert_equal(  # pytype: disable=wrong-arg-types
          2 <= x, ds([None, arolla.present(), arolla.present()])
      )
    with self.subTest('or'):
      testing.assert_equal((x & mask) | y, ds([1, 5, 3]))
      # With auto-boxing
      testing.assert_equal((x & mask) | 4, ds([1, 4, 3]))
      # __ror__ with auto-boxing
      testing.assert_equal(None | y, ds([4, 5, 6]))
    with self.subTest('xor'):
      testing.assert_equal(
          ds([arolla.present(), None]) ^ ds([None, arolla.present()]),
          ds([arolla.present(), arolla.present()])
      )
      # With auto-boxing
      testing.assert_equal(
          ds([arolla.present(), None]) ^ None, ds([arolla.present(), None])
      )
      # __rxor__ with auto-boxing
      testing.assert_equal(
          None ^ ds([arolla.present(), None]), ds([arolla.present(), None])
      )
    with self.subTest('not'):
      testing.assert_equal(~mask, ds([None, arolla.present(), None]))

  def test_embed_schema_entity(self):
    db = bag()
    x = db.new(a=ds([1, 2]))
    x_object = x.embed_schema()
    testing.assert_equal(
        x_object.get_schema(), schema_constants.OBJECT.with_bag(db)
    )
    testing.assert_equal(x_object.a, x.a)
    schema_attr = x_object.get_attr('__schema__')
    testing.assert_equal(
        schema_attr == x.get_schema(), ds([arolla.present(), arolla.present()])
    )

  def test_embed_schema_primitive(self):
    testing.assert_equal(
        ds([1, 2, 3]).embed_schema(), ds([1, 2, 3], schema_constants.OBJECT)
    )

  def test_embed_schema_none(self):
    testing.assert_equal(
        ds(None).embed_schema(), ds(None, schema_constants.OBJECT)
    )
    testing.assert_equal(
        ds([None]).embed_schema(), ds([None], schema_constants.OBJECT)
    )
    testing.assert_equal(
        ds([[None, None], [None], []]).embed_schema(),
        ds([[None, None], [None], []], schema_constants.OBJECT),
    )

  # More comprehensive tests are in the schema_get_primitive_schema_test.py.
  def test_get_dtype(self):
    testing.assert_equal(ds([1, 2, 3]).get_dtype(), schema_constants.INT32)
    testing.assert_equal(
        bag().new(x=1).get_dtype(), schema_constants.INT32 & None
    )

  def test_get_obj_schema(self):
    x = ds([1, None, 1.1], schema_constants.OBJECT)
    expected = ds([schema_constants.INT32, None, schema_constants.FLOAT32])
    testing.assert_equal(x.get_obj_schema(), expected)

    db = bag().empty()
    obj = db.obj(x=1)
    x = ds([1, 1.2, obj, 'a'])
    expected = ds([
        schema_constants.INT32,
        schema_constants.FLOAT32,
        obj.get_attr('__schema__'),
        schema_constants.STRING,
    ])
    testing.assert_equal(x.get_obj_schema(), expected)

  def test_get_item_schema(self):
    db = bag().empty()
    x = db.list_schema(schema_constants.INT32)
    testing.assert_equal(x.get_item_schema().no_bag(), schema_constants.INT32)

  def test_get_key_value_schema(self):
    db = bag().empty()
    x = db.dict_schema(schema_constants.INT64, schema_constants.INT32)
    testing.assert_equal(x.get_key_schema().no_bag(), schema_constants.INT64)
    testing.assert_equal(x.get_value_schema().no_bag(), schema_constants.INT32)

  def test_with_schema_from_obj(self):
    entity = bag().new(x=1)
    obj = entity.embed_schema()
    testing.assert_equal(obj.with_schema_from_obj(), entity)

    with self.assertRaisesRegex(
        ValueError, 'DataSlice cannot have an implicit schema as its schema'
    ):
      bag().obj(x=1).with_schema_from_obj()

  def test_follow(self):
    x = kde.new().eval()
    testing.assert_equal(kde.nofollow(x).eval().follow(), x)
    with self.assertRaisesRegex(ValueError, 'a nofollow schema is required'):
      ds([1, 2, 3]).follow()

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_extract(self, pass_schema):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = o.extract(o.get_schema())
    else:
      result = o.extract()

    self.assertNotEqual(o.get_bag().fingerprint, result.get_bag().fingerprint)
    testing.assert_equal(result.no_bag(), o.no_bag())
    testing.assert_equal(result.b.no_bag(), o.b.no_bag())
    testing.assert_equal(result.c.no_bag(), o.c.no_bag())
    testing.assert_equal(result.b.a.no_bag(), o.b.a.no_bag())
    testing.assert_equal(result.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        result.b.get_schema().no_bag(), o.b.get_schema().no_bag()
    )

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_extract_bag(self, pass_schema):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = o.extract_bag(o.get_schema())
    else:
      result = o.extract_bag()

    self.assertNotEqual(o.get_bag().fingerprint, result.fingerprint)
    res_o = o.with_bag(result)
    testing.assert_equal(res_o.b.no_bag(), o.b.no_bag())
    testing.assert_equal(res_o.c.no_bag(), o.c.no_bag())
    testing.assert_equal(res_o.b.a.no_bag(), o.b.a.no_bag())
    testing.assert_equal(res_o.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        res_o.b.get_schema().no_bag(), o.b.get_schema().no_bag()
    )

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_clone(self, pass_schema):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = o.clone(schema=o.get_schema())
    else:
      result = o.clone()

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_bag(), o.no_bag())
    testing.assert_equal(result.b.no_bag(), o.b.no_bag())
    testing.assert_equal(result.c.no_bag(), o.c.no_bag())
    testing.assert_equal(result.b.a.no_bag(), o.b.a.no_bag())
    testing.assert_equal(result.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        result.b.get_schema().no_bag(), o.b.get_schema().no_bag()
    )

  def test_clone_with_overrides(self):
    x = bag().obj(y=bag().obj(a=1), z=bag().list([2, 3]))
    res = x.clone(z=bag().list([12]), t=bag().obj(b=5))
    testing.assert_equivalent(res.y.extract(), x.y.extract())
    testing.assert_equal(res.z[:].no_bag(), ds([12]))
    testing.assert_equal(res.t.b.no_bag(), ds(5))

  def test_clone_non_deterministic(self):
    x = bag().obj(a=1)
    self.assertNotEqual(x.clone(), x.clone())

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_shallow_clone(self, pass_schema):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.new(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = o.shallow_clone(schema=o.get_schema())
    else:
      result = o.shallow_clone()

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_bag(), o.no_bag())
    testing.assert_equal(result.b.no_bag(), o.b.no_bag())
    testing.assert_equal(result.c.no_bag(), o.c.no_bag())
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex("attribute 'a' is missing"),
    ):
      _ = result.b.a

  def test_shallow_clone_with_overrides(self):
    x = bag().obj(y=bag().obj(a=1), z=bag().list([2, 3]))
    res = x.shallow_clone(z=bag().list([12]), t=bag().obj(b=5))
    testing.assert_equivalent(res.y.no_bag(), x.y.no_bag())
    testing.assert_equal(res.z[:].no_bag(), ds([12]))
    testing.assert_equal(res.t.b.no_bag(), ds(5))

  def test_shallow_clone_non_deterministic(self):
    x = bag().obj(a=1)
    self.assertNotEqual(x.shallow_clone(), x.shallow_clone())

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_deep_clone(self, pass_schema):
    db = bag()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    o.set_attr('self', o)
    if pass_schema:
      result = o.deep_clone(o.get_schema())
    else:
      result = o.deep_clone()

    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.no_bag(), o.no_bag())
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.b.no_bag(), o.b.no_bag())
    testing.assert_equal(result.c.no_bag(), o.c.no_bag())
    testing.assert_equal(result.b.a.no_bag(), o.b.a.no_bag())
    testing.assert_equal(result.self.no_bag(), result.no_bag())
    testing.assert_equal(result.get_schema().no_bag(), schema_constants.OBJECT)
    testing.assert_equal(
        result.b.a.get_schema().no_bag(), o.b.a.get_schema().no_bag()
    )

  def test_deep_clone_with_overrides(self):
    x = bag().obj(y=bag().obj(a=1), z=bag().list([2, 3]))
    res = x.deep_clone(z=bag().list([12]), t=bag().obj(b=5))
    testing.assert_equal(res.y.a.no_bag(), ds(1))
    testing.assert_equal(res.z[:].no_bag(), ds([12]))
    testing.assert_equal(res.t.b.no_bag(), ds(5))

  def test_deep_clone_non_deterministic(self):
    x = bag().obj(a=1)
    self.assertNotEqual(x.deep_clone(), x.deep_clone())

  @parameterized.product(
      pass_schema=[True, False],
  )
  def test_deep_uuid(self, pass_schema):
    db = data_bag.DataBag.empty()
    b_slice = db.new(a=ds([1, None, 2]))
    o = db.obj(b=b_slice, c=ds(['foo', 'bar', 'baz']))
    if pass_schema:
      result = o.deep_uuid(o.get_schema())
    else:
      result = o.deep_uuid()
    with self.assertRaisesRegex(AssertionError, 'not equal by fingerprint'):
      testing.assert_equal(result.S[0], result.S[1])
    odb = data_bag.DataBag.empty()
    o2 = odb.obj(b=odb.new(a=1), c='foo')
    result2 = o2.deep_uuid()
    testing.assert_equal(result2, result.S[0])

  def test_deep_uuid_with_schema_and_seed(self):
    s = bag().new_schema(x=schema_constants.INT32)
    x = bag().obj(x=42, y='abc')
    _ = x.deep_uuid(schema=s, seed='seed')

  def test_call(self):
    with self.assertRaisesRegex(
        TypeError, "data_slice.DataSlice' object is not callable"
    ):
      _ = ds([1, 2, 3])()

  def test_with_name(self):
    x = ds([1, 2, 3])
    y = x.with_name('foo')
    self.assertIs(y, x)

  def test_signatures(self):
    # Tests that all methods have an inspectable signature. This is not added
    # automatically for methods defined in CPython and requires the docstring
    # to follow a specific format.
    for fn_name in dir(data_slice.DataSlice):
      if fn_name.startswith('_'):
        continue
      fn = getattr(data_slice.DataSlice, fn_name)
      if callable(fn):
        _ = inspect.signature(fn)  # Shouldn't raise.

  def test_new(self):
    with self.assertRaisesRegex(NotImplementedError, 'only Schema'):
      _ = ds([1, 2, 3]).new()


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
    x_fb = x.enriched(db)
    with self.assertRaisesRegex(ValueError, 'immutable'):
      x_fb.get_schema().w = schema_constants.INT32
      x_fb.w = x_fb.q + 1

  def test_immutable_with_merging(self):
    db = bag()
    x = db.new(q=1)
    x.get_schema().w = schema_constants.INT32
    x_fb = x.enriched(db)
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

    merged_x = x.enriched(fb_bag)

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
    merged_x = new_x.enriched(db, fb_bag)
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

    merged_x = x.enriched(fb_bag)

    testing.assert_equal(
        merged_x.abc,
        ds([314, None]).with_bag(merged_x.get_bag()),
    )
    testing.assert_equal(
        merged_x.xyz,
        ds([315, '3.17']).with_bag(merged_x.get_bag()),
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

    merged_x = x.enriched(fb_bag)

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
    merged_x = merged_x.enriched(db, fb_bag)
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
      merged_bag = dct.with_bag(merged_bag).enriched(db).get_bag()

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
    with self.assertRaisesRegex(
        TypeError,
        'slice indices must be integers or None or have an __index__ method',
    ):
      _ = [4, 5, 6][ds([4]) : 7]
    with self.assertRaisesRegex(
        TypeError, 'argument must be a string or a real number'
    ):
      float(ds([3.14]))

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

  # More comprehensive tests are in the core_with_attrs_test.py.
  def test_with_attrs(self):
    obj1 = bag().obj(x=1, y=2)
    obj2 = obj1.with_attrs(x=3, z=4)
    testing.assert_equal(obj2.x.no_bag(), ds(3))
    testing.assert_equal(obj2.y.no_bag(), ds(2))
    testing.assert_equal(obj2.z.no_bag(), ds(4))

  # More comprehensive tests are in the core_strict_with_attrs_test.py.
  def test_strict_with_attrs(self):
    e1 = bag().new(x=1, y=2)
    e2 = e1.strict_with_attrs(x=3, y=4)
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
    obj2 = obj1.with_attr('x', 3).with_attr('z', 4)
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

  def test_repr_with_params(self):
    d = fns.obj(a=fns.obj(b=fns.obj(c=fns.obj(d=1))))
    html_str = d._repr_with_params(format_html=True, depth=1)  # pylint: disable=protected-access
    self.assertIn('schema-attr="a"', html_str)
    self.assertNotIn('schema-attr="b"', html_str)

    html_str = d._repr_with_params(format_html=True, depth=2)  # pylint: disable=protected-access
    self.assertIn('schema-attr="a"', html_str)
    self.assertIn('schema-attr="b"', html_str)
    self.assertNotIn('schema-attr="c"', html_str)

    self.assertLess(
        len(ds('a'*1000)._repr_with_params(unbounded_type_max_len=492, depth=2)),  # pylint: disable=protected-access
        500)  # Extra characters are necessary for quotes and ellipsis

    d = fns.obj(a=fns.obj(b=fns.obj(c=fns.obj(d=1))))
    self.assertRegex(
        d._repr_with_params(depth=1), r'Obj\(a=\$[0-9a-zA-Z]{22}\)'  # pylint: disable=protected-access
    )
    self.assertRegex(
        d._repr_with_params(depth=2), r'Obj\(a=Obj\(b=\$[0-9a-zA-Z]{22}\)\)'  # pylint: disable=protected-access
    )

    # Check item_limit behavior
    self.assertEqual(
        fns.list([1, 2, 3, 4, 5])._repr_with_params(item_limit=2),  # pylint: disable=protected-access
        'List[1, 2, ...]',
    )
    self.assertNotIn(
        fns.list(list(range(100)))._repr_with_params(item_limit=-1),  # pylint: disable=protected-access
        '...',
    )

    with self.assertRaisesRegex(TypeError, 'depth must be an integer'):
      d._repr_with_params(depth={})  # pylint: disable=protected-access
    with self.assertRaisesRegex(TypeError, 'item_limit must be an integer'):
      d._repr_with_params(item_limit='abc')  # pylint: disable=protected-access
    with self.assertRaisesRegex(TypeError,
                                'unbounded_type_max_len must be an integer'):
      d._repr_with_params(unbounded_type_max_len={})  # pylint: disable=protected-access
    with self.assertRaisesRegex(TypeError,
                                'format_html must be a boolean'):
      d._repr_with_params(format_html=20)  # pylint: disable=protected-access

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


if __name__ == '__main__':
  absltest.main()
