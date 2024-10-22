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

"""Tests for kde.py.map_py operator."""

import functools
import re
import threading

from absl.testing import absltest
from absl.testing import parameterized
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class PyMapPyTest(parameterized.TestCase):

  def test_map_py_single_arg(self):
    def add_one(x):
      return x + 1 if x is not None else None

    x = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    res = expr_eval.eval(kde.py.map_py(add_one, x))
    testing.assert_equal(
        res.no_bag(), ds([[2, 3, None, 5], [None, None], [8, 9, 10]])
    )

  def test_map_py_object_argument(self):
    x = functions.obj(y=ds([[1, 2], [3]]), z=ds([[6, 7], [8]]))
    res = expr_eval.eval(kde.py.map_py(lambda x: x.y + x.z, x))
    testing.assert_equal(res.no_bag(), ds([[7, 9], [11]]))

    # In all modes except SKIP we can infer the qtype for empty values in
    # x.y and x.z. To work around this, we use with_schema in the lambda.
    my_add = lambda x: x.y + x.z
    x = functions.obj(
        y=ds([[1, 2], [3, None, 5]]),
        z=ds([[6, 7], [8, 9, None]]),
    )
    res = expr_eval.eval(kde.py.map_py(my_add, x))
    testing.assert_equal(res.no_bag(), ds([[7, 9], [11, None, None]]))

    with self.subTest('object_results'):

      def my_lambda(x):
        if x.y < 3:
          return functions.obj(x=1, y=2)
        return functions.obj(x=2, y=1)

      x = functions.obj(y=ds([[1, 2], [3]]))
      res = expr_eval.eval(kde.py.map_py(my_lambda, x))
      testing.assert_equal(res.x.no_bag(), ds([[1, 1], [2]]))
      testing.assert_equal(res.y.no_bag(), ds([[2, 2], [1]]))
      # TODO: b/323305977 - This should be addressed in .from_py.
      # self.assertFalse(res.is_mutable())

      res = expr_eval.eval(kde.py.map_py(my_lambda, x.S[0, 0]))
      testing.assert_equal(res.x.no_bag(), ds(1))
      testing.assert_equal(res.y.no_bag(), ds(2))
      self.assertTrue(res.is_mutable())

  def test_map_py_return_none(self):
    def return_none(x):
      del x
      return None

    val = ds([[1], [None, None], [7, 8, 9]])
    res = expr_eval.eval(kde.py.map_py(return_none, val))
    testing.assert_equal(
        res.no_bag(), ds([[None], [None, None], [None, None, None]])
    )

  def test_map_py_single_thread(self):
    thread_idents = {threading.get_ident()}

    def add_one(x):
      thread_idents.add(threading.get_ident())
      return x + 1

    val = ds(list(range(10**3)))
    _ = expr_eval.eval(kde.py.map_py(add_one, val, max_threads=1))
    self.assertEqual(thread_idents, {threading.get_ident()})

  def test_map_py_multi_thread(self):
    thread_idents = {threading.get_ident()}

    def add_one(x):
      thread_idents.add(threading.get_ident())
      return x + 1

    val = ds(list(range(10**3)))
    _ = expr_eval.eval(kde.py.map_py(add_one, val, max_threads=10))
    self.assertGreater(thread_idents, {threading.get_ident()})

  def test_map_py_multi_args(self):
    def add_all(x, y, z):
      if x is None or y is None or z is None:
        return None
      return x + y + z

    val1 = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    val2 = ds([[0, 1, None, 2], [3, 4], [6, 7, 8]])
    val3 = ds([[2, None, 4, 5], [6, 7], [None, 9, 10]])
    res = expr_eval.eval(kde.py.map_py(add_all, val1, val2, val3))
    testing.assert_equal(
        res.no_bag(), ds([[3, None, None, 11], [None, None], [None, 24, 27]])
    )

  def test_map_py_texting_output(self):
    def as_string(x):
      return str(x) if x is not None else None

    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    res = expr_eval.eval(kde.py.map_py(as_string, val))
    testing.assert_equal(
        res.no_bag(), ds([['1', '2', None, '4'], [None, None], ['7', '8', '9']])
    )

  def test_map_py_texting_input(self):
    def as_string(x):
      return int(x) if x is not None else None

    val = ds([['1', '2', None, '4'], [None, None], ['7', '8', '9']])
    res = expr_eval.eval(kde.py.map_py(as_string, val))
    testing.assert_equal(
        res.no_bag(), ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    )

  def test_map_py_with_qtype(self):
    def add_one(x):
      return x + 1 if x is not None else None

    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    res = expr_eval.eval(
        kde.py.map_py(add_one, val, schema=schema_constants.FLOAT32)
    )
    testing.assert_equal(
        res.no_bag(),
        ds([[2.0, 3.0, None, 5.0], [None, None], [8.0, 9.0, 10.0]]),
    )

    res = expr_eval.eval(
        kde.py.map_py(add_one, ds([]), schema=schema_constants.FLOAT32)
    )
    testing.assert_equal(res.no_bag(), ds([], schema_constants.FLOAT32))

  def test_map_py_with_schema(self):
    schema = functions.new_schema(
        u=schema_constants.INT32, v=schema_constants.INT32
    )

    def my_func_dynamic_schema(x):
      return None if x is None else functions.new(u=x, v=x + 1)

    def my_func_any_schema(x):
      return (
          None
          if x is None
          else functions.new(u=x, v=x + 1, schema=schema_constants.ANY)
      )

    def my_func_correct_schema(x):
      return None if x is None else functions.new(u=x, v=x + 1, schema=schema)

    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    schema = functions.new_schema(
        u=schema_constants.INT32, v=schema_constants.INT32
    )
    res = expr_eval.eval(
        kde.py.map_py(my_func_correct_schema, val, schema=schema)
    )
    self.assertEqual(res.get_schema(), schema)
    testing.assert_equal(
        res.v.no_bag(), ds([[2, 3, None, 5], [None, None], [8, 9, 10]])
    )
    # TODO: b/323305977 - This should be addressed in .from_py.
    # self.assertFalse(res.is_mutable())

    res = expr_eval.eval(
        kde.py.map_py(my_func_correct_schema, ds([]), schema=schema)
    )
    self.assertEqual(res.get_schema(), schema)
    testing.assert_equal(res.v.no_bag(), ds([], schema_constants.INT32))
    self.assertTrue(res.is_mutable())
    # TODO: b/323305977 - This should be addressed in .from_py.
    # self.assertIs(res.get_bag(), schema.get_bag())

    res = expr_eval.eval(
        kde.py.map_py(
            my_func_correct_schema, val, schema=schema_constants.OBJECT
        )
    )
    self.assertEqual(res.get_ndim(), 2)
    self.assertEqual(res.get_schema(), schema_constants.OBJECT)
    self.assertEqual(res.get_obj_schema().S[2, 1], schema)
    self.assertEqual(res.get_obj_schema().S[2, 1].u, schema_constants.INT32)
    self.assertEqual(res.get_obj_schema().S[2, 1].v, schema_constants.INT32)
    testing.assert_equal(
        res.v.no_bag(), ds([[2, 3, None, 5], [None, None], [8, 9, 10]])
    )
    # TODO: b/323305977 - This should be addressed in .from_py.
    # self.assertFalse(res.is_mutable())

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape('cannot find a common schema for provided schemas'),
    ):
      _ = expr_eval.eval(
          kde.py.map_py(my_func_dynamic_schema, ds([1, 2]), schema=schema)
      )

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape('the schema for List item is incompatible'),
    ):
      _ = expr_eval.eval(
          kde.py.map_py(my_func_any_schema, ds([1, 2]), schema=schema)
      )

    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected a schema, got schema=1'
    ):
      _ = expr_eval.eval(kde.py.map_py(my_func_correct_schema, val, schema=1))

    db = data_bag.DataBag.empty()
    schema_same_bag = db.new_schema(
        u=schema_constants.INT32, v=schema_constants.INT32
    )

    def my_func_same_bag(schema, x):
      return None if x is None else db.new(u=x, v=x + 1, schema=schema)

    res = expr_eval.eval(
        kde.py.map_py(
            functools.partial(my_func_same_bag, schema_same_bag),
            val,
            schema=schema_same_bag,
        )
    )
    self.assertEqual(res.get_schema(), schema_same_bag)
    testing.assert_equal(
        res.v.no_bag(), ds([[2, 3, None, 5], [None, None], [8, 9, 10]])
    )
    self.assertTrue(res.is_mutable())
    # TODO: b/323305977 - This should be addressed in .from_py.
    # self.assertIs(res.get_bag(), db)

    res = expr_eval.eval(
        kde.py.map_py(
            functools.partial(my_func_same_bag, schema), val, schema=schema
        )
    )
    self.assertEqual(res.get_schema(), schema)
    testing.assert_equal(
        res.v.no_bag(), ds([[2, 3, None, 5], [None, None], [8, 9, 10]])
    )
    # We had to create new triples to adopt the schema, so the DataBag is new.
    # TODO: b/323305977 - This should be addressed in .from_py.
    # self.assertFalse(res.is_mutable())

    res = expr_eval.eval(
        kde.py.map_py(
            functools.partial(my_func_same_bag, schema_same_bag),
            val,
            schema=schema_constants.OBJECT,
        )
    )
    self.assertEqual(res.get_schema(), schema_constants.OBJECT)
    self.assertEqual(res.get_obj_schema().S[2, 1].u, schema_constants.INT32)
    self.assertEqual(res.get_obj_schema().S[2, 1], schema_same_bag)
    testing.assert_equal(
        res.v.no_bag(), ds([[2, 3, None, 5], [None, None], [8, 9, 10]])
    )
    # We had to create new triples to embed the schema, so the DataBag is new.
    # TODO: b/323305977 - This should be addressed in .from_py.
    # self.assertFalse(res.is_mutable())

  def test_map_py_empty_input(self):
    def add_one(x):
      return x + 1 if x is not None else None

    val = ds([[]])
    res = expr_eval.eval(
        kde.py.map_py(add_one, val, schema=schema_constants.FLOAT32)
    )
    testing.assert_equal(res.no_bag(), ds([[]], schema_constants.FLOAT32))

  def test_map_py_scalar_input(self):
    def add_one(x):
      return x + 1 if x is not None else None

    val = ds(5)
    res = expr_eval.eval(kde.py.map_py(add_one, val))
    testing.assert_equal(res.no_bag(), ds(6))

  def test_map_py_auto_expand(self):
    def my_add(x, y):
      if x is None or y is None:
        return None
      return x + y

    val1 = ds(1)
    val2 = ds([[0, 1, None, 2], [3, 4], [6, 7, 8]])
    res = expr_eval.eval(kde.py.map_py(my_add, val1, val2))
    testing.assert_equal(res.no_bag(), ds([[1, 2, None, 3], [4, 5], [7, 8, 9]]))

  def test_map_py_raw_input(self):
    def my_add(x, y):
      if x is None or y is None:
        return None
      return x + y

    res = expr_eval.eval(
        kde.py.map_py(my_add, 1, ds([[0, 1, None, 2], [3, 4], [6, 7, 8]]))
    )
    testing.assert_equal(res.no_bag(), ds([[1, 2, None, 3], [4, 5], [7, 8, 9]]))

  def test_map_py_dict(self):
    def as_dict(x):
      return {'x': x, 'y': x + 1 if x is not None else 57}

    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    res = expr_eval.eval(kde.py.map_py(as_dict, val))
    testing.assert_equal(
        res['x'].no_bag(), ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    )
    testing.assert_equal(
        res['y'].no_bag(), ds([[2, 3, 57, 5], [57, 57], [8, 9, 10]])
    )

  def test_map_py_invalid_qtype(self):
    def as_set(x):
      return {x}

    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])

    with self.assertRaisesRegex(ValueError, 'unsupported type: "set"'):
      expr_eval.eval(kde.py.map_py(as_set, val))

  def test_map_py_incompatible_inputs(self):
    def add_x_y(x, y):
      if x is None or y is None:
        return None
      return x + y

    val1 = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    val2 = ds([[0, 1, None, 2], [3, 4], []])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'DataSlice with shape=JaggedShape(3, [4, 2, 0]) cannot be expanded'
            ' to shape=JaggedShape(3, [4, 2, 3])'
        ),
    ):
      expr_eval.eval(kde.py.map_py(add_x_y, val1, val2))

  def test_map_py_kwargs(self):
    def my_fn(x, y, z=2, **kwargs):
      if x is None or y is None or z is None:
        return None
      return x + y + z + kwargs.get('w', 5)

    x = ds([[0, 0, 1], [None, 1, 0]])
    y = ds([[0, None, 1], [-1, 0, 1]])
    z = ds([[1, 2, 3], [4, 5, 6]])
    w = ds([[-1, -1, 0], [1, 0, 0]])
    res = expr_eval.eval(kde.py.map_py(my_fn, x, y=y, z=z, w=w))
    res2 = expr_eval.eval(kde.py.map_py(my_fn, x, y, z=z, w=w))
    res3 = expr_eval.eval(kde.py.map_py(my_fn, x, y, z, w=w))
    res4 = expr_eval.eval(kde.py.map_py(my_fn, x, y, z))
    testing.assert_equal(res.no_bag(), ds([[0, None, 5], [None, 6, 7]]))
    testing.assert_equal(res.no_bag(), res2.no_bag())
    testing.assert_equal(res.no_bag(), res3.no_bag())
    testing.assert_equal(res4.no_bag(), ds([[6, None, 10], [None, 11, 12]]))

  def test_map_py_no_inputs(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected at least one input DataSlice, got none'
    ):
      expr_eval.eval(kde.py.map_py(lambda: None))

  def test_map_py_item_completed_callback(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected a python callable, got item_completed_callback=1'
    ):
      expr_eval.eval(
          kde.py.map_py(
              lambda x: None, ds(list(range(10))), item_completed_callback=1
          )
      )

  def test_map_py_ndim(self):
    val = ds([[[1, 2, None, 4], [None, None], [7, 8, 9]], [[3, 3, 5]]])
    with self.subTest('ndim_1'):

      def agg_count(x):
        return len([i for i in x if i is not None])

      res = expr_eval.eval(kde.py.map_py(agg_count, val, ndim=1))
      testing.assert_equal(res.no_bag(), ds([[3, 0, 3], [3]]))

    with self.subTest('ndim_2'):

      def agg_count2(x):
        return sum([agg_count(y) for y in x])

      res = expr_eval.eval(kde.py.map_py(agg_count2, val, ndim=2))
      testing.assert_equal(res.no_bag(), ds([6, 3]))

    with self.subTest('ndim_max'):

      def agg_count3(x):
        return sum([agg_count2(y) for y in x])

      res = expr_eval.eval(kde.py.map_py(agg_count3, val, ndim=3))
      testing.assert_equal(res.no_bag(), ds(9))

    with self.subTest('ndim_invalid'):
      with self.assertRaisesWithLiteralMatch(
          ValueError, 'ndim should be between 0 and 3, got ndim=-1'
      ):
        expr_eval.eval(kde.py.map_py(agg_count, val, ndim=-1))

      with self.assertRaisesWithLiteralMatch(
          ValueError, 'ndim should be between 0 and 3, got ndim=4'
      ):
        expr_eval.eval(kde.py.map_py(agg_count, val, ndim=4))

  def test_map_py_expanded_results(self):
    val = ds(
        [[1, 2, None], [4, 5]],
    )

    with self.subTest('expand_one_dim'):

      def ranges(x):
        return list(range(x or 0))

      res = expr_eval.eval(kde.py.map_py(ranges, val))
      self.assertEqual(res.get_ndim(), 2)
      self.assertEqual(
          res[:].to_py(),  # TODO: Change to res.to_py().
          [[[0], [0, 1], []], [[0, 1, 2, 3], [0, 1, 2, 3, 4]]],
      )

    with self.subTest('expand_several_dims'):

      def expnd(x):
        return [[x, -1]]

      res = expr_eval.eval(kde.py.map_py(expnd, val))
      self.assertEqual(res.get_ndim(), 2)
      self.assertEqual(
          res[:][:].to_py(),  # TODO: Change to res.to_py().
          [[[[1, -1]], [[2, -1]], [[None, -1]]], [[[4, -1]], [[5, -1]]]],
      )

    with self.subTest('agg_and_expand'):

      def agg_and_expand(x):
        return [sum(y for y in x if y is not None), -1]

      res = expr_eval.eval(kde.py.map_py(agg_and_expand, val, ndim=1))
      self.assertEqual(res.get_ndim(), 1)
      self.assertEqual(
          res[:].to_py(),  # TODO: Change to res.to_py().
          [[3, -1], [9, -1]],
      )

    with self.subTest('expand_sparse'):

      def expand_sparse(x):
        return [x] if x is not None else None

      res = expr_eval.eval(kde.py.map_py(expand_sparse, val))
      self.assertEqual(res.get_ndim(), 2)
      # TODO: Change to
      #     self.assertEqual(res.to_py(), [[[1], [2], None], [[4], [5]]])
      self.assertEqual(res[:].to_py(), [[[1], [2], []], [[4], [5]]])

  def test_map_py_mixed_scalars_and_slices(self):
    res = expr_eval.eval(
        kde.py.map_py(lambda x: x if x % 2 else ds(x), ds([1, 2, 3, 4]))
    )
    testing.assert_equal(res.no_bag(), ds([1, 2, 3, 4]))

  @parameterized.parameters(1, 10)
  def test_map_py_with_item_completed_callback(self, max_threads):
    size = 100
    counter = 0
    total = 0
    main_thread = threading.current_thread()

    def increment_counter(result):
      nonlocal counter, total
      self.assertIs(
          threading.current_thread(),
          main_thread,
          'item_completed_callback should be called from the main thread',
      )
      counter += 1
      total += result

    res = expr_eval.eval(
        kde.py.map_py(
            lambda x: x + 1,
            ds(list(range(size))),
            max_threads=max_threads,
            item_completed_callback=increment_counter,
        )
    )
    testing.assert_equal(res.no_bag(), ds(list(range(1, size + 1))))
    self.assertEqual(counter, size)
    self.assertEqual(total, sum(range(size)) + size)

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.py.map_py(I.fn, I.arg)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.py.map_py, kde.map_py))

  def test_repr(self):
    self.assertEqual(
        repr(kde.py.map_py(I.fn, I.x, a=I.a)),
        'kde.py.map_py(I.fn, I.x, schema=DataItem(None, schema: NONE),'
        ' max_threads=DataItem(1, schema: INT32), ndim=DataItem(0, schema:'
        ' INT32), item_completed_callback=DataItem(None, schema: NONE), a=I.a)',
    )


if __name__ == '__main__':
  absltest.main()
