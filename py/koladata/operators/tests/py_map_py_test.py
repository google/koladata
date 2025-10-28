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

"""Tests for kde.py.map_py operator."""

import functools
import re
import threading
import time

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.testdata import py_map_py_testdata
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde


class PyMapPyTest(parameterized.TestCase):

  @parameterized.named_parameters(*py_map_py_testdata.TEST_CASES)
  def test_eval(self, fn, args, kwargs, expected):
    res = kd.py.map_py(fn, *args, **kwargs)
    testing.assert_equivalent(res, expected)
    # Currently assert_equivalent ignores schema on empty slices, so we check
    # schema separately.
    testing.assert_equal(
        res.get_schema().no_bag(), expected.get_schema().no_bag()
    )
    self.assertFalse(res.is_mutable())

  def test_map_py_object_argument(self):
    x = functions.obj(y=ds([[1, 2], [3]]), z=ds([[6, 7], [8]]))
    res = kd.py.map_py(lambda x: x.y + x.z, x)
    testing.assert_equal(res.no_bag(), ds([[7, 9], [11]]))

    # In all modes except SKIP we can infer the qtype for empty values in
    # x.y and x.z. To work around this, we use with_schema in the lambda.
    my_add = lambda x: x.y + x.z
    x = functions.obj(
        y=ds([[1, 2], [3, None, 5]]), z=ds([[6, 7], [8, 9, None]])
    )
    res = kd.py.map_py(my_add, x)
    testing.assert_equal(res.no_bag(), ds([[7, 9], [11, None, None]]))

    with self.subTest('object_results'):

      def my_lambda(x):
        if x.y < 3:
          return functions.obj(x=1, y=2)
        return functions.obj(x=2, y=1)

      x = functions.obj(y=ds([[1, 2], [3]]))
      res = kd.py.map_py(my_lambda, x)
      testing.assert_equal(res.x.no_bag(), ds([[1, 1], [2]]))
      testing.assert_equal(res.y.no_bag(), ds([[2, 2], [1]]))
      self.assertFalse(res.is_mutable())

      res = kd.py.map_py(my_lambda, x.S[0, 0])
      testing.assert_equal(res.x.no_bag(), ds(1))
      testing.assert_equal(res.y.no_bag(), ds(2))
      self.assertFalse(res.is_mutable())

  def test_map_py_single_thread(self):
    thread_idents = {threading.get_ident()}

    def add_one(x):
      thread_idents.add(threading.get_ident())
      return x + 1

    val = ds(list(range(10**3)))
    _ = kd.py.map_py(add_one, val, max_threads=1)
    self.assertEqual(thread_idents, {threading.get_ident()})

  def test_map_py_multi_thread(self):
    thread_idents = {threading.get_ident()}

    def add_one(x):
      thread_idents.add(threading.get_ident())
      return x + 1

    val = ds(list(range(10**3)))
    _ = kd.py.map_py(add_one, val, max_threads=10)
    self.assertGreater(thread_idents, {threading.get_ident()})

  def test_map_py_with_qtype(self):
    def add_one(x):
      return x + 1

    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    res = kd.py.map_py(add_one, val, schema=schema_constants.FLOAT32)
    testing.assert_equal(
        res.no_bag(),
        ds([[2.0, 3.0, None, 5.0], [None, None], [8.0, 9.0, 10.0]]),
    )

    res = kd.py.map_py(add_one, ds([]), schema=schema_constants.FLOAT32)
    testing.assert_equal(res.no_bag(), ds([], schema_constants.FLOAT32))

  def test_map_py_with_schema(self):
    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    schema = bag().new_schema(
        u=schema_constants.INT32, v=schema_constants.INT32
    )  # mutable

    def my_func_dynamic_schema(x):
      return functions.new(u=x, v=x + 1)

    def my_func_obj_schema(x):
      return functions.new(u=x, v=x + 1, schema=schema_constants.OBJECT)

    def my_func_correct_schema(x):
      return functions.new(u=x, v=x + 1, schema=schema)

    res = kd.py.map_py(my_func_correct_schema, val, schema=schema)
    self.assertEqual(res.get_schema(), schema)
    testing.assert_equal(
        res.v.no_bag(), ds([[2, 3, None, 5], [None, None], [8, 9, 10]])
    )
    self.assertFalse(res.is_mutable())

    # Use eval_op to preserve mutability of the DataSlice which is tested below.
    res = kd.py.map_py(my_func_correct_schema, ds([]), schema=schema)
    self.assertEqual(res.get_schema(), schema)
    testing.assert_equal(res.v.no_bag(), ds([], schema_constants.INT32))
    self.assertTrue(res.is_mutable())
    testing.assert_equal(res.get_bag(), schema.get_bag())

    res = kd.py.map_py(
        my_func_correct_schema, val, schema=schema_constants.OBJECT
    )
    self.assertEqual(res.get_ndim(), 2)
    self.assertEqual(res.get_schema(), schema_constants.OBJECT)
    self.assertEqual(res.get_obj_schema().S[2, 1], schema)
    self.assertEqual(res.get_obj_schema().S[2, 1].u, schema_constants.INT32)
    self.assertEqual(res.get_obj_schema().S[2, 1].v, schema_constants.INT32)
    testing.assert_equal(
        res.v.no_bag(), ds([[2, 3, None, 5], [None, None], [8, 9, 10]])
    )
    self.assertFalse(res.is_mutable())

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('cannot find a common schema')
        ),
    ):
      _ = kd.py.map_py(my_func_dynamic_schema, ds([1, 2]), schema=schema)

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('cannot create Item(s) with the provided schema: OBJECT')
        ),
    ):
      _ = kd.py.map_py(my_func_obj_schema, ds([1, 2]), schema=schema)

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('expected a schema, got schema=1')
        ),
    ):
      _ = kd.py.map_py(my_func_correct_schema, val, schema=1)

    with self.assertRaisesRegex(
        ValueError,
        re.escape("""the schema is incompatible:
expected schema: ENTITY(u=INT32, v=INT32)
assigned schema: ENTITY(u=INT64)"""),
    ):
      _ = kd.py.map_py(
          my_func_correct_schema,
          val,
          schema=kd.schema.new_schema(u=schema_constants.INT64),
      )

    db = data_bag.DataBag.empty_mutable()
    schema_same_bag = db.new_schema(
        u=schema_constants.INT32, v=schema_constants.INT32
    )

    def my_func_same_bag(schema, x):
      return db.new(u=x, v=x + 1, schema=schema)

    res = kd.py.map_py(
        functools.partial(my_func_same_bag, schema_same_bag),
        val,
        schema=schema_same_bag,
    )
    self.assertEqual(res.get_schema(), schema_same_bag)
    testing.assert_equal(
        res.v.no_bag(), ds([[2, 3, None, 5], [None, None], [8, 9, 10]])
    )
    self.assertTrue(res.is_mutable())
    testing.assert_equal(res.get_bag(), db)

    res = kd.py.map_py(
        functools.partial(my_func_same_bag, schema), val, schema=schema
    )
    self.assertEqual(res.get_schema(), schema)
    testing.assert_equal(
        res.v.no_bag(), ds([[2, 3, None, 5], [None, None], [8, 9, 10]])
    )
    self.assertFalse(res.is_mutable())

    res = kd.py.map_py(
        functools.partial(my_func_same_bag, schema_same_bag),
        val,
        schema=schema_constants.OBJECT,
    )
    self.assertEqual(res.get_schema(), schema_constants.OBJECT)
    self.assertEqual(res.get_obj_schema().S[2, 1].u, schema_constants.INT32)
    self.assertEqual(res.get_obj_schema().S[2, 1], schema_same_bag)
    testing.assert_equal(
        res.v.no_bag(), ds([[2, 3, None, 5], [None, None], [8, 9, 10]])
    )
    self.assertFalse(res.is_mutable())

  def test_map_py_invalid_qtype(self):
    def as_set(x):
      return {x}

    val = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex('unsupported type: set'),
    ):
      kd.py.map_py(as_set, val)

  def test_map_py_incompatible_inputs(self):
    def add_x_y(x, y):
      return x + y

    val1 = ds([[1, 2, None, 4], [None, None], [7, 8, 9]])
    val2 = ds([[0, 1, None, 2], [3, 4], []])
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                'DataSlice with shape=JaggedShape(3, [4, 2, 0]) cannot be'
                ' expanded to shape=JaggedShape(3, [4, 2, 3])'
            )
        ),
    ):
      kd.py.map_py(add_x_y, val1, val2)

  def test_map_py_no_inputs(self):
    with self.assertRaisesWithPredicateMatch(
        TypeError,
        arolla.testing.any_cause_message_regex(
            'expected at least one input DataSlice, got none'
        ),
    ):
      kd.py.map_py(lambda: None)

  def test_map_py_item_invalid_fn(self):
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('expected a python callable, got fn=PyObject{1}')
        ),
    ):
      kd.py.map_py(arolla.abc.PyObject(1), ds(list(range(10))))

  def test_map_py_invalid_item_completed_callback(self):
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                'expected a python callable, got'
                ' item_completed_callback=DataItem(1, schema: INT32)'
            )
        ),
    ):
      kd.py.map_py(
          lambda x: None, ds(list(range(10))), item_completed_callback=1
      )

  def test_map_py_ndim_errors(self):
    val = ds([[[1, 2, None, 4], [None, None], [7, 8, 9]], [[3, 3, 5]]])

    def agg_count(x):
      return len([i for i in x if i is not None])

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'ndim should be between 0 and 3, got ndim=-1'
        ),
    ):
      kd.py.map_py(agg_count, val, ndim=-1)

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'ndim should be between 0 and 3, got ndim=4'
        ),
    ):
      kd.py.map_py(agg_count, val, ndim=4)

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'expected a scalar integer, got ndim=None'
        ),
    ):
      kd.py.map_py(agg_count, val, ndim=None)

  def test_map_py_mixed_scalars_and_slices(self):
    res = kd.py.map_py(lambda x: x if x % 2 else ds(x), ds([1, 2, 3, 4]))
    testing.assert_equal(res.no_bag(), ds([1, 2, 3, 4]))

  def test_map_py_invalid_include_missing(self):
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            'expected a scalar boolean, got include_missing=1'
        ),
    ):
      _ = kd.py.map_py(lambda x: x, ds([0]), include_missing=1)
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            '`include_missing=False` can only be used with `ndim=0`',
        ),
    ):
      _ = kd.py.map_py(lambda x: x, ds([0]), ndim=1, include_missing=False)

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

    res = kd.py.map_py(
        lambda x: x + 1,
        ds(list(range(size))),
        max_threads=max_threads,
        item_completed_callback=increment_counter,
    )
    testing.assert_equal(res.no_bag(), ds(list(range(1, size + 1))))
    self.assertEqual(counter, size)
    self.assertEqual(total, sum(range(size)) + size)

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.py.map_py(I.fn, I.arg)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.py.map_py, kde.map_py))

  def test_repr(self):
    self.assertEqual(
        repr(kde.py.map_py(I.fn, I.x, a=I.a)),
        'kd.py.map_py(I.fn, I.x,'
        ' schema=DataItem(None, schema: NONE),'
        ' max_threads=DataItem(1, schema: INT32),'
        ' ndim=DataItem(0, schema: INT32),'
        ' include_missing=DataItem(None, schema: NONE),'
        ' item_completed_callback=DataItem(None, schema: NONE),'
        ' a=I.a'
        ')',
    )

  def test_cancellation(self):
    n = 5
    start_barrier = threading.Barrier(n, action=arolla.abc.simulate_SIGINT)
    stop_barrier = threading.Barrier(n)

    def fn(_):
      start_barrier.wait(0.1)
      try:
        while True:
          time.sleep(0.02)
          arolla.abc.raise_if_cancelled()
      finally:
        stop_barrier.wait(0.1)

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(r'\[CANCELLED\].*interrupted'),
    ):
      kd.py.map_py(fn, ds(list(range(n))), max_threads=2 * n)

  def test_understandable_error(self):
    def foo(x):
      return x + I.x

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'unsupported type, arolla.abc.expr.Expr, for value:\n\n '
            f' {ds(1) + I.x}'
        ),
    ):
      kd.py.map_py(foo, ds(1))
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_note_regex(
            re.escape(
                f'Error occurred during evaluation of kd.map_py with fn={foo}'
            )
        ),
    ):
      kd.py.map_py(foo, ds(1))


if __name__ == '__main__':
  absltest.main()
