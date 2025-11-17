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
import types
from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd
from koladata.ext.view import kv
from koladata.ext.view import test_utils
from koladata.operators.tests.testdata import py_map_py_testdata
from optree import pytree


Obj = types.SimpleNamespace


def maybe_add(x, y):
  if x is None:
    return -1
  if y is None:
    return -2
  return x + y


class DeepMapTest(parameterized.TestCase):

  @parameterized.parameters(
      # Single arg.
      ((kv.view([1, None, 2]),), lambda x: x * 2, [2, None, 4]),
      ((kv.view([1, None, 2])[:],), lambda x: x * 2, (2, None, 4)),
      (
          (kv.view([{'x': 1, 'y': 2, 'z': None}]),),
          lambda x: x * 2,
          [{'x': 2, 'y': 4, 'z': None}],
      ),
      (
          (kv.view([Obj(x=1), None, Obj(x=2)]),),
          lambda o: o.x * 2,
          [2, None, 4],
      ),
      # 2 args.
      ((kv.view(1), kv.view(2)), lambda x, y: x + y, 3),
      ((kv.view(1), None), lambda x, y: x + y, None),
      (
          (kv.view([1, 2, 3]), kv.view([4, None, 6])),
          lambda x, y: x + y,
          [5, None, 9],
      ),
      (
          (kv.view([1, 2, 3])[:], kv.view([4, None, 6])[:]),
          lambda x, y: x + y,
          (5, None, 9),
      ),
      (
          (kv.view([1, 2, None, None])[:], kv.view([5, None, 7, None])[:]),
          lambda x, y: x + y,
          (6, None, None, None),
      ),
      # Broadcasting.
      ((kv.view([1, 2, 3])[:], kv.view(4)), lambda x, y: x + y, (5, 6, 7)),
      (
          (kv.view([[1, 2], [3, 4]])[:], kv.view([5, 6])),
          lambda x, y: x + y,
          ([6, 8], [8, 10]),
      ),
      # 3 args.
      (
          (kv.view([1, 2, 3])[:], kv.view(4), kv.view([-1, None, 5])[:]),
          lambda x, y, z: x + y + z,
          (4, None, 12),
      ),
  )
  def test_eval(self, args, fn, expected):
    self.assertEqual(kv.deep_map(fn, *args).get(), expected)
    if len(args) == 1:
      # Sanity check.
      self.assertEqual(args[0].deep_map(fn).get(), expected)

  @parameterized.parameters(
      (
          (kv.view([1, None, 2]),),
          lambda x: -1 if x is None else x * 2,
          [2, -1, 4],
      ),
      ((kv.view([None, 2, 3]), kv.view([4, None, 6])), maybe_add, [-1, -2, 9]),
      ((kv.view([None, 2, 3]), kv.view([None, 5, 6])), maybe_add, [-1, 7, 9]),
  )
  def test_include_missing(self, args, fn, expected):
    self.assertEqual(
        kv.deep_map(
            fn,
            *args,
            include_missing=True,
        ).get(),
        expected,
    )
    if len(args) == 1:
      # Sanity check.
      self.assertEqual(
          args[0].deep_map(fn, include_missing=True).get(), expected
      )

  @parameterized.named_parameters(*py_map_py_testdata.TEST_CASES)
  def test_map_py_parity(self, fn, args, kwargs, expected):
    args = [
        test_utils.from_ds(x) if isinstance(x, kd.types.DataSlice) else x
        for x in args
    ]
    kwargs = {
        k: test_utils.from_ds(v) if isinstance(v, kd.types.DataSlice) else v
        for k, v in kwargs.items()
        if k != 'schema'
    }
    for kw in kwargs:
      if kw != 'include_missing':
        self.skipTest(f'kwarg {kw} is not supported in deep_map')

    expected = test_utils.from_ds(expected)

    with self.subTest('default_behavior'):
      res = kv.deep_map(fn, *args, **kwargs)
      test_utils.assert_equal(res, expected)

    with self.subTest('optree_map_parity'):
      # Ensures that our concept of "traversal" and "include_missing"
      # corresponds with OpTree's.
      args = [arg.implode(-1) for arg in kv.align(*args)]
      res = kv.deep_map(fn, *args, **kwargs)
      test_utils.assert_equal(res, expected.implode(-1))

  def test_auto_boxing_scalar_value(self):
    self.assertEqual(kv.deep_map(lambda x: x * 2, 1).get(), 2)

  def test_no_auto_boxing_for_object(self):
    x = Obj(a=1)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a View or a boxable typle, got namespace(a=1)'),
    ):
      kv.deep_map(lambda x: x.a, x)  # pytype: disable=wrong-arg-types

  def test_incompatible_views(self):
    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 2 and 3',
    ):
      kv.deep_map(lambda x, y: x + y, kv.view([1, 2, 3])[:], kv.view([4, 5])[:])

  def test_incompatible_structures(self):
    with self.assertRaisesRegex(ValueError, 'list arity mismatch'):
      kv.deep_map(lambda x, y: x + y, kv.view([1, 2, 3]), kv.view([4, 5]))

  def test_pytree_does_not_broadcast(self):
    # Does _not_ broadcast, unlike pytree.broadcast_map.
    with self.assertRaisesRegex(ValueError, 'Expected an instance of list'):
      kv.deep_map(lambda x, y: x + y, kv.view([1, 2, 3]), kv.view(2))

  def test_pytree_attempts_broadcast_with_none(self):
    with self.assertRaisesRegex(ValueError, 'Expected an instance of list'):
      kv.deep_map(lambda x, y: None, kv.view([1, 2]), kv.view(None))

  def test_custom_handler(self):
    class Cls:

      def __init__(self, x, y):
        self.x = x
        self.y = y

    with self.assertRaisesRegex(TypeError, 'unsupported operand'):
      kv.deep_map(lambda x: x + 1, kv.view(Cls(1, 2)))

    pytree.register_node(
        Cls,
        lambda c: ((c.x, c.y), None, None),
        lambda _, children: Cls(*children),
        namespace='test_custom_handler',
    )

    # Need to specify the namespace.
    with self.assertRaisesRegex(TypeError, 'unsupported operand'):
      kv.deep_map(lambda x: x + 1, kv.view(Cls(1, 2)))

    res = kv.deep_map(
        lambda x: x + 1, kv.view(Cls(1, 2)), namespace='test_custom_handler'
    ).get()
    self.assertIsInstance(res, Cls)
    self.assertEqual(res.x, 2)
    self.assertEqual(res.y, 3)
    pytree.unregister_node(Cls, namespace='test_custom_handler')


if __name__ == '__main__':
  absltest.main()
