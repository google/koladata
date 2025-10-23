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
from koladata.ext.view import clib_py_ext


class MapStructuresTest(absltest.TestCase):

  def test_eval(self):
    self.assertEqual(clib_py_ext.map_structures(lambda x: x + 1, (0,), (1,)), 2)
    self.assertEqual(clib_py_ext.map_structures(lambda x: x + 1, [0], [1]), 2)
    self.assertEqual(
        clib_py_ext.map_structures(lambda x, y: x - y, (0, 0), (1, 2)), -1
    )
    self.assertEqual(
        clib_py_ext.map_structures(lambda x: x + 1, (1,), ([1, 2],)), [2, 3]
    )
    self.assertEqual(
        clib_py_ext.map_structures(
            lambda x, y: x - y, (1, 1), ([1, 2], [3, 5])
        ),
        [-2, -3],
    )
    self.assertEqual(
        clib_py_ext.map_structures(lambda x, y: x - y, (0, 1), (1, [3, 5])),
        [-2, -4],
    )
    self.assertEqual(
        clib_py_ext.map_structures(lambda x, y: x - y, (1, 0), ([3, 5], 1)),
        [2, 4],
    )
    self.assertEqual(
        clib_py_ext.map_structures(
            lambda x, y, z: x - y + z, (2, 1, 0), ([[3, 5], [7]], [2, 5], 4)
        ),
        [[5, 7], [6]],
    )
    # Kwargs
    self.assertEqual(
        clib_py_ext.map_structures(lambda x: x + 1, (0,), (1,), ('x',)), 2
    )
    self.assertEqual(
        clib_py_ext.map_structures(
            lambda x, y, z: x - y + z,
            (2, 0, 1),
            ([[3, 5], [7]], 4, [2, 5]),
            ('z', 'y'),
        ),
        [[5, 7], [6]],
    )
    # Many args
    self.assertEqual(
        clib_py_ext.map_structures(
            lambda *xs: sum(xs), (0,) * 20, tuple(range(20))
        ),
        sum(range(20)),
    )

  # * Not str kwnames.
  def test_calling_errors(self):
    with self.assertRaisesRegex(
        TypeError,
        'expected 3 or 4 positional arguments but 1 were given',
    ):
      clib_py_ext.map_structures(lambda x: x + 1)
    with self.assertRaisesRegex(
        TypeError,
        'expected 3 or 4 positional arguments but 5 were given',
    ):
      clib_py_ext.map_structures(lambda x: x + 1, 1, 2, 3, 4)
    with self.assertRaisesRegex(
        TypeError, 'expected the first arg to be a callable, got int'
    ):
      clib_py_ext.map_structures(1, (0,), (1,))
    with self.assertRaisesRegex(
        TypeError, 'expected the second arg to be a tuple / list, got int'
    ):
      clib_py_ext.map_structures(lambda x: x + 1, 0, 2)
    with self.assertRaisesRegex(
        TypeError, 'expected the third arg to be a tuple / list, got int'
    ):
      clib_py_ext.map_structures(lambda x: x + 1, (0,), 2)
    with self.assertRaisesRegex(
        TypeError, 'expected the fourth arg to be a tuple, got int'
    ):
      clib_py_ext.map_structures(lambda x: x + 1, (0,), (2,), 3)
    with self.assertRaisesRegex(
        TypeError,
        'expected the number of kwnames to be at most the number of args, but'
        ' got 2 kwnames and 1 args',
    ):
      clib_py_ext.map_structures(lambda x: x + 1, (0,), (2,), ('a', 'b'))
    with self.assertRaisesRegex(
        TypeError,
        re.escape(
            'expected the number of depths to be the same as the number of'
            ' args, but got len(depths): 1, number of args + kwargs: 2'
        ),
    ):
      clib_py_ext.map_structures(lambda x: x + 1, (0,), (1, 2))
    with self.assertRaisesRegex(TypeError, 'expected at least one arg'):
      clib_py_ext.map_structures(lambda: 1, (), ())
    with self.assertRaisesRegex(
        TypeError, 'expected all depths to be integers, got str'
    ):
      clib_py_ext.map_structures(lambda x: 1, ('abc',), (1,))
    with self.assertRaisesRegex(
        ValueError, 'expected all depths to be non-negative, got -5'
    ):
      clib_py_ext.map_structures(lambda x: x + 1, (-5,), (1,))

  def test_eval_errors(self):
    with self.assertRaisesRegex(ValueError, 'test error'):

      def fn(x):
        del x
        raise ValueError('test error')

      clib_py_ext.map_structures(fn, (0,), (1,))

    with self.assertRaisesRegex(
        TypeError, "missing 1 required positional argument: 'y'"
    ):
      clib_py_ext.map_structures(lambda x, y: x + y, (0,), (1,))
    with self.assertRaisesRegex(
        TypeError,
        'expected all lists to be the same length when depth > 0, got 1 and 2',
    ):
      clib_py_ext.map_structures(lambda x, y: x + y, (1, 1), ([1, 2], [3]))
    with self.assertRaisesRegex(
        TypeError,
        'expected the structure to be a list when depth > 0, got int',
    ):
      clib_py_ext.map_structures(lambda x: x, (1,), (1,))
    with self.assertRaisesRegex(TypeError, 'keywords must be strings'):
      clib_py_ext.map_structures(lambda x: x + 1, (0,), (1,), (1,))


if __name__ == '__main__':
  absltest.main()
