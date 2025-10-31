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
from koladata.ext.view import kv

Obj = types.SimpleNamespace


class SetItemTest(absltest.TestCase):

  def test_scalar_dict(self):
    x = {'a': 1}
    kv.set_item(kv.view(x), 'a', 2)
    self.assertEqual(x, {'a': 2})
    kv.set_item(kv.view(x), 'b', 3)
    self.assertEqual(x, {'a': 2, 'b': 3})

  def test_scalar_list(self):
    x = [10, 20]
    kv.set_item(kv.view(x), 0, 1)
    self.assertEqual(x, [1, 20])
    kv.set_item(kv.view(x), 1, 2)
    self.assertEqual(x, [1, 2])

  def test_1d_dict(self):
    x = [{'a': 1}, {'a': 2}, {}]
    kv.set_item(kv.view(x)[:], 'a', 3)
    self.assertEqual(x, [{'a': 3}, {'a': 3}, {'a': 3}])
    kv.set_item(kv.view(x)[:], 'b', kv.view([4, 5, 6])[:])
    self.assertEqual(x, [{'a': 3, 'b': 4}, {'a': 3, 'b': 5}, {'a': 3, 'b': 6}])

  def test_1d_list(self):
    x = [[10], [20, 30], [40]]
    kv.set_item(kv.view(x)[:], 0, kv.view([1, 2, 4])[:])
    self.assertEqual(x, [[1], [2, 30], [4]])

  def test_2d_dict(self):
    x = [[{'a': 1}], [{'a': 2}, {}]]
    v = kv.view(x)[:][:]
    kv.set_item(v, 'a', 3)
    self.assertEqual(x, [[{'a': 3}], [{'a': 3}, {'a': 3}]])

  def test_set_item_none_in_view(self):
    x = [{'a': 1}, None, {'a': 2}]
    v = kv.view(x)[:]
    kv.set_item(v, 'a', 3)
    self.assertEqual(x, [{'a': 3}, None, {'a': 3}])

  def test_set_item_none_in_key(self):
    x = [{'a': 1}, {'b': 2}]
    v = kv.view(x)[:]
    kv.set_item(v, kv.view(['a', None])[:], 3)
    self.assertEqual(x, [{'a': 3}, {'b': 2}])

  def test_set_item_none_in_value(self):
    x = [{'a': 1}, {'a': 2}]
    v = kv.view(x)[:]
    kv.set_item(v, 'a', kv.view([3, None])[:])
    self.assertEqual(x, [{'a': 3}, {'a': None}])

  def test_broadcasting_value(self):
    x = [[{'a': 1}], [{'a': 2}, {}]]
    v = kv.view(x)[:][:]
    kv.set_item(v, 'b', kv.view([4, 5])[:])
    self.assertEqual(x, [[{'a': 1, 'b': 4}], [{'a': 2, 'b': 5}, {'b': 5}]])

  def test_broadcasting_key(self):
    x = [[{'a': 1}], [{'a': 2}, {'b': 3}]]
    v = kv.view(x)[:][:]
    kv.set_item(v, kv.view(['c', 'd'])[:], 10)
    self.assertEqual(
        x, [[{'a': 1, 'c': 10}], [{'a': 2, 'd': 10}, {'b': 3, 'd': 10}]]
    )

  def test_multiple_keys_same_dict(self):
    x = {}
    v = kv.view(x)
    kv.set_item(v, kv.view(['c', 'd'])[:], 10)
    self.assertEqual(x, {'c': 10, 'd': 10})
    kv.set_item(v, kv.view(['e', 'f'])[:], kv.view([20, 30])[:])
    self.assertEqual(x, {'c': 10, 'd': 10, 'e': 20, 'f': 30})

  def test_value_depth_too_high_fails(self):
    x = [{}]
    with self.assertRaisesRegex(
        ValueError,
        'The value being set must have same or lower depth',
    ):
      kv.set_item(kv.view(x)[:], 'a', kv.view([[1, 2]])[:][:])

  def test_slice_fails(self):
    x = [[1, 2, 3]]
    with self.assertRaisesRegex(
        ValueError,
        re.escape('slice is not yet supported in View.__setitem__'),
    ):
      kv.set_item(kv.view(x)[:], slice(None), 1)  # pytype: disable=wrong-arg-types

  def test_auto_boxing_scalar_value(self):
    x = {'a': 1}
    kv.set_item(kv.view(x), 'a', 10)
    self.assertEqual(x, {'a': 10})

  def test_no_auto_boxing_of_dict(self):
    x = {'a': 1}
    with self.assertRaisesRegex(
        ValueError,
        re.escape("Cannot automatically box {'a': 1}"),
    ):
      kv.set_item(x, kv.view('a'), kv.view(10))  # pytype: disable=wrong-arg-types

  def test_set_item_with_view_value(self):
    x = {'a': 1}
    kv.set_item(kv.view(x), 'a', kv.view(11))
    self.assertEqual(x, {'a': 11})

  def test_auto_boxing_none(self):
    # This should not fail.
    kv.set_item(None, 'a', 1)

  def test_multiple_instances_of_same_object(self):
    x = {'a': 1}
    kv.set_item(kv.view([x, x])[:], 'a', kv.view([2, 3])[:])
    self.assertEqual(x, {'a': 3})

  def test_multiple_instances_of_same_key(self):
    x = {'a': 1}
    kv.set_item(kv.view(x), kv.view(['a', 'a'])[:], kv.view([2, 3])[:])
    self.assertEqual(x, {'a': 3})

  def test_index_error_is_ignored(self):
    x = [[1], []]
    kv.set_item(kv.view(x)[:], 1, 10)
    self.assertEqual(x, [[1], []])
    kv.set_item(kv.view(x)[:], 0, 20)
    self.assertEqual(x, [[20], []])

  def test_negative_indices(self):
    x = [[1, 2], [3, 4, 5]]
    kv.set_item(kv.view(x)[:], -1, 10)
    self.assertEqual(x, [[1, 10], [3, 4, 10]])


if __name__ == '__main__':
  absltest.main()
