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


class SetAttrTest(absltest.TestCase):

  def test_scalar(self):
    x = Obj(a=1)
    kv.set_attr(kv.view(x), 'a', 2)
    self.assertEqual(x.a, 2)
    kv.set_attr(kv.view(x), 'b', 3)
    self.assertEqual(x.b, 3)

  def test_1d(self):
    x = [Obj(a=1), Obj(a=2), Obj()]
    kv.set_attr(kv.view(x)[:], 'a', 3)
    self.assertEqual(x[0].a, 3)
    self.assertEqual(x[1].a, 3)
    self.assertEqual(x[2].a, 3)
    kv.set_attr(kv.view(x)[:], 'b', kv.view([4, 5, 6])[:])
    self.assertEqual(x[0].b, 4)
    self.assertEqual(x[1].b, 5)
    self.assertEqual(x[2].b, 6)

  def test_2d(self):
    x = [[Obj(a=1)], [Obj(a=2), Obj()]]
    v = kv.view(x)[:][:]
    kv.set_attr(v, 'a', 3)
    self.assertEqual(x[0][0].a, 3)
    self.assertEqual(x[1][0].a, 3)
    self.assertEqual(x[1][1].a, 3)

  def test_set_attr_none_in_view(self):
    x = [Obj(a=1), None, Obj(a=2)]
    v = kv.view(x)[:]
    kv.set_attr(v, 'a', 3)
    self.assertEqual(x[0].a, 3)
    self.assertEqual(x[2].a, 3)

  def test_set_attr_none_in_value(self):
    x = [Obj(a=1), Obj(a=2)]
    v = kv.view(x)[:]
    kv.set_attr(v, 'a', kv.view([3, None])[:])
    self.assertEqual(x[0].a, 3)
    self.assertIsNone(x[1].a)

  def test_broadcasting(self):
    x = [[Obj(a=1)], [Obj(a=2), Obj()]]
    v = kv.view(x)[:][:]
    kv.set_attr(v, 'b', kv.view([4, 5])[:])
    self.assertEqual(x[0][0].b, 4)
    self.assertEqual(x[1][0].b, 5)
    self.assertEqual(x[1][1].b, 5)

  def test_value_depth_too_high_fails(self):
    x = [Obj(a=1)]
    with self.assertRaisesRegex(
        ValueError,
        'The value being set as attribute must have same or lower depth',
    ):
      kv.set_attr(kv.view(x)[:], 'a', kv.view([[1, 2]])[:][:])

  def test_auto_boxing_scalar_value(self):
    x = Obj(a=1)
    kv.set_attr(kv.view(x), 'a', 10)
    self.assertEqual(x.a, 10)

  def test_no_auto_boxing_for_object(self):
    x = Obj(a=1)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('Cannot automatically box namespace(a=1)'),
    ):
      kv.set_attr(x, 'a', kv.view(10))  # pytype: disable=wrong-arg-types

  def test_set_attr_with_view_value(self):
    x = Obj(a=1)
    kv.set_attr(kv.view(x), 'a', kv.view(11))
    self.assertEqual(x.a, 11)

  def test_set_attr_on_none(self):
    # This should not fail.
    kv.set_attr(None, 'a', 1)

  def test_multiple_instances_of_same_object(self):
    x = Obj(a=1)
    kv.set_attr(kv.view([x, x])[:], 'a', kv.view([2, 3])[:])
    self.assertEqual(x.a, 3)


if __name__ == '__main__':
  absltest.main()
