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


# NOTE: We don't re-use the Koda tests here since the View operator lacks
# support for both `schema` and `**overrides` making the Koda tests too
# specific.
class DeepCloneTest(absltest.TestCase):

  def test_scalar(self):
    x = Obj(a=1)
    self.assertIs(kv.view(x).get(), x)  # Sanity check.
    y = kv.deep_clone(kv.view(x))
    self.assertIsNot(y.get(), x)
    self.assertEqual(y.get(), x)

  def test_multidim(self):
    x = [[[Obj(a=1)], [Obj(a=2), None]], [[Obj(a=3)]]]
    y = kv.deep_clone(kv.view(x)[:])
    y[:][:].set_attr('a', 4)
    self.assertEqual(x, [[[Obj(a=1)], [Obj(a=2), None]], [[Obj(a=3)]]])
    self.assertEqual(y.get(), ([[Obj(a=4)], [Obj(a=4), None]], [[Obj(a=4)]]))

  def test_auto_boxing_scalar_value(self):
    self.assertEqual(kv.deep_clone(1).get(), 1)

  def test_no_auto_boxing_for_object(self):
    x = Obj(a=1)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('Cannot automatically box namespace(a=1)'),
    ):
      kv.deep_clone(x)  # pytype: disable=wrong-arg-types

  def test_deep_clone_on_none(self):
    # This should not fail.
    self.assertIsNone(kv.deep_clone(None).get())

  def test_multiple_instances_of_same_object(self):
    x = Obj(a=1)
    y = kv.deep_clone(kv.view([x, x])[:])
    self.assertIsNot(y.get()[0], x)
    self.assertIsNot(y.get()[1], x)
    self.assertEqual(y.get()[0], x)
    self.assertEqual(y.get()[1], x)
    self.assertIs(y.get()[0], y.get()[1])  # Identical items stay identical.

  def test_custom_deep_copy(self):
    class Foo:
      def __init__(self, x):
        self.x = x

      def __deepcopy__(self, memo):
        return Foo(self.x + 1)

    f = Foo(1)
    self.assertEqual(kv.deep_clone(kv.view(f)).get().x, 2)

  def test_self_reference(self):
    x = {'x': None}
    x['x'] = x
    y = kv.deep_clone(kv.view(x))
    self.assertIsNot(y.get(), x)
    self.assertCountEqual(y.get(), ['x'])
    self.assertIs(y.get()['x'], y.get())


if __name__ == '__main__':
  absltest.main()
