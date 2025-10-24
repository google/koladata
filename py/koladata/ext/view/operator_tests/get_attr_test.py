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

import types

from absl.testing import absltest
from koladata.ext.view import kv

Obj = types.SimpleNamespace


class GetAttrTest(absltest.TestCase):

  def test_call(self):
    x = Obj(a=5, _b=6)
    self.assertEqual(kv.get_attr(kv.view(x), 'a').get(), 5)
    self.assertEqual(kv.get_attr(kv.view(x), '_b').get(), 6)
    self.assertIsNone(kv.get_attr(None, 'a').get())
    x_mix = [Obj(a=1), None, Obj(a=3)]
    self.assertEqual(kv.get_attr(kv.view(x_mix)[:], 'a').get(), (1, None, 3))

  def test_nested(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    self.assertEqual(
        kv.get_attr(kv.get_attr(kv.view(x), 'a')[:], 'b').get(), (1, 2)
    )

  def test_slice_attr_on_empty_list(self):
    x = Obj(a=[])
    self.assertEqual(kv.get_attr(kv.view(x).a[:], 'b').get(), ())

  def test_missing_attr_fails(self):
    x = Obj(a=[Obj(b=1), Obj()])
    with self.assertRaisesRegex(AttributeError, "object has no attribute 'b'"):
      _ = kv.get_attr(kv.view(x).a[:], 'b')

  def test_auto_boxing(self):
    self.assertIsNone(kv.get_attr(None, 'a').get())
    with self.assertRaisesRegex(ValueError, 'Cannot automatically box'):
      _ = kv.get_attr(Obj(a=1), 'a')  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
