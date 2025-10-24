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


class ExplodeTest(absltest.TestCase):

  def test_call(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    self.assertEqual(kv.explode(kv.view(x).a).b.get(), (1, 2))
    self.assertEqual(kv.explode(kv.view(x).a).get_depth(), 1)
    x_mix = [[1], None, [2, 3]]
    self.assertEqual(
        kv.explode(kv.explode(kv.view(x_mix))).get(), ((1,), (), (2, 3))
    )
    self.assertEqual(kv.explode(kv.explode(kv.view(x_mix))).get_depth(), 2)

  def test_ndim(self):
    x_mix = [[1], None, [2, 3]]
    self.assertEqual(
        kv.explode(kv.view(x_mix), ndim=1).get(),
        ([1], None, [2, 3]),
    )
    self.assertEqual(kv.explode(kv.view(x_mix), ndim=1).get_depth(), 1)
    self.assertEqual(
        kv.explode(kv.view(x_mix), ndim=2).get(), ((1,), (), (2, 3))
    )
    self.assertEqual(kv.explode(kv.view(x_mix), ndim=2).get_depth(), 2)
    self.assertEqual(kv.explode(kv.view(x_mix), ndim=0).get(), x_mix)
    self.assertEqual(kv.explode(kv.view(x_mix), ndim=0).get_depth(), 0)
    with self.assertRaisesRegex(
        ValueError,
        'the number of dimensions to explode must be non-negative, got -1',
    ):
      _ = kv.explode(kv.view(x_mix), ndim=-1)
    with self.assertRaisesRegex(TypeError, "'int' object is not iterable"):
      _ = kv.explode(kv.view(x_mix), ndim=3)

  def test_auto_boxing(self):
    self.assertEqual(kv.explode(None).get(), ())
    with self.assertRaisesRegex(ValueError, 'Cannot automatically box'):
      _ = kv.explode([1, 2])  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
