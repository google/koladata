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


class MapTest(absltest.TestCase):

  def test_map(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    self.assertEqual(kv.map(lambda x: x + 1, kv.view(x).a[:].b).get(), (2, 3))
    self.assertEqual(kv.map(len, kv.view([[1, 2], [3]])[:]).get(), (2, 1))
    self.assertEqual(kv.map(len, kv.view([[1, 2], [3]])).get(), 2)
    self.assertEqual(
        kv.map(
            lambda x, y: x + y,
            kv.view([1, 2])[:],
            kv.view([3, 4])[:],
        ).get(),
        (4, 6),
    )
    self.assertEqual(
        kv.map(
            lambda x, y: x + y,
            kv.view([1, 2])[:],
            y=kv.view([3, 4])[:],
        ).get(),
        (4, 6),
    )

  def test_map_broadcasting(self):
    self.assertEqual(
        kv.map(lambda x, y: x + y, kv.view([1, 2])[:], 10).get(),
        (11, 12),
    )
    self.assertEqual(
        kv.map(lambda x, y: x + y, 10, kv.view([1, 2])[:]).get(),
        (11, 12),
    )
    self.assertEqual(
        kv.map(
            lambda x, y: x + y,
            kv.view([1, 2])[:],
            y=10,
        ).get(),
        (11, 12),
    )
    self.assertEqual(
        kv.map(
            lambda x, y, z: x + y + z,
            kv.view([1, 2])[:],
            kv.view([10, 20])[:],
            100,
        ).get(),
        (111, 122),
    )
    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 1 and 2',
    ):
      _ = kv.map(
          lambda x, y: x + y,
          kv.view([[1], [2]])[:][:],
          kv.view([[10, 20]])[:][:],
      ).get()
    self.assertEqual(
        kv.map(
            lambda x, y: x + y,
            kv.view([[1, 2], [3, 4]])[:][:],
            kv.view([10, 20])[:],
        ).get(),
        ((11, 12), (23, 24)),
    )

  def test_map_include_missing(self):
    self.assertEqual(
        kv.map(
            lambda x, y: x + y,
            kv.view([1, 2])[:],
            kv.view([3, None])[:],
        ).get(),
        (4, None),
    )
    self.assertEqual(
        kv.map(
            lambda x, y: x + y,
            kv.view([1, 2])[:],
            kv.view([3, None])[:],
            include_missing=False,
        ).get(),
        (4, None),
    )
    self.assertEqual(
        kv.map(
            lambda x, y: x + y if y is not None else 57,
            kv.view([1, 2])[:],
            kv.view([3, None])[:],
            include_missing=True,
        ).get(),
        (4, 57),
    )
    self.assertEqual(
        kv.map(
            lambda x, y: x + y,
            None,
            kv.view([3, 4])[:],
        ).get(),
        (None, None),
    )
    self.assertEqual(
        kv.map(
            lambda x, y: x + y if x is not None else 57,
            None,
            kv.view([3, 4])[:],
            include_missing=True,
        ).get(),
        (57, 57),
    )


if __name__ == '__main__':
  absltest.main()
