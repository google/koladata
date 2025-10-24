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

from absl.testing import absltest
from koladata.ext.view import kv


class ImplodeTest(absltest.TestCase):

  def test_call(self):
    x = [[[1, 2], [3]], [[4, None]]]
    view_3d = kv.view(x)[:][:][:]
    self.assertEqual(
        kv.implode(view_3d, ndim=0).flatten().get(), (1, 2, 3, 4, None)
    )
    self.assertEqual(
        kv.implode(view_3d, ndim=1).flatten().get(), ((1, 2), (3,), (4, None))
    )
    self.assertEqual(
        kv.implode(view_3d).flatten().get(), ((1, 2), (3,), (4, None))
    )
    self.assertEqual(
        kv.implode(view_3d, ndim=2).flatten().get(),
        (((1, 2), (3,)), ((4, None),)),
    )
    self.assertEqual(
        kv.implode(view_3d, ndim=3).flatten().get(),
        ((((1, 2), (3,)), ((4, None),)),),
    )
    self.assertEqual(
        kv.implode(view_3d, ndim=-1).flatten().get(),
        ((((1, 2), (3,)), ((4, None),)),),
    )
    self.assertEqual(
        kv.implode(view_3d, ndim=-2).flatten().get(),
        ((((1, 2), (3,)), ((4, None),)),),
    )
    with self.assertRaisesRegex(
        ValueError,
        'Cannot implode by 4 dimensions, the shape has only 3 dimensions.',
    ):
      _ = kv.implode(view_3d, ndim=4)

  def test_auto_boxing(self):
    with self.assertRaisesRegex(ValueError, 'has only 0 dimensions'):
      _ = kv.implode(None)
    with self.assertRaisesRegex(ValueError, 'Cannot automatically box'):
      _ = kv.implode([1, 2])  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
