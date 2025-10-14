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
from koladata import kd
from koladata.ext.konstructs import lens as lens_lib


Obj = types.SimpleNamespace


class LensTest(absltest.TestCase):

  def test_get(self):
    self.assertEqual(lens_lib.lens(5).get(), 5)
    x = Obj(a=1)
    self.assertIs(lens_lib.lens(x).get(), x)
    y = [[1, 2], [3, 4]]
    y_recycled = lens_lib.lens(y)[:].get()
    self.assertIsNot(y_recycled, y)
    self.assertIs(y_recycled[0], y[0])
    self.assertIs(y_recycled[1], y[1])

  def test_getattr(self):
    x = Obj(a=5)
    self.assertEqual(lens_lib.lens(x).a.get(), 5)

  def test_get_attr(self):
    x = Obj(a=5, _b=6)
    self.assertEqual(lens_lib.lens(x).get_attr('a').get(), 5)
    self.assertEqual(lens_lib.lens(x).get_attr('_b').get(), 6)

  def test_slice_attr(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    self.assertEqual(lens_lib.lens(x).a[:].b.get(), [1, 2])

  def test_explode(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    self.assertEqual(lens_lib.lens(x).a.explode().b.get(), [1, 2])
    rank1_shape = kd.shapes.new(2)
    self.assertEqual(lens_lib.lens(x).a.explode().get_shape(), rank1_shape)

  def test_map(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    self.assertEqual(lens_lib.lens(x).a[:].b.map(lambda x: x + 1).get(), [2, 3])
    self.assertEqual(lens_lib.lens([[1, 2], [3]])[:].map(len).get(), [2, 1])
    self.assertEqual(lens_lib.lens([[1, 2], [3]]).map(len).get(), 2)

  def test_nested_slice(self):
    x = Obj(
        a=[
            Obj(b=Obj(c=Obj(d=[Obj(e=1), Obj(e=2)]))),
            Obj(b=Obj(c=Obj(d=[Obj(e=3)]))),
        ]
    )
    self.assertEqual(lens_lib.lens(x).a[:].b.c.d[:].e.get(), [[1, 2], [3]])

  def test_slice_empty_list(self):
    x = Obj(
        a=[
            Obj(b=Obj(c=Obj(d=[]))),
            Obj(b=Obj(c=Obj(d=[Obj(e=3)]))),
        ]
    )
    self.assertEqual(lens_lib.lens(x).a[:].b.c.d[:].e.get(), [[], [3]])

  def test_slice_attr_on_empty_list(self):
    x = Obj(a=[])
    self.assertEqual(lens_lib.lens(x).a[:].b.get(), [])

  def test_getattr_missing_attr_fails(self):
    x = Obj(a=[Obj(b=1), Obj()])
    with self.assertRaisesRegex(AttributeError, "object has no attribute 'b'"):
      _ = lens_lib.lens(x).a[:].b

  def test_getattr_underscore_fails(self):
    x = Obj(_a=1)
    with self.assertRaisesRegex(AttributeError, '_a'):
      _ = lens_lib.lens(x)._a

  def test_getitem_unsupported_slice_fails(self):
    x = Obj(a=[1, 2, 3])
    with self.assertRaisesRegex(
        ValueError, re.escape('Only everything slice [:] is supported')
    ):
      _ = lens_lib.lens(x).a[1:]

  def test_slice_on_non_iterable_fails(self):
    x = Obj(a=[1, 2])
    with self.assertRaisesRegex(TypeError, "object of type 'int' has no len()"):
      _ = lens_lib.lens(x).a[:][:]

  def test_get_shape(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    scalar_shape = kd.shapes.new()
    rank1_shape = kd.shapes.new(2)
    self.assertEqual(lens_lib.lens(x).get_shape(), scalar_shape)
    self.assertEqual(lens_lib.lens(x).a.get_shape(), scalar_shape)
    self.assertEqual(lens_lib.lens(x).a[:].get_shape(), rank1_shape)
    self.assertEqual(lens_lib.lens(x).a[:].b.get_shape(), rank1_shape)

  def test_flatten(self):
    x = [[1, 2], [3]]
    self.assertEqual(lens_lib.lens(x).flatten().get(), [x])
    self.assertEqual(lens_lib.lens(x)[:].flatten().get(), x)
    self.assertEqual(lens_lib.lens(x)[:][:].flatten().get(), [1, 2, 3])

  def test_implode(self):
    x = [[[1, 2], [3]], [[4]]]
    lens_3d = lens_lib.lens(x)[:][:][:]
    self.assertEqual(lens_3d.implode(ndim=0).flatten().get(), [1, 2, 3, 4])
    self.assertEqual(
        lens_3d.implode(ndim=1).flatten().get(), [[1, 2], [3], [4]]
    )
    self.assertEqual(lens_3d.implode().flatten().get(), [[1, 2], [3], [4]])
    self.assertEqual(lens_3d.implode(ndim=2).flatten().get(), x)
    self.assertEqual(lens_3d.implode(ndim=3).flatten().get(), [x])
    self.assertEqual(lens_3d.implode(ndim=-1).flatten().get(), [x])
    self.assertEqual(lens_3d.implode(ndim=-2).flatten().get(), [x])
    with self.assertRaisesRegex(
        ValueError,
        'Cannot implode by 4 dimensions, the shape has only 3 dimensions.',
    ):
      _ = lens_3d.implode(ndim=4)


if __name__ == '__main__':
  absltest.main()
