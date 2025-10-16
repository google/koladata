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
    self.assertEqual(
        lens_lib.map_(lambda x: x + 1, lens_lib.lens(x).a[:].b).get(), [2, 3]
    )
    self.assertEqual(
        lens_lib.map_(len, lens_lib.lens([[1, 2], [3]])[:]).get(), [2, 1]
    )
    self.assertEqual(lens_lib.map_(len, lens_lib.lens([[1, 2], [3]])).get(), 2)
    self.assertEqual(
        lens_lib.map_(
            lambda x, y: x + y,
            lens_lib.lens([1, 2])[:],
            lens_lib.lens([3, 4])[:],
        ).get(),
        [4, 6],
    )
    self.assertEqual(
        lens_lib.map_(
            lambda x, y: x + y,
            lens_lib.lens([1, 2])[:],
            y=lens_lib.lens([3, 4])[:],
        ).get(),
        [4, 6],
    )

  def test_map_broadcasting(self):
    self.assertEqual(
        lens_lib.map_(
            lambda x, y: x + y, lens_lib.lens([1, 2])[:], lens_lib.lens(10)
        ).get(),
        [11, 12],
    )
    self.assertEqual(
        lens_lib.map_(
            lambda x, y: x + y, lens_lib.lens(10), lens_lib.lens([1, 2])[:]
        ).get(),
        [11, 12],
    )
    self.assertEqual(
        lens_lib.map_(
            lambda x, y: x + y,
            lens_lib.lens([1, 2])[:],
            y=lens_lib.lens(10),
        ).get(),
        [11, 12],
    )
    self.assertEqual(
        lens_lib.map_(
            lambda x, y, z: x + y + z,
            lens_lib.lens([1, 2])[:],
            lens_lib.lens([10, 20])[:],
            lens_lib.lens(100),
        ).get(),
        [111, 122],
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Lenses do not have a common shape. Shapes: JaggedShape(1, 2) and'
            ' JaggedShape(2, 1)'
        ),
    ):
      _ = lens_lib.map_(
          lambda x, y: x + y,
          lens_lib.lens([[1], [2]])[:][:],
          lens_lib.lens([[10, 20]])[:][:],
      ).get()
    self.assertEqual(
        lens_lib.map_(
            lambda x, y: x + y,
            lens_lib.lens([[1, 2], [3, 4]])[:][:],
            lens_lib.lens([10, 20])[:],
        ).get(),
        [[11, 12], [23, 24]],
    )

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

  def test_align(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('align() missing 1 required positional argument')
    ):
      lens_lib.align()  # pytype: disable=missing-parameter

    l1 = lens_lib.lens(1)
    (res1,) = lens_lib.align(l1)
    self.assertEqual(res1.get(), 1)

    l2 = lens_lib.lens([10, 20])[:]
    res1, res2 = lens_lib.align(l1, l2)
    self.assertEqual(res1.get(), [1, 1])
    self.assertEqual(res2.get(), [10, 20])

    res2, res1 = lens_lib.align(l2, l1)
    self.assertEqual(res1.get(), [1, 1])
    self.assertEqual(res2.get(), [10, 20])

    l3 = lens_lib.lens([30, 40])[:]
    res2, res3 = lens_lib.align(l2, l3)
    self.assertEqual(res2.get(), [10, 20])
    self.assertEqual(res3.get(), [30, 40])

    l4 = lens_lib.lens([[1, 2], [3]])[:][:]
    res2, res4 = lens_lib.align(l2, l4)
    self.assertEqual(res2.get(), [[10, 10], [20]])
    self.assertEqual(res4.get(), [[1, 2], [3]])

    l5 = lens_lib.lens([1, 2, 3])[:]
    res1, res5 = lens_lib.align(l1, l5)
    self.assertEqual(res1.get(), [1, 1, 1])
    self.assertEqual(res5.get(), [1, 2, 3])

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Lenses do not have a common shape. Shapes: JaggedShape(3) and'
            ' JaggedShape(2)'
        ),
    ):
      _ = lens_lib.align(l2, l5)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Lenses do not have a common shape. Shapes: JaggedShape(3)'
            ' and JaggedShape(2, [2, 1])'
        ),
    ):
      _ = lens_lib.align(l4, l5)

  def test_expand_to_shape(self):
    l1 = lens_lib.lens(1)
    l2 = lens_lib.lens([10, 20])[:]
    l4 = lens_lib.lens([[1, 2], [3]])[:][:]
    l5 = lens_lib.lens([1, 2, 3])[:]

    self.assertEqual(l1.expand_to_shape(l1.get_shape()).get(), 1)
    self.assertEqual(l1.expand_to_shape(l2.get_shape()).get(), [1, 1])
    self.assertEqual(l1.expand_to_shape(l5.get_shape()).get(), [1, 1, 1])
    self.assertEqual(l2.expand_to_shape(l2.get_shape()).get(), [10, 20])
    self.assertEqual(l1.expand_to_shape(l4.get_shape()).get(), [[1, 1], [1]])
    self.assertEqual(l2.expand_to_shape(l4.get_shape()).get(), [[10, 10], [20]])
    self.assertEqual(l4.expand_to_shape(l4.get_shape()).get(), [[1, 2], [3]])

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Lenses do not have a common shape. Shapes: JaggedShape(2) and'
            ' JaggedShape(3)'
        ),
    ):
      l2.expand_to_shape(l5.get_shape())
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Lenses do not have a common shape. Shapes: JaggedShape(2, [2, 1])'
            ' and JaggedShape(3)'
        ),
    ):
      l4.expand_to_shape(l5.get_shape())
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Lenses do not have a common shape. Shapes: JaggedShape(3) and'
            ' JaggedShape(2)'
        ),
    ):
      l5.expand_to_shape(l2.get_shape())

  def test_expand_to(self):
    l1 = lens_lib.lens(1)
    l2 = lens_lib.lens([10, 20])[:]
    l4 = lens_lib.lens([[1, 2], [3]])[:][:]
    l5 = lens_lib.lens([1, 2, 3])[:]

    self.assertEqual(l1.expand_to(l1).get(), 1)
    self.assertEqual(l1.expand_to(l2).get(), [1, 1])
    self.assertEqual(l1.expand_to(l5).get(), [1, 1, 1])
    self.assertEqual(l2.expand_to(l2).get(), [10, 20])
    self.assertEqual(l1.expand_to(l4).get(), [[1, 1], [1]])
    self.assertEqual(l2.expand_to(l4).get(), [[10, 10], [20]])
    self.assertEqual(l4.expand_to(l4).get(), [[1, 2], [3]])

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Lenses do not have a common shape. Shapes: JaggedShape(2) and'
            ' JaggedShape(3)'
        ),
    ):
      l2.expand_to(l5)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Lenses do not have a common shape. Shapes: JaggedShape(2, [2, 1])'
            ' and JaggedShape(3)'
        ),
    ):
      l4.expand_to(l5)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'Lenses do not have a common shape. Shapes: JaggedShape(3) and'
            ' JaggedShape(2)'
        ),
    ):
      l5.expand_to(l2)


if __name__ == '__main__':
  absltest.main()
