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
from koladata.ext.view import view as view_lib


Obj = types.SimpleNamespace


class ViewTest(absltest.TestCase):

  def test_get(self):
    self.assertEqual(view_lib.view(5).get(), 5)
    x = Obj(a=1)
    self.assertIs(view_lib.view(x).get(), x)
    y = [[1, 2], [3, 4]]
    y_recycled = view_lib.view(y)[:].get()
    self.assertIsNot(y_recycled, y)
    self.assertIs(y_recycled[0], y[0])
    self.assertIs(y_recycled[1], y[1])

  def test_getattr(self):
    x = Obj(a=5)
    self.assertEqual(view_lib.view(x).a.get(), 5)

  def test_get_attr(self):
    x = Obj(a=5, _b=6)
    self.assertEqual(view_lib.view(x).get_attr('a').get(), 5)
    self.assertEqual(view_lib.view(x).get_attr('_b').get(), 6)
    self.assertIsNone(view_lib.view(None).get_attr('a').get())
    x_mix = [Obj(a=1), None, Obj(a=3)]
    self.assertEqual(view_lib.view(x_mix)[:].get_attr('a').get(), (1, None, 3))

  def test_slice_attr(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    self.assertEqual(view_lib.view(x).a[:].b.get(), (1, 2))

  def test_explode(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    self.assertEqual(view_lib.view(x).a.explode().b.get(), (1, 2))
    self.assertEqual(view_lib.view(x).a.explode().get_depth(), 1)
    x_mix = [[1], None, [2, 3]]
    self.assertEqual(view_lib.view(x_mix)[:][:].get(), ((1,), (), (2, 3)))
    self.assertEqual(view_lib.view(x_mix)[:][:].get_depth(), 2)

  def test_map(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    self.assertEqual(
        view_lib.map_(lambda x: x + 1, view_lib.view(x).a[:].b).get(), (2, 3)
    )
    self.assertEqual(
        view_lib.map_(len, view_lib.view([[1, 2], [3]])[:]).get(), (2, 1)
    )
    self.assertEqual(view_lib.map_(len, view_lib.view([[1, 2], [3]])).get(), 2)
    self.assertEqual(
        view_lib.map_(
            lambda x, y: x + y,
            view_lib.view([1, 2])[:],
            view_lib.view([3, 4])[:],
        ).get(),
        (4, 6),
    )
    self.assertEqual(
        view_lib.map_(
            lambda x, y: x + y,
            view_lib.view([1, 2])[:],
            y=view_lib.view([3, 4])[:],
        ).get(),
        (4, 6),
    )

  def test_map_broadcasting(self):
    self.assertEqual(
        view_lib.map_(lambda x, y: x + y, view_lib.view([1, 2])[:], 10).get(),
        (11, 12),
    )
    self.assertEqual(
        view_lib.map_(lambda x, y: x + y, 10, view_lib.view([1, 2])[:]).get(),
        (11, 12),
    )
    self.assertEqual(
        view_lib.map_(
            lambda x, y: x + y,
            view_lib.view([1, 2])[:],
            y=10,
        ).get(),
        (11, 12),
    )
    self.assertEqual(
        view_lib.map_(
            lambda x, y, z: x + y + z,
            view_lib.view([1, 2])[:],
            view_lib.view([10, 20])[:],
            100,
        ).get(),
        (111, 122),
    )
    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 1 and 2',
    ):
      _ = view_lib.map_(
          lambda x, y: x + y,
          view_lib.view([[1], [2]])[:][:],
          view_lib.view([[10, 20]])[:][:],
      ).get()
    self.assertEqual(
        view_lib.map_(
            lambda x, y: x + y,
            view_lib.view([[1, 2], [3, 4]])[:][:],
            view_lib.view([10, 20])[:],
        ).get(),
        ((11, 12), (23, 24)),
    )

  def test_nested_slice(self):
    x = Obj(
        a=[
            Obj(b=Obj(c=Obj(d=[Obj(e=1), Obj(e=2)]))),
            Obj(b=Obj(c=Obj(d=[Obj(e=3)]))),
        ]
    )
    self.assertEqual(view_lib.view(x).a[:].b.c.d[:].e.get(), ((1, 2), (3,)))

  def test_slice_empty_list(self):
    x = Obj(
        a=[
            Obj(b=Obj(c=Obj(d=[]))),
            Obj(b=Obj(c=Obj(d=[Obj(e=3)]))),
        ]
    )
    self.assertEqual(view_lib.view(x).a[:].b.c.d[:].e.get(), ((), (3,)))

  def test_slice_attr_on_empty_list(self):
    x = Obj(a=[])
    self.assertEqual(view_lib.view(x).a[:].b.get(), ())

  def test_getattr_missing_attr_fails(self):
    x = Obj(a=[Obj(b=1), Obj()])
    with self.assertRaisesRegex(AttributeError, "object has no attribute 'b'"):
      _ = view_lib.view(x).a[:].b

  def test_getattr_underscore_fails(self):
    x = Obj(_a=1)
    with self.assertRaisesRegex(AttributeError, '_a'):
      _ = view_lib.view(x)._a

  def test_getitem_unsupported_slice_fails(self):
    x = Obj(a=[1, 2, 3])
    with self.assertRaisesRegex(
        ValueError, re.escape('Only everything slice [:] is supported')
    ):
      _ = view_lib.view(x).a[1:]

  def test_slice_on_non_iterable_fails(self):
    x = Obj(a=[1, 2])
    with self.assertRaisesRegex(TypeError, "'int' object is not iterable"):
      _ = view_lib.view(x).a[:][:]

  def test_get_depth(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    self.assertEqual(view_lib.view(x).get_depth(), 0)
    self.assertEqual(view_lib.view(x).a.get_depth(), 0)
    self.assertEqual(view_lib.view(x).a[:].get_depth(), 1)
    self.assertEqual(view_lib.view(x).a[:].b.get_depth(), 1)

  def test_flatten(self):
    x = [[1, 2], [3]]
    self.assertEqual(view_lib.view(x).flatten().get(), (x,))
    self.assertEqual(view_lib.view(x)[:].flatten().get(), ([1, 2], [3]))
    self.assertEqual(view_lib.view(x)[:][:].flatten().get(), (1, 2, 3))

  def test_implode(self):
    x = [[[1, 2], [3]], [[4]]]
    view_3d = view_lib.view(x)[:][:][:]
    self.assertEqual(view_3d.implode(ndim=0).flatten().get(), (1, 2, 3, 4))
    self.assertEqual(
        view_3d.implode(ndim=1).flatten().get(), ((1, 2), (3,), (4,))
    )
    self.assertEqual(view_3d.implode().flatten().get(), ((1, 2), (3,), (4,)))
    self.assertEqual(
        view_3d.implode(ndim=2).flatten().get(), (((1, 2), (3,)), ((4,),))
    )
    self.assertEqual(
        view_3d.implode(ndim=3).flatten().get(), ((((1, 2), (3,)), ((4,),)),)
    )
    self.assertEqual(
        view_3d.implode(ndim=-1).flatten().get(), ((((1, 2), (3,)), ((4,),)),)
    )
    self.assertEqual(
        view_3d.implode(ndim=-2).flatten().get(), ((((1, 2), (3,)), ((4,),)),)
    )
    with self.assertRaisesRegex(
        ValueError,
        'Cannot implode by 4 dimensions, the shape has only 3 dimensions.',
    ):
      _ = view_3d.implode(ndim=4)

  def test_align(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('align() missing 1 required positional argument')
    ):
      view_lib.align()  # pytype: disable=missing-parameter

    l1 = view_lib.view(1)
    (res1,) = view_lib.align(l1)
    self.assertEqual(res1.get(), 1)

    l2 = view_lib.view([10, 20])[:]
    res1, res2 = view_lib.align(l1, l2)
    self.assertEqual(res1.get(), (1, 1))
    self.assertEqual(res2.get(), (10, 20))

    res2, res1 = view_lib.align(l2, l1)
    self.assertEqual(res1.get(), (1, 1))
    self.assertEqual(res2.get(), (10, 20))

    l3 = view_lib.view([30, 40])[:]
    res2, res3 = view_lib.align(l2, l3)
    self.assertEqual(res2.get(), (10, 20))
    self.assertEqual(res3.get(), (30, 40))

    l4 = view_lib.view([[1, 2], [3]])[:][:]
    res2, res4 = view_lib.align(l2, l4)
    self.assertEqual(res2.get(), ((10, 10), (20,)))
    self.assertEqual(res4.get(), ((1, 2), (3,)))

    l5 = view_lib.view([1, 2, 3])[:]
    res1, res5 = view_lib.align(l1, l5)
    self.assertEqual(res1.get(), (1, 1, 1))
    self.assertEqual(res5.get(), (1, 2, 3))

    res1, res2 = view_lib.align(1, l2)
    self.assertEqual(res1.get(), (1, 1))
    self.assertEqual(res2.get(), (10, 20))
    res2, res1 = view_lib.align(l2, 1)
    self.assertEqual(res1.get(), (1, 1))
    self.assertEqual(res2.get(), (10, 20))

    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 2 and 3',
    ):
      _ = view_lib.align(l2, l5)
    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 2 and 3',
    ):
      _ = view_lib.align(l4, l5)

  def test_expand_to(self):
    l1 = view_lib.view(1)
    l2 = view_lib.view([10, 20])[:]
    l4 = view_lib.view([[1, 2], [3]])[:][:]
    l5 = view_lib.view([1, 2, 3])[:]

    self.assertEqual(l1.expand_to(l1).get(), 1)
    self.assertEqual(l1.expand_to(l2).get(), (1, 1))
    self.assertEqual(l1.expand_to(l5).get(), (1, 1, 1))
    self.assertEqual(l2.expand_to(l2).get(), (10, 20))
    self.assertEqual(l1.expand_to(l4).get(), ((1, 1), (1,)))
    self.assertEqual(l2.expand_to(l4).get(), ((10, 10), (20,)))
    self.assertEqual(l4.expand_to(l4).get(), ((1, 2), (3,)))

    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 3 and 2',
    ):
      l2.expand_to(l5)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'a View with depth 2 cannot be broadcasted to a View with depth 1'
        ),
    ):
      l4.expand_to(l5)
    with self.assertRaisesRegex(
        TypeError,
        'expected all tuples to be the same length when depth > 0, got 2 and 3',
    ):
      l5.expand_to(l2)

  def test_box(self):
    l1 = view_lib.view(1)
    self.assertIs(view_lib.box(l1), l1)
    self.assertEqual(view_lib.box(1).get(), 1)
    self.assertEqual(view_lib.box(1.0).get(), 1.0)
    self.assertEqual(view_lib.box('foo').get(), 'foo')
    self.assertEqual(view_lib.box(b'foo').get(), b'foo')
    self.assertEqual(view_lib.box(True).get(), True)
    self.assertIsNone(view_lib.box(None).get())
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "Cannot automatically box [1, 2] of type <class 'list'> to a view."
            ' Use kv.view() explicitly if you want to construct a view from'
            ' it.'
        ),
    ):
      view_lib.box([1, 2])


if __name__ == '__main__':
  absltest.main()
