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
from koladata.ext.view import mask_constants
from koladata.ext.view import test_utils
from koladata.ext.view import view as view_lib


Obj = types.SimpleNamespace


# For most methods of View, more comprehensive tests are in operator_tests/
# folder. This test covers only the basic cases and differences with operators.
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
    x = Obj(_b=6)
    self.assertEqual(view_lib.view(x).get_attr('_b').get(), 6)

  def test_setattr(self):
    x = [Obj(a=5), Obj(b=7)]
    v = view_lib.view(x)[:]
    v.a = 6
    self.assertEqual(x[0].a, 6)
    self.assertEqual(x[1].a, 6)
    self.assertEqual(x[1].b, 7)

  def test_set_attr(self):
    x = [Obj(_b=6), Obj(a=8)]
    v = view_lib.view(x)[:]
    v.set_attr('_b', 7)
    self.assertEqual(x[0]._b, 7)
    self.assertEqual(x[1]._b, 7)
    self.assertEqual(x[1].a, 8)

  def test_setitem(self):
    x = [{1: 5, -1: 7}, [1, 2, 3]]
    v = view_lib.view(x)[:]
    v[-1] = 6
    self.assertEqual(x, [{1: 5, -1: 6}, [1, 2, 6]])

  def test_set_item(self):
    x = [{1: 5, -1: 7}, [1, 2, 3]]
    v = view_lib.view(x)[:]
    v.set_item(-1, 6)
    self.assertEqual(x, [{1: 5, -1: 6}, [1, 2, 6]])

  def test_setattr_underscore_fails(self):
    x = Obj(_a=1)
    v = view_lib.view(x)
    with self.assertRaisesRegex(AttributeError, '_a'):
      v._a = 2

  def test_explode(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    self.assertEqual(view_lib.view(x).a.explode().b.get(), (1, 2))

  def test_map(self):
    self.assertEqual(
        view_lib.view([1, None, 2])[:].map(lambda x: x * 2).get(), (2, None, 4)
    )
    self.assertEqual(
        view_lib.view([1, 2]).map(lambda x: x * 2).get(), [1, 2, 1, 2]
    )
    self.assertEqual(
        view_lib.view([1, None, 2])[:]
        .map(lambda x: x is None, include_missing=True)
        .get(),
        (False, True, False),
    )

  def test_agg_map(self):
    self.assertEqual(
        view_lib.view([[None, 1, 2], []])[:][:].map(len, ndim=1).get(), (3, 0)
    )
    self.assertEqual(
        view_lib.view([[None, 1, 2], []])[:][:]
        .map(list, ndim=1)
        .map(list, ndim=1)
        .get(),
        [[None, 1, 2], []],
    )
    with self.assertRaisesRegex(
        ValueError,
        'invalid argument ndim=1, only values smaller or equal to 0 are'
        ' supported for the given view',
    ):
      _ = view_lib.view(5).map(len, ndim=1)
    with self.assertRaisesRegex(
        ValueError, 'invalid argument ndim=-1, must be non-negative'
    ):
      _ = view_lib.view(5).map(len, ndim=-1)
    self.assertEqual(
        view_lib.view([[None, 1, 2], []])[:][:]
        .map(len, ndim=1, include_missing=True)
        .get(),
        (3, 0),
    )
    with self.assertRaisesRegex(
        ValueError, 'include_missing=False can only be used with ndim=0'
    ):
      _ = view_lib.view([[None, 1, 2], []])[:][:].map(
          len, ndim=1, include_missing=False
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

  def test_getattr_missing_attr_fails(self):
    x = Obj(a=[Obj(b=1), Obj()])
    with self.assertRaisesRegex(AttributeError, "object has no attribute 'b'"):
      _ = view_lib.view(x).a[:].b

  def test_getattr_underscore_fails(self):
    x = Obj(_a=1)
    with self.assertRaisesRegex(AttributeError, '_a'):
      _ = view_lib.view(x)._a

  def test_getitem_non_full_slice(self):
    x = Obj(a=[1, 2, 3])
    self.assertEqual(view_lib.view(x).a[1:].get(), (2, 3))

  def test_slice_on_non_iterable_fails(self):
    x = Obj(a=[1, 2])
    with self.assertRaisesRegex(TypeError, "'int' object is not subscriptable"):
      _ = view_lib.view(x).a[:][:]

  def test_get_depth(self):
    x = Obj(a=[Obj(b=1), Obj(b=2)])
    self.assertEqual(view_lib.view(x).get_depth(), 0)
    self.assertEqual(view_lib.view(x).a.get_depth(), 0)
    self.assertEqual(view_lib.view(x).a[:].get_depth(), 1)
    self.assertEqual(view_lib.view(x).a[:].b.get_depth(), 1)

  def test_flatten(self):
    x = [[1, None, 2], [3]]
    self.assertEqual(view_lib.view(x)[:][:].flatten().get(), (1, None, 2, 3))

  def test_implode(self):
    x = [[[1, 2], [3]], [[4, None]]]
    view_3d = view_lib.view(x)[:][:][:]
    self.assertEqual(
        view_3d.implode().flatten().get(), ((1, 2), (3,), (4, None))
    )
    self.assertEqual(
        view_3d.implode(ndim=2).flatten().get(), (((1, 2), (3,)), ((4, None),))
    )

  def test_expand_to(self):
    l1 = view_lib.view([1, 2])[:]
    l2 = view_lib.view([[10, 20], [None]])[:][:]
    self.assertEqual(l1.expand_to(l2).get(), ((1, 1), (2,)))
    self.assertEqual(
        l1.expand_to(l2, ndim=1).get(), (((1, 2), (1, 2)), ((1, 2),))
    )

  def test_box(self):
    l1 = view_lib.view(1)
    self.assertIs(view_lib.box(l1), l1)
    self.assertEqual(view_lib.box(1).get(), 1)
    self.assertEqual(view_lib.box(1.0).get(), 1.0)
    self.assertEqual(view_lib.box('foo').get(), 'foo')
    self.assertEqual(view_lib.box(b'foo').get(), b'foo')
    self.assertEqual(view_lib.box(True).get(), True)
    self.assertIsNone(view_lib.box(None).get())
    self.assertIs(
        view_lib.box(mask_constants.present).get(), mask_constants.present
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "Cannot automatically box [1, 2] of type <class 'list'> to a view."
            ' Use kv.view() explicitly if you want to construct a view from'
            ' it.'
        ),
    ):
      view_lib.box([1, 2])  # pytype: disable=wrong-arg-types

  def test_box_and_unbox_scalar(self):
    l1 = view_lib.view(1)
    self.assertEqual(view_lib.box_and_unbox_scalar(l1), 1)
    self.assertEqual(view_lib.box_and_unbox_scalar(1), 1)
    self.assertEqual(view_lib.box_and_unbox_scalar(1.0), 1.0)
    self.assertEqual(view_lib.box_and_unbox_scalar('foo'), 'foo')
    self.assertEqual(view_lib.box_and_unbox_scalar(b'foo'), b'foo')
    self.assertEqual(view_lib.box_and_unbox_scalar(True), True)
    self.assertIsNone(view_lib.box_and_unbox_scalar(None))
    self.assertIs(
        view_lib.box_and_unbox_scalar(mask_constants.present),
        mask_constants.present,
    )
    with self.assertRaisesRegex(ValueError, 'expected a scalar, got depth 1'):
      view_lib.box_and_unbox_scalar(view_lib.view([1, 2])[:])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "Cannot automatically box [1, 2] of type <class 'list'> to a view."
            ' Use kv.view() explicitly if you want to construct a view from'
            ' it.'
        ),
    ):
      view_lib.box_and_unbox_scalar([1, 2])  # pytype: disable=wrong-arg-types

  def test_and_boxing(self):
    a = view_lib.view(1)
    b = view_lib.view(mask_constants.present)
    test_utils.assert_equal(a & mask_constants.present, a)
    test_utils.assert_equal(1 & b, a)

  def test_or_boxing(self):
    a = view_lib.view(None)
    b = view_lib.view(1)
    test_utils.assert_equal(a | 1, b)
    test_utils.assert_equal(None | b, b)

  def test_repr(self):
    self.assertEqual(
        repr(view_lib.view(1)),
        """<View(
  obj=1,
  depth=0,
)>""",
    )
    self.assertEqual(
        repr(view_lib.view([[1, 2], [3]])),
        """<View(
  obj=[[1, 2], [3]],
  depth=0,
)>""",
    )
    self.assertEqual(
        repr(view_lib.view([[1, 2], [3]])[:][:]),
        """<View(
  obj=((1, 2), (3,)),
  depth=2,
)>""",
    )

  def test_iter_raises_error(self):
    with self.assertRaisesRegex(
        ValueError, 'iteration over a view is not supported yet'
    ):
      iter(view_lib.view([1, 2])[:])

    with self.assertRaisesRegex(
        ValueError, 'iteration over a view is not supported yet'
    ):
      list(view_lib.view([1, 2])[:])

  def test_bool(self):
    self.assertTrue(view_lib.view(mask_constants.present))
    self.assertFalse(view_lib.view(None))
    with self.assertRaisesRegex(
        ValueError,
        'can only use views with depth=0 in if-statements, got depth=1',
    ):
      bool(view_lib.view([None])[:])
    with self.assertRaisesRegex(
        ValueError,
        'can only use views with kv.present or None in if-statements, got 1',
    ):
      bool(view_lib.view(1))

  def test_append(self):
    x = [[], [1]]
    view_lib.view(x)[:].append(view_lib.view([None, 20])[:])
    self.assertEqual(x, [[None], [1, 20]])

  def test_deep_clone(self):
    obj = Obj(a=1)
    a = view_lib.view(obj)
    # Sanity check.
    a.set_attr('a', 2)
    self.assertEqual(obj.a, 2)
    # Cloning.
    b = a.deep_clone()
    b.set_attr('a', 3)
    self.assertEqual(b.a.get(), 3)
    self.assertEqual(obj.a, 2)


if __name__ == '__main__':
  absltest.main()
