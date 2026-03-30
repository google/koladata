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

# Tests for to_py with output_class when __future__.annotations are imported.

from __future__ import annotations  # this is needed for the purpose
# of this test.
import dataclasses
import types as _py_types
from typing import Optional
from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.functions import object_factories
from koladata.functions import py_conversions
from koladata.operators import kde_operators

kde = kde_operators.kde


def mutable_obj():
  return object_factories.mutable_bag().obj()


@dataclasses.dataclass(frozen=True)
class Obj1:
  a: int = 123
  b: str = 'abc'


@dataclasses.dataclass
class RecursiveObj:
  a: int = 123
  b: RecursiveObj | None = None
  c: list[RecursiveObj] = dataclasses.field(default_factory=list)
  d: dict[str, RecursiveObj] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class Recursive:
  itself: Recursive | None = None


class ToPyWithAnnotationsTest(absltest.TestCase):

  def test_output_class(self):
    @dataclasses.dataclass
    class Obj2:
      o1: Obj1
      o2: Obj1
      i: int

    root = fns.new(
        o1=fns.obj(a=1, b='x'), o2=fns.obj(a=2, b='y'), i=fns.int64(3)
    )
    o1 = Obj1(a=1, b='x')
    o2 = Obj1(a=2, b='y')
    root_obj = Obj2(o1=o1, o2=o2, i=3)
    converted = py_conversions.to_py(root, output_class=Obj2)
    self.assertEqual(root_obj, converted)

  def test_output_class_primitive_types_in_dataclass(self):
    @dataclasses.dataclass
    class Obj:
      x1: int
      x2: int
      y: float

    root = fns.obj(x1=123, x2=456, y='abc')
    with self.assertRaisesRegex(
        ValueError, 'value is text, but requested output class is not str'
    ):
      _ = py_conversions.to_py(root, output_class=Obj)
    converted = py_conversions.to_py(
        fns.obj(x1=123, x2=fns.int64(456), y=3.0), output_class=Obj
    )
    self.assertAlmostEqual(converted, Obj(x1=123, x2=456, y=3.0))

  def test_output_class_optional(self):
    @dataclasses.dataclass
    class ObjWithOptional:
      o1: Optional[Obj1]
      o2: Obj1 | None

    root = fns.obj(o1=fns.obj(a=1, b='x'), o2=fns.obj(a=1, b='x'))
    o1 = Obj1(a=1, b='x')
    root_obj = ObjWithOptional(o1=o1, o2=o1)
    converted = py_conversions.to_py(root, output_class=ObjWithOptional)
    self.assertEqual(converted, root_obj)

  def test_output_class_unsupported_type(self):
    class Obj:
      a: Obj1

    with self.assertRaisesRegex(
        ValueError, 'only dataclasses or SimpleNamespace are supported'
    ):
      _ = py_conversions.to_py(
          fns.obj(a=fns.obj(a=1)),
          output_class=Obj,
      )

  def test_output_class_simple_namespace_in_dataclass(self):
    @dataclasses.dataclass
    class Obj:
      a: int
      b: str
      c: _py_types.SimpleNamespace

    root = fns.obj(a=1, b='x', c=fns.obj(d=2, e='y', f=fns.obj(g=3, h='z')))
    output_obj = Obj(
        a=1,
        b='x',
        c=_py_types.SimpleNamespace(
            d=2, e='y', f=_py_types.SimpleNamespace(g=3, h='z')
        ),
    )
    converted = py_conversions.to_py(
        root, max_depth=5, output_class=output_obj.__class__
    )
    self.assertEqual(converted, output_obj)

  def test_recursive_obj_no_output_class(self):
    obj = fns.new(a=123, c=fns.list(), d=fns.dict()).fork_bag()
    obj.b = obj
    obj.c.append(obj)
    obj.d['a'] = obj
    converted = py_conversions.to_py(obj, max_depth=-1)

    self.assertEqual(converted.a, 123)
    self.assertIs(converted, converted.b)
    self.assertIs(converted, converted.c[0])
    self.assertIs(converted, converted.d['a'])

  def test_recursive_obj_with_output_class_simple_namespace_fails(self):
    obj = fns.new(a=123, c=fns.list(), d=fns.dict()).fork_bag()
    obj.b = obj
    obj.c.append(obj)
    obj.d['a'] = obj

    with self.assertRaisesRegex(
        ValueError,
        'recursive structures with output_class are not supported',
    ):
      _ = py_conversions.to_py(
          obj, max_depth=-1, output_class=_py_types.SimpleNamespace
      )

    with self.assertRaisesRegex(
        ValueError,
        'recursive structures with output_class are not supported',
    ):
      _ = py_conversions.to_py(
          obj.b, max_depth=-1, output_class=_py_types.SimpleNamespace
      )

  def test_output_class_with_recursive_obj_fails(self):
    x = fns.new().fork_bag()
    x.itself = x

    with self.assertRaisesRegex(
        ValueError,
        'recursive structures with output_class are not supported',
    ):
      _ = py_conversions.to_py(x, max_depth=-1, output_class=Recursive)

  def test_too_deep_obj_with_output_class_fails(self):
    x = fns.new(a=fns.new(b=fns.new(c=1)))
    with self.assertRaisesRegex(
        ValueError,
        'object depth exceeds the maximum depth, which is not supported '
        'together with output_class',
    ):
      _ = py_conversions.to_py(
          x, max_depth=2, output_class=_py_types.SimpleNamespace
      )


if __name__ == '__main__':
  absltest.main()
