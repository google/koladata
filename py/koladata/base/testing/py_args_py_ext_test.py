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
from koladata.base.testing import py_args_py_ext


class ParseArgsTest(absltest.TestCase):

  def test_pos_kw_2_args(self):
    pos_kw_2_args = py_args_py_ext.pos_kw_2_args

    self.assertEqual(pos_kw_2_args(a=9, b=5), 4)
    self.assertEqual(pos_kw_2_args(9, 5), 4)
    self.assertEqual(pos_kw_2_args(b=5, a=9), 4)
    self.assertEqual(pos_kw_2_args(1, b=5), -4)

    with self.assertRaisesRegex(
        TypeError, 'got multiple values for argument \'b\''
    ):
      pos_kw_2_args(1, 4, b=12)

    with self.assertRaisesRegex(TypeError, 'got an unexpected keyword \'c\''):
      pos_kw_2_args(1, 4, c=12)

    with self.assertRaisesRegex(TypeError, 'both arguments should be present'):
      pos_kw_2_args(1)

    with self.assertRaisesRegex(
        TypeError, 'accepts 0 to 2 positional arguments but 3 were given'
    ):
      pos_kw_2_args(1, 2, 3)

  def test_pos_only_and_pos_kw_3_args(self):
    pos_only_and_pos_kw_3_args = py_args_py_ext.pos_only_and_pos_kw_3_args

    self.assertEqual(
        pos_only_and_pos_kw_3_args(None, None, a=5, b=4, c=2), (5, 4, 2)
    )
    self.assertEqual(pos_only_and_pos_kw_3_args(None, None, 5), (5, None, None))
    # 'b' is 2nd positional-keyword in `ArgNames`.
    self.assertEqual(
        pos_only_and_pos_kw_3_args(None, None, b=5), (None, 5, None)
    )
    # 'c' is 3rd positional-keyword in `ArgNames`.
    self.assertEqual(
        pos_only_and_pos_kw_3_args(None, None, c=5), (None, None, 5)
    )
    # 'a' is 1st and 'c' is 3rd positional-keyword in `ArgNames`.
    self.assertEqual(
        pos_only_and_pos_kw_3_args(None, None, c=5, a=12), (12, None, 5)
    )
    self.assertEqual(pos_only_and_pos_kw_3_args(None, None), (None, None, None))
    with self.assertRaisesRegex(
        TypeError, 'accepts 2 to 5 positional arguments but 1 was given'
    ):
      pos_only_and_pos_kw_3_args(None)
    with self.assertRaisesRegex(
        TypeError, 'accepts 2 to 5 positional arguments but 0 were given'
    ):
      pos_only_and_pos_kw_3_args()

    with self.assertRaisesRegex(
        TypeError, 'got multiple values for argument \'b\''
    ):
      pos_only_and_pos_kw_3_args(None, None, 1, 4, b=12)

    with self.assertRaisesRegex(
        TypeError, 'got multiple values for argument \'a\''
    ):
      pos_only_and_pos_kw_3_args(None, None, 1, a=12)

    with self.assertRaisesRegex(TypeError, 'got an unexpected keyword \'d\''):
      pos_only_and_pos_kw_3_args(None, None, d=12)

    with self.assertRaisesRegex(
        TypeError, 'accepts 2 to 5 positional arguments but 6 were given'
    ):
      pos_only_and_pos_kw_3_args(1, 2, 3, 4, 5, 6)

  def test_kwargs(self):
    kwargs = py_args_py_ext.kwargs

    self.assertEqual(kwargs(a=1, b=2, c=3), (('a', 'b', 'c'), (1, 2, 3)))
    self.assertEqual(kwargs(), ((), ()))

    with self.assertRaisesRegex(
        TypeError, 'accepts 0 positional arguments but 1 was given'
    ):
      kwargs(42, a=1, b=2)

    with self.assertRaisesRegex(
        TypeError, 'accepts 0 positional arguments but 2 were given'
    ):
      kwargs(11, 42, a=1, b=2)

  def test_pos_kw_2_and_kwargs(self):
    pos_kw_2_and_kwargs = py_args_py_ext.pos_kw_2_and_kwargs

    self.assertEqual(
        pos_kw_2_and_kwargs(p=1, q=2, r=3),
        ((None, None), ('p', 'q', 'r'), (1, 2, 3))
    )
    self.assertEqual(pos_kw_2_and_kwargs(), ((None, None), (), ()))
    self.assertEqual(
        pos_kw_2_and_kwargs(42, 12, p=42), ((42, 12), ('p',), (42,))
    )
    self.assertEqual(
        pos_kw_2_and_kwargs(42, p=12, b=15), ((42, 15), ('p',), (12,))
    )
    self.assertEqual(
        pos_kw_2_and_kwargs(q=42, p=12, b=15, a=17),
        ((17, 15), ('q', 'p'), (42, 12))
    )

    with self.assertRaisesRegex(
        TypeError, 'accepts 0 to 2 positional arguments but 3 were given'
    ):
      pos_kw_2_and_kwargs(42, 12, 15, p=1, q=2)

    with self.assertRaisesRegex(
        TypeError, 'got multiple values for argument \'b\''
    ):
      pos_kw_2_and_kwargs(None, None, b=1, p=4)

  def test_keyword_only(self):
    kw_only = py_args_py_ext.kw_only

    self.assertEqual(kw_only(a=42), (42, None))
    self.assertEqual(kw_only(b=42), (None, 42))
    self.assertEqual(kw_only(b=12, a=42), (42, 12))
    with self.assertRaisesRegex(TypeError, 'got an unexpected keyword \'c\''):
      _ = kw_only(b=12, a=42, c=57)
    with self.assertRaisesRegex(TypeError, 'accepts 0 positional arguments'):
      _ = kw_only(15, b=12, a=42)

  def test_keyword_only_and_positional_only(self):
    kw_only_and_pos_only = py_args_py_ext.kw_only_and_pos_only

    self.assertEqual(kw_only_and_pos_only(12, 15), (12, 15, None, None))
    self.assertEqual(kw_only_and_pos_only(12, 15, b=17), (12, 15, None, 17))
    with self.assertRaisesRegex(TypeError, 'got an unexpected keyword \'c\''):
      _ = kw_only_and_pos_only(12, 42, c=57)
    with self.assertRaisesRegex(
        TypeError, 'accepts 2 positional-only arguments but 1 was given'
    ):
      _ = kw_only_and_pos_only(15, a=42)

  def test_keyword_only_and_variadic_kwargs(self):
    kw_only_and_var_kwargs = py_args_py_ext.kw_only_and_var_kwargs

    self.assertEqual(
        kw_only_and_var_kwargs(c=12, d='abc', b=42),
        (None, 42, ('c', 'd'), (12, 'abc'))
    )
    self.assertEqual(
        kw_only_and_var_kwargs(c=12, a=57, d='abc', b=42),
        (57, 42, ('c', 'd'), (12, 'abc'))
    )
    with self.assertRaisesRegex(
        TypeError, 'accepts 0 positional arguments but 1 was given'
    ):
      _ = kw_only_and_var_kwargs(15, a=42)

  def test_keyword_only_positional_kwargs_and_variadic_kwargs(self):
    kw_only_pos_only_and_var_kwargs = (
        py_args_py_ext.kw_only_pos_only_and_var_kwargs
    )

    self.assertEqual(
        kw_only_pos_only_and_var_kwargs(12, 42, b=57, c='abc'),
        (None, 57, ('c',), ('abc',), 12, 42)
    )
    self.assertEqual(
        kw_only_pos_only_and_var_kwargs(None, 42, b=57, c='abc', a=42),
        (42, 57, ('c',), ('abc',), None, 42)
    )
    self.assertEqual(
        kw_only_pos_only_and_var_kwargs(None, b=57, c='abc', a=42),
        (42, 57, ('c',), ('abc',), None, None)
    )
    self.assertEqual(
        kw_only_pos_only_and_var_kwargs(b=57, c='abc', a=42),
        (42, 57, ('c',), ('abc',), None, None)
    )
    with self.assertRaisesRegex(
        TypeError, 'accepts 0 to 2 positional arguments but 3 were given'
    ):
      _ = kw_only_pos_only_and_var_kwargs(15, 52, 37, a=42)

  def test_optional_positional_only(self):
    opt_pos_only = py_args_py_ext.optional_positional_only

    self.assertEqual(opt_pos_only(12, 42, 24, 21), (12, 42, 24, 21))
    self.assertEqual(opt_pos_only(12, 42, pos_2=24, pos_1=21), (12, 42, 21, 24))
    self.assertEqual(opt_pos_only(12, 42, 24, pos_2=21), (12, 42, 24, 21))

    self.assertEqual(opt_pos_only(), (None, None, None, None))
    self.assertEqual(opt_pos_only(12), (12, None, None, None))
    self.assertEqual(opt_pos_only(12, 42), (12, 42, None, None))
    self.assertEqual(opt_pos_only(12, 42, pos_2=37), (12, 42, None, 37))
    self.assertEqual(opt_pos_only(12, pos_2=37), (12, None, None, 37))
    self.assertEqual(opt_pos_only(12, pos_1=42, pos_2=37), (12, None, 42, 37))
    self.assertEqual(opt_pos_only(pos_1=42, pos_2=37), (None, None, 42, 37))

    with self.assertRaisesRegex(
        TypeError, 'accepts 0 to 4 positional arguments but 5 were given'
    ):
      _ = opt_pos_only(15, 52, 37, 42, 12)


if __name__ == '__main__':
  absltest.main()
