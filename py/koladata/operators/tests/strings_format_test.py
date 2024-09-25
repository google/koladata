# Copyright 2024 Google LLC
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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class StringsFormatTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('foo'), {}, ds('foo')),
      (ds(b'foo'), {}, ds(b'foo')),
      (ds('{v} foo'), dict(v=ds('bar')), ds('bar foo')),
      (ds(b'{v} foo'), dict(v=ds(b'bar')), ds(b'bar foo')),
      (ds('{v} foo'), dict(v=ds(1)), ds('1 foo')),
      (ds(b'{v} foo'), dict(v=ds(1)), ds(b'1 foo')),
      (ds('{v} foo'), dict(v=ds(None)), ds(None, schema_constants.TEXT)),
      (ds(b'{v} foo'), dict(v=ds(None)), ds(None, schema_constants.BYTES)),
      (
          ds('{v} foo'),
          dict(v=ds(None, schema_constants.OBJECT)),
          ds(None, schema_constants.TEXT),
      ),
      (
          ds(b'{v} foo'),
          dict(v=ds(None, schema_constants.OBJECT)),
          ds(None, schema_constants.BYTES),
      ),
      # ds(1) ignored.
      (ds('{v} foo'), dict(v=ds('bar'), q=ds(1)), ds('bar foo')),
      (ds(b'{v} foo'), dict(v=ds(b'bar'), q=ds(1)), ds(b'bar foo')),
      # 2 args
      (ds('{v} foo {q}'), dict(v=ds('bar'), q=ds(1)), ds('bar foo 1')),
      (ds(b'{v} foo {q}'), dict(v=ds(b'bar'), q=ds(1)), ds(b'bar foo 1')),
      # used twice
      (ds('{q}) {v} foo {q}'), dict(v=ds('bar'), q=ds(1)), ds('1) bar foo 1')),
      (
          ds(b'{q}) {v} foo {q}'),
          dict(v=ds(b'bar'), q=ds(1)),
          ds(b'1) bar foo 1'),
      ),
      # slice of formats
      (ds(['+{v}', '-{v}']), dict(v=ds('bar')), ds(['+bar', '-bar'])),
      (ds([b'+{v}', b'-{v}']), dict(v=ds(b'bar')), ds([b'+bar', b'-bar'])),
      # multidimensional
      (
          ds([['+{v}', '-{v}'], ['+{v}', '-{v}']]),
          dict(v=ds('bar')),
          ds([['+bar', '-bar'], ['+bar', '-bar']]),
      ),
      (
          ds([[b'+{v}', b'-{v}'], [b'+{v}', b'-{v}']]),
          dict(v=ds([b'bar', b'foo'])),
          ds([[b'+bar', b'-bar'], [b'+foo', b'-foo']]),
      ),
      (
          ds(b'={v}'),
          dict(v=ds([[[[b'bar', b'foo']]]])),
          ds([[[[b'=bar', b'=foo']]]]),
      ),
      # slice of formats with ANY
      (
          ds(['+{v}', '-{v}'], schema_constants.ANY),
          dict(v=ds('bar')),
          ds(['+bar', '-bar'], schema_constants.ANY),
      ),
      (
          ds([b'+{v}', b'-{v}'], schema_constants.ANY),
          dict(v=ds(b'bar')),
          ds([b'+bar', b'-bar'], schema_constants.ANY),
      ),
      # Large arity.
      (ds('{x0}'), {f'x{i}': ds(f'foo{i}') for i in range(30)}, ds('foo0')),
      # Format respect type
      (ds('{v:03d} != {q:02}'), dict(v=ds(5), q=ds(7)), ds('005 != 07')),
      (
          ds(b'{v:06.3f} != {q:07.4}'),
          dict(v=ds(5.7), q=ds(7.5)),
          ds(b'05.700 != 07.5000'),
      ),
      (
          ds(b'{v:017} != {q:017}'),
          dict(v=ds(57.0), q=ds(57)),
          ds(b'0000000057.000000 != 00000000000000057'),
      ),
      # Empty and unknown.
      (ds([None, None]), {}, ds([None, None])),
      (ds([[None, None], [None]]), {}, ds([[None, None], [None]])),
      (
          ds(None),
          dict(unused_but_lifting_arg=ds([['foo'], ['bar']])),
          ds([[None], [None]]),
      ),
      (
          ds([[None], [None]]),
          dict(unused_arg=ds(1)),
          ds([[None], [None]]),
      ),
      (
          ds([[[[None, None]]], [[[None]]]]),
          {},
          ds([[[[None, None]]], [[[None]]]]),
      ),
      (
          ds([None, None], schema_constants.TEXT),
          {},
          ds([None, None], schema_constants.TEXT),
      ),
      (
          ds([None, None], schema_constants.BYTES),
          {},
          ds([None, None], schema_constants.BYTES),
      ),
      (
          ds([None, None], schema_constants.OBJECT),
          {},
          ds([None, None], schema_constants.OBJECT),
      ),
      (ds([None, None]), dict(v=ds('abc')), ds([None, None])),
      (ds([None, None]), dict(f=ds(1)), ds([None, None])),
      (ds([None, None]), dict(f=ds(1), z=ds(b'abc')), ds([None, None])),
  )
  def test_eval(self, fmt, kwargs, expected):
    result = expr_eval.eval(kde.strings.format(fmt, **kwargs))
    testing.assert_equal(result, expected)

  def test_examples_from_docstring(self):
    testing.assert_equal(
        expr_eval.eval(
            kde.strings.format(ds(['Hello {n}!', 'Goodbye {n}!']), n='World')
        ),
        ds(['Hello World!', 'Goodbye World!']),
    )
    testing.assert_equal(
        expr_eval.eval(kde.strings.format('{a} + {b} = {c}', a=1, b=2, c=3)),
        ds('1 + 2 = 3'),
    )
    testing.assert_equal(
        expr_eval.eval(
            kde.strings.format(
                '{a} + {b} = {c}', a=ds([1, 2]), b=ds([2, 3]), c=ds([3, 5])
            )
        ),
        ds(['1 + 2 = 3', '2 + 3 = 5']),
    )
    testing.assert_equal(
        expr_eval.eval(
            kde.strings.format(
                '({a:03} + {b:e}) * {c:.2f} ='
                ' {a:02d} * {c:3d} + {b:07.3f} * {c:08.4f}',
                a=5,
                b=5.7,
                c=75,
            )
        ),
        ds('(005 + 5.700000e+00) * 75.00 = 05 *  75 + 005.700 * 075.0000'),
    )

  def test_incompatible_text_bytes_types_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('unsupported argument types (TEXT,TEXT,BYTES)')
    ):
      expr_eval.eval(kde.strings.format(ds('{v}'), v=ds(b'foo')))

  def test_unsupported_types_error(self):
    with self.assertRaisesRegex(ValueError, 'no primitive schema'):
      expr_eval.eval(kde.strings.format(ds('{v}'), v=kde.uuid()))
    with self.assertRaisesRegex(ValueError, 'no primitive schema'):
      expr_eval.eval(
          kde.strings.format(
              ds('{v}'), v=kde.with_schema(kde.uuid(), schema_constants.ANY)
          )
      )
    with self.assertRaisesRegex(
        ValueError, 'unsupported argument types.*UNIT'
    ):
      expr_eval.eval(kde.strings.format(ds('{v}'), v=ds(arolla.present())))

  def test_missing_input_error(self):
    with self.assertRaisesRegex(ValueError, "argument name 'v' is not found"):
      expr_eval.eval(kde.strings.format(ds('{v}')))

  def test_wrong_schema_empty_format_input_error(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported argument types.*INT64'
    ):
      expr_eval.eval(kde.strings.format(ds(None, schema_constants.INT64)))

  def test_mixed_slice_error(self):
    with self.assertRaisesRegex(
        ValueError, 'DataSlice with mixed types is not supported'
    ):
      expr_eval.eval(kde.strings.format(ds('{v}'), v=ds([1, 'foo'])))

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.strings.format('{x}', x=I.x)))


if __name__ == '__main__':
  absltest.main()
