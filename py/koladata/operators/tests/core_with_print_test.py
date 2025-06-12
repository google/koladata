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

"""Tests for kde.core.with_print operator."""

import contextlib
import io

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.base import init as _
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


# The operator is agnostic to the type of the first argument, we test just a
# few.
POSSIBLE_QTYPES = [
    arolla.INT32,
    test_qtypes.DATA_SLICE,
    test_qtypes.DATA_BAG,
    arolla.make_tuple_qtype(),
    arolla.make_tuple_qtype(test_qtypes.DATA_SLICE),
    arolla.make_tuple_qtype(test_qtypes.DATA_SLICE, test_qtypes.DATA_SLICE),
    arolla.make_tuple_qtype(
        test_qtypes.DATA_SLICE, test_qtypes.DATA_SLICE, test_qtypes.DATA_SLICE
    ),
    arolla.make_tuple_qtype(test_qtypes.DATA_SLICE, test_qtypes.DATA_BAG),
    test_qtypes.NON_DETERMINISTIC_TOKEN,
]


def generate_qtypes():
  for x in POSSIBLE_QTYPES:
    for arity in range(4):
      yield (
          x,
          arolla.make_tuple_qtype(*[test_qtypes.DATA_SLICE] * arity),
          test_qtypes.DATA_SLICE,
          test_qtypes.DATA_SLICE,
          test_qtypes.NON_DETERMINISTIC_TOKEN,
          x,
      )


QTYPES = frozenset(generate_qtypes())


class CoreWithPrintTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.with_print,
        QTYPES,
        possible_qtypes=POSSIBLE_QTYPES,
        max_arity=5,
    )

  @parameterized.parameters(
      ds(['foo', 'bar', 'baz']),
      data_bag.DataBag.empty(),
      arolla.int32(57),
  )
  def test_propagate_x(self, x):
    expr = kde.core.with_print(I.x, 'some message to log')
    with contextlib.redirect_stdout(io.StringIO()) as printed:
      eval_result = expr_eval.eval(expr, x=x)
      expr_eval.eval(expr, x=x)

    self.assertEqual(
        printed.getvalue(), 'some message to log\nsome message to log\n'
    )
    testing.assert_equivalent(eval_result, x)

  @parameterized.parameters(
      ([], dict(), '\n'),
      ([], dict(end=''), ''),
      (['school', 57], dict(), 'school 57\n'),
      (['school', 57], dict(sep=''), 'school57\n'),
      (['school', 57], dict(sep='', end=''), 'school57'),
      (list('school'), dict(), 's c h o o l\n'),
      # Non-string sep and end are allowed for simplicity.
      (['school', '-'], dict(sep=5, end=7), 'school5-7'),
      # We expect DataSliceToStr to be well tested, so just a few cases here.
      (['school', ds([57])], dict(), 'school [57]\n'),
      (['school', ds(b'57')], dict(), "school b'57'\n"),
      (
          ['school', ds(list(range(1000)))],
          dict(),
          'school [0, 1, 2, 3, 4, ...]\n',
      ),
  )
  def test_string_formatting(self, args, kwargs, expected):
    x = ds(['foo', 'bar', 'baz'])
    expr = kde.core.with_print(I.x, *args, **kwargs)
    with contextlib.redirect_stdout(io.StringIO()) as printed:
      eval_result = expr_eval.eval(expr, x=x)

    self.assertEqual(printed.getvalue(), expected)
    testing.assert_equal(eval_result, x)

  def test_two_similar_nodes(self):
    x = ds([1, 2, 3])
    # A sum of two identical expressions.
    expr = kde.core.with_print(
        I.x, 'some message to log'
    ) + kde.core.with_print(I.x, 'some message to log')
    with contextlib.redirect_stdout(io.StringIO()) as printed:
      eval_result = expr_eval.eval(expr, x=x)

    self.assertEqual(
        printed.getvalue(), 'some message to log\nsome message to log\n'
    )
    testing.assert_equal(eval_result, ds([2, 4, 6]))

  def test_same_node_twice(self):
    x = ds([1, 2, 3])
    logged_x = kde.core.with_print(I.x, 'some message to log')
    expr = logged_x + logged_x
    with contextlib.redirect_stdout(io.StringIO()) as printed:
      eval_result = expr_eval.eval(expr, x=x)

    self.assertEqual(printed.getvalue(), 'some message to log\n')
    testing.assert_equal(eval_result, ds([2, 4, 6]))

  def test_no_literal_folding(self):
    expr = (
        kde.core.with_print(ds([1, 2, 3]), 'some message to log')
        + 1
    )
    with contextlib.redirect_stdout(io.StringIO()) as printed:
      eval_result = expr_eval.eval(expr)
      expr_eval.eval(expr)
      expr_eval.eval(expr)

    self.assertEqual(
        printed.getvalue(),
        'some message to log\nsome message to log\nsome message to log\n',
    )
    testing.assert_equal(eval_result, ds([2, 3, 4]))

  def test_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected all arguments to be DATA_SLICE, got args:'
        ' tuple<DATA_SLICE,DATA_BAG>',
    ):
      kde.core.with_print(
          I.x, ds(['some message to log']), data_bag.DataBag.empty()
      )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.core.with_print(I.x, I.y))
    )


if __name__ == '__main__':
  absltest.main()
