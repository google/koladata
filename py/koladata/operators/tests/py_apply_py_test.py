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
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class PyApplyPyTest(parameterized.TestCase):

  def test_1_dim(self):
    x = ds([1, 2, 3])
    y = ds([4, 5, 6])
    testing.assert_equal(
        expr_eval.eval(kde.py.apply_py(lambda x, y: x + y, x, y)),
        ds([5, 7, 9]),
    )

  def test_0_1_dim(self):
    x = ds(1)
    y = ds([1, 2, None])
    testing.assert_equal(
        expr_eval.eval(kde.py.apply_py(lambda x, y: x + y, x, y)),
        ds([2, 3, None]),
    )

  def test_with_kwargs(self):
    x = ds([1, 2, 3])
    y = ds([4, 5, 6])
    testing.assert_equal(
        expr_eval.eval(kde.py.apply_py(lambda x, y: x + y, y=y, x=x)),
        ds([5, 7, 9]),
    )

  def test_argument_binding(self):
    bound_args = {}

    def fn(
        pos_only=ds(0), /, pos_or_kw=ds(1), *var_pos, kw_only=ds(2), **var_kw
    ):
      bound_args['pos_only'] = pos_only
      bound_args['pos_or_kw'] = pos_or_kw
      bound_args['var_pos'] = var_pos
      bound_args['kw_only'] = kw_only
      bound_args['var_kw'] = var_kw
      return ds(None)

    with self.subTest('default_values'):
      bound_args = {}
      expr_eval.eval(kde.py.apply_py(fn))
      self.assertEqual(
          bound_args,
          {
              'pos_only': ds(0),
              'pos_or_kw': ds(1),
              'var_pos': (),
              'kw_only': ds(2),
              'var_kw': {},
          },
      )

    with self.subTest('generic'):
      bound_args = {}
      expr_eval.eval(kde.py.apply_py(fn, 10, 11, 12, 13, x=14, pos_only=15))
      self.assertEqual(
          bound_args,
          {
              'pos_only': ds(10),
              'pos_or_kw': ds(11),
              'var_pos': (ds(12), ds(13)),
              'kw_only': ds(2),
              'var_kw': {'x': ds(14), 'pos_only': ds(15)},
          },
      )

  def test_result_boxing(self):
    testing.assert_equal(
        expr_eval.eval(kde.py.apply_py(lambda: 1)),
        ds(1),
    )

  def test_error_call_non_callable(self):
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape('expected a python callable, got fn=PyObject{{}}')
        ),
    ):
      expr_eval.eval(kde.py.apply_py(arolla.abc.PyObject({})))

  def test_error_unexpected_return_type(self):
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                'expected the result to have qtype DATA_SLICE, got'
                ' tuple<INT32>; consider specifying the `return_type_as=`'
                ' parameter'
            )
        ),
    ):
      expr_eval.eval(kde.py.apply_py(lambda: arolla.tuple(1)))
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                'expected the result to have qtype DATA_BAG, got tuple<INT32>'
            )
        ),
    ):
      expr_eval.eval(
          kde.py.apply_py(
              lambda: arolla.tuple(1), return_type_as=data_bag.DataBag
          )
      )

  def test_explicit_return_type(self):
    testing.assert_equal(
        expr_eval.eval(
            kde.py.apply_py(
                lambda: arolla.tuple(1), return_type_as=arolla.tuple(2)
            )
        ),
        arolla.tuple(1),
    )
    res = expr_eval.eval(
        kde.py.apply_py(lambda: fns.bag(), return_type_as=data_bag.DataBag)  # pylint: disable=unnecessary-lambda
    )
    self.assertIsInstance(res, data_bag.DataBag)

  # Note: We cannot test the qtype signatures of this operator
  # because it accepts any values as inputs.
  #
  # def test_qtype_signatures(self): ...

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.py.apply_py(I.fn, I.cond)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.py.apply_py, kde.apply_py))

  def test_repr(self):
    self.assertEqual(
        repr(kde.py.apply_py(I.fn, I.x, a=I.a)),
        'kd.py.apply_py(I.fn, I.x, return_type_as=unspecified, a=I.a)',
    )


if __name__ == '__main__':
  absltest.main()
