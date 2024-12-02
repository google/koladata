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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.functor import signature_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
S = I.self
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class FunctorCallTest(absltest.TestCase):

  def test_call_simple(self):
    fn = functor_factories.expr_fn(
        returns=I.x + V.foo,
        foo=I.y * I.x,
    )
    testing.assert_equal(expr_eval.eval(kde.call(fn, x=2, y=3)), ds(8))
    # Unused inputs are ignored with the "default" signature.
    testing.assert_equal(expr_eval.eval(kde.call(fn, x=2, y=3, z=4)), ds(8))

  def test_call_with_self(self):
    fn = functor_factories.expr_fn(
        returns=S.x + V.foo,
        foo=S.y * S.x,
    )
    testing.assert_equal(expr_eval.eval(kde.call(fn, fns.new(x=2, y=3))), ds(8))

  def test_call_explicit_signature(self):
    fn = functor_factories.expr_fn(
        returns=I.x + V.foo,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
            signature_utils.parameter(
                'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
        ]),
        foo=I.y,
    )
    testing.assert_equal(expr_eval.eval(kde.call(fn, 1, 2)), ds(3))
    testing.assert_equal(expr_eval.eval(kde.call(fn, 1, y=2)), ds(3))

  def test_call_with_no_expr(self):
    fn = functor_factories.expr_fn(57, signature=signature_utils.signature([]))
    testing.assert_equal(expr_eval.eval(kde.call(fn)).no_bag(), ds(57))

  def test_positional_only(self):
    fn = functor_factories.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_ONLY
            ),
        ]),
    )
    testing.assert_equal(expr_eval.eval(kde.call(fn, 57)), ds(57))
    with self.assertRaisesRegex(
        ValueError, re.escape('unknown keyword arguments: [x]')
    ):
      _ = expr_eval.eval(kde.call(fn, x=57))

  def test_keyword_only(self):
    fn = functor_factories.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.KEYWORD_ONLY
            ),
        ]),
    )
    testing.assert_equal(expr_eval.eval(kde.call(fn, x=57)), ds(57))
    with self.assertRaisesRegex(ValueError, 'too many positional arguments'):
      _ = expr_eval.eval(kde.call(fn, 57))

  def test_var_positional(self):
    fn = functor_factories.expr_fn(
        returns=kde.tuple.get_nth(I.x, 1),
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.VAR_POSITIONAL
            ),
        ]),
    )
    testing.assert_equal(expr_eval.eval(kde.call(fn, 1, 2, 3)), ds(2))

  def test_var_keyword(self):
    fn = functor_factories.expr_fn(
        returns=arolla.M.namedtuple.get_field(I.x, 'y'),
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.VAR_KEYWORD
            ),
        ]),
    )
    testing.assert_equal(expr_eval.eval(kde.call(fn, x=1, y=2, z=3)), ds(2))

  def test_default_value(self):
    fn = functor_factories.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_ONLY, 57
            ),
        ]),
    )
    testing.assert_equal(expr_eval.eval(kde.call(fn)).no_bag(), ds(57))
    testing.assert_equal(expr_eval.eval(kde.call(fn, 43)), ds(43))

  def test_obj_as_default_value(self):
    fn = functor_factories.expr_fn(
        returns=I.x,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x',
                signature_utils.ParameterKind.POSITIONAL_ONLY,
                fns.new(foo=57),
            ),
        ]),
    )
    testing.assert_equal(expr_eval.eval(kde.call(fn)).foo.no_bag(), ds(57))
    testing.assert_equal(expr_eval.eval(kde.call(fn, 43)), ds(43))

  def test_call_eval_error(self):
    fn = functor_factories.expr_fn(
        returns=I.x.foo,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
        ]),
    )
    testing.assert_equal(
        expr_eval.eval(kde.call(fn, fns.new(foo=57))).no_bag(), ds(57)
    )
    with self.assertRaisesRegex(ValueError, "the attribute 'foo' is missing"):
      _ = expr_eval.eval(kde.call(fn, fns.new(bar=57)))

  def test_call_non_dataslice_inputs(self):
    fn = functor_factories.expr_fn(kde.tuple.get_nth(I.x, 1))
    testing.assert_equal(
        expr_eval.eval(kde.call(fn, x=arolla.tuple(ds(1), ds(2), ds(3)))), ds(2)
    )

  def test_call_returns_non_dataslice(self):
    fn = functor_factories.expr_fn(I.x)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the functor was called with `DATA_SLICE` as the output type, but'
            ' the computation resulted in type `tuple<INT32,INT32>` instead'
        ),
    ):
      _ = expr_eval.eval(kde.call(fn, x=arolla.tuple(1, 2)))
    res = expr_eval.eval(
        kde.call(
            fn,
            x=arolla.tuple(1, 2),
            return_type_as=arolla.tuple(5, 7),
        )
    )
    testing.assert_equal(res, arolla.tuple(1, 2))

  def test_call_returns_databag(self):
    fn = functor_factories.expr_fn(I.x.get_bag())
    obj = fns.obj(x=1)
    res = expr_eval.eval(
        kde.call(
            fn,
            x=obj,
            return_type_as=data_bag.DataBag,
        )
    )
    testing.assert_equal(res, obj.get_bag())

  def test_call_return_type_errors(self):
    fn = functor_factories.expr_fn(I.x)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('object with unsupported type: "type"'),
    ):
      _ = expr_eval.eval(kde.call(fn, x=1, return_type_as=int))

  def test_call_with_functor_as_input(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    testing.assert_equal(
        expr_eval.eval(kde.call(I.fn, x=I.u, y=I.v), fn=fn, u=2, v=3), ds(5)
    )

  def test_call_with_computed_functor(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    testing.assert_equal(
        expr_eval.eval(
            kde.call(I.my_functors.fn, x=I.u, y=I.v),
            my_functors=fns.new(fn=fn),
            u=2,
            v=3,
        ),
        ds(5),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.call(I.fn)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.functor.call, kde.call))

  def test_repr(self):
    self.assertEqual(
        repr(kde.functor.call(I.fn, I.x, I.y, a=I.z)),
        'kde.functor.call(I.fn, I.x, I.y, return_type_as=DataItem(None, schema:'
        ' NONE), a=I.z)',
    )


if __name__ == '__main__':
  absltest.main()
