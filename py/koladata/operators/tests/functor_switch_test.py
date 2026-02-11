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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import schema_constants


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kd = eager_op_utils.operators_container('kd')
kd_SWITCH_DEFAULT = kde_operators.SWITCH_DEFAULT


class FunctorSwitchTest(absltest.TestCase):

  def test_simple(self):
    expr = kde.switch(
        I.k,
        {
            'a': lambda x, y: x + y,
            'b': lambda x, y: x * y,
            kd_SWITCH_DEFAULT: lambda *unused, **unused_kwargs: 57,
        },
        I.x,
        y=I.y,
    )
    testing.assert_equal(expr_eval.eval(expr, k='a', x=10, y=2), ds(12))
    testing.assert_equal(expr_eval.eval(expr, k='b', x=10, y=2), ds(20))
    testing.assert_equal(expr_eval.eval(expr, k='c', x=10, y=2), ds(57))
    testing.assert_equal(expr_eval.eval(expr, k=None, x=10, y=2), ds(57))

  def test_missing_case_fn(self):
    # TODO: b/481263258 - Add a test for missing key (failing or not, depending
    # on the decision in the bug).
    expr = kde.switch(
        I.k,
        {
            'a': lambda x, y: x + y,
            'b': None,
            kd_SWITCH_DEFAULT: lambda *unused, **unused_kwargs: 57,
        },
        I.x,
        y=I.y,
    )
    testing.assert_equal(expr_eval.eval(expr, k='a', x=10, y=2), ds(12))
    # None value for 'b' is ignored and default is used instead.
    testing.assert_equal(expr_eval.eval(expr, k='b', x=10, y=2), ds(57))
    testing.assert_equal(expr_eval.eval(expr, k='c', x=10, y=2), ds(57))

  def test_duplicate_case_keys(self):
    # We inherit kd.dict behavior for duplicate keys: the last one wins.
    fn1 = functor_factories.fn(lambda x: x + 1)
    fn2 = functor_factories.fn(lambda x: x + 2)
    fn3 = functor_factories.fn(lambda x: x + 3)
    # We bypass the binding policy here in order to create two slices with
    # duplicate keys.
    expr = arolla.abc.bind_op(
        kde.functor.switch,
        I.k,  # key
        ds(['a', 'b', 'a'], schema=schema_constants.OBJECT),  # case_keys
        ds([fn1, fn2, fn3]),  # case_fns
        ds(None),  # return_type_as
        arolla.M.core.make_tuple(I.x),  # args
        arolla.M.namedtuple.make(),  # kwargs
        py_boxing.NON_DETERMINISTIC_TOKEN_LEAF,
    )
    testing.assert_equal(expr_eval.eval(expr, k='a', x=10), ds(13))
    testing.assert_equal(expr_eval.eval(expr, k='b', x=10), ds(12))

  def test_non_string_keys(self):
    expr = kde.functor.switch(
        I.k, {1: lambda x: x + 1, 2: lambda x: x * 2, 3: lambda _: 57}, I.x
    )
    testing.assert_equal(expr_eval.eval(expr, k=1, x=10), ds(11))
    testing.assert_equal(expr_eval.eval(expr, k=2, x=10), ds(20))
    testing.assert_equal(expr_eval.eval(expr, k=3, x=10), ds(57))

  def test_with_default(self):
    expr = kde.functor.switch(
        I.k,
        {'a': lambda x: x + 1, kd_SWITCH_DEFAULT: lambda x: x + 100},
        x=I.x,
    )
    testing.assert_equal(expr_eval.eval(expr, k='a', x=10), ds(11))
    testing.assert_equal(expr_eval.eval(expr, k='missing', x=10), ds(110))

  def test_eager_mode(self):
    res = kd.switch('a', {'a': lambda x: x + 1, 'b': lambda x: x * 2}, x=10)
    testing.assert_equal(res, ds(11))

  def test_eager_mode_with_default(self):
    res = kd.switch(
        'missing',
        {'a': lambda x: x + 1, kd_SWITCH_DEFAULT: lambda x: x + 100},
        x=10,
    )
    testing.assert_equal(res, ds(110))

  def test_koda_dict_cases(self):
    cases = kd.dict({
        'a': functor_factories.fn(lambda x: x + 1),
        'b': functor_factories.fn(lambda x: x * 2),
    })
    expr = kde.functor.switch(I.k, cases, x=I.x)
    testing.assert_equal(expr_eval.eval(expr, k='a', x=10), ds(11))

  def test_return_type_as(self):
    expr = kde.functor.switch(
        I.k,
        {
            'x': lambda x, y: (x, x),
            'y': lambda x, y: (y, y),
            kd_SWITCH_DEFAULT: lambda x, y: (x, y),
        },
        return_type_as=(None, None),
        x=I.x,
        y=I.y,
    )
    testing.assert_equal(
        expr_eval.eval(expr, k='x', x=1, y=2),
        kd.tuple(1, 1),
    )
    testing.assert_equal(
        expr_eval.eval(expr, k='y', x=1, y=2),
        kd.tuple(2, 2),
    )
    testing.assert_equal(
        expr_eval.eval(expr, k='missing', x=1, y=2),
        kd.tuple(1, 2),
    )

  def test_non_scalar_key(self):
    expr = kde.functor.switch(
        I.k, {'a': lambda x: x + 1, 'b': lambda x: x * 2}, x=I.x
    )
    with self.assertRaisesRegex(ValueError, 'kd.switch: key must be a scalar'):
      expr_eval.eval(expr, k=ds(['a', 'b', 'a']), x=ds([10, 10, 20]))

  def test_non_scalar_options(self):
    options = kd.dict({'a': functor_factories.fn(lambda x: x + 1)}).repeat(2)
    expr = kde.functor.switch(I.k, options, x=I.x)
    with self.assertRaisesRegex(ValueError, 'case_keys must be a 1D DataSlice'):
      expr_eval.eval(expr, k='a', x=10)

  def test_missing_without_default(self):
    expr = kde.functor.switch(I.k, {'a': lambda x: x + 1}, x=I.x)
    with self.assertRaisesRegex(
        ValueError,
        'kd.switch: key not found in cases and no default provided',
    ):
      expr_eval.eval(expr, k='missing', x=10)

  def test_non_callable_in_cases(self):
    expr = kde.functor.switch(I.k, {'a': 123, 'b': lambda x: x * 2}, x=I.x)
    with self.assertRaisesRegex(
        ValueError, r'the first argument of kd\.call must be a functor, got'
    ):
      expr_eval.eval(expr, k='a', x=10)

  def test_missing_key_with_default(self):
    expr = kde.functor.switch(
        I.k,
        {'a': lambda x: x + 1, kd_SWITCH_DEFAULT: lambda x: x + 100},
        x=I.x,
    )
    testing.assert_equal(expr_eval.eval(expr, k=ds(None), x=10), ds(110))

  def test_missing_key_without_default(self):
    expr = kde.functor.switch(I.k, {'a': lambda x: x + 1}, x=I.x)
    with self.assertRaisesRegex(
        ValueError,
        'kd.switch: key not found in cases and no default provided',
    ):
      expr_eval.eval(expr, k=ds(None), x=10)

  def test_bind_qvalues(self):
    fn1 = functor_factories.fn(lambda x: x + 1)
    fn2 = functor_factories.fn(lambda x: x * 2)
    key = 'a'
    cases = {'a': fn1, 'b': fn2}
    args = (10,)
    kwargs = dict(y=2)
    expr = kde.functor.switch(key, cases, *args, **kwargs)
    k, c_keys, c_fns, rta, a, kw, nd = expr.node_deps
    testing.assert_equal(k.qvalue, ds('a'))
    testing.assert_equal(c_keys.qvalue, ds(['a', 'b']))
    testing.assert_equivalent(c_fns.qvalue, ds([fn1, fn2]))
    testing.assert_equal(rta.qvalue, ds(None))
    testing.assert_equal(a.qvalue, kd.tuple(ds(10)))
    testing.assert_equal(kw.qvalue, kd.namedtuple(y=ds(2)))
    testing.assert_non_deterministic_exprs_equal(
        nd, optools.unified_non_deterministic_arg()
    )

  def test_bind_expr_cases(self):
    fn1 = functor_factories.fn(lambda x: x + 1)
    cases = kde.dict(kde.slice([I.case_key]), kde.slice([fn1]))
    expr = kde.functor.switch('a', cases)
    _, c_keys, c_fns, _, _, _, _ = expr.node_deps
    testing.assert_equal(c_keys, kde.get_keys(cases))
    testing.assert_equal(c_fns, kde.get_values(cases))

  def test_bind_expr_args_kwargs(self):
    fn1 = functor_factories.fn(lambda x: x + 1)
    cases = {'a': fn1}
    args = (I.x,)
    kwargs = dict(y=I.y)
    expr = kde.functor.switch('a', cases, *args, **kwargs)
    _, _, _, _, a, kw, _ = expr.node_deps
    testing.assert_equal(a, kde.tuples.tuple(I.x))
    testing.assert_equal(kw, kde.tuples.namedtuple(y=I.y))

  def test_repr(self):
    expr = kde.functor.switch(
        I.k,
        {
            # Non-lexicographic order.
            'b': lambda x: kde.attrs(x, foo='b'),
            'a': lambda x: kde.attrs(x, foo='a'),
            kd_SWITCH_DEFAULT: lambda x: kde.attrs(x, foo='default'),
        },
        return_type_as=kde.attrs(I.k),
        x=I.x,
    )
    self.assertEqual(
        repr(expr),
        """kd.functor.switch(
    I.k,
    'b': Functor FunctorSwitchTest.test_repr.<locals>.<lambda>[x](
      returns=kd.attrs(I.x, overwrite_schema=DataItem(False, schema: BOOLEAN), foo=DataItem('b', schema: STRING)),
    ),
    'a': Functor FunctorSwitchTest.test_repr.<locals>.<lambda>[x](
      returns=kd.attrs(I.x, overwrite_schema=DataItem(False, schema: BOOLEAN), foo=DataItem('a', schema: STRING)),
    ),
    kd.SWITCH_DEFAULT: Functor FunctorSwitchTest.test_repr.<locals>.<lambda>[x](
      returns=kd.attrs(I.x, overwrite_schema=DataItem(False, schema: BOOLEAN), foo=DataItem('default', schema: STRING)),
    ),
    x=I.x, return_type_as=kd.attrs(I.k, overwrite_schema=DataItem(False, schema: BOOLEAN)))""",
    )

  def test_repr_non_literal_cases(self):
    expr = kde.functor.switch(
        I.k,
        I.cases,
        return_type_as=kde.attrs(I.k),
        x=I.x,
    )
    self.assertEqual(
        repr(expr),
        """kd.functor.switch(
    I.k,
    kd.dict(kd.get_keys(I.cases), kd.get_values(I.cases, unspecified)),
    x=I.x, return_type_as=kd.attrs(I.k, overwrite_schema=DataItem(False, schema: BOOLEAN)))""",
    )

  def test_view(self):
    expr = kde.functor.switch(
        I.k,
        {
            'a': lambda x: x + 1,
            'b': lambda x: x - 1,
        },
        x=I.x,
    )
    self.assertTrue(view.has_koda_view(expr))


if __name__ == '__main__':
  absltest.main()
