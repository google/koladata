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

"""Tests for functor_factories."""

import re

from absl.testing import absltest
from koladata import kd as user_facing_kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import view as _
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.functor import signature_utils
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import schema_constants

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kde')
kde = kde_operators.kde
kdi = user_facing_kd.kdi
pack_expr = introspection.pack_expr


class FunctorFactoriesTest(absltest.TestCase):

  def test_fn_simple(self):
    v = fns.new(foo=57)
    signature = signature_utils.signature([
        signature_utils.parameter(
            'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
        signature_utils.parameter(
            'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
    ])
    fn = functor_factories.fn(
        returns=I.x + V.foo, signature=signature, foo=I.y, bar=v
    )
    self.assertEqual(fn.returns, pack_expr(I.x + V.foo))
    self.assertEqual(fn.get_attr('__signature__'), signature)
    self.assertEqual(
        fn.get_attr('__signature__').parameters[:].name.to_py(), ['x', 'y']
    )
    self.assertEqual(fn.foo, pack_expr(I.y))
    self.assertEqual(fn.bar, v)
    self.assertEqual(fn.bar.foo, 57)

  def test_fn_default_signature(self):
    v = fns.new(foo=57)
    fn = functor_factories.fn(returns=I.x + V.foo, foo=I.y, bar=v)
    signature = fn.get_attr('__signature__')
    self.assertEqual(
        signature.parameters[:].name.to_py(),
        ['self', 'x', 'y', '__extra_inputs__'],
    )
    self.assertEqual(
        signature.parameters[:].kind.to_py(),
        [
            signature_utils.ParameterKind.POSITIONAL_ONLY,
            signature_utils.ParameterKind.KEYWORD_ONLY,
            signature_utils.ParameterKind.KEYWORD_ONLY,
            signature_utils.ParameterKind.VAR_KEYWORD,
        ],
    )

  def test_fn_with_slice(self):
    with self.assertRaisesRegex(ValueError, 'returns must be a data item'):
      _ = functor_factories.fn(
          ds([1, 2]), signature=signature_utils.signature([])
      )

  def test_fn_bad_signature(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting signature to be a DataSlice, got int'
    ):
      _ = functor_factories.fn(
          returns=I.x,
          signature=57,
      )

  def test_is_fn(self):
    fn = functor_factories.fn(57, signature=signature_utils.signature([]))
    self.assertTrue(functor_factories.is_fn(fn))
    self.assertEqual(
        functor_factories.is_fn(fn).get_schema(), schema_constants.MASK
    )
    del fn.returns
    self.assertFalse(functor_factories.is_fn(fn))
    self.assertEqual(
        functor_factories.is_fn(fn).get_schema(), schema_constants.MASK
    )
    self.assertFalse(functor_factories.is_fn(57))

  def test_auto_variables(self):
    x = ds([1, -2, 3, -4])
    fn = functor_factories.fn(I.x * x)
    testing.assert_equal(fn(x=x), ds([1, 4, 9, 16]))
    self.assertNotIn('aux_0', dir(fn))

    fn = functor_factories.fn(I.x * x, auto_variables=True)
    testing.assert_equal(fn(x=x), ds([1, 4, 9, 16]))
    testing.assert_equal(fn.aux_0[:].no_db(), ds([1, -2, 3, -4]))

    fn2 = functor_factories.fn(kde.call(fn, x=I.y), auto_variables=True)
    testing.assert_equal(fn2(y=x), ds([1, 4, 9, 16]))
    self.assertEqual(fn2.aux_0, fn)

    fn3 = functor_factories.fn(
        kde.with_name(fn, 'foo')(x=I.y), auto_variables=True
    )
    testing.assert_equal(fn3(y=x), ds([1, 4, 9, 16]))
    self.assertEqual(fn3.foo, fn)

    fn4 = functor_factories.fn(
        kde.with_name(py_boxing.as_expr(fn), 'foo')(x=I.y)
        + kde.with_name(py_boxing.as_expr(1) + py_boxing.as_expr(2), 'bar'),
        auto_variables=True,
    )
    testing.assert_equal(fn4(y=x), ds([4, 7, 12, 19]))
    self.assertEqual(fn4.foo, fn)
    testing.assert_equal(
        expr_eval.eval(introspection.unpack_expr(fn4.bar)), ds(3)
    )

    fn5 = functor_factories.fn(
        kde.with_name(py_boxing.as_expr(ds([[1, 2], [3]])), 'foo'),
        auto_variables=True,
    )
    testing.assert_equal(fn5().no_db(), ds([[1, 2], [3]]))
    testing.assert_equal(fn5.foo[:][:].no_db(), ds([[1, 2], [3]]))
    self.assertNotIn('aux_0', dir(fn5))

    # TODO: Make this work.
    # fn6 = functor_factories.fn(
    #     kde.slice([1, 2, 3]).with_name('foo'), auto_variables=True
    # )
    # testing.assert_equal(fn6(), ds([1, 2, 3]))
    # testing.assert_equal(fn6.foo[:][:].no_db(), ds([1, 2, 3]))
    # self.assertNotIn('aux_0', dir(fn6))

  def test_auto_variables_nested_names(self):
    x = kde.with_name(kde.with_name(I.x, 'foo'), 'bar')
    fn = functor_factories.fn(x, auto_variables=True)
    testing.assert_equal(fn(x=1), ds(1))
    testing.assert_equal(introspection.unpack_expr(fn.returns), V.bar)
    testing.assert_equal(introspection.unpack_expr(fn.bar), V.foo)
    testing.assert_equal(introspection.unpack_expr(fn.foo), I.x)

  def test_auto_variables_and_existing_variables(self):
    x = ds([1, -2, 3, -4])
    y = ds([2, -2, -3, -4])
    shared = kde.with_name(x, 'foo')
    fn = functor_factories.fn(
        shared + kde.with_name(y, 'foo') + V.foo,
        # TODO: Make V.foo_1[:] work.
        foo=kde.explode(V.foo_1) + shared,
        foo_1=fns.list([4, 5, -1, 7]),
        auto_variables=True,
    )
    testing.assert_equal(fn(), ds([8, -1, 2, -5]))
    self.assertIn('foo_0', dir(fn))
    self.assertIn('foo_2', dir(fn))
    self.assertNotIn(
        'foo_3', dir(fn)
    )  # To make sure we don't have too many copies.

  def test_trace_py_fn(self):

    def my_model(x):
      # TODO: Make this work with
      # kd.slice([1.0, 0.5, 1.5]).with_name('weights')
      weights = user_facing_kd.with_name(kdi.slice([1.0, 0.5, 1.5]), 'weights')
      return user_facing_kd.agg_sum(
          user_facing_kd.stack(x.a, x.b, x.c) * weights
      )

    testing.assert_equal(
        my_model(fns.obj(a=1.0, b=2.0, c=3.0)),
        ds(1.0 * 1.0 + 2.0 * 0.5 + 3.0 * 1.5),
    )

    fn = functor_factories.trace_py_fn(my_model)
    testing.assert_equal(
        fn(x=fns.obj(a=1.0, b=2.0, c=3.0)),
        ds(1.0 * 1.0 + 2.0 * 0.5 + 3.0 * 1.5),
    )
    testing.assert_equal(fn.weights[:].no_db(), ds([1.0, 0.5, 1.5]))
    fn.weights = fns.list([2.0, 3.0, 4.0])
    testing.assert_equal(
        fn(x=fns.obj(a=1.0, b=2.0, c=3.0)),
        ds(1.0 * 2.0 + 2.0 * 3.0 + 3.0 * 4.0),
    )

    fn = functor_factories.trace_py_fn(my_model, auto_variables=False)
    testing.assert_equal(
        fn(x=fns.obj(a=1.0, b=2.0, c=3.0)),
        ds(1.0 * 1.0 + 2.0 * 0.5 + 3.0 * 1.5),
    )
    self.assertNotIn('weights', dir(fn))

    fn = functor_factories.trace_py_fn(my_model, x=fns.obj(a=3.0, b=4.0, c=5.0))
    testing.assert_equal(fn(), ds(3.0 * 1.0 + 4.0 * 0.5 + 5.0 * 1.5))
    testing.assert_equal(
        fn(x=fns.obj(a=1.0, b=2.0, c=3.0)),
        ds(1.0 * 1.0 + 2.0 * 0.5 + 3.0 * 1.5),
    )

    fn = functor_factories.trace_py_fn(lambda x, y=1: x + y)
    testing.assert_equal(fn(2), ds(3))
    testing.assert_equal(fn(2, 3), ds(5))

    fn = functor_factories.trace_py_fn(lambda x, y=1, /: x + y)
    testing.assert_equal(fn(2), ds(3))
    testing.assert_equal(fn(2, 3), ds(5))

    fn = functor_factories.trace_py_fn(lambda x, *unused: x + 1)
    testing.assert_equal(fn(2), ds(3))
    testing.assert_equal(fn(2, 3), ds(3))

    fn = functor_factories.trace_py_fn(lambda x, **unused_kwargs: x + 1)
    testing.assert_equal(fn(2), ds(3))
    testing.assert_equal(fn(2, foo=3), ds(3))

  def test_trace_py_fn_expr_defaults(self):
    fn = functor_factories.trace_py_fn(lambda x, y, **unused: x + y, y=2 * I.z)
    testing.assert_equal(fn(x=2, z=3), ds(8))

  def test_py_fn_simple(self):
    def f(x, y):
      return x + y

    testing.assert_equal(kd.call(functor_factories.py_fn(f), x=1, y=2), ds(3))
    testing.assert_equal(
        kd.call(
            functor_factories.py_fn(f),
            x=ds([1, 2, 3]),
            y=ds([4, 5, 6]),
        ),
        ds([5, 7, 9]),
    )

  def test_py_fn_var_keyword(self):
    def f_kwargs(x, y, **kwargs):
      return x + y + kwargs['z']

    fn = functor_factories.py_fn(f_kwargs)
    testing.assert_equal(kd.call(fn, x=1, y=2, z=3), ds(6))
    testing.assert_equal(
        kd.call(fn, x=ds([1, 2]), y=ds([3, 4]), z=ds([5, 6])),
        ds([9, 12]),
    )
    testing.assert_equal(
        kd.call(functor_factories.py_fn(f_kwargs, x=1, y=2, z=3)), ds(6)
    )
    # Extra kwargs are ignored
    testing.assert_equal(kd.call(fn, x=1, y=2, z=3, w=4), ds(6))

  def test_py_fn_default_arguments(self):
    testing.assert_equal(
        kd.call(functor_factories.py_fn(lambda x, y=1: x + y), x=5), ds(6)
    )
    testing.assert_equal(
        kd.call(functor_factories.py_fn(lambda x, y=1: x + y), x=5, y=2),
        ds(7),
    )
    testing.assert_equal(
        kd.call(functor_factories.py_fn(lambda x, y: x + y, y=1), x=5),
        ds(6),
    )
    testing.assert_equal(
        kd.call(functor_factories.py_fn(lambda x, y: x + y, y=1), x=5, y=2),
        ds(7),
    )
    testing.assert_equal(
        kd.call(functor_factories.py_fn(lambda x, y: x + y, y=2 * I.x), x=5),
        ds(15),
    )

    def default_none(x, y=None):
      if y is None:
        y = 1
      return x + y

    testing.assert_equal(
        kd.call(functor_factories.py_fn(default_none), x=5), ds(6)
    )

  def test_py_fn_positional_params(self):
    def var_positional(*args):
      return sum(args)

    testing.assert_equal(
        kd.call(functor_factories.py_fn(var_positional), 1, 2, 3), ds(6)
    )

    def positional_only(args, /):
      return args

    testing.assert_equal(
        kd.call(functor_factories.py_fn(positional_only), 1), ds(1)
    )
    with self.assertRaisesRegex(
        TypeError, 'positional-only arguments passed as keyword'
    ):
      _ = kd.call(functor_factories.py_fn(positional_only), args=1)

    with self.assertRaisesRegex(
        TypeError, "missing 1 required positional argument: 'y'"
    ):
      _ = kd.call(
          functor_factories.py_fn(lambda x, y: x + y), fns.obj(x=1, y=2)
      )
    testing.assert_equal(
        kd.call(
            functor_factories.py_fn(lambda foo: foo.x + foo.y),
            fns.obj(x=1, y=2),
        ),
        ds(3),
    )
    testing.assert_equal(
        kd.call(
            functor_factories.py_fn(lambda foo, /: foo.x + foo.y),
            fns.obj(x=1, y=2),
        ),
        ds(3),
    )
    with self.assertRaisesRegex(TypeError, "unexpected keyword argument 'y'"):
      _ = (
          kd.call(
              functor_factories.py_fn(lambda foo: foo.x + foo.y),
              fns.obj(x=1, y=2),
              y=5,
          ),
          6,
      )
    testing.assert_equal(
        kd.call(functor_factories.py_fn(lambda foo, bar=1, /: foo + bar), 2),
        ds(3),
    )
    testing.assert_equal(
        kd.call(functor_factories.py_fn(lambda foo, bar=1, /: foo + bar), 2, 4),
        ds(6),
    )

  def test_py_fn_no_params(self):
    def no_params():
      return 1

    f = functor_factories.py_fn(no_params)
    testing.assert_equal(kd.call(f), ds(1))

  def test_py_fn_after_clone(self):
    def after_clone(x, y):
      return x + y + 1

    fn = kd.clone(functor_factories.py_fn(after_clone))
    testing.assert_equal(kd.call(fn, x=1, y=1), ds(3))

    def after_clone2(x, y=None, z=2):
      if y is None:
        y = 0
      return x + y + z

    fn = kd.clone(functor_factories.py_fn(after_clone2))
    testing.assert_equal(kd.call(fn, x=1), ds(3))
    testing.assert_equal(kd.call(fn, x=1, z=1), ds(2))
    testing.assert_equal(kd.call(fn, x=1, y=1), ds(4))
    testing.assert_equal(kd.call(fn, x=1, y=1, z=3), ds(5))

  def test_py_fn_partial_params(self):
    f = functor_factories.py_fn(lambda x, y, z: x + y + z, x=1)
    testing.assert_equal(kd.call(f, y=2, z=3), ds(6))

  def test_py_fn_list_as_param_default(self):
    def list_default(x=[1, 2]):  # pylint: disable=dangerous-default-value
      return len(x)

    testing.assert_equal(kd.call(functor_factories.py_fn(list_default)), ds(2))

  def test_bind_full_params(self):
    fn = functor_factories.fn(I.x + I.y)
    f = functor_factories.bind(fn, x=0, y=1)
    testing.assert_equal(kd.call(f), ds(1))

  def test_bind_partial_params(self):
    fn = functor_factories.fn(I.x + I.y)
    f = functor_factories.bind(fn, x=0)
    testing.assert_equal(kd.call(f, y=1), ds(1))

  def test_bind_params_not_in_sig(self):
    fn = functor_factories.fn(I.x + I.y)
    f = functor_factories.bind(fn, x=0, z=2)
    testing.assert_equal(kd.call(f, y=1), ds(1))

  def test_override_bound_params(self):
    fn = functor_factories.fn(I.x + I.y)
    f = functor_factories.bind(fn, x=0)
    testing.assert_equal(kd.call(f, x=2, y=1), ds(3))

  def test_bind_to_expr(self):
    fn = functor_factories.fn(I.x + I.y)
    f = functor_factories.bind(fn, y=I.z)
    testing.assert_equal(kd.call(f, x=1, z=2), ds(3))

  def test_bind_to_packed_expr(self):
    fn = functor_factories.fn(I.x + I.y)
    f = functor_factories.bind(fn, y=introspection.pack_expr(I.z))
    testing.assert_equal(kd.call(f, x=1, z=2), ds(3))

  def test_bind_to_expr_twice(self):
    fn = functor_factories.fn(I.x + I.y)
    f1 = functor_factories.bind(fn, y=I.z)
    f2 = functor_factories.bind(f1, z=I.w)
    testing.assert_equal(kd.call(f2, x=1, w=2), ds(3))

  def test_bind_does_not_overwrite_assignments(self):
    fn = functor_factories.fn(V.x + I.y, x=I.z + 1)
    f = functor_factories.bind(fn, x=0)
    testing.assert_equal(kd.call(f, z=0, y=1), ds(2))

  def test_bind_bindings_override_signature_defaults(self):
    fn = functor_factories.py_fn(lambda x, y=1: x + y)
    f = functor_factories.bind(fn, y=2)
    testing.assert_equal(kd.call(f, x=0), ds(2))

  def test_bind_partial_params_fails(self):
    fn = functor_factories.fn(I.x + I.y)
    f = functor_factories.bind(fn, x=0)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('no value provided for keyword only parameter [y]'),
    ):
      _ = kd.call(f)

  # TODO: Make this work.
  # def test_bind_fn_variable(self):
  #   # Note the usage of kde.bind when we need to bind a function variable.
  #   f = functor_factories.fn(
  #       kde.call(V.bound_fn, y=I.y),
  #       bound_fn=kde.bind(V.z.extract(), x=0),
  #       z=functor_factories.fn(I.x + I.y),
  #   )
  #   testing.assert_equal(kd.call(f, y=1), ds(1))

  def test_bind_py_fn(self):
    f = functor_factories.bind(
        functor_factories.py_fn(lambda x, y, **kwargs: x + y), x=1, y=I.z
    )
    testing.assert_equal(kd.call(f, z=2), ds(3))
    f = functor_factories.bind(
        functor_factories.py_fn(lambda x, y: x + y), x=1, y=I.z
    )
    with self.assertRaisesRegex(TypeError, "unexpected keyword argument 'z'"):
      # This forwards argument 'z' to the underlying Python function as well,
      # which does not accept it.
      _ = kd.call(f, z=2)

  # TODO: Make this work.
  # def test_bind_as_kd_op(self):
  #   fn = functor_factories.fn(I.x + I.y)
  #   f = kd.bind(fn, x=0, y=1)
  #   testing.assert_equal(kd.call(f), ds(1))

  def test_bind_with_self(self):
    fn = functor_factories.fn(I.x + I.y + I.self)
    f = functor_factories.bind(fn, x=1)
    testing.assert_equal(kd.call(f, 2, y=3), ds(6))
    testing.assert_equal(kd.call(f, 2, y=3, x=10), ds(15))

  def test_bind_positional(self):
    fn = functor_factories.py_fn(lambda x, /: x)
    f = functor_factories.bind(fn, x=1)
    with self.assertRaisesRegex(
        TypeError,
        'positional-only arguments passed as keyword arguments',
    ):
      _ = f()

    fn = functor_factories.py_fn(lambda x: x)
    f = functor_factories.bind(fn, x=1)
    testing.assert_equal(f().no_db(), ds(1))
    with self.assertRaisesRegex(
        TypeError,
        'got multiple values for argument',
    ):
      _ = f(1)


if __name__ == '__main__':
  absltest.main()
