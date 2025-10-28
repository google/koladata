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

import functools
import re
import traceback

from absl.testing import absltest
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import view as _
from koladata.extension_types import extension_types
from koladata.functions import functions as fns
from koladata.functions import object_factories
from koladata.functions import py_conversions
from koladata.functions import s11n
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import schema_constants
from koladata.types import signature_utils


I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde
kdi = user_facing_kd.eager
pack_expr = introspection.pack_expr


@extension_types.extension_type()
class TestExtension:
  x: schema_constants.INT32

  def fn(self, v):
    return self.x + v


def test_add_fn(x, y=2, **kwargs):
  """Returns something.

  Args:
    x: Input.
    y: Input.
    **kwargs: Input.
  """
  return x + y + kwargs['z']


def test_simple_add_fn(x, y):
  return x + y


def test_identity_fn(x):
  return x


def test_identity_fn_2(x):
  return x


@functools.wraps(test_identity_fn_2)
def test_bad_identity_fn(x):
  return x + 1


def extension_type_fn(x: TestExtension):
  return x.fn(2)


class FunctorFactoriesTest(absltest.TestCase):

  def test_expr_fn_simple(self):
    v = fns.new(foo=57)
    signature = signature_utils.signature([
        signature_utils.parameter(
            'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
        signature_utils.parameter(
            'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
    ])
    fn = functor_factories.expr_fn(
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

  def test_expr_fn_default_signature(self):
    v = fns.new(foo=57)
    fn = functor_factories.expr_fn(returns=I.x + V.foo, foo=I.y, bar=v)
    signature = fn.get_attr('__signature__')
    self.assertEqual(
        signature.parameters[:].name.to_py(),
        ['self', 'x', 'y', '__extra_inputs__'],
    )
    self.assertEqual(
        signature.parameters[:].kind.internal_as_py(),
        [
            signature_utils.ParameterKind.POSITIONAL_ONLY,
            signature_utils.ParameterKind.KEYWORD_ONLY,
            signature_utils.ParameterKind.KEYWORD_ONLY,
            signature_utils.ParameterKind.VAR_KEYWORD,
        ],
    )

  def test_expr_fn_with_slice(self):
    with self.assertRaisesRegex(ValueError, 'returns must be a data item'):
      _ = functor_factories.expr_fn(
          ds([1, 2]), signature=signature_utils.signature([])
      )

  def test_expr_fn_bad_signature(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting signature to be a DataSlice, got int'
    ):
      _ = functor_factories.expr_fn(  # pytype: disable=wrong-arg-types
          returns=I.x,
          signature=57,
      )

  def test_is_fn(self):
    fn = functor_factories.expr_fn(57, signature=signature_utils.signature([]))
    self.assertTrue(functor_factories.is_fn(fn))
    self.assertEqual(
        functor_factories.is_fn(fn).get_schema(), schema_constants.MASK
    )
    fn = fn.with_attrs(returns=None)
    self.assertFalse(functor_factories.is_fn(fn))
    self.assertEqual(
        functor_factories.is_fn(fn).get_schema(), schema_constants.MASK
    )
    self.assertFalse(functor_factories.is_fn(57))
    self.assertFalse(functor_factories.is_fn(ds(57)))

  def test_auto_variables(self):
    x = ds([1, -2, 3, -4])
    fn = functor_factories.expr_fn(I.x * x)
    testing.assert_equal(fn(x=x), ds([1, 4, 9, 16]))
    self.assertNotIn('_aux_0', kdi.dir(fn))

    fn = functor_factories.expr_fn(I.x * x, auto_variables=True)
    testing.assert_equal(fn(x=x), ds([1, 4, 9, 16]))
    testing.assert_equal(fn.get_attr('_aux_0')[:].no_bag(), ds([1, -2, 3, -4]))

    fn2 = functor_factories.expr_fn(kde.call(fn, x=I.y), auto_variables=True)
    testing.assert_equal(fn2(y=x), ds([1, 4, 9, 16]))
    self.assertEqual(fn2.get_attr('_aux_0'), fn)

    fn3 = functor_factories.expr_fn(
        kde.with_name(fn, 'foo')(x=I.y), auto_variables=True
    )
    testing.assert_equal(fn3(y=x), ds([1, 4, 9, 16]))
    self.assertEqual(fn3.foo, fn)

    fn4 = functor_factories.expr_fn(
        kde.with_name(py_boxing.as_expr(fn), 'foo')(x=I.y)
        + kde.with_name(py_boxing.as_expr(1) + py_boxing.as_expr(2), 'bar'),
        auto_variables=True,
    )
    testing.assert_equal(fn4(y=x), ds([4, 7, 12, 19]))
    self.assertEqual(fn4.foo, fn)
    testing.assert_equal(
        expr_eval.eval(introspection.unpack_expr(fn4.bar)), ds(3)
    )

    fn5 = functor_factories.expr_fn(
        kde.with_name(py_boxing.as_expr(ds([[1, 2], [3]])), 'foo'),
        auto_variables=True,
    )
    testing.assert_equal(fn5().no_bag(), ds([[1, 2], [3]]))
    testing.assert_equal(fn5.foo[:][:].no_bag(), ds([[1, 2], [3]]))
    self.assertNotIn('_aux_0', kdi.dir(fn5))

    fn6 = functor_factories.expr_fn(
        kde.slice([1, 2, 3]).with_name('foo'), auto_variables=True
    )
    testing.assert_equal(fn6().no_bag(), ds([1, 2, 3]))
    testing.assert_equal(fn6.foo[:].no_bag(), ds([1, 2, 3]))
    self.assertNotIn('_aux_0', kdi.dir(fn6))

  def test_trace_py_fn(self):

    def my_model(x):
      weights = user_facing_kd.slice([1.0, 0.5, 1.5]).with_name('weights')
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
    testing.assert_equal(fn.weights[:].no_bag(), ds([1.0, 0.5, 1.5]))
    fn = fn.with_attrs(weights=fns.list([2.0, 3.0, 4.0]))
    testing.assert_equal(
        fn(x=fns.obj(a=1.0, b=2.0, c=3.0)),
        ds(1.0 * 2.0 + 2.0 * 3.0 + 3.0 * 4.0),
    )

    fn = functor_factories.trace_py_fn(my_model, auto_variables=False)
    testing.assert_equal(
        fn(x=fns.obj(a=1.0, b=2.0, c=3.0)),
        ds(1.0 * 1.0 + 2.0 * 0.5 + 3.0 * 1.5),
    )
    self.assertNotIn('weights', kdi.dir(fn))

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

  def test_trace_py_fn_docstring(self):
    def my_model(x):
      """Returns my model.

      Args:
        x: Input.
      """
      return x

    testing.assert_equal(
        functor_factories.trace_py_fn(my_model).get_attr('__doc__').no_bag(),
        ds('Returns my model.\n\nArgs:\n  x: Input.'),
    )

  def test_trace_py_fn_qualname(self):
    def my_model(x):
      return x

    testing.assert_equal(
        functor_factories.trace_py_fn(my_model)
        .get_attr('__qualname__')
        .no_bag(),
        ds('FunctorFactoriesTest.test_trace_py_fn_qualname.<locals>.my_model'),
    )

  def test_trace_py_fn_module(self):
    def my_model(x):
      return x

    testing.assert_equal(
        functor_factories.trace_py_fn(my_model).get_attr('__module__').no_bag(),
        ds('__main__'),
    )

  def test_trace_py_fn_select(self):
    def select_fn(val):
      return user_facing_kd.select(val, lambda x: x >= 2)

    fn = functor_factories.trace_py_fn(select_fn)
    testing.assert_equivalent(fn(ds([1, 2, 3])), ds([2, 3]))

    def select_functor_filter(val):
      return user_facing_kd.select(val, functor_factories.expr_fn(I.self >= 2))

    fn = functor_factories.trace_py_fn(select_functor_filter)
    testing.assert_equivalent(fn(ds([1, 2, 3])), ds([2, 3]))

  def test_trace_py_fn_expr_defaults(self):
    fn = functor_factories.trace_py_fn(lambda x, y, **unused: x + y, y=2 * I.z)
    testing.assert_equal(fn(x=2, z=3), ds(8))

  def test_trace_py_fn_auto_boxed_item_name(self):

    def my_model(x):
      return user_facing_kd.with_name(57, 'foo') * x

    fn = functor_factories.trace_py_fn(my_model)
    testing.assert_equal(fn(x=2), ds(114))
    testing.assert_equal(fn.foo.no_bag(), ds(57))

  def test_trace_py_fn_explicit_item_name(self):

    def my_model(x):
      return user_facing_kd.item(57).with_name('foo') * x

    fn = functor_factories.trace_py_fn(my_model)
    testing.assert_equal(fn(x=2), ds(114))
    testing.assert_equal(fn.foo.no_bag(), ds(57))

  def test_trace_py_fn_explicit_item_as_slice_name(self):

    def my_model(x):
      return user_facing_kd.slice(57).with_name('foo') * x

    fn = functor_factories.trace_py_fn(my_model)
    testing.assert_equal(fn(x=2), ds(114))
    testing.assert_equal(fn.foo.no_bag(), ds(57))

  def test_trace_py_fn_non_primitive_item_name(self):

    itemid = kd.new_itemid()

    def my_model():
      return user_facing_kd.with_name(itemid, 'foo')

    fn = functor_factories.trace_py_fn(my_model)
    testing.assert_equal(fn().no_bag(), itemid)
    testing.assert_equal(fn.foo.no_bag(), itemid)

  def test_trace_py_fn_non_primitive_item_name_with_bag(self):

    my_item = kd.new(a=1)

    def my_model():
      return user_facing_kd.with_name(my_item, 'foo').a

    fn = functor_factories.trace_py_fn(my_model)
    testing.assert_equal(fn().no_bag(), ds(1))
    testing.assert_equal(fn.foo.no_bag(), my_item.no_bag())

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

  def test_py_fn_py_reference(self):
    def f(x, y, z):
      return x + y + z.py_value().a

    class _A:

      def __init__(self, a):
        self.a = a

    testing.assert_equal(
        kd.call(
            functor_factories.py_fn(f),
            x=1,
            y=2,
            z=kdi.py_reference(_A(3)),
        ),
        ds(6),
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

  def test_py_fn_docstring(self):
    def f(x):
      """Returns my model.

      Args:
        x: Input.
      """
      return x

    testing.assert_equal(
        functor_factories.py_fn(f).get_attr('__doc__').no_bag(),
        ds('Returns my model.\n\nArgs:\n  x: Input.'),
    )

  def test_py_fn_qualname(self):
    def my_py_fn(x):
      return x

    testing.assert_equal(
        functor_factories.py_fn(my_py_fn).get_attr('__qualname__').no_bag(),
        ds('FunctorFactoriesTest.test_py_fn_qualname.<locals>.my_py_fn'),
    )

  def test_py_fn_module(self):
    def my_py_fn(x):
      return x

    testing.assert_equal(
        functor_factories.py_fn(my_py_fn).get_attr('__module__').no_bag(),
        ds('__main__'),
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
    with self.assertRaisesWithPredicateMatch(
        TypeError,
        arolla.testing.any_cause_message_regex(
            'positional-only arguments passed as keyword'
        ),
    ):
      _ = kd.call(functor_factories.py_fn(positional_only), args=1)

    with self.assertRaisesWithPredicateMatch(
        TypeError,
        arolla.testing.any_cause_message_regex(
            "missing 1 required positional argument: 'y'"
        ),
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
    with self.assertRaisesWithPredicateMatch(
        TypeError,
        arolla.testing.any_cause_message_regex(
            "unexpected keyword argument 'y'"
        ),
    ):
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

  def test_py_fn_explicit_return_type(self):
    testing.assert_equal(
        kd.call(
            functor_factories.py_fn(
                lambda: arolla.tuple(1), return_type_as=arolla.tuple(2)
            ),
            return_type_as=arolla.tuple(2),
        ),
        arolla.tuple(1),
    )
    res = kd.call(
        functor_factories.py_fn(
            lambda: object_factories.mutable_bag(), return_type_as=data_bag.DataBag  # pylint: disable=unnecessary-lambda
        ),
        return_type_as=data_bag.DataBag,
    )
    self.assertIsInstance(res, data_bag.DataBag)

  def test_fstr_fn_simple(self):
    testing.assert_equal(
        kd.call(functor_factories.fstr_fn(f'{I.x:s} {I.y:s}'), x=1, y=2),
        ds('1 2'),
    )

    testing.assert_equal(
        kd.call(functor_factories.fstr_fn(f'{(I.x + I.y):s}'), x=1, y=2),
        ds('3'),
    )

  def test_fstr_fn_expr(self):
    testing.assert_equal(
        kd.call(
            functor_factories.fstr_fn(f'{kde.select(I.x, kdi.present):s}'),
            x=ds([1, None]),
        ),
        ds(['1', None]),
    )

  def test_fstr_fn_variable(self):
    testing.assert_equal(
        kd.call(functor_factories.fstr_fn(f'{V.x:s} {I.y:s}', x=1), y=2),
        ds('1 2'),
    )

  def test_fstr_fn_no_substitutions(self):
    with self.assertRaisesRegex(ValueError, 'FString has nothing to format'):
      _ = kd.call(functor_factories.fstr_fn('abc'))

    with self.assertRaisesRegex(ValueError, 'FString has nothing to format'):
      # we need to use fstring to avoid the error (f'{I.x}')
      _ = kd.call(functor_factories.fstr_fn('{I.x}'), x=1)

  def test_bind_full_params(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    f = functor_factories.bind(fn, x=0, y=1)
    testing.assert_equal(kd.call(f), ds(1))

  def test_bind_partial_params(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    f = functor_factories.bind(fn, x=0)
    testing.assert_equal(kd.call(f, y=1), ds(1))

  def test_bind_params_not_in_sig(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    f = functor_factories.bind(fn, x=0, z=2)
    testing.assert_equal(kd.call(f, y=1), ds(1))

  def test_override_bound_params(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    f = functor_factories.bind(fn, x=0)
    testing.assert_equal(kd.call(f, x=2, y=1), ds(3))

  def test_bind_to_expr(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    f = functor_factories.bind(fn, y=I.z)
    testing.assert_equal(kd.call(f, x=1, z=2), ds(3))

  def test_bind_to_expr_with_variables(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    f = functor_factories.bind(fn, y=V.z * 2, z=3)
    testing.assert_equal(kd.call(f, x=1), ds(7))

  def test_bind_no_inner_functor_without_expr(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    f = functor_factories.bind(fn, y=5)
    testing.assert_equal(kd.call(f, x=1), ds(6))
    testing.assert_equal(f.y, ds(5).with_bag(f.get_bag()))

  def test_bind_to_packed_expr(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    f = functor_factories.bind(fn, y=introspection.pack_expr(I.z))
    testing.assert_equal(kd.call(f, x=1, z=2), ds(3))

  def test_bind_to_expr_twice(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    f1 = functor_factories.bind(fn, y=I.z)
    f2 = functor_factories.bind(f1, z=I.w)
    testing.assert_equal(kd.call(f2, x=1, w=2), ds(3))

  def test_bind_does_not_overwrite_assignments(self):
    fn = functor_factories.expr_fn(V.x + I.y, x=I.z + 1)
    f = functor_factories.bind(fn, x=0)
    testing.assert_equal(kd.call(f, z=0, y=1), ds(2))

  def test_bind_bindings_override_signature_defaults(self):
    fn = functor_factories.py_fn(lambda x, y=1: x + y)
    f = functor_factories.bind(fn, y=2)
    testing.assert_equal(kd.call(f, x=0), ds(2))

  def test_bind_partial_params_fails(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    f = functor_factories.bind(fn, x=0)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('no value provided for keyword only parameter [y]'),
    ):
      _ = kd.call(f)

  def test_bind_fn_variable(self):
    # Note the usage of kde.bind when we need to bind a function variable.
    f = functor_factories.expr_fn(
        kde.call(V.bound_fn, y=I.y),
        bound_fn=kde.bind(V.z.extract(), x=0),
        z=functor_factories.expr_fn(I.x + I.y),
    )
    testing.assert_equal(kd.call(f, y=1), ds(1))

  def test_bind_py_fn(self):
    f = functor_factories.bind(
        functor_factories.py_fn(lambda x, y, **kwargs: x + y), x=1, y=I.z
    )
    testing.assert_equal(kd.call(f, z=2), ds(3))
    f = functor_factories.bind(
        functor_factories.py_fn(lambda x, y: x + y), x=1, y=I.z
    )
    with self.assertRaisesWithPredicateMatch(
        TypeError,
        arolla.testing.any_cause_message_regex(
            "unexpected keyword argument 'z'"
        ),
    ):
      # This forwards argument 'z' to the underlying Python function as well,
      # which does not accept it.
      _ = kd.call(f, z=2)

  def test_bind_as_kd_op(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    f = kd.bind(fn, x=0, y=1)
    testing.assert_equal(kd.call(f), ds(1))

  def test_bind_with_self(self):
    fn = functor_factories.expr_fn(I.x + I.y + I.self)
    f = functor_factories.bind(fn, x=1)
    testing.assert_equal(kd.call(f, 2, y=3), ds(6))
    testing.assert_equal(kd.call(f, 2, y=3, x=10), ds(15))

  def test_bind_positional(self):
    fn = functor_factories.py_fn(lambda x, /: x)
    f = functor_factories.bind(fn, x=1)
    with self.assertRaisesWithPredicateMatch(
        TypeError,
        arolla.testing.any_cause_message_regex(
            'positional-only arguments passed as keyword arguments'
        ),
    ):
      _ = f()

    fn = functor_factories.py_fn(lambda x: x)
    f = functor_factories.bind(fn, x=1)
    testing.assert_equal(f().no_bag(), ds(1))
    with self.assertRaisesWithPredicateMatch(
        TypeError,
        arolla.testing.any_cause_message_regex(
            'got multiple values for argument'
        ),
    ):
      _ = f(1)

  def test_bind_explicit_return_type(self):
    example_tuple = arolla.tuple(ds(0), ds(0))
    fn = functor_factories.py_fn(
        lambda x: arolla.tuple(x, x), return_type_as=example_tuple
    )
    f = functor_factories.bind(fn, return_type_as=example_tuple, x=2)
    testing.assert_equal(
        f(return_type_as=example_tuple), arolla.tuple(ds(2), ds(2))
    )
    fn = functor_factories.py_fn(
        lambda x: object_factories.mutable_bag(), return_type_as=data_bag.DataBag  # pylint: disable=unnecessary-lambda
    )
    f = functor_factories.bind(fn, return_type_as=data_bag.DataBag, x=2)
    res = f(return_type_as=data_bag.DataBag)
    self.assertIsInstance(res, data_bag.DataBag)

  def test_bind_non_functor(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('bind() expects a functor')
    ):
      functor_factories.bind(ds(1), y=2)

  def test_bind_args_kwargs(self):
    sig = signature_utils.signature([
        signature_utils.parameter(
            'x', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
        signature_utils.parameter(
            'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
        ),
    ])
    fn = functor_factories.expr_fn(
        I.x + I.y,
        signature=sig,
    )
    f = functor_factories.bind(fn, 1, 2)
    testing.assert_equal(kd.call(f), ds(3))
    f = functor_factories.bind(fn, 1)
    testing.assert_equal(kd.call(f, 2), ds(3))
    f = functor_factories.bind(fn)
    testing.assert_equal(kd.call(f, 1, 2), ds(3))

    f = functor_factories.bind(fn, 1, y=2)
    testing.assert_equal(kd.call(f), ds(3))

    f = functor_factories.bind(fn)
    testing.assert_equal(kd.call(f, x=1, y=2), ds(3))

    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex('parameter .y. specified twice'),
    ):
      kd.call(functor_factories.bind(fn, 1), 2, y=2)

    f = functor_factories.bind(
        functor_factories.allow_arbitrary_unused_inputs(fn), 1, I.z
    )
    testing.assert_equal(kd.call(f, z=2), ds(3))

    f = functor_factories.bind(
        functor_factories.allow_arbitrary_unused_inputs(fn), V.z * 2, z=3
    )
    testing.assert_equal(kd.call(f, 1), ds(7))

    f = functor_factories.bind(
        functor_factories.py_fn(lambda x, y, **kwargs: x + y), 1, y=I.z
    )
    testing.assert_equal(kd.call(f, z=2), ds(3))

  def test_fn_expr(self):
    testing.assert_equal(
        kd.call(functor_factories.fn(I.x + I.y), x=ds([1, 2]), y=ds([3, 4])),
        ds([4, 6]),
    )
    testing.assert_equal(
        kd.call(
            functor_factories.fn(I.x + kde.explode(V.y), y=fns.list([3, 4])),
            x=ds([1, 2]),
        ),
        ds([4, 6]),
    )

  def test_fn_packed_expr(self):
    testing.assert_equal(
        kd.call(
            functor_factories.fn(introspection.pack_expr(I.x + I.y)),
            x=ds([1, 2]),
            y=ds([3, 4]),
        ),
        ds([4, 6]),
    )

  def test_fn_expr_dataslice(self):
    testing.assert_equal(
        kd.call(functor_factories.fn(I.x + I.y), x=ds([1, 2]), y=ds([3, 4])),
        ds([4, 6]),
    )

  def test_fn_python_function(self):
    fn1 = functor_factories.fn(lambda x, y: x + y)
    fn2 = functor_factories.fn(lambda x, y: x + y, use_tracing=True)
    fn3 = functor_factories.fn(
        lambda x, y: ds([a + b for a, b in zip(x.to_py(), y.to_py())]),
        use_tracing=False,
    )
    testing.assert_equal(kd.call(fn1, x=ds([1, 2]), y=ds([3, 4])), ds([4, 6]))
    testing.assert_equal(kd.call(fn2, x=ds([1, 2]), y=ds([3, 4])), ds([4, 6]))
    testing.assert_equal(kd.call(fn3, x=ds([1, 2]), y=ds([3, 4])), ds([4, 6]))
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(fn2.returns), I.x + I.y
    )
    testing.assert_traced_exprs_equal(
        introspection.unpack_expr(fn1.returns), I.x + I.y
    )

    fn4 = functor_factories.fn(lambda x, y: x + y, use_tracing=True, y=5)
    fn5 = functor_factories.fn(lambda x, y: x + y, use_tracing=False, y=5)
    testing.assert_equal(kd.call(fn4, x=ds([1, 2])), ds([6, 7]))
    testing.assert_equal(kd.call(fn5, x=ds([1, 2])), ds([6, 7]))

  def test_fn_functools_partial(self):
    def f(x, y):
      return x + y

    with self.subTest('positional-arg'):
      fn1 = functor_factories.fn(functools.partial(f, 1), use_tracing=True)
      testing.assert_equal(kd.call(fn1, y=ds([3, 4])), ds([4, 5]))
      testing.assert_traced_exprs_equal(
          introspection.unpack_expr(fn1.returns), 1 + I.y
      )
    with self.subTest('keyword-arg'):
      fn2 = functor_factories.fn(functools.partial(f, x=2), use_tracing=True)
      testing.assert_equal(kd.call(fn2, y=ds([3, 4])), ds([5, 6]))
      testing.assert_equal(kd.call(fn2, x=ds([1, 2]), y=ds([3, 4])), ds([4, 6]))
      testing.assert_traced_exprs_equal(
          introspection.unpack_expr(fn2.returns), I.x + I.y
      )

  def test_fn_existing_fn(self):
    existing_fn = functor_factories.expr_fn(I.x + I.y)
    testing.assert_equal(
        kd.call(functor_factories.fn(existing_fn), x=ds([1, 2]), y=ds([3, 4])),
        ds([4, 6]),
    )

  def test_fn_existing_fn_fails(self):
    existing_fn = functor_factories.expr_fn(I.x + I.y)

    with self.assertRaisesWithLiteralMatch(
        ValueError, 'passed kwargs when calling fn on an existing functor'
    ):
      functor_factories.fn(existing_fn, x=1)

  def test_fn_unknown_type(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'cannot convert 57 into a functor'
    ):
      functor_factories.fn(57)

  def test_map_py_fn_by_reference(self):
    def f(x, y):
      return x + y if x < 2 else x

    self.assertEqual(
        kd.call(functor_factories.map_py_fn(f), x=1, y=2).to_py(), 3
    )

    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(f),
            x=kdi.slice([1, 2, 3]),
            y=kdi.slice([4, 5, 6]),
        ).to_py(),
        [5, 2, 3],
    )
    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(lambda x, y: x + y, x=1), y=2
        ).to_py(),
        3,
    )

  def test_map_py_fn_args_passed_to_eval(self):
    def f_kwargs(x, y, **kwargs):
      return x + y + kwargs['z']

    map_fn = functor_factories.map_py_fn(f_kwargs)
    self.assertEqual(kd.call(map_fn, x=1, y=2, z=3).to_py(), 6)
    self.assertEqual(
        kd.call(
            map_fn,
            x=kdi.slice([1, 2]),
            y=kdi.slice([3, 4]),
            z=kdi.slice([5, 6]),
        ).to_py(),
        [9, 12],
    )
    map_fn = functor_factories.map_py_fn(f_kwargs, x=0, z=1, unused=2)
    self.assertEqual(kd.call(map_fn, x=1, y=2, z=3).to_py(), 6)
    self.assertEqual(kd.call(map_fn, y=2).to_py(), 3)

  def test_map_py_fn_ndim(self):
    def fn(x):
      return len(x) if isinstance(x, list) else 0

    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(fn), x=kdi.slice([1, 2, 3])
        ).to_py(),
        [0, 0, 0],
    )

    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(fn, ndim=1), x=kdi.slice([1, 2, 3])
        ).to_py(),
        3,
    )

  def test_map_py_fn_with_missing_items(self):
    def fn(x):
      return -1 if x is None else x + 1

    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(fn), x=kdi.slice([1, None, None])
        ).to_py(),
        [2, None, None],
    )
    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(fn, include_missing=True),
            x=kdi.slice([1, None, None]),
        ).to_py(),
        [2, -1, -1],
    )
    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(fn, include_missing=False),
            x=kdi.slice([1, None, None]),
        ).to_py(),
        [2, None, None],
    )

  def test_map_py_fn_default_arguments(self):
    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(lambda x, y=1: x + y),
            x=kdi.slice([1, 2, 3]),
        ).to_py(),
        [2, 3, 4],
    )
    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(lambda x, y=1: x + y), x=5, y=2
        ).to_py(),
        7,
    )
    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(lambda x, y: x + y, y=1), x=5
        ).to_py(),
        6,
    )
    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(lambda x, y: x + y, y=1), x=5, y=2
        ).to_py(),
        7,
    )

  def test_map_py_fn_var_keyword(self):
    def g(x, **kwargs):
      return x + kwargs['y']

    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(g, y=1), x=kdi.slice([1, 2, 3])
        ).to_py(),
        [2, 3, 4],
    )
    # Extra kwargs are ignored
    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(g, y=1, z=4), x=kdi.slice([1, 2, 3])
        ).to_py(),
        [2, 3, 4],
    )

  def test_map_py_fn_positional_params(self):
    def var_positional(*args):
      return sum(args)

    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(var_positional),
            kdi.slice([1, 2]),
            kdi.slice([3, 4]),
        ).to_py(),
        [4, 6],
    )

    def positional_only(args, /):
      return args

    self.assertEqual(
        kd.call(
            functor_factories.map_py_fn(positional_only), kdi.slice([1, 2])
        ).to_py(),
        [1, 2],
    )

  def test_map_py_fn_expr_for_function(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected a function, got I.f'
    ):
      functor_factories.map_py_fn(I.f, x=I.y)

  def test_get_signature(self):
    fn = functor_factories.expr_fn(I.x + I.y)
    sig = functor_factories.get_signature(fn)
    testing.assert_equal(
        sig.parameters[:].name.no_bag(),
        ds(['self', 'x', 'y', '__extra_inputs__']),
    )
    testing.assert_equal(
        sig.parameters[:].kind.no_bag(),
        ds([
            signature_utils.ParameterKind.POSITIONAL_ONLY,
            signature_utils.ParameterKind.KEYWORD_ONLY,
            signature_utils.ParameterKind.KEYWORD_ONLY,
            signature_utils.ParameterKind.VAR_KEYWORD,
        ]).no_bag(),
    )
    fn2 = functor_factories.expr_fn(
        I.x + I.y,
        signature=signature_utils.signature([
            signature_utils.parameter(
                'x', signature_utils.ParameterKind.POSITIONAL_ONLY
            ),
            signature_utils.parameter(
                'y', signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD
            ),
        ]),
    )
    sig = functor_factories.get_signature(fn2)
    testing.assert_equal(sig.parameters[:].name.no_bag(), ds(['x', 'y']))
    testing.assert_equal(
        sig.parameters[:].kind.no_bag(),
        ds([
            signature_utils.ParameterKind.POSITIONAL_ONLY,
            signature_utils.ParameterKind.POSITIONAL_OR_KEYWORD,
        ]).no_bag(),
    )

  def test_allow_arbitrary_unused_inputs(self):
    fn = functor_factories.fn(lambda x, y: x + y)
    self.assertEqual(fn(x=1, y=2), 3)
    with self.assertRaisesRegex(ValueError, 'unknown keyword arguments.*z'):
      _ = fn(x=1, y=2, z=3)
    fn2 = functor_factories.allow_arbitrary_unused_inputs(fn)
    self.assertEqual(fn2(x=1, y=2), 3)
    self.assertEqual(fn2(x=1, y=2, z=3), 3)

  def test_allow_arbitrary_unused_inputs_no_effect_on_py_fn(self):
    # py_fn creates a wrapper function with a variadic keyword argument,
    # so allow_arbitrary_unused_inputs has no effect anymore.
    fn = functor_factories.py_fn(lambda x, y: x + y)
    self.assertEqual(fn(x=1, y=2), 3)
    with self.assertRaisesWithPredicateMatch(
        TypeError,
        arolla.testing.any_cause_message_regex(
            "got an unexpected keyword argument 'z'"
        ),
    ):
      _ = fn(x=1, y=2, z=3)
    fn2 = functor_factories.allow_arbitrary_unused_inputs(fn)
    self.assertEqual(fn2(x=1, y=2), 3)
    with self.assertRaisesWithPredicateMatch(
        TypeError,
        arolla.testing.any_cause_message_regex(
            "got an unexpected keyword argument 'z'"
        ),
    ):
      _ = fn2(x=1, y=2, z=3)

  def test_evaluation_errors(self):
    # See also test_evaluation_errors in tracing_decorator_test.py.

    fn1 = functor_factories.expr_fn(
        V.y + 1,
        y=kde.annotation.source_location(
            1 // I.x, 'fn1', 'my_file.py', 57, 0, '  y = 1 // I.x'
        ),
    )
    fn2 = functor_factories.expr_fn(
        V.z + 1,
        z=kde.annotation.source_location(
            fn1(x=V.y), 'fn2', 'my_file.py', 58, 0, '  z = fn1(y)'
        ),
        y=I.x,
    )
    fn3 = functor_factories.bind(fn2, x=0)
    # No source location annotations in fn3.

    try:
      kd.call(fn3)
    except ValueError as e:
      ex = e

    self.assertEqual(
        str(ex),  # pylint: disable=undefined-variable
        'kd.math.floordiv: division by zero',
    )
    tb = '\n'.join(traceback.format_tb(ex.__traceback__))  # pylint: disable=undefined-variable
    self.assertIn('File "my_file.py", line 57, in fn1', tb)
    self.assertIn('File "my_file.py", line 58, in', tb)
    self.assertNotIn('File ""', tb)
    self.assertNotIn('line 0', tb)

  def test_extension_type_py_fn(self):

    def fn(x: TestExtension):
      return x.fn(2)

    e = TestExtension(x=1)
    self.assertEqual(functor_factories.py_fn(fn)(e), 3)

  def test_extension_type_trace_py_fn(self):

    def fn(x: TestExtension):
      return x.fn(2)

    e = TestExtension(x=1)
    self.assertEqual(functor_factories.trace_py_fn(fn)(e), 3)

  def test_py_fn_not_serializable(self):

    def my_fn(x):
      return x + 1

    with self.assertRaisesRegex(
        ValueError, 'missing serialization codec for.*my_fn'
    ):
      s11n.dumps(functor_factories.py_fn(my_fn))

    loaded_fn = s11n.loads(
        s11n.dumps(functor_factories.py_fn(py_conversions.py_reference(my_fn)))
    )
    testing.assert_equal(loaded_fn(1), ds(2))

  def test_register_py_fn(self):
    fn = functor_factories.register_py_fn(test_add_fn)

    with self.subTest('doc'):
      testing.assert_equal(
          fn.get_attr('__doc__').no_bag(),
          ds(
              'Returns something.\n\nArgs:\n  x: Input.\n  y: Input.\n '
              ' **kwargs: Input.'
          ),
      )

    with self.subTest('qualname'):
      testing.assert_equal(
          fn.get_attr('__qualname__').no_bag(), ds('test_add_fn')
      )

    with self.subTest('module'):
      testing.assert_equal(fn.get_attr('__module__').no_bag(), ds('__main__'))

    with self.subTest('eval'):
      testing.assert_equal(kd.call(fn, x=1, y=3, z=4), ds(8))
      testing.assert_equal(kd.call(fn, 1, y=3, z=4, w=5), ds(8))
      testing.assert_equal(kd.call(fn, 1, z=4), ds(7))

    with self.subTest('serialization'):
      loaded_fn = s11n.loads(s11n.dumps(fn))
      testing.assert_equal(kd.call(loaded_fn, 1, z=4), ds(7))

    with self.subTest('registered'):
      op = fn.returns.to_py().unquote().op
      self.assertIsInstance(op, arolla.types.RegisteredOperator)
      self.assertEqual(op.display_name, '__main__.test_add_fn')

  def test_register_py_fn_defaults(self):
    fn = functor_factories.register_py_fn(test_simple_add_fn, y=2)
    testing.assert_equal(kd.call(fn, x=1), ds(3))

  def test_register_py_fn_local_error(self):
    def fn(x):
      return x

    with self.assertRaisesRegex(
        ValueError, 'attempt to register an operator with invalid name'
    ):
      functor_factories.register_py_fn(fn)

  def test_py_fn_register_fn_as_operator_no_module_error(self):
    del test_identity_fn.__module__
    with self.assertRaisesRegex(
        ValueError,
        'qualname and module must be set on the function when registering it as'
        ' operator',
    ):
      functor_factories.register_py_fn(test_identity_fn)

  def test_register_py_fn_reregister_error(self):
    _ = functor_factories.register_py_fn(test_identity_fn_2)
    with self.assertRaisesRegex(
        ValueError, "operator '__main__.test_identity_fn_2' already exists"
    ):
      _ = functor_factories.register_py_fn(test_bad_identity_fn)
    fn = functor_factories.register_py_fn(
        test_bad_identity_fn, unsafe_override=True
    )
    testing.assert_equal(fn(1), ds(2))

  def test_register_py_fn_extension_type(self):
    e = TestExtension(x=1)
    self.assertEqual(functor_factories.register_py_fn(extension_type_fn)(e), 3)


if __name__ == '__main__':
  absltest.main()
