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
from koladata import kd as user_facing_kd
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import view as _
from koladata.functions import functions as fns
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.functor import py_functors_py_ext as _py_functors_py_ext
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import schema_constants


I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde
kdi = user_facing_kd.eager
pack_expr = introspection.pack_expr


class AutoVariablesTest(absltest.TestCase):

  def test_nested_names(self):
    x = kde.with_name(kde.with_name(I.x, 'foo'), 'bar')
    fn = functor_factories.expr_fn(x, auto_variables=True)
    testing.assert_equal(fn(x=1), ds(1))
    testing.assert_equal(introspection.unpack_expr(fn.returns), V.bar)
    testing.assert_equal(introspection.unpack_expr(fn.bar), V.foo)
    testing.assert_equal(introspection.unpack_expr(fn.foo), I.x)

  def test_existing_variables(self):
    x = ds([1, -2, 3, -4])
    y = ds([2, -2, -3, -4])
    shared = kde.with_name(x, 'foo')
    fn = functor_factories.expr_fn(
        shared + kde.with_name(y, 'foo') + V.foo,
        foo=V.foo_1[:] + shared,
        foo_1=fns.list([4, 5, -1, 7]),
        auto_variables=True,
    )
    testing.assert_equal(fn(), ds([8, -1, 2, -5]))
    self.assertIn('foo_0', kdi.dir(fn))
    self.assertIn('foo_2', kdi.dir(fn))
    self.assertNotIn(
        'foo_3', kdi.dir(fn)
    )  # To make sure we don't have too many copies.

  def test_with_none(self):
    fn = functor_factories.expr_fn(
        I.x + ds(None),
        auto_variables=True,
    )
    # We should not introduce variables for ds(None), it would be too much
    # noise.
    testing.assert_equal(introspection.unpack_expr(fn.returns), I.x + ds(None))

  def test_does_not_lose_cast(self):
    fn = functor_factories.expr_fn(
        kde.slice(kdi.slice([1, 2, 3]), schema_constants.INT64),
        auto_variables=True,
    )
    testing.assert_equal(fn().no_bag(), ds([1, 2, 3], schema_constants.INT64))

  def test_dont_duplicate_computations(self):
    eval_count = 0

    def my_op(x):
      nonlocal eval_count
      eval_count += 1
      return x

    base = kde.py.apply_py(my_op, I.x)
    expr = (base + 1).with_name('foo') + (base + 2).with_name('bar')
    fn = functor_factories.expr_fn(expr, auto_variables=True)
    testing.assert_equal(fn(x=1), ds(5))
    self.assertEqual(eval_count, 1)

  def test_dont_create_spurious_variables(self):
    base = I.x * I.x
    expr = (base + 1) + (base + 2)
    fn = functor_factories.expr_fn(expr, auto_variables=True)
    testing.assert_equal(fn(x=1), ds(5))
    # Even though "base" is repeated twice, it should not be extracted as a
    # variable.
    self.assertCountEqual(fns.dir(fn), ['__signature__', 'returns'])

  def test_dont_create_too_many_variables(self):
    base = (I.x + 1) * 2
    expr = (base + 1).with_name('foo') + (base + 2).with_name('bar')
    fn = functor_factories.expr_fn(expr, auto_variables=True)
    testing.assert_equal(fn(x=1), ds(11))
    # This should create just one auxiliary variable, for "base".
    self.assertCountEqual(
        fns.dir(fn), ['__signature__', 'returns', '_aux_0', 'foo', 'bar']
    )

  def test_dont_duplicate_computations_across_existing_variables(self):
    eval_count = 0

    def my_op(x):
      nonlocal eval_count
      eval_count += 1
      return x

    base = kde.py.apply_py(my_op, I.x)
    fn = functor_factories.expr_fn(
        V.v1 + V.v2, v1=base + 1, v2=base + 2, auto_variables=True
    )
    testing.assert_equal(fn(x=1), ds(5))
    self.assertEqual(eval_count, 1)

    fn = functor_factories.expr_fn(
        V.v1 + V.v2, v1=base, v2=base, auto_variables=True
    )
    testing.assert_equal(fn(x=1), ds(2))
    self.assertEqual(eval_count, 2)

  def test_two_names_for_a_literal(self):
    base = py_boxing.literal(ds([1, 2, 3]))
    fn = functor_factories.expr_fn(
        base.with_name('foo') + base.with_name('bar'), auto_variables=True
    )
    testing.assert_equal(fn(), ds([2, 4, 6]))
    # Make sure that only one auxiliary variable for the literal is created.
    self.assertCountEqual(
        fns.dir(fn), ['__signature__', 'returns', 'foo', 'bar', '_aux_0']
    )
    testing.assert_equal(fn.get_attr('_aux_0')[:].no_bag(), ds([1, 2, 3]))
    testing.assert_equal(
        introspection.unpack_expr(fn.foo),
        kde.explode(V._aux_0, ndim=1),
    )
    testing.assert_equal(
        introspection.unpack_expr(fn.bar),
        kde.explode(V._aux_0, ndim=1),
    )

  def test_shared_literal(self):
    base = py_boxing.literal(ds([1, 2, 3]))
    fn = functor_factories.expr_fn(
        (base + 1).with_name('foo') + (base + 2).with_name('bar'),
        auto_variables=True,
    )
    testing.assert_equal(fn(), ds([5, 7, 9]))
    # Make sure that only one auxiliary variable for the literal is created.
    self.assertCountEqual(
        fns.dir(fn), ['__signature__', 'returns', 'foo', 'bar', '_aux_0']
    )
    testing.assert_equal(fn.get_attr('_aux_0')[:].no_bag(), ds([1, 2, 3]))
    testing.assert_equal(
        introspection.unpack_expr(fn.foo),
        kde.explode(V._aux_0, ndim=1) + 1,
    )
    testing.assert_equal(
        introspection.unpack_expr(fn.bar),
        kde.explode(V._aux_0, ndim=1) + 2,
    )

  def test_shared_literal_used_in_shared_node(self):
    base = py_boxing.literal(ds([1, 2, 3]))
    fn = functor_factories.expr_fn(
        (base + 1).with_name('foo') + ((base + 1) + base.with_name('bar')),
        auto_variables=True,
    )
    testing.assert_equal(fn(), ds([5, 8, 11]))
    self.assertCountEqual(
        fns.dir(fn),
        ['__signature__', 'returns', 'foo', 'bar', '_aux_0', '_aux_1'],
    )
    testing.assert_equal(fn.get_attr('_aux_0')[:].no_bag(), ds([1, 2, 3]))
    testing.assert_equal(
        introspection.unpack_expr(fn.get_attr('_aux_1')),
        kde.explode(V._aux_0, ndim=1) + 1,
    )
    testing.assert_equal(introspection.unpack_expr(fn.foo), V._aux_1)
    testing.assert_equal(
        introspection.unpack_expr(fn.bar),
        kde.explode(V._aux_0, ndim=1),
    )

  def test_extracts_single_object_without_bag(self):
    obj = kd.obj(bar=1).no_bag()
    foo = py_boxing.literal(obj)
    fn = functor_factories.expr_fn(foo.bar, auto_variables=True)
    self.assertCountEqual(fns.dir(fn), ['__signature__', 'returns', '_aux_0'])
    testing.assert_equal(fn.get_attr('_aux_0').no_bag(), obj)

  def test_two_uses_of_input(self):
    fn = functor_factories.expr_fn(
        V.foo + V.bar,
        foo=I.x,
        bar=I.x,
        auto_variables=True,
    )
    testing.assert_equal(fn(x=1), ds(2))
    self.assertCountEqual(
        fns.dir(fn), ['__signature__', 'returns', 'foo', 'bar']
    )
    testing.assert_equal(introspection.unpack_expr(fn.foo), I.x)

  def test_two_uses_of_number(self):
    fn = functor_factories.expr_fn(
        V.foo + V.bar,
        foo=py_boxing.literal(ds(57)),
        bar=py_boxing.literal(ds(57)),
        auto_variables=True,
    )
    testing.assert_equal(fn(x=1), ds(114))
    self.assertCountEqual(
        fns.dir(fn), ['__signature__', 'returns', 'foo', 'bar']
    )
    testing.assert_equal(
        introspection.unpack_expr(fn.foo), py_boxing.literal(ds(57))
    )

  def test_nested_duplicate_computations(self):
    eval_count = 0

    def my_op(x):
      nonlocal eval_count
      eval_count += 1
      return x

    expr = I.x + 1
    expr = kde.py.apply_py(my_op, expr)
    expr = (expr + 1).with_name('foo') + (expr + 2).with_name('bar')
    expr = kde.py.apply_py(my_op, expr)
    expr = (expr + 1).with_name('baz') + (expr + 2).with_name('qux')
    fn = functor_factories.expr_fn(expr, auto_variables=True)
    testing.assert_equal(fn(x=1), ds(17))
    self.assertEqual(eval_count, 2)
    self.assertCountEqual(
        fns.dir(fn),
        [
            '__signature__',
            'returns',
            'foo',
            'bar',
            'baz',
            'qux',
            '_aux_0',
            '_aux_1',
        ],
    )

  def test_extract_extra_nodes(self):
    base = (I.x + 1) * 2
    expr = (base + 1).with_name('foo') + (base + 2)

    fn = functor_factories.expr_fn(expr, auto_variables=False)
    testing.assert_equal(fn(x=1), ds(11))
    self.assertCountEqual(fns.dir(fn), ['__signature__', 'returns'])

    fn0 = _py_functors_py_ext.auto_variables(fn)
    testing.assert_equal(fn0(x=1), ds(11))
    self.assertCountEqual(
        fns.dir(fn0), ['__signature__', 'returns', '_aux_0', 'foo']
    )
    # `base` is extracted because used in both `foo` and `returns`
    testing.assert_equal(
        introspection.unpack_expr(fn0.get_attr('_aux_0')), base
    )
    testing.assert_equal(introspection.unpack_expr(fn0.foo), V._aux_0 + 1)

    # Doesn't change anything because `base` is already extracted
    fn1 = _py_functors_py_ext.auto_variables(fn0, [base.fingerprint])
    testing.assert_equal(fn1(x=1), ds(11))
    self.assertCountEqual(
        fns.dir(fn1), ['__signature__', 'returns', '_aux_0', 'foo']
    )
    testing.assert_equal(
        introspection.unpack_expr(fn1.get_attr('_aux_0')), base
    )
    testing.assert_equal(introspection.unpack_expr(fn1.foo), V._aux_0 + 1)

    # Extract also `base+2`
    fn2 = _py_functors_py_ext.auto_variables(
        fn, [base.fingerprint, (base + 2).fingerprint]
    )
    testing.assert_equal(fn2(x=1), ds(11))
    self.assertCountEqual(
        fns.dir(fn2),
        ['__signature__', 'returns', '_aux_0', '_aux_1', 'foo'],
    )
    testing.assert_equal(
        introspection.unpack_expr(fn2.get_attr('_aux_0')), base
    )
    testing.assert_equal(
        introspection.unpack_expr(fn2.get_attr('_aux_1')),
        V._aux_0 + 2,
    )
    testing.assert_equal(introspection.unpack_expr(fn2.foo), V._aux_0 + 1)

    # Extract input and literal
    literal_1 = user_facing_kd.expr.literal(ds(1))
    fn3 = _py_functors_py_ext.auto_variables(
        fn, [I.x.fingerprint, literal_1.fingerprint]
    )
    testing.assert_equal(fn3(x=1), ds(11))
    self.assertCountEqual(
        fns.dir(fn3),
        ['__signature__', 'returns', '_aux_0', '_aux_1', '_aux_2', 'foo'],
    )
    testing.assert_equal(introspection.unpack_expr(fn3.get_attr('_aux_0')), I.x)
    testing.assert_equal(
        introspection.unpack_expr(fn3.get_attr('_aux_1')), literal_1
    )
    testing.assert_equal(
        introspection.unpack_expr(fn3.get_attr('_aux_2')),
        (V._aux_0 + V._aux_1) * 2,
    )
    testing.assert_equal(
        introspection.unpack_expr(fn3.foo),
        V._aux_2 + V._aux_1,
    )


if __name__ == '__main__':
  absltest.main()
