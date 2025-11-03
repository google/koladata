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
from koladata.expr import input_container
from koladata.expr import source_location
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.util import kd_functools

I = input_container.InputContainer('I')
kd_lazy = kde_operators.kde


class SourceLocationTest(absltest.TestCase):

  def test_annotate_with_current_source_location(self):
    @kd_functools.skip_from_functor_stack_trace
    def foo():
      return source_location.annotate_with_current_source_location(I.x + 1)

    def bar():
      return foo()

    def baz():
      return bar()

    annotated_expr = baz()
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        annotated_expr.op,
        arolla.abc.lookup_operator('kd.annotation.source_location'),
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        annotated_expr.node_deps[0],
        I.x + 1,
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        annotated_expr.node_deps[1].qvalue, arolla.text('bar')
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        annotated_expr.node_deps[2].qvalue,
        arolla.text(
            'py/koladata/expr/source_location_test.py'
        ),
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        annotated_expr.node_deps[5].qvalue,
        arolla.text('      return foo()'),
    )

  def test_attaching_source_location(self):
    @arolla.optools.add_to_registry(if_present='unsafe_override')
    @arolla.optools.as_lambda_operator('test.plus_one')
    def plus_one(x):
      return x + 1

    plus_one_with_source_location = source_location.attaching_source_location(
        plus_one
    )

    with self.subTest('wraps_operator'):
      self.assertTrue(callable(plus_one_with_source_location))
      expr = plus_one_with_source_location(I.x)
      testing.assert_traced_exprs_equal(expr, plus_one(I.x))
      testing.assert_equal(expr.op, kd_lazy.annotation.source_location)
      self.assertIn('source_location_test.py', str(expr.node_deps[2].qvalue))

    container = source_location.attaching_source_location(
        arolla.OperatorsContainer(
            unsafe_extra_namespaces=['test', 'test.subcontainer']
        )
    )
    self.assertIsInstance(container, source_location._OperatorsContainerWrapper)
    self.assertIsInstance(
        container.test, source_location._OperatorsContainerWrapper
    )

    with self.subTest('wraps_container'):
      expr = container.test.plus_one(I.x)
      testing.assert_traced_exprs_equal(expr, plus_one(I.x))
      testing.assert_equal(expr.op, kd_lazy.annotation.source_location)
      self.assertIn('source_location_test.py', str(expr.node_deps[2].qvalue))

    with self.subTest('wraps_nested_container'):
      self.assertIsInstance(
          container.test.subcontainer,
          source_location._OperatorsContainerWrapper,
      )
      nested_plus_one = arolla.optools.add_to_registry(
          'test.subcontainer.plus_one', if_present='unsafe_override'
      )(plus_one)
      expr = container.test.subcontainer.plus_one(I.x)
      testing.assert_traced_exprs_equal(expr, nested_plus_one(I.x))
      testing.assert_equal(expr.op, kd_lazy.annotation.source_location)
      self.assertIn('source_location_test.py', str(expr.node_deps[2].qvalue))


if __name__ == '__main__':
  absltest.main()
