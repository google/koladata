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
from koladata.operators import kde_operators as _
from koladata.util import kd_functools

I = input_container.InputContainer('I')


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


if __name__ == '__main__':
  absltest.main()
