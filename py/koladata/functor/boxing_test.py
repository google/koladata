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

from absl.testing import absltest
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import py_boxing


ds = data_slice.DataSlice.from_vals


class BoxingTest(absltest.TestCase):

  def test_simple(self):
    fn = py_boxing.as_qvalue(lambda x: x + 1)
    testing.assert_equal(fn(5), ds(6))
    fn = py_boxing.as_qvalue(functools.partial(lambda x, y: x + y, y=1))
    testing.assert_equal(fn(5), ds(6))

  def test_error(self):
    with self.assertRaisesRegex(
        TypeError,
        re.escape("__bool__ disabled for 'arolla.abc.expr.Expr'")
    ):
      _ = py_boxing.as_qvalue(lambda x: 6 if x == 5 else 7)

    with self.assertRaisesWithPredicateMatch(
        TypeError,
        arolla.testing.any_note_regex(
            'Error occurred during tracing of the function.*If you only need'
            ' Python evaluation, you can use `kd[.]py_fn[(]fn[)]` instead',
        ),
    ):
      _ = py_boxing.as_qvalue(lambda x: 6 if x == 5 else 7)

  # TODO: make aux variables disappear here.
  def test_aux_variable_for_boxed_named_fn(self):
    yes_executed = False
    no_executed = False

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def yes(x):
      nonlocal yes_executed
      yes_executed = True
      return x + 1

    @tracing_decorator.TraceAsFnDecorator(py_fn=True)
    def no(x):
      nonlocal no_executed
      no_executed = True
      return x - 1

    def short_circuit_fn(x):
      return user_facing_kd.cond(x > 3, yes, no)(x)

    fn = functor_factories.trace_py_fn(short_circuit_fn)

    testing.assert_equal(fn(3), ds(2))
    self.assertFalse(yes_executed)
    self.assertTrue(no_executed)

    self.assertCountEqual(
        fn.get_attr_names(intersection=True),
        [
            'returns',
            '_aux_0',
            '_aux_1',
            '__signature__',
            '__qualname__',
            '__module__',
        ],
    )
    # We do not want to enforce which of 0/1 corresponds to which of yes/no.
    self.assertCountEqual(
        set(fn.get_attr('_aux_0').get_attr_names(intersection=True))
        | set(fn.get_attr('_aux_1').get_attr_names(intersection=True)),
        [
            'returns',
            'yes',
            '_yes_result',
            'no',
            '_no_result',
            '__signature__',
            '__qualname__',
            '__module__',
        ],
    )


if __name__ == '__main__':
  absltest.main()
