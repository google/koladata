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

"""Tests for koda_internal.non_deterministic_identity operator."""

from absl.testing import absltest
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.operators import kde_operators as _

M = arolla.M

I = input_container.InputContainer('I')

non_deterministic_identity_op = arolla.abc.lookup_operator(
    'koda_internal.non_deterministic_identity'
)


class NonDeterministicIdentityOpTest(absltest.TestCase):

  def test_eval(self):
    expr = non_deterministic_identity_op(I.x)
    self.assertEqual(expr_eval.eval(expr, x=1), 1)
    self.assertEqual(expr_eval.eval(expr, x='foo'), 'foo')

  def test_non_deterministic_eval(self):
    def fn():
      nonlocal counter
      counter += 1
      return arolla.unit()

    fn = arolla.abc.PyObject(fn)

    with self.subTest('without_non_deterministic_identity'):
      expr = M.py.call(fn, arolla.UNIT)
      counter = 0
      _ = expr_eval.eval(expr)
      self.assertEqual(counter, 1)
      _ = expr_eval.eval(expr)
      self.assertEqual(counter, 1)

    with self.subTest('with_non_deterministic_identity'):
      expr = M.py.call(non_deterministic_identity_op(fn), arolla.UNIT)
      counter = 0
      _ = expr_eval.eval(expr)
      self.assertEqual(counter, 1)
      _ = expr_eval.eval(expr)
      self.assertEqual(counter, 2)


if __name__ == '__main__':
  absltest.main()
