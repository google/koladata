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

"""Tests for kdf."""

from absl.testing import absltest
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import view as _
from koladata.functor import kdf
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class KdfTest(absltest.TestCase):

  def test_simple(self):
    fn = kdf.fn(returns=I.x + V.foo, foo=I.y)
    testing.assert_equal(kdf.call(fn, x=1, y=2), ds(3))
    testing.assert_equal(fn(x=1, y=2), ds(3))
    self.assertTrue(kdf.is_fn(fn))
    self.assertFalse(kdf.is_fn(57))

  def test_factorial(self):
    fn = kdf.fn(
        kde.cond(I.n == 0, V.stop, V.go)(n=I.n),
        go=kdf.fn(I.n * V.rec(n=I.n - 1)),
        stop=kdf.fn(1),
    )
    fn.go.rec = fn
    testing.assert_equal(kdf.call(fn, n=5), ds(120))

  def test_trace_py_fn(self):
    fn = kdf.trace_py_fn(lambda x, y: x + y)
    testing.assert_equal(kdf.call(fn, x=1, y=2), ds(3))
    testing.assert_equal(introspection.unpack_expr(fn.returns), I.x + I.y)

  def test_py_fn(self):
    fn = kdf.py_fn(lambda x, y: x + 1 if y == 2 else x + 3)
    testing.assert_equal(kdf.call(fn, x=1, y=2), ds(2))
    testing.assert_equal(kdf.call(fn, x=1, y=3), ds(4))

  def test_bind(self):
    fn = kdf.fn(I.x + I.y)
    f = kdf.bind(fn, x=1)
    testing.assert_equal(kdf.call(f, y=2), ds(3))


if __name__ == '__main__':
  absltest.main()
