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
from koladata.expr import view as _
from koladata.functor import kdf
from koladata.operators import kde_operators

I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
kde = kde_operators.kde


class KdfTest(absltest.TestCase):

  def test_simple(self):
    fn = kdf.fn(returns=I.x + V.foo, foo=I.y)
    self.assertEqual(kdf.call(fn, x=1, y=2), 3)
    self.assertEqual(fn(x=1, y=2), 3)
    self.assertTrue(kdf.is_fn(fn))
    self.assertFalse(kdf.is_fn(57))

  def test_factorial(self):
    fn = kdf.fn(
        kde.cond(I.n == 0, V.stop, V.go)(n=I.n),
        go=kdf.fn(I.n * V.rec(n=I.n - 1)),
        stop=kdf.fn(1),
    )
    fn.go.rec = fn
    self.assertEqual(kdf.call(fn, n=5), 120)


if __name__ == '__main__':
  absltest.main()
