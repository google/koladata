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

import os.path

from absl import flags
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata import kd
from koladata.serving.testing import test_functors

FLAGS = flags.FLAGS


class SerializedTestFunctorsTest(parameterized.TestCase):

  def load_functor_bytes(self, functor_name: str) -> kd.types.DataSlice:
    filename = os.path.join(
        FLAGS.test_srcdir,
        f'py/koladata/serving/testing/{functor_name}.kd',
    )
    with open(filename, 'rb') as f:
      return f.read()

  def load_functor(self, functor_name: str) -> kd.types.DataSlice:
    slices, exprs = arolla.s11n.riegeli_loads_many(
        self.load_functor_bytes(functor_name)
    )

    self.assertLen(slices, 1)
    self.assertEmpty(exprs)
    self.assertIsInstance(slices[0], kd.types.DataSlice)

    return slices[0]

  def test_serialized_ask_about_serving(self):
    ask_about_serving = self.load_functor(
        'serialized_test_functors_ask_about_serving'
    )
    kd.testing.assert_equal(
        kd.call(ask_about_serving, lambda x: "don't know"),
        kd.slice("don't know"),
    )

  # Validate all 10 functors to check there is no ordering issue.
  @parameterized.parameters(range(1, 11))
  def test_serialized_plus_n(self, n: int):
    plus_n = self.load_functor(f'serialized_test_functors_plus_{n}')
    kd.testing.assert_equal(
        kd.call(plus_n, kd.slice([1, 2, 3])),
        kd.slice([1, 2, 3]) + n,
    )

  def test_serialized_non_deterministic_functor(self):
    non_deterministic_functor_1 = self.load_functor(
        'serialized_test_functors_non_deterministic_functor'
    )
    non_deterministic_functor_2 = self.load_functor(
        'other_serialized_test_functors_for_determinism_test_'
        'non_deterministic_functor'
    )
    # Functors are coming from different build rules, so the deterministic seeds
    # are different.
    kd.testing.assert_not_equal(
        non_deterministic_functor_1,
        non_deterministic_functor_2,
    )
    # But the content is the same.
    kd.testing.assert_non_deterministic_exprs_equal(
        kd.expr.unpack_expr(non_deterministic_functor_1.returns),
        kd.expr.unpack_expr(non_deterministic_functor_2.returns),
    )

    non_deterministic_functor_3 = self.load_functor(
        'other_serialized_test_functors_for_determinism_test_'
        'simplified_non_deterministic_functor'
    )
    # Functor 3 has source locations stripped, but otherwise the exprs are
    # equal.
    kd.testing.assert_traced_non_deterministic_exprs_equal(
        kd.expr.unpack_expr(non_deterministic_functor_3.returns),
        kd.expr.unpack_expr(non_deterministic_functor_1.returns),
    )

    expected_result = kd.obj(
        args=test_functors.XYSchema.new(x=1, y=2),
        literal=test_functors.XYSchema.new(x=57, y=7),
    )

    kd.testing.assert_equivalent(
        non_deterministic_functor_1(1, 2),
        expected_result,
        schemas_equality=True,
    )
    kd.testing.assert_equivalent(
        non_deterministic_functor_2(1, 2),
        expected_result,
        schemas_equality=True,
    )
    kd.testing.assert_equivalent(
        non_deterministic_functor_3(1, 2),
        expected_result,
        schemas_equality=True,
    )


if __name__ == '__main__':
  absltest.main()
