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

FLAGS = flags.FLAGS


class SerializedTestFunctorsTest(parameterized.TestCase):

  def load_serialized_slice(self, functor_name: str) -> kd.types.DataSlice:
    filename = os.path.join(
        FLAGS.test_srcdir,
        'py/koladata/serving/testing/'
        f'serialized_test_functors_{functor_name}.kd',
    )
    with open(filename, 'rb') as f:
      slices, exprs = arolla.s11n.riegeli_loads_many(f.read())

    self.assertLen(slices, 1)
    self.assertEmpty(exprs)
    self.assertIsInstance(slices[0], kd.types.DataSlice)

    return slices[0]

  def test_serialized_ask_about_serving(self):
    ask_about_serving = self.load_serialized_slice('ask_about_serving')
    kd.testing.assert_equal(
        kd.call(ask_about_serving, lambda x: "don't know"),
        kd.slice("don't know"),
    )

  # Validate all 10 functors to check there is no ordering issue.
  @parameterized.parameters(range(1, 11))
  def test_serialized_plus_n(self, n: int):
    plus_n = self.load_serialized_slice(f'plus_{n}')
    kd.testing.assert_equal(
        kd.call(plus_n, kd.slice([1, 2, 3])),
        kd.slice([1, 2, 3]) + n,
    )


if __name__ == '__main__':
  absltest.main()
