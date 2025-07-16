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
import typing

from absl import flags
from absl.testing import absltest
from arolla import arolla
from koladata import kd

FLAGS = flags.FLAGS


class SerializedTestFunctorsTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self.filename = os.path.join(
        FLAGS.test_srcdir,
        'py/koladata/serving/testing/'
        'serialized_test_functors.kd',
    )

  def test_serialized_test_functors(self):
    with open(self.filename, 'rb') as f:
      serialized_slices = f.read()
    slices, exprs = arolla.s11n.riegeli_loads_many(serialized_slices)
    self.assertLen(slices, 3)
    self.assertEmpty(exprs)

    kd.testing.assert_equal(
        slices[0], kd.slice(['ask_about_serving', 'plus_one'])
    )
    plus_one = typing.cast(kd.types.DataSlice, slices[2])
    kd.testing.assert_equal(
        kd.call(plus_one, kd.slice([1, 2, 3])),
        kd.slice([2, 3, 4])
    )


if __name__ == '__main__':
  absltest.main()
