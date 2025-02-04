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

import dataclasses
from absl.testing import absltest
from koladata.base.py_conversions import dataclasses_util


class DataclassesPyUtilTest(absltest.TestCase):

  def test_creates_dataclass(self):
    attr_names = ['x', 'y', 'z']
    obj = dataclasses_util.make_dataclass(attr_names)(1, 2.0, 'abc')
    self.assertEqual(dataclasses.asdict(obj), {'x': 1, 'y': 2.0, 'z': 'abc'})

  def test_creates_dataclass_with_default_values(self):
    attr_names = ['x', 'y', 'z']
    obj = dataclasses_util.make_dataclass(attr_names)(1, 2.0)
    self.assertEqual(dataclasses.asdict(obj), {'x': 1, 'y': 2.0, 'z': None})

  def test_dataclass_eq(self):
    attr_names = ['x', 'y', 'z']

    obj = dataclasses_util.make_dataclass(attr_names)(1, 2.0, 'abc')

    @dataclasses.dataclass
    class Obj:
      x: int
      y: float
      z: str

    expected = Obj(1, 2.0, 'abc')

    # Test that comparison is commutative.
    self.assertEqual(obj, expected)
    self.assertEqual(expected, obj)


if __name__ == '__main__':
  absltest.main()
