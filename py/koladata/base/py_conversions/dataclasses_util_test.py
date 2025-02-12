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
from koladata.base.py_conversions import testing_clib
from koladata.types import data_slice

ds = data_slice.DataSlice.from_vals


class DataclassesUtilTest(absltest.TestCase):

  def test_make_empty_dataclass(self):
    util = testing_clib.DataClassesUtil()
    obj = util.make_dataclass_instance([])
    self.assertEqual(dataclasses.asdict(obj), {})

  def test_make_dataclass_with_fields(self):
    util = testing_clib.DataClassesUtil()
    obj = util.make_dataclass_instance(['b', 'c', 'a'])
    # Attributes are sorted alphabetically.
    self.assertEqual(dataclasses.asdict(obj), {'a': None, 'b': None, 'c': None})

  def test_make_empty_dataclass_repeated_fields(self):
    util = testing_clib.DataClassesUtil()
    with self.assertRaisesRegex(ValueError, 'could not create a new dataclass'):
      _ = util.make_dataclass_instance(['a', 'a'])

  def test_make_empty_dataclass_empty_field_name(self):
    util = testing_clib.DataClassesUtil()
    with self.assertRaisesRegex(ValueError, 'could not create a new dataclass'):
      _ = util.make_dataclass_instance([''])

  def test_make_dataclass_from_different_instances(self):
    util1 = testing_clib.DataClassesUtil()
    util2 = testing_clib.DataClassesUtil()
    obj1 = util1.make_dataclass_instance(['a'])
    obj2 = util2.make_dataclass_instance(['a'])
    self.assertEqual(obj1, obj2)
    self.assertNotEqual(obj1.__class__, obj2.__class__)

  def test_make_dataclass_caches_classes(self):
    util = testing_clib.DataClassesUtil()
    obj1 = util.make_dataclass_instance(['a'])
    obj2 = util.make_dataclass_instance(['a'])
    obj3 = util.make_dataclass_instance(['b'])
    self.assertEqual(obj1, obj2)
    self.assertEqual(obj1.__class__, obj2.__class__)
    self.assertNotEqual(obj1, obj3)
    self.assertNotEqual(obj1.__class__, obj3.__class__)


if __name__ == '__main__':
  absltest.main()
