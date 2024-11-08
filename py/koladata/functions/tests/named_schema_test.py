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

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.testing import testing
from koladata.types import data_slice

ds = data_slice.DataSlice.from_vals
bag = fns.bag


class UuSchemaTest(absltest.TestCase):

  def test_simple_schema(self):
    schema = fns.named_schema('name')
    self.assertCountEqual(dir(schema), [])

  def test_equal_by_fingerprint(self):
    x = fns.named_schema('name')
    y = fns.named_schema(ds('name'))
    testing.assert_equal(x, y.with_bag(x.get_bag()))

  def test_equal_not_by_fingerprint(self):
    x = fns.named_schema('name1')
    y = fns.named_schema('name2')
    self.assertNotEqual(x.fingerprint, y.fingerprint)

  def test_name_as_keyword_argument(self):
    x = fns.named_schema('name')
    y = fns.named_schema(name='name')
    testing.assert_equal(x, y.with_bag(x.get_bag()))

  def test_bag_arg(self):
    db = bag()
    schema = fns.named_schema('name', db=db)
    no_bag_schema = fns.named_schema('name')
    testing.assert_equal(schema, no_bag_schema.with_bag(db))

  def test_alias(self):
    self.assertIs(fns.named_schema, fns.schema.named_schema)


if __name__ == '__main__':
  absltest.main()
