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

import re

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals
bag = fns.bag


class NamedSchemaTest(absltest.TestCase):

  def test_mutability(self):
    self.assertFalse(fns.named_schema('my_schema').is_mutable())

  def test_simple_schema(self):
    schema = fns.named_schema('name')
    self.assertCountEqual(fns.dir(schema), [])

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

  def test_attrs(self):
    schema = fns.named_schema('name', a=schema_constants.FLOAT32)
    schema2 = fns.named_schema('name')
    testing.assert_equal(
        schema.a, schema_constants.FLOAT32.with_bag(schema.get_bag())
    )
    testing.assert_equal(schema, schema2.with_bag(schema.get_bag()))

  def test_nested_attrs(self):
    schema = fns.named_schema('name', a=schema_constants.FLOAT32)
    outer_schema = fns.named_schema('name2', x=schema)
    testing.assert_equal(
        outer_schema.x.a,
        schema_constants.FLOAT32.with_bag(outer_schema.get_bag()),
    )

  def test_wrong_attr_type(self):
    with self.assertRaisesRegex(
        ValueError, 'only schemas can be assigned as attributes of schemas'
    ):
      fns.named_schema('name', a=1.0)
    with self.assertRaisesRegex(
        ValueError, 'only schemas can be assigned as attributes of schemas'
    ):
      fns.named_schema('name', a=ds(1.0))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'must have the same or less number of dimensions as foo, got ' +
            'foo.get_ndim(): 0 < values.get_ndim(): 1'
        )
    ):
      fns.named_schema('name', a=ds([schema_constants.INT32]))

  def test_alias(self):
    self.assertIs(fns.named_schema, fns.schema.named_schema)


if __name__ == '__main__':
  absltest.main()
