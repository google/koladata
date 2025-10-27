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

from absl.testing import absltest
from arolla import arolla
from koladata.functions import functions as fns
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals


class EmbedSchemaTest(absltest.TestCase):

  def test_entity_to_object(self):
    db = fns.mutable_bag()
    x = db.new(a=ds([1, 2]))
    x_object = fns.embed_schema(x)
    testing.assert_equal(
        x_object.get_schema(), schema_constants.OBJECT.with_bag(db)
    )
    testing.assert_equal(x_object.a, x.a)
    schema_attr = x_object.get_attr('__schema__')
    testing.assert_equal(
        schema_attr == x.get_schema(), ds([arolla.present(), arolla.present()])
    )

  def test_primitive(self):
    testing.assert_equal(
        fns.embed_schema(ds([1, 2, 3])), ds([1, 2, 3], schema_constants.OBJECT)
    )

  def test_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'schema embedding is only supported for a DataSlice with primitive, '
        'entity, list or dict schemas, got ITEMID',
    ):
      fns.embed_schema(ds([None], schema_constants.ITEMID))


if __name__ == '__main__':
  absltest.main()
