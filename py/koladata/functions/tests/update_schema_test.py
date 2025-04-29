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
from koladata.operators import kde_operators
from koladata.types import data_slice
from koladata.types import schema_constants

kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class UpdateSchemaTest(absltest.TestCase):

  def test_update_schema(self):
    o = fns.new().fork_bag()
    fns.update_schema(o, x=schema_constants.INT32, y=schema_constants.FLOAT32)
    self.assertEqual(o.get_schema().x, schema_constants.INT32)
    self.assertEqual(o.get_schema().y, schema_constants.FLOAT32)

    fns.update_schema(
        o, x=kde.schema.new_schema(z=schema_constants.INT32).eval()
    )
    self.assertEqual(o.get_schema().x.z, schema_constants.INT32)

    o = ds([fns.obj(fns.new()), fns.obj()]).fork_bag()
    fns.update_schema(
        o,
        x=schema_constants.INT32,
        y=ds([schema_constants.FLOAT32, schema_constants.STRING]),
    )
    self.assertEqual(o.S[0].get_obj_schema().x, schema_constants.INT32)
    self.assertEqual(o.S[1].get_obj_schema().x, schema_constants.INT32)
    self.assertEqual(o.S[0].get_obj_schema().y, schema_constants.FLOAT32)
    self.assertEqual(o.S[1].get_obj_schema().y, schema_constants.STRING)


if __name__ == '__main__':
  absltest.main()
