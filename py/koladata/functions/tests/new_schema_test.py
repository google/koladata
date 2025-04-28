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
from arolla import arolla
from koladata.functions import functions as fns
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals
bag = fns.bag


class NewSchemaTest(absltest.TestCase):

  def test_mutability(self):
    self.assertFalse(
        fns.schema.new_schema(a=schema_constants.INT32).is_mutable()
    )

  def test_simple_schema(self):
    schema = fns.schema.new_schema(
        a=schema_constants.INT32, b=schema_constants.STRING
    )

    testing.assert_equal(schema.a.no_bag(), schema_constants.INT32)
    testing.assert_equal(schema.b.no_bag(), schema_constants.STRING)

  def test_nested_schema_with_adoption(self):
    schema_b = fns.schema.new_schema(a=schema_constants.INT32)
    schema = fns.schema.new_schema(a=schema_constants.INT32, b=schema_b)
    self.assertNotEqual(
        schema_b.get_bag().fingerprint, schema.get_bag().fingerprint
    )
    testing.assert_equal(schema.a.no_bag(), schema_constants.INT32)
    testing.assert_equal(schema.b.a.no_bag(), schema_constants.INT32)

  def test_list_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('passing a Python list to a Koda operation is ambiguous'),
    ) as cm:
      _ = fns.schema.new_schema(a=schema_constants.INT32, b=[1, 2, 3])
    self.assertEqual(
        cm.exception.__notes__,
        ['Error occurred while processing argument: `b`'],
    )

  def test_dict_error(self):
    with self.assertRaisesRegex(
        ValueError, 'object with unsupported type: dict'
    ) as cm:
      _ = fns.schema.new_schema(a=schema_constants.INT32, b={'a': 1})
    self.assertEqual(
        cm.exception.__notes__,
        ['Error occurred while processing argument: `b`'],
    )

  def test_non_dataslice_qvalue_error(self):
    with self.assertRaisesRegex(
        ValueError,
        r'expected all arguments to be DATA_SLICE, got kwargs:.*TEXT',
    ):
      _ = fns.schema.new_schema(
          a=schema_constants.INT32,
          b=arolla.text('hello'),
      )

  def test_non_schema_dataslice_error(self):
    with self.assertRaisesRegex(
        ValueError,
        "schema's schema must be SCHEMA, got: OBJECT",
    ):
      _ = fns.schema.new_schema(
          a=schema_constants.INT32,
          b=fns.obj(a=schema_constants.INT32),
      )


if __name__ == '__main__':
  absltest.main()
