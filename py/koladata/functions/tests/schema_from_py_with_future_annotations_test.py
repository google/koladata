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

from __future__ import annotations  # MUST BE IMPORTED.

import dataclasses

from absl.testing import absltest
from koladata.functions import schema
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.types import schema_constants


kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde


class SchemaFromPyWithFutureAnnotationsTest(absltest.TestCase):

  def test_schema_from_py_primitives(self):
    # Sanity check that primitives also work - no special handling is required
    # to make them work with deferred types however.
    self.assertEqual(schema.schema_from_py(int), schema_constants.INT64)
    self.assertEqual(schema.schema_from_py(float), schema_constants.FLOAT32)
    self.assertEqual(schema.schema_from_py(bool), schema_constants.BOOLEAN)
    self.assertEqual(schema.schema_from_py(str), schema_constants.STRING)
    self.assertEqual(schema.schema_from_py(bytes), schema_constants.BYTES)
    self.assertEqual(
        schema.schema_from_py(bytes | None), schema_constants.BYTES
    )

  def test_schema_from_inherited_py_dataclasses(self):
    # Tests that annotations from inherited dataclasses are included.

    @dataclasses.dataclass
    class MyParentClass:
      x: int

    @dataclasses.dataclass
    class MyChildClass(MyParentClass):
      y: str

    my_class_schema = schema.schema_from_py(MyChildClass)
    self.assertEqual(my_class_schema.x, schema_constants.INT64)
    self.assertEqual(my_class_schema.y, schema_constants.STRING)
    self.assertEqual(my_class_schema, schema.schema_from_py(MyChildClass))
    self.assertFalse(my_class_schema.is_mutable())


if __name__ == '__main__':
  absltest.main()
