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

import re

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.functions.tests import test_pb2
from koladata.testing import testing
from koladata.types import schema_constants


class SchemaFromProtoTest(absltest.TestCase):

  def test_schema_itemid_consistent_with_from_proto(self):
    schema = fns.schema_from_proto(test_pb2.MessageA)
    self.assertFalse(schema.get_bag().is_mutable())
    testing.assert_equal(
        schema.no_bag(),
        fns.from_proto(test_pb2.MessageA()).get_schema().no_bag(),
    )

  def test_fields_no_extensions(self):
    schema = fns.schema_from_proto(test_pb2.MessageA)
    self.assertFalse(schema.get_bag().is_mutable())
    self.assertCountEqual(
        schema.get_attr_names(intersection=True),
        ['some_text', 'some_float', 'message_b_list', 'message_set_extensions'],
    )
    testing.assert_equal(schema.some_text.no_bag(), schema_constants.STRING)
    testing.assert_equal(schema.some_float.no_bag(), schema_constants.FLOAT32)
    testing.assert_equal(
        schema.message_b_list.get_item_schema().no_bag(),
        fns.schema_from_proto(test_pb2.MessageB).no_bag(),
    )
    testing.assert_equal(
        schema.message_set_extensions.no_bag(),
        fns.schema_from_proto(test_pb2.MessageSet).no_bag(),
    )

  def test_fields_no_extensions_with_map_and_oneof(self):
    schema = fns.schema_from_proto(test_pb2.MessageC)
    self.assertCountEqual(
        schema.get_attr_names(intersection=True),
        [
            'message_field',
            'int32_field',
            'bytes_field',
            'repeated_message_field',
            'repeated_int32_field',
            'repeated_bytes_field',
            'map_int32_int32_field',
            'map_int32_message_field',
            'bool_field',
            'float_field',
            'double_field',
            'oneof_int32_field',
            'oneof_bytes_field',
            'oneof_message_field',
        ],
    )
    self.assertFalse(schema.get_bag().is_mutable())
    testing.assert_equal(
        schema.map_int32_int32_field.get_key_schema().no_bag(),
        schema_constants.INT32,
    )
    testing.assert_equal(
        schema.map_int32_int32_field.get_value_schema().no_bag(),
        schema_constants.INT32,
    )
    testing.assert_equal(
        schema.map_int32_message_field.get_key_schema().no_bag(),
        schema_constants.INT32,
    )
    testing.assert_equal(
        schema.map_int32_message_field.get_value_schema().no_bag(),
        schema.no_bag(),
    )
    testing.assert_equal(
        schema.oneof_int32_field.no_bag(), schema_constants.INT32
    )
    testing.assert_equal(
        schema.oneof_bytes_field.no_bag(), schema_constants.BYTES
    )
    testing.assert_equal(schema.oneof_message_field.no_bag(), schema.no_bag())

  def test_extensions(self):
    schema = fns.schema_from_proto(
        test_pb2.MessageA,
        extensions=[
            '(koladata.functions.testing.MessageAExtension.message_a_extension)',
            '(koladata.functions.testing.MessageAExtension.message_a_extension).(koladata.functions.testing.MessageAExtensionExtension.message_a_extension_extension)',
            'message_set_extensions.(koladata.functions.testing.MessageAExtension.message_set_extension)',
            'message_b_list.(koladata.functions.testing.MessageBExtension.message_b_extension)',
        ],
    )
    self.assertFalse(schema.get_bag().is_mutable())
    self.assertCountEqual(
        schema.get_attr_names(intersection=True),
        [
            'some_text',
            'some_float',
            'message_b_list',
            'message_set_extensions',
            '(koladata.functions.testing.MessageAExtension.message_a_extension)',
        ],
    )

    self.assertCountEqual(
        getattr(
            schema,
            '(koladata.functions.testing.MessageAExtension.message_a_extension)',
        ).get_attr_names(intersection=True),
        [
            'extra',
            '(koladata.functions.testing.MessageAExtensionExtension.message_a_extension_extension)',
        ],
    )

    self.assertCountEqual(
        schema.message_set_extensions.get_attr_names(intersection=True),
        [
            '(koladata.functions.testing.MessageAExtension.message_set_extension)'
        ],
    )

    self.assertCountEqual(
        schema.message_b_list.get_item_schema().get_attr_names(
            intersection=True
        ),
        [
            'text',
            '(koladata.functions.testing.MessageBExtension.message_b_extension)',
        ],
    )

  def test_extension_on_wrong_message_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'extension'
            ' "koladata.functions.testing.MessageBExtension.message_b_extension"'
            " exists, but isn't an extension on target message type"
            ' "koladata.functions.testing.MessageA", expected'
            ' "koladata.functions.testing.MessageB"'
        ),
    ):
      _ = fns.schema_from_proto(
          test_pb2.MessageA,
          extensions=[
              '(koladata.functions.testing.MessageBExtension.message_b_extension)'
          ],
      )


if __name__ == '__main__':
  absltest.main()
