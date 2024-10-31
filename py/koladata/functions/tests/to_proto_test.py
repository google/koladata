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
from koladata.functions.tests import test_pb2
from koladata.types import data_bag
from koladata.types import data_slice


ds = data_slice.DataSlice.from_vals


class ToProtoTest(absltest.TestCase):

  def test_empty_slice(self):
    messages = fns.to_proto(ds([]), test_pb2.MessageC)
    self.assertEmpty(messages)

  def test_invalid_input_no_bag(self):
    with self.assertRaisesRegex(
        ValueError,
        'cannot get available attributes without a DataBag',
    ):
      _ = fns.to_proto(fns.obj().no_bag(), test_pb2.MessageC)

  def test_invalid_input_invalid_ndim(self):
    bag = data_bag.DataBag.empty()
    with self.assertRaisesRegex(
        ValueError,
        re.escape('to_proto expects a DataSlice with ndim 0 or 1, got ndim=2'),
    ):
      _ = fns.to_proto(ds([[]]).with_bag(bag), test_pb2.MessageC)

  def test_invalid_input_primitive(self):
    bag = data_bag.DataBag.empty()
    with self.assertRaisesRegex(
        ValueError,
        'proto message should have only entities/objects, found INT32',
    ):
      _ = fns.to_proto(ds(1).with_bag(bag), test_pb2.MessageC)

  def test_single_empty_message(self):
    message = fns.to_proto(fns.obj(), test_pb2.MessageC)
    expected_message = test_pb2.MessageC()
    self.assertTrue(message, expected_message)

  def test_single_none_message(self):
    bag = data_bag.DataBag.empty()
    message = fns.to_proto(ds(None).with_bag(bag), test_pb2.MessageC)
    expected_message = test_pb2.MessageC()
    self.assertTrue(message, expected_message)

  def test_single_message(self):
    message = fns.to_proto(fns.new(int32_field=1), test_pb2.MessageC)
    expected_message = test_pb2.MessageC(int32_field=1)
    self.assertTrue(message, expected_message)

  def test_single_message_object(self):
    message = fns.to_proto(fns.obj(int32_field=1), test_pb2.MessageC)
    expected_message = test_pb2.MessageC(int32_field=1)
    self.assertTrue(message, expected_message)

  def test_single_message_any(self):
    message = fns.to_proto(fns.new(int32_field=1).as_any(), test_pb2.MessageC)
    expected_message = test_pb2.MessageC(int32_field=1)
    self.assertTrue(message, expected_message)

  def test_multiple_messages(self):
    messages = fns.to_proto(
        ds([
            fns.obj(int32_field=1),
            fns.obj(int32_field=2),
            fns.obj(int32_field=3),
        ]),
        test_pb2.MessageC,
    )
    expected_messages = [
        test_pb2.MessageC(int32_field=1),
        test_pb2.MessageC(int32_field=2),
        test_pb2.MessageC(int32_field=3),
    ]
    self.assertTrue(messages, expected_messages)

  def test_extension_field(self):
    x = fns.obj()
    x.set_attr(
        '(koladata.functions.testing.MessageAExtension.message_a_extension)',
        fns.obj(extra=123),
    )

    message = fns.to_proto(x, test_pb2.MessageA)
    expected_message = test_pb2.MessageA()
    expected_message.Extensions[
        test_pb2.MessageAExtension.message_a_extension
    ].extra = 123
    self.assertTrue(message, expected_message)

  def test_invalid_input_list_none(self):
    with self.assertRaisesRegex(
        ValueError,
        'proto repeated field repeated_int32_field cannot represent missing'
        ' values',
    ):
      _ = fns.to_proto(
          fns.obj(repeated_int32_field=[1, None, 2]), test_pb2.MessageC
      )

  def test_roundtrip_with_extensions(self):
    m = test_pb2.MessageA(
        some_text='thing 1',
        some_float=1.0,
        message_b_list=[
            test_pb2.MessageB(text='a'),
            test_pb2.MessageB(text='b'),
            test_pb2.MessageB(text='c'),
        ]
    )

    m.message_set_extensions.Extensions[
        test_pb2.MessageAExtension.message_set_extension
    ].extra = 1
    m.Extensions[test_pb2.MessageAExtension.message_a_extension].extra = 2
    m.Extensions[test_pb2.MessageAExtension.message_a_extension].Extensions[
        test_pb2.MessageAExtensionExtension.message_a_extension_extension
    ].extra = 3
    m.message_b_list[0].Extensions[
        test_pb2.MessageBExtension.message_b_extension
    ].extra = 4
    m.message_b_list[2].Extensions[
        test_pb2.MessageBExtension.message_b_extension
    ].extra = 5

    x = fns.from_proto(
        m,
        extensions=[
            '(koladata.functions.testing.MessageAExtension.message_a_extension)',
            '(koladata.functions.testing.MessageAExtension.message_a_extension).(koladata.functions.testing.MessageAExtensionExtension.message_a_extension_extension)',
            'message_set_extensions.(koladata.functions.testing.MessageAExtension.message_set_extension)',
            'message_b_list.(koladata.functions.testing.MessageBExtension.message_b_extension)',
        ],
    )

    self.assertEqual(fns.to_proto(x, test_pb2.MessageA), m)


if __name__ == '__main__':
  absltest.main()
