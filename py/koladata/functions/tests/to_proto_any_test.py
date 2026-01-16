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
from google.protobuf import descriptor_pool
from koladata.functions import proto_conversions
from koladata.functions.tests import test_pb2
from koladata.testing import testing
from koladata.types import data_slice
from google.protobuf import any_pb2

ds = data_slice.DataSlice.from_vals


class ToProtoAnyTest(absltest.TestCase):

  def test_zero_items(self):
    x = ds([])
    res = proto_conversions.to_proto_any(x)
    self.assertEqual(res, [])

  def test_single_item(self):
    m = test_pb2.MessageA(some_text='thing 1')
    x = proto_conversions.from_proto(m)
    res = proto_conversions.to_proto_any(x)
    self.assertIsInstance(res, any_pb2.Any)
    unpacked_m = test_pb2.MessageA()
    res.Unpack(unpacked_m)
    self.assertEqual(unpacked_m, m)

  def test_single_none(self):
    x = ds(None)
    res = proto_conversions.to_proto_any(x)
    self.assertIsNone(res)

  def test_list_with_none(self):
    m1 = test_pb2.MessageA(some_text='thing 1')
    x = proto_conversions.from_proto([m1, None])
    res = proto_conversions.to_proto_any(x)
    self.assertIsInstance(res, list)
    self.assertLen(res, 2)
    self.assertIsInstance(res[0], any_pb2.Any)
    self.assertIsNone(res[1])
    unpacked_m1 = test_pb2.MessageA()
    res[0].Unpack(unpacked_m1)
    self.assertEqual(unpacked_m1, m1)

  def test_multiple_messages_same_type(self):
    m1 = test_pb2.MessageA(some_text='thing 1')
    m2 = test_pb2.MessageA(some_text='thing 2')
    x = proto_conversions.from_proto([m1, m2])
    res = proto_conversions.to_proto_any(x)
    self.assertIsInstance(res, list)
    self.assertLen(res, 2)
    unpacked_m1 = test_pb2.MessageA()
    res[0].Unpack(unpacked_m1)
    self.assertEqual(unpacked_m1, m1)
    unpacked_m2 = test_pb2.MessageA()
    res[1].Unpack(unpacked_m2)
    self.assertEqual(unpacked_m2, m2)

  def test_multiple_messages_different_types(self):
    m1 = test_pb2.MessageA(some_text='thing 1')
    m2 = test_pb2.MessageB(text='thing 2')
    # Need to use from_proto_any to get them into the same DataSlice
    any_m1 = any_pb2.Any()
    any_m1.Pack(m1)
    any_m2 = any_pb2.Any()
    any_m2.Pack(m2)
    x = proto_conversions.from_proto_any([any_m1, any_m2])

    res = proto_conversions.to_proto_any(x)
    self.assertIsInstance(res, list)
    self.assertLen(res, 2)
    unpacked_m1 = test_pb2.MessageA()
    res[0].Unpack(unpacked_m1)
    self.assertEqual(unpacked_m1, m1)
    unpacked_m2 = test_pb2.MessageB()
    res[1].Unpack(unpacked_m2)
    self.assertEqual(unpacked_m2, m2)

  def test_nested_list_input(self):
    m1 = test_pb2.MessageA(some_text='1')
    any_m1 = any_pb2.Any()
    any_m1.Pack(m1)
    m2 = test_pb2.MessageA(some_text='2')
    any_m2 = any_pb2.Any()
    any_m2.Pack(m2)
    m3 = test_pb2.MessageB(text='3')
    any_m3 = any_pb2.Any()
    any_m3.Pack(m3)

    x = proto_conversions.from_proto_any([[any_m1, None, any_m2], [], [any_m3]])
    res = proto_conversions.to_proto_any(x)
    testing.assert_equivalent(proto_conversions.from_proto_any(res), x)

  def test_not_from_proto(self):
    x = ds([1, 2, 3]).implode()
    with self.assertRaisesRegex(
        ValueError,
        'to_proto_any expects entities converted from proto messages',
    ):
      proto_conversions.to_proto_any(x)

  def test_empty_descriptor_pool(self):
    m = test_pb2.MessageA(some_text='thing 1')
    x = proto_conversions.from_proto(m)
    pool = descriptor_pool.DescriptorPool()
    with self.assertRaisesRegex(KeyError, 'MessageA'):
      proto_conversions.to_proto_any(x, descriptor_pool=pool)


if __name__ == '__main__':
  absltest.main()
