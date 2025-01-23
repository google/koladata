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

import random
import re

from absl.testing import absltest
from arolla import arolla
from arolla.s11n.testing import codec_test_case
from koladata import kd
from koladata.testing import testing as kd_testing
from koladata.types import data_slice
from koladata.types import schema_constants

from koladata.s11n import codec_pb2 as _
from arolla.jagged_shape.dense_array.serialization_codecs import jagged_shape_codec_pb2 as _
from arolla.serialization_codecs.dense_array import dense_array_codec_pb2 as _


def _get_text_header(data_slice_impl_proto=None):
  if data_slice_impl_proto is None:
    data_slice_impl_proto = """
      data_slice_compact {
          types_buffer: "\x02\x02"
          i32: 1
          i32: 2
      }"""
  return """
    version: 2
    decoding_steps {  # [0]
      codec { name: "koladata.s11n" }
    }
    decoding_steps {  # [1]
      value {
        codec_index: 0
        [koladata.s11n.KodaV1Proto.extension] {
          data_slice_impl_value {
            %s
          }
        }
      }
    }
    decoding_steps {  # [2]
      codec {
        name: "arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension"
      }
    }
    decoding_steps {  # [3]
      codec {
        name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
      }
    }
    decoding_steps {  # [4]
      value {
        codec_index: 3
        [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
          dense_array_int64_value {
            size: 2
            values: 0
            values: 2
          }
        }
      }
    }
    decoding_steps {  # [5]
      value {
        input_value_indices: 4
        codec_index: 3
        [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
          dense_array_edge_value {
            edge_type: SPLIT_POINTS
          }
        }
      }
    }
    decoding_steps {  # [6]
      value {
        input_value_indices: 5
        codec_index: 2
        [arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension] {
          jagged_dense_array_shape_value: true
        }
      }
    }
    decoding_steps {  # [7]
      value {
        codec_index: 0
        [koladata.s11n.KodaV1Proto.extension] {
          internal_data_item_value {
            dtype: 1
          }
        }
      }
    }
  """ % data_slice_impl_proto


def _get_text_footer_data_slice():
  return """
      decoding_steps {
        value {
          input_value_indices: 1
          input_value_indices: 6
          input_value_indices: 7
          codec_index: 0
          [koladata.s11n.KodaV1Proto.extension] {
            data_slice_value: true
          }
        }
      }
      decoding_steps {
        output_value_index: 8
      }
    """


def _get_data_slice_test_case(impl_proto=None):
  return _get_text_header(impl_proto) + _get_text_footer_data_slice()


class DataSliceS11NTest(codec_test_case.S11nCodecTestCase):

  def test_qtype(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "koladata.s11n" }
      }
      decoding_steps {
        value {
          codec_index: 0
          [koladata.s11n.KodaV1Proto.extension] {
            data_slice_qtype: true
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """
    value = data_slice.DataSlice.from_vals(None)
    self.assertDumpsEqual(value.qtype, text)
    self.assertLoadsEqual(text, value.qtype)

  def test_slice(self):
    value = data_slice.DataSlice.from_vals([1, 2, 3])
    data = arolla.s11n.dumps(value)
    res = arolla.s11n.loads(data)
    kd_testing.assert_equal(res, value)

  def test_big_slice(self):
    value = data_slice.DataSlice.from_vals(list(range(10**7)))
    data = arolla.s11n.dumps(value)
    res = arolla.s11n.loads(data)
    kd_testing.assert_equal(res, value)

  def test_slice_mixed(self):
    items = [
        arolla.int32(1),
        arolla.int32(2),
        None,
        arolla.int64(2),
        arolla.int64(7),
        arolla.float32(3),
        arolla.float32(13),
        arolla.float64(4),
        arolla.float64(46),
        arolla.boolean(True),
        arolla.boolean(False),
        arolla.unit(),
        arolla.text('hello'),
        arolla.text('hi'),
        arolla.bytes(b'ciao'),
        arolla.bytes(b'good bye'),
        schema_constants.BYTES,
        schema_constants.STRING,
        arolla.quote(arolla.L.x),
        arolla.quote(arolla.L.y),
    ]
    items = items * 37
    for _ in range(1000):  # run many times to stress test correctness
      random.shuffle(items)
      value = data_slice.DataSlice.from_vals(
          items, schema=schema_constants.OBJECT  # in order to avoid casting
      )
      data = arolla.s11n.dumps(value)
      res = arolla.s11n.loads(data)
      kd_testing.assert_equal(res, value)

  def test_obj_one_allocation(self):
    value = kd.obj(x=kd.slice(list(range(1000))))
    data = arolla.s11n.dumps(value)
    res = arolla.s11n.loads(data)
    kd.testing.assert_equivalent(res, value)

  def test_obj_different_allocations(self):
    valuex = kd.obj(x=kd.slice(list(range(1000))))
    valuey = kd.obj(y=kd.slice(list(range(1000))))
    value = (valuex & (valuex.x % 2 == 0)) | (valuey & (valuey.y % 2 == 0))
    data = arolla.s11n.dumps(value)
    res = arolla.s11n.loads(data)
    kd.testing.assert_equivalent(res, value)

  def test_removed_value_single_object(self):
    x = kd.obj().with_attrs(a=None)
    x_deserialized = kd.loads(kd.dumps(x))
    kd.testing.assert_equivalent(x.a.with_bag(None), kd.slice(None))
    kd.testing.assert_equivalent(
        x_deserialized.a.with_bag(None), kd.slice(None)
    )
    # Verifying that value is actually removed.
    z = x_deserialized.enriched(kd.attrs(x, a=1))
    kd.testing.assert_equivalent(
        z.a.with_bag(None), kd.slice(None)
    )

  def test_correct_case(self):
    text = _get_data_slice_test_case()
    value = data_slice.DataSlice.from_vals([1, 2])
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def test_correct_case_old_format(self):
    text = _get_data_slice_test_case("""
      data_item_vector {
          values { i32: 1 }
          values { i32: 2 }
      }""")
    value = data_slice.DataSlice.from_vals([1, 2])
    # Dump is not equal, because newer format is being produced.
    self.assertLoadsEqual(text, value)

  def test_data_slice_compact_decoder_errors_invalid_type_index(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('invalid type index'),
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x02\x00"
          i32: 1
        }
      """))

    with self.assertRaisesRegex(
        ValueError,
        re.escape('invalid type index'),
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x00\x02"
          i32: 1
        }
      """))

    with self.assertRaisesRegex(
        ValueError,
        re.escape('invalid type index'),
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\xa2\x02"
          i32: 1
        }
      """))

    with self.assertRaisesRegex(
        ValueError,
        re.escape('invalid type index'),
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x02\xa2"
          i32: 1
        }
      """))

  def test_data_slice_compact_decoder_errors_wrong_number_of_values(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('DataSliceCompactProto has not enough values for type INT32'),
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x02\x02"
          i32: 1
        }
      """))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'DataSliceCompactProto has not enough values for type EXPR_QUOTE'
        ),
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x0b\x0b"
        }
      """))

    with self.assertRaisesRegex(
        ValueError,
        re.escape('DataSliceCompactProto has unused values for type INT32'),
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x02\x02"
          i32: 1
          i32: 2
          i32: 3
        }
      """))

  def test_data_slice_compact_decoder_errors_extra_input_values_for_unused_type(
      self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('DataSliceCompactProto has unused values for type INT32'),
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x03\x03"
          i32: 1
          i32: 2
          i64: 1
          i64: 2
        }
      """))
    with self.assertRaisesRegex(
        ValueError,
        re.escape('DataSliceCompactProto has unused values for type FLOAT32'),
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x02\x02"
          i32: 1
          i32: 2
          f32: 2
        }
      """))

  def test_data_slice_compact_decoder_errors_wrong_object_ids(self):
    with self.assertRaisesRegex(
        ValueError,
        'DataSliceCompactProto has different number of hi and lo values',
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x01\x01"
          object_id {
            hi: 1
            hi: 2
            lo: 2
          }
        }
      """))
    with self.assertRaisesRegex(
        ValueError,
        'DataSliceCompactProto has different number of hi and lo values',
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x01\x01"
          object_id {
            hi: 1
            lo: 2
            lo: 1
          }
        }
      """))
    with self.assertRaisesRegex(
        ValueError,
        'DataSliceCompactProto has different number of hi and lo values',
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x02"
          i32: 1
          object_id {
            hi: 1
            hi: 2
            lo: 1
          }
        }
      """))
    with self.assertRaisesRegex(
        ValueError,
        'DataSliceCompactProto has not enough values for type OBJECT_ID',
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x01\x01"
          object_id {
            hi: 1
            lo: 1
          }
        }
      """))
    with self.assertRaisesRegex(
        ValueError,
        'DataSliceCompactProto has unused values for type OBJECT_ID',
    ):
      self.parse_container_text_proto(_get_data_slice_test_case("""
        data_slice_compact {
          types_buffer: "\x02"
          i32: 1
          object_id {
            hi: 1
            lo: 1
          }
        }
      """))

  def test_decoder_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('wrong number of input_values in DecodeDataSliceValue: 2'),
    ):
      header = _get_text_header()
      self.parse_container_text_proto(header + """
          decoding_steps {
            value {
              input_value_indices: 1
              input_value_indices: 6
              [koladata.s11n.KodaV1Proto.extension] {
                data_slice_value: true
              }
            }
          }
          """)

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'type mismatch: expected C++ type `arolla::DenseArrayEdge`'
            ' (DENSE_ARRAY_EDGE), got'
            ' `arolla::JaggedShape<arolla::DenseArrayEdge>`'
        ),
    ):
      self.parse_container_text_proto(header + """
          decoding_steps {
            value {
              input_value_indices: 1
              input_value_indices: 5
              input_value_indices: 6
              [koladata.s11n.KodaV1Proto.extension] {
                data_slice_value: true
              }
            }
          }
          """)

    with self.assertRaisesRegex(
        ValueError,
        re.escape('got more input_values than expected'),
    ):
      self.parse_container_text_proto(header + """
          decoding_steps {
            value {
              input_value_indices: 6
              [koladata.s11n.KodaV1Proto.extension] {
                data_slice_impl_value {
                  data_item_vector {
                    values { i32: 1 }
                    values { i32: 2 }
                  }
                }
              }
            }
          }
          """)


if __name__ == '__main__':
  absltest.main()
