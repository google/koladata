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

"""Tests for data_slice_s11n."""

import re

from absl.testing import absltest
from arolla import arolla
from arolla.s11n.testing import codec_test_case
from koladata.testing import testing as kd_testing
from koladata.types import data_slice

from koladata.s11n import codec_pb2 as _
from arolla.jagged_shape.dense_array.serialization_codecs import jagged_shape_codec_pb2 as _
from arolla.serialization_codecs.dense_array import dense_array_codec_pb2 as _


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

  def _get_text_header(self):
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
              data_item_vector {
                values { i32: 1 }
                values { i32: 2 }
              }
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
    """

  def test_correct_case(self):
    text = self._get_text_header() + """
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
    value = data_slice.DataSlice.from_vals([1, 2])
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def test_decoder_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('wrong number of input_values in DecodeDataSliceValue: 2'),
    ):
      self.parse_container_text_proto(self._get_text_header() + """
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
      self.parse_container_text_proto(self._get_text_header() + """
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
      self.parse_container_text_proto(self._get_text_header() + """
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
