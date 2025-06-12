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
from arolla.s11n.testing import codec_test_case
# Register kde ops for e.g. JaggedShape.from-edges().
from koladata.operators import kde_operators as _
from koladata.types import jagged_shape

from arolla.jagged_shape.dense_array.serialization_codecs import jagged_shape_codec_pb2 as _
from arolla.serialization_codecs.dense_array import dense_array_codec_pb2 as _
from koladata.s11n import codec_pb2 as _


class JaggedShapeS11nTest(codec_test_case.S11nCodecTestCase):

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
            jagged_shape_qtype: true
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """
    self.assertDumpsEqual(jagged_shape.JAGGED_SHAPE, text)
    self.assertLoadsEqual(text, jagged_shape.JAGGED_SHAPE)

  def test_value(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "koladata.s11n" }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension"
        }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 2
          [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
            dense_array_int64_value {
              size: 2
              values: 0
              values: 2
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 3
          codec_index: 2
          [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
            dense_array_edge_value {
              edge_type: SPLIT_POINTS
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 4
          codec_index: 1
          [arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension] {
            jagged_dense_array_shape_value: true
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 5
          codec_index: 0
          [koladata.s11n.KodaV1Proto.extension] {
            jagged_shape_value: true
          }
        }
      }
      decoding_steps {
        output_value_index: 6
      }
    """
    koda_shape = jagged_shape.JaggedShape.from_edges(
        arolla.types.DenseArrayEdge.from_sizes([2]),
    )
    self.assertDumpsEqual(koda_shape, text)
    self.assertLoadsEqual(text, koda_shape)

  def test_decode_wrong_inputs_number_error(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "koladata.s11n" }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension"
        }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 2
          [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
            dense_array_int64_value {
              size: 2
              values: 0
              values: 2
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 3
          codec_index: 2
          [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
            dense_array_edge_value {
              edge_type: SPLIT_POINTS
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 4
          codec_index: 1
          [arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension] {
            jagged_dense_array_shape_value: true
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 5
          input_value_indices: 4
          codec_index: 0
          [koladata.s11n.KodaV1Proto.extension] {
            jagged_shape_value: true
          }
        }
      }
      decoding_steps {
        output_value_index: 6
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        'wrong number of input_values in DecodeJaggedShapeValue: expected 1,'
        ' got 2',
    ):
      _ = self.parse_container_text_proto(text)

  def test_decode_wrong_input_value_error(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "koladata.s11n" }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension"
        }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 2
          [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
            dense_array_int64_value {
              size: 2
              values: 0
              values: 2
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 3
          codec_index: 2
          [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
            dense_array_edge_value {
              edge_type: SPLIT_POINTS
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 4
          codec_index: 1
          [arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension] {
            jagged_dense_array_shape_value: true
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 4
          codec_index: 0
          [koladata.s11n.KodaV1Proto.extension] {
            jagged_shape_value: true
          }
        }
      }
      decoding_steps {
        output_value_index: 6
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        'expected a JaggedDenseArrayShape, got dense_array_edge',
    ):
      _ = self.parse_container_text_proto(text)


if __name__ == '__main__':
  absltest.main()
