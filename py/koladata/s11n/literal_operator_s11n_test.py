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

"""Tests for literal_operator_s11n."""

import re

from absl.testing import absltest
from arolla import arolla
from arolla.s11n.testing import codec_test_case
from koladata.types import literal_operator

from arolla.serialization_codecs.generic import scalar_codec_pb2 as _
from koladata.s11n import codec_pb2 as _


class LiteralOperatorS11NTest(codec_test_case.S11nCodecTestCase):

  def test_value(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "koladata.s11n" }
      }
      decoding_steps {
        codec { name: "arolla.serialization_codecs.ScalarV1Proto.extension" }
      }
      decoding_steps {
        value {
          codec_index: 1
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            unspecified_value: true
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          codec_index: 0
          [koladata.s11n.KodaV1Proto.extension] {
            literal_operator: true
          }
        }
      }
      decoding_steps {
        output_value_index: 3
      }
    """
    op = literal_operator.literal(arolla.unspecified()).op
    self.assertDumpsEqual(op, text)
    self.assertLoadsEqual(text, op)

  def test_not_serializable_value_error(self):
    dummy_value = arolla.types.PyObject(object())
    op = literal_operator.literal(dummy_value).op
    with self.assertRaisesRegex(
        ValueError,
        re.escape('EncodeValue(op->value()); value=LITERAL_OPERATOR'),
    ):
      arolla.s11n.dumps(op)

  def test_missing_literal_operator_marking_error(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "koladata.s11n" }
      }
      decoding_steps {
        value {
          [koladata.s11n.KodaV1Proto.extension] {
          }
        }
      }
    """
    with self.assertRaisesRegex(ValueError, 'missing value'):
      self.parse_container_text_proto(text)

  def test_missing_input_error(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "koladata.s11n" }
      }
      decoding_steps {
        value {
          [koladata.s11n.KodaV1Proto.extension] {
            literal_operator: true
          }
        }
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        'expected 1 input_value_index, got 0; value=LITERAL_OPERATOR',
    ):
      self.parse_container_text_proto(text)


if __name__ == '__main__':
  absltest.main()
