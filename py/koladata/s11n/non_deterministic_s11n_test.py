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
from arolla.s11n.testing import codec_test_case
from koladata.types import qtypes

from koladata.s11n import codec_pb2 as _


class NonDeterministicS11NTest(codec_test_case.S11nCodecTestCase):

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
            non_deterministic_token_qtype: true
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """
    self.assertDumpsEqual(qtypes.NON_DETERMINISTIC_TOKEN, text)
    self.assertLoadsEqual(text, qtypes.NON_DETERMINISTIC_TOKEN)


if __name__ == '__main__':
  absltest.main()
