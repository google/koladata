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

"""Tests for koda_internal.input."""

import re
from absl.testing import absltest
from arolla import arolla
# Register the view on `koda_internal.input`.
from koladata.expr import input_container as _  # pylint: disable=unused-import
from koladata.expr import view
from koladata.operators import kde_operators as _  # pylint: disable=unused-import

_KOLA_INPUT_OP = arolla.abc.lookup_operator('koda_internal.input')


class KolaInputTest(absltest.TestCase):

  def test_output_qtype(self):
    self.assertIsNone(_KOLA_INPUT_OP('I', 'x').qtype)

  def test_eval_error(self):
    expr = _KOLA_INPUT_OP('I', 'x')
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'I.x cannot be evaluated - please provide data to `I` inputs and'
            ' substitute all `V` variables'
        ),
    ):
      arolla.eval(expr)

  def test_decayed_eval_error(self):
    expr = arolla.abc.decay_registered_operator(_KOLA_INPUT_OP)('I', 'x')
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "koda_internal.input('I', 'x') cannot be evaluated - please provide"
            ' data to `I` inputs and substitute all `V` variables'
        ),
    ):
      arolla.eval(expr)

  def test_non_literal_container_name_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected container_name to be a literal')
    ):
      _KOLA_INPUT_OP(arolla.M.annotation.qtype(arolla.L.x, arolla.TEXT), 'x')

  def test_non_literal_input_key_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected input_key to be a literal')
    ):
      _KOLA_INPUT_OP('V', arolla.M.annotation.qtype(arolla.L.x, arolla.TEXT))

  def test_non_text_container_name_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected container_name to be a TEXT')
    ):
      _KOLA_INPUT_OP(arolla.int32(1), 'x')

  def test_non_text_input_key_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected input_key to be a TEXT')
    ):
      _KOLA_INPUT_OP('V', arolla.int32(1))

  def test_container_name_is_not_identifier_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected container_name to be an identifier')
    ):
      _KOLA_INPUT_OP('V.x', 'x')

  def test_input_key_is_identifier_repr(self):
    expr = _KOLA_INPUT_OP('I', 'x')
    self.assertEqual(repr(expr), 'I.x')

  def test_input_key_is_not_identifier_repr(self):
    expr = _KOLA_INPUT_OP('I', 'a.b')
    self.assertEqual(repr(expr), "I['a.b']")

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(_KOLA_INPUT_OP('I', 'x')))


if __name__ == '__main__':
  absltest.main()
