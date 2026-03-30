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
from koladata.expr import expr_eval
from koladata.operators import kde_operators
from koladata.types import data_slice

kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class JsonFilterJsonTest(absltest.TestCase):

  def test_scalar_input(self):
    result = expr_eval.eval(
        kde.json.filter_json(ds('{"a": 1, "b": 2}'), ds('$.a'))
    )
    self.assertEqual(result.to_py(), ['1'])

  def test_multiple_matches(self):
    result = expr_eval.eval(
        kde.json.filter_json(ds('{"a": [1, 2], "b": 3}'), ds('$.a[*]'))
    )
    self.assertEqual(result.to_py(), ['1', '2'])

  def test_dataslice_input(self):
    result = expr_eval.eval(
        kde.json.filter_json(ds(['{"a": 1}', '{"a": 2}']), ds('$.a'))
    )
    self.assertEqual(result.to_py(), [['1'], ['2']])

  def test_multidimensional_dataslice_input(self):
    x = ds([['{"a": 1}'], ['{"a": 2}', '{"a": 3}']])
    result = expr_eval.eval(kde.json.filter_json(x, ds('$.a')))
    self.assertEqual(result.to_py(), [[['1']], [['2'], ['3']]])

  def test_missing_values(self):
    result = expr_eval.eval(
        kde.json.filter_json(ds(['{"a": 1}', None]), ds('$.a'))
    )
    self.assertEqual(result.to_py(), [['1'], []])

  def test_no_matches(self):
    result = expr_eval.eval(kde.json.filter_json(ds('{"a": 1}'), ds('$.b')))
    self.assertEqual(result.to_py(), [])

  def test_error_non_string_input(self):
    with self.assertRaisesRegex(ValueError, 'must be a slice of STRING'):
      expr_eval.eval(kde.json.filter_json(ds(1), ds('$.a')))

  def test_error_non_scalar_filter(self):
    with self.assertRaisesRegex(ValueError, 'must be an item holding STRING'):
      expr_eval.eval(kde.json.filter_json(ds('{"a": 1}'), ds(['$.a'])))


if __name__ == '__main__':
  absltest.main()
