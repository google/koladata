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
from absl.testing import parameterized
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kd = eager_op_utils.operators_container(top_level_arolla_container=kde)


# Primary behavior tests for the underlying code are in
# json_stream_salvage_test.py; this operator is just a convenience wrapper
# around that one.
class JsonSalvageTest(parameterized.TestCase):

  def test_basic(self):
    result = kd.json.salvage(ds('{"x": "y"}'))
    testing.assert_equal(result, ds('{"x":"y"}'))

  def test_slice(self):
    result = kd.json.salvage(ds(['{"x": "y"}', '{"a": 1}']))
    testing.assert_equal(result, ds(['{"x":"y"}', '{"a":1}']))

  def test_salvage_non_json(self):
    self.assertEqual(kd.json.salvage(ds('None')).to_py(), 'null')
    self.assertEqual(kd.json.salvage(ds('True')).to_py(), 'true')
    self.assertEqual(kd.json.salvage(ds('False')).to_py(), 'false')
    self.assertEqual(kd.json.salvage(ds('0o123')).to_py(), '83')
    self.assertEqual(kd.json.salvage(ds('0b1011')).to_py(), '11')
    self.assertEqual(kd.json.salvage(ds("'''\n\n\n'''")).to_py(), '"\\n\\n\\n"')
    self.assertEqual(
        kd.json.salvage(ds('None True False')).to_py(), 'null\ntrue\nfalse'
    )

  def test_incomplete_json(self):
    salvaged = kd.json.salvage(ds("""
    {"results": [
      {"name": "person1", "attributes": [{"name": "age", "value": "30"}]},
      {"name": "person2", "attr
    """))
    self.assertEqual(
        salvaged.to_py(),
        '{"results":[{"name":"person1","attributes":[{"name":"age","value":"30"}]},{"name":"person2","attr\\n'
        '    ":null}]null}',
    )

    salvaged = kd.json.salvage(ds("""
    {"results": [
      {"name": "person1", "attributes": [{"name": "age", "value": "30"}]},
      {"name": "person2", "attributes": [
    """))
    self.assertEqual(
        salvaged.to_py(),
        '{"results":[{"name":"person1","attributes":[{"name":"age","value":"30"}]},{"name":"person2","attributes":[]null}]null}',
    )

    salvaged = kd.json.salvage(ds("""
    {"results": [
      {"name": "person1", "attributes": [{"name": "age", "value": "30"}]},
      {"name": "person2", "attributes": [{"na
    """))
    self.assertEqual(
        salvaged.to_py(),
        '{"results":[{"name":"person1","attributes":[{"name":"age","value":"30"}]},{"name":"person2","attributes":[{"na\\n'
        '    ":null}]null}]null}',
    )

  def test_missing(self):
    result = kd.json.salvage(ds([None], schema=schema_constants.STRING))
    testing.assert_equal(result, ds([None], schema=schema_constants.STRING))

  def test_bad_arguments(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'argument `x` must be a slice of STRING, got a slice of INT32'
        ),
    ):
      kd.json.salvage(ds(123))

    with self.assertRaisesRegex(
        ValueError,
        'argument `allow_nan` must be an item holding BOOLEAN, got an item of'
        ' INT32',
    ):
      kd.json.salvage(ds('{}'), allow_nan=123)

    with self.assertRaisesRegex(
        ValueError,
        'argument `ensure_ascii` must be an item holding BOOLEAN, got an item'
        ' of INT32',
    ):
      kd.json.salvage(ds('{}'), ensure_ascii=123)

    with self.assertRaisesRegex(
        ValueError,
        'argument `max_depth` must be a slice of integer values',
    ):
      kd.json.salvage(ds('{}'), max_depth='x')

  def test_non_default_options(self):
    self.assertEqual(kd.json.salvage(ds('NaN')).to_py(), '"NaN"')
    self.assertEqual(kd.json.salvage(ds('NaN'), allow_nan=True).to_py(), 'NaN')

    self.assertEqual(
        kd.json.salvage(ds('"✨"'), ensure_ascii=True).to_py(), '"\\u2728"'
    )
    self.assertEqual(
        kd.json.salvage(ds('"✨"'), ensure_ascii=False).to_py(), '"✨"'
    )

    nested = '[' * 200 + ']' * 200
    self.assertEqual(
        kd.json.salvage(ds(nested), max_depth=300).to_py(), nested
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.json.salvage(I.x)))


if __name__ == '__main__':
  absltest.main()
