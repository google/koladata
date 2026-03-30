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
from koladata.expr import input_container
from koladata.functor.parallel import clib as stream_clib
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes
from koladata.types import data_slice


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kde_internal = kde_operators.internal


def tokenize(s: str):
  # split into tokens of 1-3 characters
  tokens = re.findall(r'(.{1})(.{1,3})?', s, flags=re.DOTALL)
  return [item for sub in tokens for item in sub if item]  # pylint:disable=g-complex-comprehension


class KodaInternalParallelStreamStringFromJsonTest(absltest.TestCase):

  def test_basic(self):
    data = r"""
{
  "query": "some \"test\" data",
  some_invalid:json_still_can_be_parsed,
  escaped: "line1\nline2\ttab\bbackspace\fformfeed\rreturn\"quote\\backslash\u0020utf\ud83d\udE80"
}
{ "query": "the second occurrence is ignored" }
"""

    test_cases = {
        '$.query': 'some "test" data',
        '$.escaped': (
            'line1\nline2\ttab\bbackspace\fformfeed\rreturn"quote\\backslash'
            ' utf🚀'
        ),
    }

    for f, expected in test_cases.items():
      expr = kde_internal.parallel.stream_string_from_json(
          kde_internal.parallel.get_default_executor(),
          kde_internal.parallel.stream_from_1d_slice(I.arg),
          ds(f),
      )
      res = expr.eval(arg=ds(tokenize(data)))
      self.assertIsInstance(res, stream_clib.Stream)
      self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
      res_list = res.read_all(timeout=None)
      res_str = ''.join([str(x) for x in res_list])
      self.assertEqual(res_str, expected)

  def test_errors(self):
    # Field not found
    expr = kde_internal.parallel.stream_string_from_json(
        kde_internal.parallel.get_default_executor(),
        kde_internal.parallel.stream_from_1d_slice(I.arg),
        ds('$.not_found'),
    )
    with self.assertRaisesRegex(
        ValueError, r"field '\$.not_found' is not found in JSON stream"):
      _ = expr.eval(arg=ds(['{}'])).read_all(timeout=None)

    # Not a string
    expr = kde_internal.parallel.stream_string_from_json(
        kde_internal.parallel.get_default_executor(),
        kde_internal.parallel.stream_from_1d_slice(I.arg),
        ds('$.a'),
    )
    with self.assertRaisesRegex(
        ValueError, r"field '\$.a' is not found in JSON stream"):
      _ = expr.eval(arg=ds(['{"a": 123}'])).read_all(timeout=None)

    # Input not a string stream
    expr = kde_internal.parallel.stream_string_from_json(
        kde_internal.parallel.get_default_executor(),
        kde_internal.parallel.stream_from_1d_slice(I.arg),
        ds('$.a'),
    )
    # kde_internal.parallel.stream_from_1d_slice(I.arg) where arg is not strings
    with self.assertRaisesRegex(ValueError, 'string expected'):
      _ = expr.eval(arg=ds([1, 2, 3])).read_all(timeout=None)


if __name__ == '__main__':
  absltest.main()
