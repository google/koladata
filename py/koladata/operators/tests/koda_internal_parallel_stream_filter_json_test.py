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
from koladata.testing import testing
from koladata.types import data_slice


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kde_internal = kde_operators.internal


def tokenize(s: str):
  # split into tokens of 1-3 characters
  tokens = re.findall(r'(.{1})(.{1,3})?', s, flags=re.DOTALL)
  return [item for sub in tokens for item in sub if item]  # pylint:disable=g-complex-comprehension


class KodaInternalParallelStreamFilterJsonTest(absltest.TestCase):

  def test_basic(self):
    # Note: it is not a fully valid json, but still can be parsed.
    data = r"""
```json
{
  "query": "some \"test\" data",
  "some_bool" : true,
  'ints': [1,2,3],
  "str":
         '
x'

  ,"docs": [
    {
      "id": 1 ,
      "type": 'invoice'
    },
    null,
    {
      id: -2,
      "type": report
    }
  ]
}
```"""

    test_cases = {
        '$.query': [r'"some \"test\" data"'],
        '$.str': ['"\\nx"'],
        '$.ints': ['[1,2,3]'],
        '$.docs[*]': [
            '{"id":1,"type":"invoice"}',
            'null',
            '{"id":-2,"type":"report"}',
        ],
        '$.docs[*].id': ['1', '-2'],
    }

    for f in test_cases:
      expected_results = test_cases[f]
      expr = kde_internal.parallel.stream_filter_json(
          kde_internal.parallel.get_default_executor(),
          kde_internal.parallel.stream_from_1d_slice(I.arg),
          ds(f),
      )
      res = expr.eval(arg=ds(tokenize(data)))
      self.assertIsInstance(res, stream_clib.Stream)
      self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
      res_list = res.read_all(timeout=None)
      for i in range(min(len(res_list), len(expected_results))):
        testing.assert_equal(res_list[i], ds(expected_results[i]))
      self.assertLen(res_list, len(expected_results))

  def test_errors(self):
    error_cases = {
        '[}': "expected value begin, got '}' at pos 1",
        '{]': "unexpected ']' at pos 1",
        '[1, 2, 3]]': "unexpected ']' at pos 7",
        '{ "a": true': 'unexpected end of json stream',
        '[1, 2, 3,': 'unexpected end of json stream',
        '"string': 'unexpected end of json stream',
    }

    for s in error_cases:
      expected_error = error_cases[s]
      expr = kde_internal.parallel.stream_filter_json(
          kde_internal.parallel.get_default_executor(),
          kde_internal.parallel.stream_from_1d_slice(I.arg),
          ds('$'),
      )
      with self.assertRaisesRegex(ValueError, expected_error):
        _ = expr.eval(arg=ds(tokenize(s))).read_all(timeout=None)


if __name__ == '__main__':
  absltest.main()
