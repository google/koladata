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
from arolla import arolla
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice

kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class TuplesGetNamedtupleFieldTest(parameterized.TestCase):

  @parameterized.parameters(
      (kde.namedtuple(a=0, b=1).eval(), 'a', ds(0)),
      (kde.namedtuple(a=0, b=1).eval(), 'b', ds(1)),
      (kde.namedtuple(a=0, b=1).eval(), ds('a'), ds(0)),
      (
          kde.namedtuple(a=0, b=arolla.text('a')).eval(),
          ds('b'),
          arolla.text('a'),
      ),
  )
  def test_get_namedtuple_field(self, namedtuple, field_name, expected):
    testing.assert_equal(
        fns.tuples.get_namedtuple_field(namedtuple, field_name), expected
    )

  @parameterized.parameters(b'a', 1, ds(b'a'), ds(['a']))
  def test_not_str_error(self, field_name):
    with self.assertRaisesRegex(
        TypeError,
        re.escape(f'expected a value convertible to a str, got: {field_name}'),
    ):
      fns.tuples.get_namedtuple_field(kde.namedtuple(a=0).eval(), field_name)

  def test_not_namedtuple_error(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('expected a NamedTuple, got: 1')
    ):
      fns.tuples.get_namedtuple_field(1, 'a')

  def test_not_a_field_error(self):
    with self.assertRaisesRegex(
        KeyError, re.escape('`b` is not found in `namedtuple<a=DATA_SLICE>')
    ):
      fns.tuples.get_namedtuple_field(kde.namedtuple(a=0).eval(), 'b')


if __name__ == '__main__':
  absltest.main()
