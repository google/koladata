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

import time

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.base import init as _
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants


I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
DATA_SLICE = test_qtypes.DATA_SLICE
DATA_BAG = test_qtypes.DATA_BAG
NON_DETERMINISTIC_TOKEN = test_qtypes.NON_DETERMINISTIC_TOKEN


class CoreWithTimestampTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.with_timestamp,
        [
            (
                DATA_SLICE,
                NON_DETERMINISTIC_TOKEN,
                arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
            ),
            (
                DATA_BAG,
                NON_DETERMINISTIC_TOKEN,
                arolla.make_tuple_qtype(DATA_BAG, DATA_SLICE),
            ),
            (
                NON_DETERMINISTIC_TOKEN,
                NON_DETERMINISTIC_TOKEN,
                arolla.make_tuple_qtype(NON_DETERMINISTIC_TOKEN, DATA_SLICE),
            )
        ],
        possible_qtypes=[DATA_SLICE, DATA_BAG, NON_DETERMINISTIC_TOKEN],
    )

  def test_eval(self):
    expr = kde.core.with_timestamp(ds([1, 2, 3]))
    res, timestamp = expr.eval()
    testing.assert_equal(res, ds([1, 2, 3]))
    self.assertIsInstance(timestamp, data_slice.DataSlice)
    self.assertEqual(timestamp.get_schema(), schema_constants.FLOAT64)
    later_timestamp = time.time()
    self.assertLess(timestamp.to_py(), later_timestamp)
    # Later but not that much later.
    self.assertGreater(timestamp.to_py(), later_timestamp - 5)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.core.with_timestamp(I.x)
        )
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.with_timestamp, kde.with_timestamp)
    )


if __name__ == '__main__':
  absltest.main()
