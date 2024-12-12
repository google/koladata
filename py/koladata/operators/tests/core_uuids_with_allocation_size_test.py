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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
kde = kde_operators.kde


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreUuidsWithAllocationSizeTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          '',
          5,
          '',
          ds(5),
      ),
      (
          'foo',
          10,
          'foo',
          10,
      ),
      (
          ds('seed'),
          100,
          'seed',
          100,
      ),
  )
  def test_equal(self, lhs_seed, lhs_size, rhs_seed, rhs_size):
    lhs = expr_eval.eval(
        kde.core.uuids_with_allocation_size(seed=lhs_seed, size=lhs_size)
    )
    rhs = expr_eval.eval(
        kde.core.uuids_with_allocation_size(seed=rhs_seed, size=rhs_size)
    )
    testing.assert_equal(lhs, rhs)

  @parameterized.parameters(
      (
          'seed',
          10,
          'another_seed',
          10,
      ),
      (
          'seed',
          10,
          'seed',
          11,
      ),
      (
          'seed',
          10,
          'seed',
          100,
      ),
      (
          'seed',
          10,
          'seed2',
          20,
      ),
  )
  def test_not_equal(self, lhs_seed, lhs_size, rhs_seed, rhs_size):
    lhs = expr_eval.eval(
        kde.core.uuids_with_allocation_size(seed=lhs_seed, size=lhs_size)
    )
    rhs = expr_eval.eval(
        kde.core.uuids_with_allocation_size(seed=rhs_seed, size=rhs_size)
    )
    self.assertNotEqual(lhs.no_bag().fingerprint, rhs.no_bag().fingerprint)

  def test_no_size(self):
    with self.assertRaisesRegex(
        TypeError, re.escape("missing 1 required keyword-only argument: 'size'")
    ):
      _ = expr_eval.eval(kde.core.uuids_with_allocation_size('foo'))

  @parameterized.parameters(
      (
          5,
          10,
          re.escape(
              'requires seed to be DataItem holding a STRING, got DataItem(5,'
              ' schema: INT32)'
          ),
      ),
      (
          ds(['seed1', 'seed2']),
          10,
          re.escape(
              'requires seed to be DataItem holding a STRING, got'
              " DataSlice(['seed1', 'seed2']"
          ),
      ),
      (
          'seed',
          ds([1, 2, 3]),
          re.escape(
              'requires size to be a scalar, got DataSlice([1, 2, 3], schema:'
              ' INT32, shape: JaggedShape(3))'
          ),
      ),
      (
          'seed',
          'size',
          re.escape(
              "requires size to be castable to int64, got DataItem('size',"
              ' schema: STRING)'
          ),
      ),
  )
  def test_error(self, seed, size, err_regex):
    with self.assertRaisesRegex(
        ValueError,
        err_regex,
    ):
      _ = expr_eval.eval(
          kde.core.uuids_with_allocation_size(seed=seed, size=size)
      )

  def test_non_data_slice_binding(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got size: UNSPECIFIED',
    ):
      _ = kde.core.uuids_with_allocation_size(
          seed=ds('foo'),
          size=arolla.unspecified(),
      )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.core.uuids_with_allocation_size(seed=I.seed, size=I.size)
        )
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.core.uuids_with_allocation_size, kde.uuids_with_allocation_size
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.uuids_with_allocation_size(seed='foo', size=I.size)),
        "kde.core.uuids_with_allocation_size(DataItem('foo', schema:"
        ' STRING), size=I.size)',
    )
    self.assertEqual(
        repr(kde.core.uuids_with_allocation_size('foo', size=I.size)),
        "kde.core.uuids_with_allocation_size(DataItem('foo', schema:"
        ' STRING), size=I.size)',
    )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.uuids_with_allocation_size,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )


if __name__ == '__main__':
  absltest.main()
