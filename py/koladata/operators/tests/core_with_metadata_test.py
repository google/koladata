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
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

eager = eager_op_utils.operators_container('kd')
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    *(
        (DATA_SLICE, attrs_qtype, DATA_SLICE)
        for attrs_qtype in test_qtypes.NAMEDTUPLES_OF_DATA_SLICES
    ),
    # etc. for all possible namedtuples with DATA_SLICE values.
])


class CoreWithMetadataTest(absltest.TestCase):

  def test_schema(self):
    db = fns.mutable_bag()
    s1 = db.new_schema(x=schema_constants.INT32)
    s2 = db.new_schema(x=schema_constants.OBJECT)
    x = ds([s1, s2])
    updated_x = expr_eval.eval(kde.core.with_metadata(x, text=ds(['foo', 1])))

    values = updated_x.get_attr('__schema_metadata__').get_attr('text')
    testing.assert_equal(values, ds(['foo', 1]).with_bag(updated_x.get_bag()))

  def test_multiple_args(self):
    db = fns.mutable_bag()
    s1 = db.new_schema(x=schema_constants.INT32)
    s2 = db.new_schema(x=schema_constants.OBJECT)
    x = ds([s1, s2])
    x = expr_eval.eval(
        kde.core.with_metadata(x, text=ds(['foo', 1]), name=ds(['s1', 's2']))
    )

    values_text = x.get_attr('__schema_metadata__').get_attr('text')
    testing.assert_equal(values_text, ds(['foo', 1]).with_bag(x.get_bag()))
    values_name = x.get_attr('__schema_metadata__').get_attr('name')
    testing.assert_equal(values_name, ds(['s1', 's2']).with_bag(x.get_bag()))

  def test_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'failed to create metadata; cannot create for a DataSlice with ITEMID '
        'schema',
    ):
      expr_eval.eval(
          kde.core.with_metadata(ds([None], schema_constants.ITEMID)),
          text=ds(['foo']),
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.with_metadata,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.with_metadata(I.x, a=I.y)),
        'kd.core.with_metadata(I.x, a=I.y)',
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.with_metadata(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.with_metadata, kde.with_metadata)
    )


if __name__ == '__main__':
  absltest.main()
