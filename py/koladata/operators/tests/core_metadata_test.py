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
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import object_factories
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')

ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_BAG = qtypes.DATA_BAG
DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    (DATA_SLICE, DATA_BAG),
    *(
        (DATA_SLICE, attrs_qtype, DATA_BAG)
        for attrs_qtype in test_qtypes.NAMEDTUPLES_OF_DATA_SLICES
    ),
    # etc. for all possible namedtuples with DATA_SLICE values.
])


class CoreMetadataTest(absltest.TestCase):

  def test_schema(self):
    db = object_factories.mutable_bag()
    s1 = db.new_schema(x=schema_constants.INT32)
    s2 = db.new_schema(x=schema_constants.OBJECT)
    x = ds([s1, s2]).freeze_bag()
    metadata_update = kd.core.metadata(x, text=ds(['foo', 1]))
    updated_x = x.updated(metadata_update)

    values = updated_x.get_attr('__schema_metadata__').get_attr('text')
    testing.assert_equal(values, ds(['foo', 1]).with_bag(updated_x.get_bag()))

  def test_multiple_calls(self):
    db = object_factories.mutable_bag()
    s1 = db.new_schema(x=schema_constants.INT32)
    s2 = db.new_schema(x=schema_constants.OBJECT)
    x = ds([s1, s2]).freeze_bag()
    metadata_update = kd.core.metadata(x, text=ds(['foo', 1]))
    x = x.updated(metadata_update)

    metadata_update = kd.core.metadata(x, name=ds(['s1', 's2']))
    x = x.updated(metadata_update)

    values_text = x.get_attr('__schema_metadata__').get_attr('text')
    testing.assert_equal(values_text, ds(['foo', 1]).with_bag(x.get_bag()))
    values_name = x.get_attr('__schema_metadata__').get_attr('name')
    testing.assert_equal(values_name, ds(['s1', 's2']).with_bag(x.get_bag()))

    metadata_update = kd.core.metadata(x, text=ds(['bar', 2]))
    x = x.updated(metadata_update)

    values_text = x.get_attr('__schema_metadata__').get_attr('text')
    testing.assert_equal(values_text, ds(['bar', 2]).with_bag(x.get_bag()))
    values_name = x.get_attr('__schema_metadata__').get_attr('name')
    testing.assert_equal(values_name, ds(['s1', 's2']).with_bag(x.get_bag()))

  def test_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'failed to create metadata; cannot create for a DataSlice with ITEMID '
        'schema',
    ):
      kd.core.metadata(ds([None], schema_constants.ITEMID), text=ds(['foo']))
    with self.assertRaisesRegex(
        ValueError,
        r'failed to create metadata; cannot create for a DataSlice with'
        r' ENTITY\(x=INT32\) schema',
    ):
      db = object_factories.mutable_bag()
      schema = db.new_schema(x=schema_constants.INT32)
      kd.core.metadata(ds([None], schema), text=ds(['foo']))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.metadata,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.metadata(I.x, a=I.y)), 'kd.core.metadata(I.x, a=I.y)'
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.metadata(I.x, a=I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.metadata, kde.metadata))


if __name__ == '__main__':
  absltest.main()
