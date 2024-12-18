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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE
DATA_BAG = qtypes.DATA_BAG


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_BAG),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_BAG),
    (DATA_SLICE, DATA_SLICE, arolla.UNSPECIFIED, DATA_BAG),
])


class CoreDictUpdateTest(parameterized.TestCase):

  def test_eval_keys_values(self):
    x1 = fns.dict(ds([1, 2, 3]), ds([4, 5, 6]))

    with self.assertRaisesRegex(
        exceptions.KodaError, 'the schema for Dict key is incompatible.'
    ):
      # x1 schema is DICT[INT32, INT32]
      _ = expr_eval.eval(
          kde.dict_update(x1, ds([1, 3, 'x']), ds([8, 'y', 'z']))
      )

    db2 = expr_eval.eval(kde.dict_update(x1, ds([1, 3, 7]), ds([8, 9, 10])))
    testing.assert_dicts_equal(
        x1.updated(db2), fns.dict(ds([1, 2, 3, 7]), ds([8, 5, 9, 10]))
    )
    testing.assert_dicts_equal(
        x1.with_bag(db2), fns.dict(ds([1, 3, 7]), ds([8, 9, 10]))
    )

  def test_eval_keys_values_slice_broadcast(self):
    x1 = ds([
        fns.dict(ds([1, 2, 3]), ds([4, 5, 6])),
        fns.dict(ds([7, 8]), ds([9, 10])),
    ])

    db2 = expr_eval.eval(kde.dict_update(x1, ds(1), ds(2)))
    testing.assert_dicts_equal(
        x1.updated(db2), ds([
            fns.dict(ds([1, 2, 3]), ds([2, 5, 6])),
            fns.dict(ds([1, 7, 8]), ds([2, 9, 10])),
        ])
    )
    testing.assert_dicts_equal(
        x1.with_bag(db2),
        ds([
            fns.dict(ds([1]), ds([2])),
            fns.dict(ds([1]), ds([2])),
        ]),
    )

  def test_eval_keys_values_slice(self):
    x1 = ds([
        fns.dict(ds([1, 2, 3]), ds([4, 5, 6])),
        fns.dict(ds([7, 8]), ds([9, 10])),
    ])

    db2 = expr_eval.eval(
        kde.dict_update(x1, ds([[1, 3, 7], [1, 2]]), ds([[8, 9, 10], [2, 1]]))
    )
    testing.assert_dicts_equal(
        x1.updated(db2), ds([
            fns.dict(ds([1, 2, 3, 7]), ds([8, 5, 9, 10])),
            fns.dict(ds([1, 2, 7, 8]), ds([2, 1, 9, 10])),
        ])
    )
    testing.assert_dicts_equal(
        x1.with_bag(db2),
        ds([
            fns.dict(ds([1, 3, 7]), ds([8, 9, 10])),
            fns.dict(ds([1, 2]), ds([2, 1])),
        ]),
    )

  def test_eval_keys_values_object_key_value_schema(self):
    x1 = fns.dict(
        ds([1, 2, 3]),
        ds([4, 5, 6]),
        key_schema=schema_constants.OBJECT,
        value_schema=schema_constants.OBJECT,
    )
    db2 = expr_eval.eval(
        kde.dict_update(x1, ds([1, 3, 'x']), ds([8, 'y', 'z']))
    )
    testing.assert_dicts_equal(
        x1.updated(db2), fns.dict(ds([1, 2, 3, 'x']), ds([8, 5, 'y', 'z']))
    )
    testing.assert_dicts_equal(
        x1.with_bag(db2), fns.dict(ds([1, 3, 'x']), ds([8, 'y', 'z']))
    )

  def test_eval_keys_values_embedded_schema(self):
    x1 = bag().dict(ds([1, 2, 3]), ds([4, 5, 6])).embed_schema()

    with self.assertRaisesRegex(
        exceptions.KodaError, 'the schema for Dict key is incompatible.'
    ):
      # x1 schema is DICT[INT32, INT32]
      _ = expr_eval.eval(
          kde.dict_update(x1, ds([1, 3, 'x']), ds([8, 'y', 'z']))
      )

    db2 = expr_eval.eval(kde.dict_update(x1, ds([1, 3, 7]), ds([8, 9, 10])))
    testing.assert_dicts_equal(
        x1.updated(db2), fns.dict(ds([1, 2, 3, 7]), ds([8, 5, 9, 10]))
    )
    testing.assert_dicts_equal(
        x1.with_bag(db2), fns.dict(ds([1, 3, 7]), ds([8, 9, 10]))
    )

  def test_eval_dicts(self):
    x1 = fns.dict(ds([1, 2, 3]), ds([4, 5, 6]))
    db2 = expr_eval.eval(
        kde.dict_update(x1, fns.dict(ds([1, 3, 7]), ds([8, 9, 10])))
    )
    testing.assert_dicts_equal(
        x1.updated(db2), fns.dict(ds([1, 2, 3, 7]), ds([8, 5, 9, 10]))
    )
    testing.assert_dicts_equal(
        x1.with_bag(db2), fns.dict(ds([1, 3, 7]), ds([8, 9, 10]))
    )

  def test_error_primitive_schema(self):
    with self.assertRaisesRegex(ValueError, 'expected a DataSlice of dicts'):
      _ = kde.dicts.dict_update(
          ds(0).with_bag(bag()), fns.dict({'x': 1})
      ).eval()

  def test_error_not_dict_embedded_schema(self):
    with self.assertRaisesRegex(ValueError, 'expected a DataSlice of dicts'):
      _ = expr_eval.eval(
          kde.dicts.dict_update(bag().obj(x=1), fns.dict({'x': 1}))
      )

  def test_error_no_databag(self):
    o = fns.new(x=1).no_bag()
    with self.assertRaisesRegex(
        ValueError,
        'cannot update a DataSlice of dicts without a DataBag',
    ):
      _ = kde.dicts.dict_update(o, fns.dict({'x': 1})).eval()

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.dicts.dict_update,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.dicts.dict_update(I.x, I.keys, I.values))
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.dicts.dict_update, kde.dict_update))


if __name__ == '__main__':
  absltest.main()
