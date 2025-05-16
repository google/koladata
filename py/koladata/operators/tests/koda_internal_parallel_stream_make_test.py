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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor.parallel import clib
from koladata.operators import koda_internal_parallel
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals


class KodaInternalParallelStreamMakeTest(absltest.TestCase):

  def test_make(self):
    res = expr_eval.eval(koda_internal_parallel.stream_make(1, 2))
    self.assertIsInstance(res, clib.Stream)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = res.read_all(timeout=0)
    self.assertLen(res_list, 2)
    testing.assert_equal(res_list[0], ds(1))
    testing.assert_equal(res_list[1], ds(2))

  def test_make_with_bags(self):
    db1 = data_bag.DataBag.empty()
    db2 = data_bag.DataBag.empty()
    res = expr_eval.eval(koda_internal_parallel.stream_make(db1, db2))
    self.assertIsInstance(res, clib.Stream)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_BAG)
    res_list = res.read_all(timeout=0)
    self.assertLen(res_list, 2)
    testing.assert_equal(res_list[0], db1)
    testing.assert_equal(res_list[1], db2)

  def test_make_with_bags_explicit_value_type_as(self):
    db1 = data_bag.DataBag.empty()
    db2 = data_bag.DataBag.empty()
    res = expr_eval.eval(
        koda_internal_parallel.stream_make(
            db1, db2, value_type_as=data_bag.DataBag
        )
    )
    self.assertIsInstance(res, clib.Stream)
    self.assertEqual(res.qtype.value_qtype, qtypes.DATA_BAG)
    res_list = res.read_all(timeout=0)
    self.assertLen(res_list, 2)
    testing.assert_equal(res_list[0], db1)
    testing.assert_equal(res_list[1], db2)

  def test_make_with_bags_wrong_value_type_as(self):
    db1 = data_bag.DataBag.empty()
    db2 = data_bag.DataBag.empty()
    with self.assertRaisesRegex(
        ValueError, re.escape('items must be compatible with value_type_as')
    ):
      _ = expr_eval.eval(
          koda_internal_parallel.stream_make(db1, db2, value_type_as=ds(1))
      )

  def test_make_mixed_types(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('all items must have the same type')
    ):
      _ = expr_eval.eval(
          koda_internal_parallel.stream_make(1, data_bag.DataBag.empty())
      )

  def test_make_empty(self):
    res = expr_eval.eval(koda_internal_parallel.stream_make())
    self.assertIsInstance(res, clib.Stream)
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_SLICE)
    res_list = res.read_all(timeout=0)
    self.assertEmpty(res_list)

  def test_make_empty_with_value_type_as(self):
    res = expr_eval.eval(
        koda_internal_parallel.stream_make(value_type_as=data_bag.DataBag)
    )
    self.assertIsInstance(res, clib.Stream)
    testing.assert_equal(res.qtype.value_qtype, qtypes.DATA_BAG)
    res_list = res.read_all(timeout=0)
    self.assertEmpty(res_list)

  def test_view(self):
    self.assertTrue(view.has_koda_view(koda_internal_parallel.stream_make()))

  def test_repr(self):
    self.assertEqual(
        repr(koda_internal_parallel.stream_make(I.a, I.b)),
        'koda_internal.parallel.stream_make('
        'I.a, I.b, value_type_as=unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
