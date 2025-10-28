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

import math
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.functions import object_factories
from koladata.operators import kde_operators
from koladata.operators.tests.testdata import lists_implode_testdata
from koladata.testing import testing
from koladata.types import data_slice


kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class ImplodeTest(parameterized.TestCase):

  def test_mutability(self):
    self.assertFalse(object_factories.implode(ds([1, None])).is_mutable())

  @parameterized.parameters(*lists_implode_testdata.TEST_CASES)
  def test_eval(self, x, ndim, expected):
    # Test behavior with explicit existing DataBag.
    db = object_factories.mutable_bag()
    x = db.adopt(x)
    expected = db.adopt(expected)
    result = db.implode(x, ndim)
    testing.assert_equivalent(result, expected)
    self.assertEqual(result.get_bag().fingerprint, x.get_bag().fingerprint)

    # Test behavior with implicit new DataBag.
    result = object_factories.implode(x, ndim)
    testing.assert_equivalent(result, expected)
    self.assertNotEqual(x.get_bag().fingerprint, result.get_bag().fingerprint)

    # Check behavior with DataItem ndim.
    result = object_factories.implode(x, ds(ndim))
    testing.assert_equivalent(result, expected)
    self.assertNotEqual(x.get_bag().fingerprint, result.get_bag().fingerprint)

  def test_eval_nan(self):
    db = object_factories.mutable_bag()
    o = db.obj(a=math.nan)
    x = ds([o])
    ndim = 1
    expected = db.list([o])
    result = db.implode(x, ndim)
    with self.assertRaisesRegex(
        AssertionError,
        r'expected\[0\].a:\nDataItem\(nan, schema: FLOAT32\)\n'
        r'-> actual\[0\].a:\nDataItem\(nan, schema: FLOAT32\)',
    ):
      testing.assert_equivalent(result, expected)

  def test_itemid(self):
    itemid = kde.allocation.new_listid_shaped_as(ds([1, 1])).eval()
    x = object_factories.implode(ds([['a', 'b'], ['c']]), ndim=1, itemid=itemid)
    testing.assert_equal(x[:].no_bag(), ds([['a', 'b'], ['c']]).no_bag())
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    triple = object_factories.new(non_existent=42)
    itemid = object_factories.implode(ds([[triple], []]))

    # Successful.
    x = object_factories.implode(
        ds([['a', 'b'], ['c']]), ndim=1, itemid=itemid.get_itemid()
    )
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex(
            "attribute 'non_existent' is missing"
        ),
    ):
      _ = triple.with_bag(x.get_bag()).non_existent

  def test_ndim_error(self):
    with self.assertRaisesRegex(TypeError, 'an integer is required'):
      object_factories.implode(ds([]), ds(None))

    with self.assertRaisesRegex(TypeError, 'an integer is required'):
      object_factories.implode(ds([]), ds([1]))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "kd.implode: cannot implode 'x' to fold the last 2 dimension(s)"
            " because 'x' only has 1 dimensions"
        ),
    ):
      object_factories.implode(ds([1, 2]), 2)


if __name__ == '__main__':
  absltest.main()
