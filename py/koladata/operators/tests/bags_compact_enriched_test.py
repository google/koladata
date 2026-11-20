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
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes


bag = data_bag.DataBag.empty_mutable
I = input_container.InputContainer('I')
kde = kde_operators.kde
kd = kde_operators.kd
ds = data_slice.DataSlice.from_vals

DATA_BAG = qtypes.DATA_BAG

QTYPES = frozenset([
    (DATA_BAG,),
    (DATA_BAG, DATA_BAG),
    (DATA_BAG, DATA_BAG, DATA_BAG),
    (DATA_BAG, DATA_BAG, DATA_BAG, DATA_BAG),
])


class BagsCompactEnrichedTest(absltest.TestCase):

  def test_compact_enriched_bag(self):
    db1 = bag()
    o1 = db1.uuobj(x=1)
    db2 = bag()
    o2 = db2.uuobj(x=1)
    o2.x = 2
    o2.y = 3
    db3 = bag()
    o3 = db3.uuobj(x=1)
    o3.x = 2
    o3.y = 4
    res = kd.bags.compact_enriched(db3, db2, db1)
    o4 = o1.stub().with_bag(res)
    testing.assert_equal(o4.x.no_bag(), ds(2))
    testing.assert_equal(o4.y.no_bag(), ds(4))

  def test_compaction_across_steps_matches_enriched(self):
    acc = kd.bags.new()
    ref = kd.bags.new()
    items = []
    for step in range(64):
      delta = bag()
      item = delta.new(step=step)
      items.append(item.no_bag())
      acc = kd.bags.compact_enriched(acc, delta)
      ref = kd.bags.enriched(ref, delta)
    # 64 equal-sized step bags collapse into a single flat DataBag (forkable).
    self.assertIsNotNone(acc.fork())
    self.assertTrue(data_bag.content_equal(acc, ref))
    for step, item in enumerate(items):
      testing.assert_equal(item.with_bag(acc).step.no_bag(), ds(step))

  def test_few_args(self):
    db = kd.bags.compact_enriched()
    self.assertEqual(db.get_approx_size(), 0)
    self.assertFalse(db.is_mutable())
    db1 = bag()
    o1 = db1.uuobj(x=1)
    db = kd.bags.compact_enriched(db1)
    self.assertGreater(db.get_approx_size(), 0)
    self.assertFalse(db.is_mutable())
    testing.assert_equal(o1.with_bag(db).x.no_bag(), ds(1))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.bags.compact_enriched,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,  # pyrefly: ignore[bad-argument-type]
        max_arity=3,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.bags.compact_enriched(I.x, I.y)))


if __name__ == '__main__':
  absltest.main()
