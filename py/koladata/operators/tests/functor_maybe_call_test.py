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
import signal
import threading
import time

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import functor_factories
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, qtypes.NON_DETERMINISTIC_TOKEN, DATA_SLICE),
])


class FunctorMaybeCallTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([1, 2, 3]),
          ds([1]),
          ds([1, 2, 3]),
      ),
      (
          functor_factories.expr_fn(I.self + 1),
          ds([1, 2, 3]),
          ds([2, 3, 4]),
      ),
  )
  def test_call_with_functor(self, maybe_fn, arg, expected):
    result = expr_eval.eval(kde.functor._maybe_call(maybe_fn, arg))
    testing.assert_equal(result, expected)

  def test_call_with_error(self):
    f = functor_factories.expr_fn(I.self.some_attr)

    data = ds([1, 2, 3])
    with self.assertRaisesRegex(
        ValueError, 'primitives do not have attributes'
    ):
      expr_eval.eval(kde.functor._maybe_call(f, data))

    f = functor_factories.expr_fn(I.self.get_bag())
    entity = data_bag.DataBag.empty().new()
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the functor is expected to be evaluated to a DataSlice, but the'
            ' result has type `DATA_BAG` instead'
        ),
    ):
      expr_eval.eval(kde.functor._maybe_call(f, entity))

    db = data_bag.DataBag.empty()
    with self.assertRaisesRegex(
        ValueError, re.escape('expected DATA_SLICE, got arg: DATA_BAG')
    ):
      expr_eval.eval(kde.functor._maybe_call(f, db))

  def test_non_determinism(self):
    fn = functor_factories.fn(kde.new(x=I.self, schema='new'))
    x = ds(42)

    expr = kde.tuple.make_tuple(
        kde.functor._maybe_call(fn, x), kde.functor._maybe_call(fn, x)
    )
    res = expr_eval.eval(expr)
    self.assertNotEqual(res[0].no_bag(), res[1].no_bag())
    testing.assert_equal(res[0].x.no_bag(), res[1].x.no_bag())

    expr = kde.functor._maybe_call(fn, x)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.no_bag(), res_2.no_bag())
    testing.assert_equal(res_1.x.no_bag(), res_2.x.no_bag())

  def test_cancellable(self):
    expr = I.self
    for _ in range(10**4):
      expr += expr
    expr = kde.functor._maybe_call(functor_factories.expr_fn(expr), I.x)
    x = ds(list(range(10**6)))

    def do_keyboard_interrupt():
      time.sleep(0.1)
      signal.raise_signal(signal.SIGINT)

    threading.Thread(target=do_keyboard_interrupt).start()
    with self.assertRaisesRegex(ValueError, re.escape('interrupt')):
      # This computation typically takes more than 10 seconds and fails
      # with a different error unless the interruption is handled by
      # the operator.
      expr_eval.eval(expr, x=x)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.functor._maybe_call,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde.functor._maybe_call(I.maybe_fn, I.arg))
    )


if __name__ == '__main__':
  absltest.main()
