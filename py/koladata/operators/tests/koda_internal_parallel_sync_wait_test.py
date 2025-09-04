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
import time

from absl.testing import absltest
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.functor.parallel import clib as _
from koladata.operators import kde_operators
from koladata.operators import koda_internal_parallel
from koladata.operators import optools
from koladata.types import data_slice

I = input_container.InputContainer('I')

py_fn = functor_factories.py_fn
ds = data_slice.DataSlice.from_vals

kde = kde_operators.kde

eager_executor = expr_eval.eval(koda_internal_parallel.get_eager_executor())


class KodaInternalParallelSyncWaitTest(absltest.TestCase):

  def test_basic(self):
    result = koda_internal_parallel.sync_wait(
        koda_internal_parallel.stream_make(1)
    ).eval()
    self.assertEqual(result, 1)

  def test_error_empty_stream(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a stream with a single item, got an empty stream'),
    ):
      _ = koda_internal_parallel.sync_wait(
          koda_internal_parallel.stream_make()
      ).eval()

  def test_error_stream_with_multiple_items(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a stream with a single item, got a stream with multiple'
            ' items'
        ),
    ):
      _ = koda_internal_parallel.sync_wait(
          koda_internal_parallel.stream_make(1, 2)
      ).eval()

  def test_error_call_from_async_task(self):
    def do_test():
      with self.assertRaisesRegex(
          ValueError,
          re.escape('sync_wait cannot be called from an asynchronous task'),
      ):
        _ = koda_internal_parallel.sync_wait(
            koda_internal_parallel.stream_make(1)
        ).eval()

    eager_executor.schedule(do_test)

  def test_delayed_item(self):
    def fn():
      time.sleep(0.02)
      return ds(1)

    result = koda_internal_parallel.sync_wait(
        koda_internal_parallel.stream_call(
            koda_internal_parallel.get_default_executor(), py_fn(fn)
        )
    ).eval()
    self.assertEqual(result, 1)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(koda_internal_parallel.sync_wait(I.stream))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            koda_internal_parallel.sync_wait,
            kde.streams.sync_wait,
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(koda_internal_parallel.sync_wait(I.stream)),
        'koda_internal.parallel.sync_wait(I.stream)',
    )
    self.assertEqual(
        repr(kde.streams.sync_wait(I.stream)),
        'kd.streams.sync_wait(I.stream)',
    )


if __name__ == '__main__':
  absltest.main()
