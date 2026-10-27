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
import threading
import time

from absl.testing import absltest
from koladata import kd
from koladata import kd_ext

I = kd.I


def countdown(batch):
  """Returns an iterable with `n - 1` for every item `n > 1` of `batch`."""
  return kd.functor.flat_map_chain(
      batch,
      lambda n: kd.if_(
          n > 1,
          lambda n: kd.iterables.make(n - 1),
          lambda n: kd.iterables.make(),
          n,
          return_type_as=kd.iterables.make(),
      ),
  )


def batch_sum(batch, initial_value=0):
  """Returns the sum of the items of `batch` plus `initial_value`."""
  return kd.functor.reduce(
      lambda acc, n: acc + n, batch, initial_value=initial_value
  )


def yield_with_countdown(batch):
  """A step yielding the items and cascading their countdowns."""
  return kd.namedtuple(yields=batch, cascades=countdown(batch))


class BatchedCascadeTest(absltest.TestCase):

  def test_basic(self):
    res = kd_ext.contrib.batched_cascade(
        kd.iterables.make(3, 2),
        yield_with_countdown,
        yields=kd.iterables.make(),
    )
    self.assertEqual([x.to_py() for x in res], [3, 2, 2, 1, 1])

  def test_batches_in_eager_mode(self):
    res = kd_ext.contrib.batched_cascade.lazy_op(
        I.input_seq,
        lambda batch: kd.namedtuple(
            yields=kd.iterables.make(batch_sum(batch)),
            cascades=countdown(batch),
        ),
        yields=kd.iterables.make(),
    )
    # Batches: [3, 2], [2, 1], [1].
    self.assertEqual(
        [x.to_py() for x in res.eval(input_seq=kd.iterables.make(3, 2))],
        [5, 3, 1],
    )

  def test_state(self):
    res = kd_ext.contrib.batched_cascade(
        kd.iterables.make(3, 2),
        lambda batch, step: kd.namedtuple(
            yields=kd.iterables.make(step),
            step=step * 10,
            cascades=countdown(batch),
        ),
        yields=kd.iterables.make(),
        step=1,
    )
    self.assertEqual([x.to_py() for x in res], [1, 10, 100])

  def test_initial_yields(self):
    res = kd_ext.contrib.batched_cascade(
        kd.iterables.make(3, 2),
        lambda batch: kd.namedtuple(yields=batch),
        yields=kd.iterables.make(0),
    )
    self.assertEqual([x.to_py() for x in res], [0, 3, 2])

  def test_empty_iterable(self):
    res = kd_ext.contrib.batched_cascade(
        kd.iterables.make(),
        yield_with_countdown,
        yields=kd.iterables.make(),
    )
    self.assertEmpty(list(res))

  def test_wrong_type_for_cascades(self):
    with self.assertRaisesRegex(ValueError, 'cascades'):
      _ = kd_ext.contrib.batched_cascade(
          kd.iterables.make(3, 2),
          lambda batch: kd.namedtuple(cascades=kd.iterables.make(kd.bag())),
          yields=kd.iterables.make(),
      )

  def test_reserved_state_name(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('`cascades` cannot be used as a state variable name'),
    ):
      _ = kd_ext.contrib.batched_cascade(
          kd.iterables.make(3, 2),
          yield_with_countdown,
          yields=kd.iterables.make(),
          cascades=kd.iterables.make(),
      )

  def test_non_iterable(self):
    with self.assertRaisesRegex(
        ValueError, 'expected an iterable type, got cascade: DATA_SLICE'
    ):
      _ = kd_ext.contrib.batched_cascade(
          kd.slice([1, 2, 3]),
          yield_with_countdown,
          yields=kd.iterables.make(),
      )

  def test_transform(self):
    fn = kd.fn(
        lambda vals: kd_ext.contrib.batched_cascade(
            vals, yield_with_countdown, yields=kd.iterables.make()
        )
    )
    self.assertIn(
        'kd_ext.contrib._parallel_batched_cascade',
        str(kd.parallel.transform(fn)),
    )

  def test_parallel(self):
    fn = kd.fn(
        lambda vals: kd_ext.contrib.batched_cascade(
            vals, yield_with_countdown, yields=kd.iterables.make()
        )
    )
    self.assertCountEqual(
        [
            x.to_py()
            for x in kd.parallel.yield_multithreaded(
                fn, vals=kd.iterables.make(3, 2)
            )
        ],
        [3, 2, 2, 1, 1],
    )

  def test_parallel_yields_order(self):
    # The yields of an item come before the yields of its cascades, even if
    # they are computed later.
    def slow_identity(n):
      if n.to_py() == 3:
        time.sleep(0.1)
      return n

    fn = kd.fn(
        lambda vals: kd_ext.contrib.batched_cascade(
            vals,
            lambda batch: kd.namedtuple(
                yields=kd.functor.flat_map_chain(
                    batch,
                    lambda n: kd.iterables.make(
                        kd.call(kd.py_fn(slow_identity), n)
                    ),
                ),
                cascades=countdown(batch),
            ),
            yields=kd.iterables.make(),
        )
    )
    res = kd.parallel.yield_multithreaded(
        fn, vals=kd.iterables.make(3), max_threads=4, timeout=10
    )
    self.assertEqual([x.to_py() for x in res], [3, 2, 1])

  def test_parallel_long_chain(self):
    fn = kd.fn(
        lambda vals: kd_ext.contrib.batched_cascade(
            vals,
            lambda batch: kd.namedtuple(
                yields=kd.iterables.make(batch_sum(batch)),
                cascades=countdown(batch),
            ),
            yields=kd.iterables.make(),
        )
    )
    total = sum(
        x.to_py()
        for x in kd.parallel.yield_multithreaded(
            fn, vals=kd.iterables.make(1000)
        )
    )
    self.assertEqual(total, 500500)

  def test_parallel_slow_item_does_not_block_batches(self):
    # The cascade of item 0 is blocked until item 10 is processed, which is
    # only possible if the next batch is formed without waiting for it.
    event = threading.Event()

    def next_item(n):
      n = n.to_py()
      if n == 0:
        if not event.wait(timeout=10):
          raise TimeoutError('item 10 was not processed')
        return 100
      if n == 1:
        return 10
      if n == 10:
        event.set()
        return 11
      return 0

    def cascade_next(n):
      m = kd.call(kd.py_fn(next_item), n)
      return kd.iterables.from_1d_slice(kd.select(kd.stack(m), m > 0))

    fn = kd.fn(
        lambda vals: kd_ext.contrib.batched_cascade(
            vals,
            lambda batch: kd.namedtuple(
                yields=batch,
                cascades=kd.functor.flat_map_interleaved(batch, cascade_next),
            ),
            yields=kd.iterables.make(),
        )
    )
    res = kd.parallel.yield_multithreaded(
        fn, vals=kd.iterables.make(0, 1), max_threads=4, timeout=10
    )
    self.assertCountEqual([x.to_py() for x in res], [0, 1, 10, 11, 100])

  def test_parallel_state(self):
    fn = kd.fn(
        lambda vals: kd_ext.contrib.batched_cascade(
            vals,
            lambda batch, total: kd.namedtuple(
                yields=kd.iterables.make(batch_sum(batch, total)),
                total=batch_sum(batch, total),
                cascades=countdown(batch),
            ),
            yields=kd.iterables.make(),
            total=0,
        )
    )
    # The running totals depend on the batching, but the last one is the sum
    # of all items.
    totals = [
        x.to_py()
        for x in kd.parallel.yield_multithreaded(
            fn, vals=kd.iterables.make(3, 2)
        )
    ]
    self.assertEqual(max(totals), 9)


if __name__ == '__main__':
  absltest.main()
