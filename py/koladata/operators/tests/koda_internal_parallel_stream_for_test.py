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
import random
import re
import threading
import time

from absl.testing import absltest
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor.parallel import clib as stream_clib
from koladata.operators import kde_operators
from koladata.operators import koda_internal_parallel
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import py_boxing
from koladata.types import qtypes

ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
py_fn = user_facing_kd.py_fn
I = input_container.InputContainer('I')


def stream_make(*args, **kwargs):
  return arolla.abc.aux_eval_op(
      'koda_internal.parallel.stream_make', *args, **kwargs
  )


def delayed_stream_make(*items, value_type_as=None, delay_per_item=0.005):
  items = list(map(py_boxing.as_qvalue, items))
  if items:
    value_qtype = items[0].qtype
  elif value_type_as is not None:
    value_qtype = value_type_as.qtype
  else:
    value_qtype = qtypes.DATA_SLICE
  result, writer = stream_clib.Stream.new(value_qtype)

  def delay_fn():
    try:
      for item in items:
        # randomize using the exponential distribution
        time.sleep(-math.log(1.0 - random.random()) * delay_per_item)
        writer.write(item)
      time.sleep(-math.log(1.0 - random.random()) * delay_per_item)
      writer.close()
    except Exception as e:  # pylint: disable=broad-exception-caught
      writer.close(e)

  threading.Thread(target=delay_fn, daemon=True).start()
  return result


class KodaInternalParalleStreamForTest(absltest.TestCase):

  def test_for_simple(self):
    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, other, returns: user_facing_kd.namedtuple(
            returns=returns * item,
        ),
        returns=1,
        other=2,
    )
    [returns] = product.eval(
        input_seq=delayed_stream_make(*range(1, 6))
    ).read_all(timeout=1)
    testing.assert_equal(returns, ds(120))

  def test_for_early_stop(self):
    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, returns: user_facing_kd.namedtuple(
            returns=returns * item,
        ),
        condition_fn=lambda returns: returns < 10,
        returns=1,
    )
    [returns] = product.eval(
        input_seq=delayed_stream_make(*range(1, 6))
    ).read_all(timeout=1)
    testing.assert_equal(returns, ds(24))

  def test_for_early_stop_with_async_condition(self):
    def condition_fn(returns):
      return delayed_stream_make(returns < 10)

    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, returns: user_facing_kd.namedtuple(
            returns=returns * item,
        ),
        condition_fn=py_fn(condition_fn, return_type_as=condition_fn(ds(0))),
        returns=1,
    )
    [returns] = product.eval(
        input_seq=delayed_stream_make(*range(1, 6))
    ).read_all(timeout=1)
    testing.assert_equal(returns, ds(24))

  def test_for_finalize(self):
    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, returns: user_facing_kd.namedtuple(
            returns=returns * item,
        ),
        finalize_fn=lambda returns: user_facing_kd.namedtuple(
            returns=-returns
        ),
        returns=1,
    )
    [returns] = product.eval(
        input_seq=delayed_stream_make(*range(1, 6))
    ).read_all(timeout=1)
    testing.assert_equal(returns, ds(-120))

  def test_for_finalize_empty_stream(self):
    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, returns: user_facing_kd.namedtuple(
            returns=returns * item,
        ),
        finalize_fn=lambda returns: user_facing_kd.namedtuple(
            returns=-returns
        ),
        returns=1,
    )
    [returns] = product.eval(input_seq=delayed_stream_make()).read_all(
        timeout=1
    )
    testing.assert_equal(returns, ds(-1))

  def test_for_early_stop_before_first_iteration(self):
    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, returns: user_facing_kd.namedtuple(
            returns=returns * item,
        ),
        condition_fn=lambda returns: returns < 0,
        returns=1,
    )
    [returns] = product.eval(
        input_seq=delayed_stream_make(*range(2, 6))
    ).read_all(timeout=1)
    testing.assert_equal(returns, ds(1))

  def test_for_early_stop_after_last_iteration_finalize_not_called(self):
    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, returns: user_facing_kd.namedtuple(
            returns=returns * item,
        ),
        finalize_fn=lambda returns: user_facing_kd.namedtuple(
            returns=-returns
        ),
        condition_fn=lambda returns: returns < 120,
        returns=1,
    )
    [returns] = product.eval(
        input_seq=delayed_stream_make(*range(1, 6))
    ).read_all(timeout=1)
    testing.assert_equal(returns, ds(120))

  def test_for_no_return_statement(self):
    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, other, returns: user_facing_kd.namedtuple(
            other=other * item,
        ),
        returns=1,
        other=2,
    )
    [returns] = product.eval(
        input_seq=delayed_stream_make(*range(1, 6))
    ).read_all(timeout=1)
    testing.assert_equal(returns, ds(1))

  def test_for_yields(self):
    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, res: user_facing_kd.namedtuple(
            yields=koda_internal_parallel.stream_make(res * item),
            res=res * item,
        ),
        res=1,
        yields=delayed_stream_make(),
    )
    self.assertEqual(
        product.eval(input_seq=delayed_stream_make(*range(1, 6))).read_all(
            timeout=1
        ),
        [1, 2, 6, 24, 120],
    )

  def test_for_yields_interleaved(self):
    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, res: user_facing_kd.namedtuple(
            yields_interleaved=koda_internal_parallel.stream_make(res * item),
            res=res * item,
        ),
        res=1,
        yields_interleaved=delayed_stream_make(),
    )
    self.assertCountEqual(
        product.eval(input_seq=delayed_stream_make(*range(1, 6))).read_all(
            timeout=1
        ),
        [ds(1), ds(2), ds(6), ds(24), ds(120)],
    )

  def test_for_yields_finalize(self):
    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, count: user_facing_kd.namedtuple(
            yields=koda_internal_parallel.stream_make(item),
            count=count + 1,
        ),
        finalize_fn=lambda count: user_facing_kd.namedtuple(
            yields=koda_internal_parallel.stream_make(-1)
        ),
        condition_fn=lambda count: (count < 3),
        yields=delayed_stream_make(),
        count=0,
    )
    self.assertEqual(
        product.eval(input_seq=delayed_stream_make(*range(3))).read_all(
            timeout=1
        ),
        [0, 1, 2],
    )
    self.assertEqual(
        product.eval(input_seq=delayed_stream_make(*range(2))).read_all(
            timeout=1
        ),
        [0, 1, -1],
    )

  def test_for_yields_no_yield_statement(self):
    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, res: user_facing_kd.namedtuple(
            res=res * item,
        ),
        res=1,
        yields=delayed_stream_make(5),
    )
    self.assertEqual(
        product.eval(input_seq=delayed_stream_make(*range(1, 6))).read_all(
            timeout=1
        ),
        [5],
    )

  def test_for_yields_interleaved_no_yield_statement(self):
    product = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, res: user_facing_kd.namedtuple(
            res=res * item,
        ),
        res=1,
        yields_interleaved=delayed_stream_make(5, 7),
    )
    self.assertEqual(
        product.eval(input_seq=delayed_stream_make(*range(1, 6))).read_all(
            timeout=1
        ),
        [5, 7],
    )

  def test_for_yields_interleaved_order(self):
    stream0, writer0 = stream_clib.Stream.new(qtypes.DATA_SLICE)
    stream1, writer1 = stream_clib.Stream.new(qtypes.DATA_SLICE)
    stream2, writer2 = stream_clib.Stream.new(qtypes.DATA_SLICE)

    def body_fn(n):
      return arolla.namedtuple(yields_interleaved=[stream1, stream2][n])

    res = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_eager_executor(),
        stream_make(0, 1),
        py_fn(body_fn, return_type_as=body_fn(ds(0))),
        yields_interleaved=stream0,
    ).eval()

    reader = res.make_reader()
    self.assertEqual(reader.read_available(), [])
    writer0.write(ds(1))
    self.assertEqual(reader.read_available(), [1])
    writer1.write(ds(2))
    self.assertEqual(reader.read_available(), [2])
    writer2.write(ds(3))
    self.assertEqual(reader.read_available(), [3])
    writer0.close()
    self.assertEqual(reader.read_available(), [])
    writer1.close()
    self.assertEqual(reader.read_available(), [])
    writer2.close()
    self.assertIsNone(reader.read_available())

  def test_for_yields_chained_order(self):
    stream0, writer0 = stream_clib.Stream.new(qtypes.DATA_SLICE)
    stream1, writer1 = stream_clib.Stream.new(qtypes.DATA_SLICE)
    stream2, writer2 = stream_clib.Stream.new(qtypes.DATA_SLICE)

    def body_fn(n):
      return arolla.namedtuple(yields=[stream1, stream2][n])

    res = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_eager_executor(),
        stream_make(0, 1),
        py_fn(body_fn, return_type_as=body_fn(ds(0))),
        yields=stream0,
    ).eval()

    reader = res.make_reader()
    self.assertEqual(reader.read_available(), [])
    writer0.write(ds(1))
    self.assertEqual(reader.read_available(), [1])
    writer1.write(ds(2))
    self.assertEqual(reader.read_available(), [])
    writer2.write(ds(3))
    self.assertEqual(reader.read_available(), [])
    writer0.close()
    self.assertEqual(reader.read_available(), [2])
    writer1.close()
    self.assertEqual(reader.read_available(), [3])
    self.assertEqual(reader.read_available(), [])
    writer2.close()
    self.assertIsNone(reader.read_available())

  def test_for_returns_bag(self):
    many_attrs = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, root, returns: user_facing_kd.namedtuple(
            returns=user_facing_kd.enriched_bag(
                returns,
                user_facing_kd.attr(
                    root, user_facing_kd.fstr(f'x{item:s}'), item
                ),
            ),
        ),
        returns=user_facing_kd.bag(),
        root=I.root,
    )
    root = user_facing_kd.new()
    [returns] = many_attrs.eval(
        input_seq=delayed_stream_make(*range(1, 4)), root=root
    ).read_all(timeout=1)
    self.assertEqual(
        root.updated(returns).to_py(obj_as_dict=True),
        {'x1': 1, 'x2': 2, 'x3': 3},
    )

  def test_for_yields_bag(self):
    many_attrs = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, root: user_facing_kd.namedtuple(
            yields=koda_internal_parallel.stream_make(
                user_facing_kd.attr(
                    root, user_facing_kd.fstr(f'x{item:s}'), item
                ),
            ),
        ),
        yields=delayed_stream_make(user_facing_kd.bag()),
        root=I.root,
    )
    root = user_facing_kd.new()
    self.assertEqual(
        root.updated(
            *many_attrs.eval(
                input_seq=delayed_stream_make(*range(1, 4)), root=root
            ).read_all(timeout=1)
        ).to_py(obj_as_dict=True),
        {'x1': 1, 'x2': 2, 'x3': 3},
    )

  def test_no_return_yield(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'exactly one of `returns`, `yields`, or `yields_interleaved` must'
            ' be specified'
        ),
    ):
      _ = koda_internal_parallel.stream_for(
          koda_internal_parallel.get_default_executor(),
          I.input_seq,
          lambda item: user_facing_kd.namedtuple(),
      )

  def test_return_and_yield(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'exactly one of `returns`, `yields`, or `yields_interleaved` must'
            ' be specified'
        ),
    ):
      _ = koda_internal_parallel.stream_for(
          koda_internal_parallel.get_default_executor(),
          I.input_seq,
          lambda item: user_facing_kd.namedtuple(),
          returns=1,
          yields=delayed_stream_make(),
      )

  def test_return_outside_yield_inside_body(self):
    loop_expr = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.namedtuple(
            yields=delayed_stream_make()
        ),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "unexpected variable 'yields'; the body functor must return a"
            ' namedtuple with a subset of initial variables'
        ),
    ):
      _ = loop_expr.eval(input_seq=delayed_stream_make(*range(1, 6))).read_all(
          timeout=1
      )

  def test_unknown_variable_inside_finalize(self):
    loop_expr = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.namedtuple(),
        finalize_fn=lambda **unused_kwargs: user_facing_kd.namedtuple(
            foo=1
        ),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "unexpected variable 'foo'; the finalize functor must return a"
            ' namedtuple with a subset of initial variables'
        ),
    ):
      _ = loop_expr.eval(input_seq=delayed_stream_make(*range(1, 6))).read_all(
          timeout=1
      )

  def test_wrong_type_for_variable(self):
    loop_expr = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.namedtuple(
            returns=user_facing_kd.tuple(1, 2)
        ),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "variable 'returns' has type DATA_SLICE, but the provided value has"
            ' type tuple<DATA_SLICE,DATA_SLICE>; the body functor must return a'
            ' namedtuple with a subset of initial variables'
        ),
    ):
      _ = loop_expr.eval(input_seq=delayed_stream_make(*range(1, 2))).read_all(
          timeout=1
      )

  def test_wrong_type_for_yields_interleaved(self):
    loop_expr = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.namedtuple(
            yields_interleaved=koda_internal_parallel.stream_make(
                user_facing_kd.bag()
            )
        ),
        yields_interleaved=koda_internal_parallel.stream_make(),
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "variable 'yields_interleaved' has type STREAM[DATA_SLICE], but the"
            ' provided value has type STREAM[DATA_BAG]; the body functor must'
            ' return a namedtuple with a subset of initial variables and'
            " 'yields_interleaved'"
        ),
    ):
      _ = loop_expr.eval(input_seq=delayed_stream_make(*range(1, 2))).read_all(
          timeout=1
      )

  def test_return_dataslice(self):
    loop_expr = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, **unused_kwargs: 2,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a namedtupe with a subset of initial variables, got type'
            ' DATA_SLICE; the body functor must return a namedtuple with a'
            ' subset of initial variables'
        ),
    ):
      _ = loop_expr.eval(input_seq=delayed_stream_make(*range(1, 6))).read_all(
          timeout=1
      )

  def test_non_mask_condition(self):
    loop_expr = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.namedtuple(),
        condition_fn=lambda **unused_kwargs: 1,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the condition value must be a data-item with schema MASK, got'
            ' DataItem(1, schema: INT32)'
        ),
    ):
      _ = loop_expr.eval(input_seq=delayed_stream_make(*range(1, 6))).read_all(
          timeout=1
      )

  def test_non_scalar_condition(self):
    loop_expr = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.namedtuple(),
        condition_fn=lambda **unused_kwargs: user_facing_kd.slice(
            [mask_constants.missing]
        ),
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the condition value must be a data-item with schema MASK, got'
            ' DataSlice([missing], schema: MASK, shape: JaggedShape(1),'
            ' bag_id: $'
        ),
    ):
      _ = loop_expr.eval(input_seq=delayed_stream_make(*range(1, 6))).read_all(
          timeout=1
      )

  def test_non_stream(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a stream, got stream: DATA_SLICE')
    ):
      _ = koda_internal_parallel.stream_for(
          koda_internal_parallel.get_default_executor(),
          kde.slice([1, 2, 3]),
          lambda item: user_facing_kd.namedtuple(),
          returns=1,
      )

  def test_non_data_slice_body(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected DATA_SLICE, got body_fn: namedtuple<>'),
    ):
      _ = koda_internal_parallel.stream_for(
          koda_internal_parallel.get_default_executor(),
          I.input_seq,
          kde.namedtuple(),
          returns=1,
      )

  def test_non_functor_body(self):
    loop_expr = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        57,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape('the first argument of kd.call must be a functor'),
    ):
      _ = loop_expr.eval(input_seq=delayed_stream_make(*range(1, 6))).read_all(
          timeout=1
      )

  def test_non_data_slice_condition(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected DATA_SLICE, got condition_fn: namedtuple<>'),
    ):
      _ = koda_internal_parallel.stream_for(
          koda_internal_parallel.get_default_executor(),
          I.input_seq,
          lambda item, **unused_kwargs: user_facing_kd.namedtuple(),
          condition_fn=kde.namedtuple(),
          returns=1,
      )

  def test_non_functor_condition(self):
    loop_expr = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.namedtuple(),
        condition_fn=mask_constants.present,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape('the first argument of kd.call must be a functor'),
    ):
      _ = loop_expr.eval(input_seq=delayed_stream_make(*range(1, 6))).read_all(
          timeout=1
      )

  def test_non_data_slice_finalize(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected DATA_SLICE, got finalize_fn: namedtuple<>'),
    ):
      _ = koda_internal_parallel.stream_for(
          koda_internal_parallel.get_default_executor(),
          I.input_seq,
          lambda item: user_facing_kd.namedtuple(),
          finalize_fn=kde.namedtuple(),
          returns=1,
      )

  def test_non_functor_finalize(self):
    loop_expr = koda_internal_parallel.stream_for(
        koda_internal_parallel.get_default_executor(),
        I.input_seq,
        lambda item, **unused_kwargs: user_facing_kd.namedtuple(),
        finalize_fn=mask_constants.present,
        returns=1,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape('the first argument of kd.call must be a functor'),
    ):
      _ = loop_expr.eval(input_seq=delayed_stream_make(*range(1, 6))).read_all(
          timeout=1
      )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.stream_for(
                I.executor, I.input_seq, I.body_fn, returns=I.returns
            )
        )
    )


if __name__ == '__main__':
  absltest.main()
