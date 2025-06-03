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

import math
import random
import re
import threading
import time

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.functor.parallel import clib as stream_clib
from koladata.operators import kde_operators
from koladata.operators import koda_internal_parallel
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
i32 = arolla.int32
f32 = arolla.float32
I = input_container.InputContainer('I')
M = arolla.M
kde = kde_operators.kde

py_fn = functor_factories.py_fn
expr_fn = functor_factories.expr_fn

default_executor = expr_eval.eval(koda_internal_parallel.get_default_executor())
eager_executor = expr_eval.eval(koda_internal_parallel.get_eager_executor())


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
  result, writer = stream_clib.make_stream(value_qtype)

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


STREAM_OF_DATA_SLICE = stream_make(value_type_as=ds(0)).qtype
STREAM_OF_INT32 = stream_make(value_type_as=i32(0)).qtype


class KodaInternalParallelStreamWhileLoopYieldsChainedTest(
    parameterized.TestCase
):

  def test_eval(self):
    condition_fn = expr_fn(I.n <= 3)

    def body_fn(n):
      return arolla.namedtuple(
          yields=delayed_stream_make(*range(10 * n, 11 * n)),
          n=n + 1,
      )

    res = koda_internal_parallel.stream_while_loop_yields_chained(
        default_executor,
        condition_fn,
        py_fn(
            body_fn,
            return_type_as=arolla.namedtuple(yields=stream_make(), n=ds(0)),
        ),
        yields=delayed_stream_make(-1),
        n=1,
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(
        res.read_all(timeout=1),
        [
            -1,  # initial
            *[10],  # first iteration
            *[20, 21],  # second iteration
            *[30, 31, 32],  # third iteration
        ],
    )

  def test_eval_with_async_condition(self):
    def condition_fn(n):
      return delayed_stream_make(n <= 3)

    def body_fn(n):
      return arolla.namedtuple(
          yields=delayed_stream_make(*range(10 * n, 11 * n)),
          n=n + 1,
      )

    res = koda_internal_parallel.stream_while_loop_yields_chained(
        default_executor,
        py_fn(condition_fn, return_type_as=stream_make()),
        py_fn(
            body_fn,
            return_type_as=arolla.namedtuple(yields=stream_make(), n=ds(0)),
        ),
        yields=delayed_stream_make(-1),
        n=1,
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(
        res.read_all(timeout=1),
        [
            -1,  # initial
            *[10],  # first iteration
            *[20, 21],  # second iteration
            *[30, 31, 32],  # third iteration
        ],
    )

  def test_eval_empty_initial_yields(self):
    condition_fn = expr_fn(I.n <= 3)
    body_fn = expr_fn(
        M.namedtuple.make(
            yields=koda_internal_parallel.stream_make(I.n), n=I.n + 1
        )
    )
    res = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor, condition_fn, body_fn, yields=stream_make(), n=0
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=0), [0, 1, 2, 3])

  def test_eval_body_without_yields(self):
    condition_fn = expr_fn(I.n <= 3)
    body_fn = expr_fn(M.namedtuple.make(n=I.n + 1))
    res = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor, condition_fn, body_fn, yields=stream_make(-1), n=0
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=0), [-1])

  def test_eval_complex(self):
    def condition_fn(*, fib0, fib1, n):
      del fib0, fib1
      return n > 0

    def body_fn(*, fib0, fib1, n):
      return arolla.namedtuple(
          yields=delayed_stream_make(fib1),
          fib0=fib1,
          fib1=fib1 + fib0,
          n=n - 1,
      )

    res = koda_internal_parallel.stream_while_loop_yields_chained(
        default_executor,
        py_fn(condition_fn),
        py_fn(
            body_fn, return_type_as=body_fn(fib0=i32(0), fib1=i32(1), n=ds(2))
        ),
        yields=stream_make(i32(0)),
        fib0=i32(0),
        fib1=i32(1),
        n=10,
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_INT32)
    self.assertEqual(
        res.read_all(timeout=1), [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
    )

  def test_composition(self):
    stream0, writer0 = stream_clib.make_stream(qtypes.DATA_SLICE)
    stream1, writer1 = stream_clib.make_stream(qtypes.DATA_SLICE)
    stream2, writer2 = stream_clib.make_stream(qtypes.DATA_SLICE)

    condition_fn = expr_fn(I.n < 2)

    def body_fn(*, n):
      return arolla.namedtuple(yields=[stream1, stream2][n], n=n + 1)

    res = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor,
        condition_fn,
        py_fn(body_fn, return_type_as=body_fn(n=ds(0))),
        yields=stream0,
        n=0,
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

  @parameterized.parameters(
      [
          ds(arolla.present(), schema_constants.MASK),
          ds(arolla.missing(), schema_constants.MASK),
      ],
      [
          ds(arolla.present(), schema_constants.OBJECT),
          ds(arolla.missing(), schema_constants.OBJECT),
      ],
      [
          stream_make(ds(arolla.present(), schema_constants.MASK)),
          stream_make(ds(arolla.missing(), schema_constants.MASK)),
      ],
      [
          stream_make(ds(arolla.present(), schema_constants.OBJECT)),
          stream_make(ds(arolla.missing(), schema_constants.OBJECT)),
      ],
  )
  def test_condition_supported_values(
      self, positive_condition, negative_condition
  ):
    condition_fn = expr_fn(I.condition)
    body_fn = expr_fn(
        M.namedtuple.make(
            yields=koda_internal_parallel.stream_make(1),
            condition=I.condition_1,
        )
    )
    res = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor,
        condition_fn,
        body_fn,
        yields=stream_make(0),
        condition=positive_condition,
        condition_1=negative_condition,
    ).eval()
    self.assertEqual(res.read_all(timeout=0), [0, 1])

  def test_error_condition_unsupported_values(self):
    condition_fn = expr_fn(I.condition)
    body_fn = expr_fn(M.namedtuple.make(never_happens=1))
    expr = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor,
        condition_fn,
        body_fn,
        yields=stream_make(),
        condition=I.condition,
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the condition value must be a data-item with schema MASK, got'
            ' DataItem(1, schema: INT32)'
        ),
    ):
      expr.eval(condition=1).read_all(timeout=0)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the condition functor must return a DATA_SLICE or'
            ' a STREAM[DATA_SLICE], but got FLOAT32'
        ),
    ):
      expr.eval(condition=f32(0.5)).read_all(timeout=0)

  def test_error_in_condition(self):
    def condition_fn(**unused):
      raise NotImplementedError('Boom!')

    body_fn = expr_fn(M.namedtuple.make(never_happens=1))
    expr = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor, py_fn(condition_fn), body_fn, yields=stream_make()
    )
    with self.assertRaisesRegex(NotImplementedError, re.escape('Boom!')):
      expr.eval(condition=1).read_all(timeout=0)

  def test_error_in_condition_stream(self):
    def condition_fn(**unused):
      del unused
      result, writer = stream_clib.make_stream(qtypes.DATA_SLICE)
      writer.close(NotImplementedError('Boom!'))
      return result

    body_fn = expr_fn(M.namedtuple.make(never_happens=1))
    expr = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor,
        py_fn(condition_fn, return_type_as=stream_make()),
        body_fn,
        yields=stream_make(),
    )
    with self.assertRaisesRegex(NotImplementedError, re.escape('Boom!')):
      expr.eval().read_all(timeout=0)

  def test_error_condition_stream_empty(self):
    def condition_fn(**unused):
      del unused
      return stream_make()

    body_fn = expr_fn(M.namedtuple.make(never_happens=1))
    expr = koda_internal_parallel.stream_while_loop_returns(
        eager_executor,
        py_fn(condition_fn, return_type_as=stream_make()),
        body_fn,
        returns=0,
    )
    with self.assertRaisesRegex(
        ValueError, re.escape('the condition functor returned an empty stream')
    ):
      expr.eval().read_all(timeout=0)

  def test_error_bad_condition_fn(self):
    condition_fn = ds(None)
    body_fn = expr_fn(M.namedtuple.make(never_happens=1))
    res = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor, condition_fn, body_fn, yields=stream_make()
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the first argument of kd.call must be a functor; error occurred'
            ' while calling `condition_fn`'
        ),
    ):
      res.read_all(timeout=0)

  def test_error_body_wrong_result_type(self):
    condition_fn = expr_fn(I.n == 0)
    body_fn = expr_fn(I.n + 1)
    res = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor, condition_fn, body_fn, yields=stream_make(), n=0
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a namedtupe with a subset of initial variables, got type'
            ' DATA_SLICE; the body functor must return a namedtuple with'
            " a subset of initial variables and 'yields'"
        ),
    ):
      res.read_all(timeout=0)

  def test_error_body_unknown_field_in_result(self):
    condition_fn = expr_fn(I.n == 0)
    body_fn = expr_fn(M.namedtuple.make(x=1))
    res = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor, condition_fn, body_fn, yields=stream_make(), n=0
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "unexpected variable 'x'; the body functor must return a namedtuple"
            " with a subset of initial variables and 'yields'"
        ),
    ):
      res.read_all(timeout=0)

  def test_error_body_wrong_field_type_in_result(self):
    condition_fn = expr_fn(I.n == 0)
    body_fn = expr_fn(M.namedtuple.make(n=i32(1)))
    res = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor, condition_fn, body_fn, yields=stream_make(), n=0
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "variable 'n' has type DATA_SLICE, but the provided value has"
            ' type INT32; the body functor must return a namedtuple with a'
            " subset of initial variables and 'yields'"
        ),
    ):
      res.read_all(timeout=0)

  def test_error_in_body(self):
    def body_fn(**unused):
      raise NotImplementedError('Boom!')

    condition_fn = expr_fn(I.n == 0)
    expr = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor,
        condition_fn,
        py_fn(body_fn, return_type_as=stream_make()),
        yields=stream_make(),
        n=0,
    )
    with self.assertRaisesRegex(NotImplementedError, re.escape('Boom!')):
      expr.eval().read_all(timeout=0)

  def test_error_bad_body_fn(self):
    condition_fn = expr_fn(I.n == 0)
    body_fn = ds(None)
    res = koda_internal_parallel.stream_while_loop_yields_chained(
        eager_executor, condition_fn, body_fn, yields=stream_make(), n=0
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the first argument of kd.call must be a functor; error occurred'
            ' while calling `body_fn`'
        ),
    ):
      res.read_all(timeout=0)

  @arolla.abc.add_default_cancellation_context
  def test_cancellation_in_condition(self):
    condition_fn = expr_fn(M.core._identity_with_cancel(I.n == 0))
    body_fn = expr_fn(M.namedtuple.make(n=I.n + 1))
    with self.assertRaisesRegex(ValueError, re.escape('[CANCELLED]')):
      koda_internal_parallel.stream_while_loop_yields_chained(
          default_executor, condition_fn, body_fn, yields=stream_make(), n=0
      ).eval().read_all(timeout=1)

  @arolla.abc.add_default_cancellation_context
  def test_cancellation_in_body(self):
    condition_fn = expr_fn(I.n == 0)
    body_fn = expr_fn(
        M.core._identity_with_cancel(M.namedtuple.make(n=I.n + 1))
    )
    with self.assertRaisesRegex(ValueError, re.escape('[CANCELLED]')):
      koda_internal_parallel.stream_while_loop_yields_chained(
          default_executor, condition_fn, body_fn, yields=stream_make(), n=0
      ).eval().read_all(timeout=1)

  def test_non_determinism(self):
    stream_1, stream_2 = expr_eval.eval(
        (
            koda_internal_parallel.stream_while_loop_yields_chained(
                I.executor, I.condition_fn, I.body_fn, yields=I.yields
            ),
            koda_internal_parallel.stream_while_loop_yields_chained(
                I.executor, I.condition_fn, I.body_fn, yields=I.yields
            ),
        ),
        executor=eager_executor,
        condition_fn=expr_fn(arolla.literal(ds(arolla.missing()))),
        body_fn=expr_fn(M.namedtuple.make()),
        yields=stream_make(),
    )
    self.assertNotEqual(stream_1.fingerprint, stream_2.fingerprint)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.stream_while_loop_yields_chained(
                I.executor, I.condition_fn, I.body_fn, yields=I.yields, n=I.n
            )
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            koda_internal_parallel.stream_while_loop_yields_chained(
                I.executor, I.condition_fn, I.body_fn, yields=I.yields, n=I.n
            )
        ),
        'koda_internal.parallel.stream_while_loop_yields_chained(I.executor,'
        ' I.condition_fn, I.body_fn, yields=I.yields, n=I.n)',
    )


if __name__ == '__main__':
  absltest.main()
