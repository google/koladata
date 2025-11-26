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
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.functor import boxing as _
from koladata.functor import functor_factories
from koladata.functor.parallel import clib as stream_clib
from koladata.operators import kde_operators
from koladata.operators import optools
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
kde_internal = kde_operators.internal

py_fn = functor_factories.py_fn
expr_fn = functor_factories.expr_fn

default_executor = expr_eval.eval(kde_internal.parallel.get_default_executor())
eager_executor = expr_eval.eval(kde_internal.parallel.get_eager_executor())


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


# An adapter for testing an internal operator.
def _stream_while_yields(executor, condition_fn, body_fn, yields, **state):
  return kde_internal.parallel._stream_while_yields(
      executor,
      py_boxing.as_qvalue_or_expr(condition_fn),
      py_boxing.as_qvalue_or_expr(body_fn),
      arolla.text('yields'),
      py_boxing.as_qvalue_or_expr(yields),
      M.namedtuple.make(
          **{k: py_boxing.as_qvalue_or_expr(v) for k, v in state.items()}
      ),
      optools.unified_non_deterministic_arg(),
  )


STREAM_OF_DATA_SLICE = stream_make(value_type_as=ds(0)).qtype
STREAM_OF_INT32 = stream_make(value_type_as=i32(0)).qtype


class KodaInternalParallelStreamWhileLoopYieldsChainedTest(
    parameterized.TestCase
):

  def test_eval(self):
    condition_fn = expr_fn(I.n <= 3)

    def body_fn(n):
      return arolla.namedtuple(yields=10 * n, n=n + 1)

    res = expr_eval.eval(
        _stream_while_yields(
            default_executor,
            condition_fn,
            py_fn(body_fn, return_type_as=body_fn(n=ds(0))),
            yields=-1,
            n=1,
        )
    )
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=1), [-1, 10, 20, 30])

  def test_eval_with_async_condition(self):
    def condition_fn(n):
      return delayed_stream_make(n <= 3)

    def body_fn(n):
      return arolla.namedtuple(yields=10 * n, n=n + 1)

    res = expr_eval.eval(
        _stream_while_yields(
            default_executor,
            py_fn(condition_fn, return_type_as=stream_make()),
            py_fn(body_fn, return_type_as=body_fn(n=ds(0))),
            yields=-1,
            n=1,
        )
    )
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=1), [-1, 10, 20, 30])

  def test_eval_empty_initial_yields(self):
    condition_fn = expr_fn(I.n <= 3)
    body_fn = expr_fn(M.namedtuple.make(yields=I.n, n=I.n + 1))
    res = expr_eval.eval(
        _stream_while_yields(
            eager_executor, condition_fn, body_fn, yields=-1, n=0
        )
    )
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=0), [-1, 0, 1, 2, 3])

  def test_eval_body_without_yields(self):
    condition_fn = expr_fn(I.n <= 3)
    body_fn = expr_fn(M.namedtuple.make(n=I.n + 1))
    res = expr_eval.eval(
        _stream_while_yields(
            eager_executor, condition_fn, body_fn, yields=-1, n=0
        )
    )
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=0), [-1])

  def test_eval_complex(self):
    def condition_fn(*, fib0, fib1, n):
      del fib0, fib1
      return delayed_stream_make(n > 0)

    def body_fn(*, fib0, fib1, n):
      return arolla.namedtuple(
          yields=fib1,
          fib0=fib1,
          fib1=fib1 + fib0,
          n=n - 1,
      )

    res = expr_eval.eval(
        _stream_while_yields(
            default_executor,
            py_fn(
                condition_fn,
                return_type_as=condition_fn(fib0=i32(0), fib1=i32(1), n=ds(2)),
            ),
            py_fn(
                body_fn,
                return_type_as=body_fn(fib0=i32(0), fib1=i32(1), n=ds(2)),
            ),
            yields=i32(0),
            fib0=i32(0),
            fib1=i32(1),
            n=10,
        )
    )
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_INT32)
    self.assertEqual(
        res.read_all(timeout=1), [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55]
    )

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
    body_fn = expr_fn(M.namedtuple.make(yields=ds(1), condition=I.condition_1))
    res = expr_eval.eval(
        _stream_while_yields(
            eager_executor,
            condition_fn,
            body_fn,
            yields=ds(0),
            condition=positive_condition,
            condition_1=negative_condition,
        )
    )
    self.assertEqual(res.read_all(timeout=0), [0, 1])

  def test_error_condition_unsupported_values(self):
    condition_fn = expr_fn(I.condition)
    body_fn = expr_fn(M.namedtuple.make(never_happens=1))
    expr = _stream_while_yields(
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
      expr_eval.eval(expr, condition=1).read_all(timeout=0)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the condition functor must return a DATA_SLICE or'
            ' a STREAM[DATA_SLICE], but got FLOAT32'
        ),
    ):
      expr_eval.eval(expr, condition=f32(0.5)).read_all(timeout=0)

  def test_error_in_condition(self):
    def condition_fn(**unused):
      raise NotImplementedError('Boom!')

    body_fn = expr_fn(M.namedtuple.make(never_happens=1))
    expr = _stream_while_yields(
        eager_executor, py_fn(condition_fn), body_fn, yields=stream_make()
    )
    with self.assertRaisesRegex(NotImplementedError, re.escape('Boom!')):
      expr_eval.eval(expr, condition=1).read_all(timeout=0)

  def test_error_in_condition_stream(self):
    def condition_fn(**unused):
      del unused
      result, writer = stream_clib.Stream.new(qtypes.DATA_SLICE)
      writer.close(NotImplementedError('Boom!'))
      return result

    body_fn = expr_fn(M.namedtuple.make(never_happens=1))
    expr = _stream_while_yields(
        eager_executor,
        py_fn(condition_fn, return_type_as=stream_make()),
        body_fn,
        yields=stream_make(),
    )
    with self.assertRaisesRegex(NotImplementedError, re.escape('Boom!')):
      expr_eval.eval(expr).read_all(timeout=0)

  def test_error_condition_stream_empty(self):
    def condition_fn(**unused):
      del unused
      return stream_make()

    body_fn = expr_fn(M.namedtuple.make(never_happens=1))
    expr = expr_eval.eval(
        _stream_while_yields(
            eager_executor,
            py_fn(condition_fn, return_type_as=condition_fn()),
            body_fn,
            yields=0,
        )
    )
    with self.assertRaisesRegex(
        ValueError, re.escape('the condition functor returned an empty stream')
    ):
      expr_eval.eval(expr).read_all(timeout=0)

  def test_error_bad_condition_fn(self):
    condition_fn = ds(None)
    body_fn = expr_fn(M.namedtuple.make(never_happens=1))
    res = expr_eval.eval(
        _stream_while_yields(eager_executor, condition_fn, body_fn, yields=0)
    )  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the first argument of kd.call must be a functor, got'
            ' DataItem(None, schema: NONE); error occurred while calling'
            ' `condition_fn`'
        ),
    ):
      res.read_all(timeout=0)

  def test_error_body_wrong_result_type(self):
    condition_fn = expr_fn(I.n == 0)
    body_fn = expr_fn(I.n + 1)
    res = expr_eval.eval(
        _stream_while_yields(
            eager_executor, condition_fn, body_fn, yields=stream_make(), n=0
        )
    )  # no error
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
    res = expr_eval.eval(
        _stream_while_yields(
            eager_executor, condition_fn, body_fn, yields=stream_make(), n=0
        )
    )  # no error
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
    res = expr_eval.eval(
        _stream_while_yields(
            eager_executor, condition_fn, body_fn, yields=stream_make(), n=0
        )
    )  # no error
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
    expr = expr_eval.eval(
        _stream_while_yields(
            eager_executor,
            condition_fn,
            py_fn(body_fn, return_type_as=stream_make()),
            yields=stream_make(),
            n=0,
        )
    )
    with self.assertRaisesRegex(NotImplementedError, re.escape('Boom!')):
      expr.read_all(timeout=0)

  def test_error_bad_body_fn(self):
    condition_fn = expr_fn(I.n == 0)
    body_fn = ds(None)
    res = expr_eval.eval(
        _stream_while_yields(
            eager_executor, condition_fn, body_fn, yields=stream_make(), n=0
        )
    )  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the first argument of kd.call must be a functor, got'
            ' DataItem(None, schema: NONE); error occurred while calling'
            ' `body_fn`'
        ),
    ):
      res.read_all(timeout=0)

  @arolla.abc.add_default_cancellation_context
  def test_cancellation_in_condition(self):
    condition_fn = expr_fn(M.core._identity_with_cancel(I.n == 0))
    body_fn = expr_fn(M.namedtuple.make(n=I.n + 1))
    with self.assertRaisesRegex(ValueError, re.escape('[CANCELLED]')):
      expr_eval.eval(
          _stream_while_yields(
              default_executor, condition_fn, body_fn, yields=stream_make(), n=0
          )
      ).read_all(timeout=1)

  @arolla.abc.add_default_cancellation_context
  def test_cancellation_in_body(self):
    condition_fn = expr_fn(I.n == 0)
    body_fn = expr_fn(
        M.core._identity_with_cancel(M.namedtuple.make(n=I.n + 1))
    )
    with self.assertRaisesRegex(ValueError, re.escape('[CANCELLED]')):
      expr_eval.eval(
          _stream_while_yields(
              default_executor, condition_fn, body_fn, yields=stream_make(), n=0
          )
      ).read_all(timeout=1)

  def test_non_determinism(self):
    stream_1, stream_2 = expr_eval.eval(
        (
            _stream_while_yields(
                I.executor, I.condition_fn, I.body_fn, yields=I.yields
            ),
            _stream_while_yields(
                I.executor, I.condition_fn, I.body_fn, yields=I.yields
            ),
        ),
        executor=eager_executor,
        condition_fn=expr_fn(arolla.literal(ds(arolla.missing()))),
        body_fn=expr_fn(M.namedtuple.make()),
        yields=stream_make(),
    )
    self.assertNotEqual(stream_1.fingerprint, stream_2.fingerprint)


if __name__ == '__main__':
  absltest.main()
