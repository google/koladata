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


STREAM_OF_DATA_SLICE = stream_make(value_type_as=ds(0)).qtype
STREAM_OF_INT32 = stream_make(value_type_as=i32(0)).qtype


class KodaInternalParallelStreamWhileLoopReturnsTest(parameterized.TestCase):

  def test_eval(self):
    condition_fn = expr_fn(I.n <= 3)
    body_fn = expr_fn(M.namedtuple.make(returns=I.returns + I.n, n=I.n + 1))
    res = koda_internal_parallel.stream_while_loop_returns(
        eager_executor, condition_fn, body_fn, returns=100, n=1
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.read_all(timeout=0), [100 + 1 + 2 + 3])

  def test_eval_with_async_condition(self):
    def async_condition_fn(n, **unused):
      del unused
      result, writer = stream_clib.make_stream(qtypes.DATA_SLICE)

      def impl():
        try:
          time.sleep(0.01)
          writer.write(ds(n <= 3))
          writer.close()
        except Exception as e:  # pylint: disable=broad-exception-caught
          writer.close(e)

      default_executor.schedule(impl)
      return result

    body_fn = expr_fn(M.namedtuple.make(returns=I.returns + I.n, n=I.n + 1))
    res = koda_internal_parallel.stream_while_loop_returns(
        default_executor,
        py_fn(async_condition_fn, return_type_as=stream_make()),
        body_fn,
        returns=100,
        n=1,
    ).eval()
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_DATA_SLICE)
    self.assertEqual(res.make_reader().read_available(), [])
    self.assertEqual(res.read_all(timeout=1.0), [100 + 1 + 2 + 3])

  @parameterized.parameters(
      (0, 0),
      (1, 1),
      (10, 55),
      (20, 6765),
      (40, 102334155),
  )
  def test_complex_eval(self, n, expected_result):
    condition_fn = expr_fn(I.n > 0)
    body_fn = expr_fn(
        M.namedtuple.make(
            returns=I.fib1,
            fib1=M.math.add(I.fib1, I.fib0),
            fib0=I.fib1,
            n=I.n - 1,
        )
    )
    res = koda_internal_parallel.stream_while_loop_returns(
        default_executor,
        condition_fn,
        body_fn,
        returns=i32(0),
        fib0=i32(0),
        fib1=i32(1),
        n=I.n,
    ).eval(n=n)
    self.assertIsInstance(res, stream_clib.Stream)
    self.assertEqual(res.qtype, STREAM_OF_INT32)
    self.assertEqual(res.read_all(timeout=None), [expected_result])

  def test_functor_args_kwags(self):
    def condition_fn(*args, **kwargs):
      self.assertEmpty(args)
      self.assertEqual(set(kwargs.keys()), {'fib0', 'fib1', 'n', 'returns'})
      return kwargs['n'] > 0

    def body_fn(*args, **kwargs):
      self.assertEmpty(args)
      self.assertEqual(set(kwargs.keys()), {'fib0', 'fib1', 'n', 'returns'})
      return arolla.namedtuple(
          returns=kwargs['fib1'],
          fib0=kwargs['fib1'],
          fib1=kwargs['fib1'] + kwargs['fib0'],
          n=kwargs['n'] - 1,
      )

    res = koda_internal_parallel.stream_while_loop_returns(
        eager_executor,
        py_fn(condition_fn),
        py_fn(
            body_fn,
            return_type_as=arolla.namedtuple(
                returns=ds(0), fib0=ds(0), fib1=ds(0), n=ds(0)
            ),
        ),
        returns=0,
        fib0=0,
        fib1=1,
        n=10,
    ).eval()
    self.assertEqual(res.read_all(timeout=0), [55])

  @parameterized.parameters(
      ds(arolla.present(), schema_constants.MASK),
      ds(arolla.present(), schema_constants.OBJECT),
  )
  def test_condition_positive(self, condition):
    condition_fn = expr_fn(I.condition)
    body_fn = expr_fn(
        M.namedtuple.make(returns=ds(1), condition=ds(arolla.missing()))
    )
    res = koda_internal_parallel.stream_while_loop_returns(
        eager_executor, condition_fn, body_fn, returns=0, condition=I.condition
    ).eval(condition=condition)
    self.assertEqual(res.read_all(timeout=0), [1])

  @parameterized.parameters(
      ds(arolla.missing(), schema_constants.MASK),
      ds(arolla.missing(), schema_constants.OBJECT),
  )
  def test_condition_negative(self, condition):
    condition_fn = expr_fn(I.condition)
    body_fn = expr_fn(
        M.namedtuple.make(returns=ds(1), condition=ds(arolla.missing()))
    )
    res = koda_internal_parallel.stream_while_loop_returns(
        eager_executor, condition_fn, body_fn, returns=0, condition=I.condition
    ).eval(condition=condition)
    self.assertEqual(res.read_all(timeout=0), [0])

  def test_error_condition_unsupported_result(self):
    condition_fn = expr_fn(I.condition)
    body_fn = expr_fn(
        M.namedtuple.make(returns=ds(1), condition=ds(arolla.missing()))
    )
    expr = koda_internal_parallel.stream_while_loop_returns(
        eager_executor, condition_fn, body_fn, returns=0, condition=I.condition
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

    body_fn = expr_fn(
        M.namedtuple.make(returns=ds(1), condition=ds(arolla.missing()))
    )
    expr = koda_internal_parallel.stream_while_loop_returns(
        eager_executor, py_fn(condition_fn), body_fn, returns=0
    )
    with self.assertRaisesRegex(NotImplementedError, re.escape('Boom!')):
      expr.eval(condition=1).read_all(timeout=0)

  def test_error_in_condition_stream(self):
    def condition_fn(**unused):
      del unused
      result, writer = stream_clib.make_stream(qtypes.DATA_SLICE)
      writer.close(NotImplementedError('Boom!'))
      return result

    body_fn = expr_fn(
        M.namedtuple.make(returns=ds(1), condition=ds(arolla.missing()))
    )
    expr = koda_internal_parallel.stream_while_loop_returns(
        eager_executor,
        py_fn(condition_fn, return_type_as=stream_make()),
        body_fn,
        returns=0,
    )
    with self.assertRaisesRegex(NotImplementedError, re.escape('Boom!')):
      expr.eval().read_all(timeout=0)

  def test_error_condition_stream_empty(self):
    def condition_fn(**unused):
      del unused
      result, writer = stream_clib.make_stream(qtypes.DATA_SLICE)
      writer.close()
      return result

    body_fn = expr_fn(
        M.namedtuple.make(returns=ds(1), condition=ds(arolla.missing()))
    )
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
    body_fn = expr_fn(M.namedtuple.make(returns=I.returns + I.n, n=I.n + 1))
    res = koda_internal_parallel.stream_while_loop_returns(
        eager_executor, condition_fn, body_fn, returns=100, n=1
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
    condition_fn = expr_fn(I.returns <= 3)
    body_fn = expr_fn(I.returns + 1)
    res = koda_internal_parallel.stream_while_loop_returns(
        eager_executor, condition_fn, body_fn, returns=0
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a namedtupe with a subset of initial variables, got type'
            ' DATA_SLICE; the body functor must return a namedtuple with'
            ' a subset of initial variables'
        ),
    ):
      res.read_all(timeout=0)

  def test_error_body_unknown_field_in_result(self):
    condition_fn = expr_fn(I.returns <= 3)
    body_fn = expr_fn(M.namedtuple.make(x=1))
    res = koda_internal_parallel.stream_while_loop_returns(
        eager_executor, condition_fn, body_fn, returns=0
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "unexpected variable 'x'; the body functor must return a namedtuple"
            ' with a subset of initial variables'
        ),
    ):
      res.read_all(timeout=0)

  def test_error_body_wrong_field_type_in_result(self):
    condition_fn = expr_fn(I.returns <= 3)
    body_fn = expr_fn(M.namedtuple.make(returns=1))
    res = koda_internal_parallel.stream_while_loop_returns(
        eager_executor, condition_fn, body_fn, returns=0
    ).eval()  # no error
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "variable 'returns' has type DATA_SLICE, but the provided value has"
            ' type INT32; the body functor must return a namedtuple with a'
            ' subset of initial variables'
        ),
    ):
      res.read_all(timeout=0)

  def test_error_in_body(self):
    def body_fn(**unused):
      raise NotImplementedError('Boom!')

    condition_fn = expr_fn(I.returns <= 3)
    expr = koda_internal_parallel.stream_while_loop_returns(
        eager_executor, condition_fn, py_fn(body_fn), returns=0
    )
    with self.assertRaisesRegex(NotImplementedError, re.escape('Boom!')):
      expr.eval().read_all(timeout=0)

  def test_error_bad_body_fn(self):
    condition_fn = expr_fn(I.n <= 3)
    body_fn = ds(None)
    res = koda_internal_parallel.stream_while_loop_returns(
        eager_executor, condition_fn, body_fn, returns=100, n=1
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
    condition_fn = expr_fn(M.core._identity_with_cancel(I.n <= 3))
    body_fn = expr_fn(M.namedtuple.make(returns=I.returns + I.n, n=I.n + 1))
    res = koda_internal_parallel.stream_while_loop_returns(
        default_executor, condition_fn, body_fn, returns=100, n=1
    ).eval()  # no error
    with self.assertRaisesRegex(ValueError, re.escape('[CANCELLED]')):
      res.read_all(timeout=1)

  @arolla.abc.add_default_cancellation_context
  def test_cancellation_in_body(self):
    condition_fn = expr_fn(I.n <= 3)
    body_fn = expr_fn(
        M.core._identity_with_cancel(
            M.namedtuple.make(returns=I.returns + I.n, n=I.n + 1)
        )
    )
    res = koda_internal_parallel.stream_while_loop_returns(
        default_executor, condition_fn, body_fn, returns=100, n=1
    ).eval()  # no error
    with self.assertRaisesRegex(ValueError, re.escape('[CANCELLED]')):
      res.read_all(timeout=1)

  def test_non_determinism(self):
    stream_1, stream_2 = expr_eval.eval(
        (
            koda_internal_parallel.stream_while_loop_returns(
                I.executor, I.condition_fn, I.body_fn, returns=I.returns
            ),
            koda_internal_parallel.stream_while_loop_returns(
                I.executor, I.condition_fn, I.body_fn, returns=I.returns
            ),
        ),
        executor=eager_executor,
        condition_fn=expr_fn(arolla.literal(ds(arolla.missing()))),
        body_fn=expr_fn(M.namedtuple.make()),
        returns=1,
    )
    self.assertNotEqual(stream_1.fingerprint, stream_2.fingerprint)

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            koda_internal_parallel.stream_while_loop_returns(
                I.executor, I.condition_fn, I.body_fn, returns=I.returns, n=I.n
            )
        )
    )

  def test_repr(self):
    self.assertEqual(
        repr(
            koda_internal_parallel.stream_while_loop_returns(
                I.executor, I.condition_fn, I.body_fn, returns=I.returns, n=I.n
            )
        ),
        'koda_internal.parallel.stream_while_loop_returns(I.executor,'
        ' I.condition_fn, I.body_fn, returns=I.returns, n=I.n)',
    )


if __name__ == '__main__':
  absltest.main()
