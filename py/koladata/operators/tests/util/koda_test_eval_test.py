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

"""Tests for koda_test_eval."""

import re
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import error_pb2
from koladata.exceptions import exceptions
from koladata.operators import eager_op_utils
from koladata.operators.tests.util import data_conversion
from koladata.operators.tests.util import koda_test_eval
from koladata.types import data_slice


M = arolla.M
L = arolla.L
P = arolla.P
kd = eager_op_utils.operators_container('kde')


@arolla.optools.add_to_registry()
@arolla.optools.as_py_function_operator(
    'kde.add_fake_for_test', qtype_inference_expr=P.x
)
def add_fake_for_test(x, y):
  if x.get_ndim() == 0 and x > 2**30:
    raise exceptions.KodaError(error_pb2.Error(error_message='fake error'))
  return data_slice.DataSlice.from_vals(
      x.as_arolla_value() + y.as_arolla_value()
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_py_function_operator(
    'kde.agg_sum_fake_for_test', qtype_inference_expr=P.x
)
def agg_sum_fake_for_test(x):
  flat_res = arolla.abc.invoke_op(
      M.math.sum,
      (x.as_arolla_value(), x.get_shape()[-1]),
  )
  return data_slice.DataSlice.from_vals(flat_res).reshape(x.get_shape()[:-1])


@arolla.optools.add_to_registry()
@arolla.optools.as_py_function_operator(
    'kde.cum_count_fake_for_test', qtype_inference_expr=P.x
)
def cum_count_fake_for_test(x):
  flat_res = arolla.abc.invoke_op(
      M.array.cum_count,
      (x.as_arolla_value(), x.get_shape()[-1]),
  )
  return data_slice.DataSlice.from_vals(flat_res).reshape(x.get_shape())


FLAGS = flags.FLAGS
FLAGS.extra_flags = [
    'kd_op_mapping:math.add:add_fake_for_test',
    'kd_op_mapping:math.sum:agg_sum_fake_for_test',
    'kd_op_mapping:array.cum_count:cum_count_fake_for_test',
]


@arolla.optools.add_to_registry()
@arolla.optools.as_py_function_operator(
    'arolla_simple_add_for_test',
    qtype_inference_expr=P.x,
)
def arolla_simple_add_for_test(x, y=arolla.unspecified()):
  if y.qtype == arolla.UNSPECIFIED:
    return x
  return x + y


class KodaTestEvalTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    koda_test_eval._get_kd_op_mapping.cache_clear()
    koda_test_eval._get_eager_koda_op.cache_clear()

  def test_eval_simple(self):
    expr = M.math.add(L.x, L.y)
    x = arolla.dense_array([1, 2, 3])
    y = arolla.dense_array([4, 5, 6])
    arolla.testing.assert_qvalue_allequal(
        koda_test_eval.eager_eval(expr, x=x, y=y), arolla.eval(expr, x=x, y=y)
    )

  def test_leaf(self):
    expr = L.x
    x = arolla.dense_array([1, 2, 3])
    arolla.testing.assert_qvalue_allequal(
        koda_test_eval.eager_eval(expr, x=x), arolla.eval(expr, x=x)
    )

  def test_literal(self):
    literal = arolla.literal(arolla.dense_array([1, 2, 3]))
    arolla.testing.assert_qvalue_allequal(
        koda_test_eval.eager_eval(literal), arolla.eval(literal)
    )

  def test_placeholder(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'placeholders are not supported'
    ):
      _ = koda_test_eval.eager_eval(
          M.annotation.qtype(arolla.P.x, arolla.FLOAT32)
      )

  def test_missing_annotated_input(self):
    with self.assertRaisesWithLiteralMatch(ValueError, 'missing input for L.x'):
      _ = koda_test_eval.eager_eval(M.annotation.qtype(L.x, arolla.FLOAT32))

  def test_annotations_are_stripped(self):
    expr = M.math.add(
        M.annotation.qtype(L.x, arolla.DENSE_ARRAY_INT32),
        M.annotation.qtype(L.y, arolla.DENSE_ARRAY_INT32),
    )
    x = arolla.dense_array([1, 2, 3])
    y = arolla.dense_array([4, 5, 6])
    arolla.testing.assert_qvalue_allequal(
        koda_test_eval.eager_eval(expr, x=x, y=y), arolla.eval(expr, x=x, y=y)
    )

  def test_non_expr_input(self):
    non_expr = arolla.dense_array([1, 2, 3])
    arolla.testing.assert_qvalue_allequal(
        koda_test_eval.eager_eval(non_expr), arolla.eval(non_expr)
    )

  @mock.patch.object(arolla, 'as_qvalue', wraps=arolla.dense_array)
  def test_non_qvalue_input(self, _):
    arolla.testing.assert_qvalue_allequal(
        koda_test_eval.eager_eval(L.x, x=[1, 2, 3]),
        arolla.dense_array([1, 2, 3]),
    )

  @mock.patch.object(arolla, 'eval', autospec=True)
  def test_arolla_eval_not_called(self, arolla_eval_mock):
    expr = M.math.add(
        arolla.dense_array([1, 2, 3]), arolla.dense_array([4, 5, 6])
    )
    _ = koda_test_eval.eager_eval(expr)
    arolla_eval_mock.assert_not_called()

  def test_koda_error_is_converted_to_arolla_error(self):
    expr = M.math.add(L.x, L.y)
    x = arolla.optional_int32(2**30 + 1)
    y = arolla.optional_int32(4)

    with self.assertRaisesRegex(ValueError, 'fake error'):
      _ = koda_test_eval.eager_eval(expr, x=x, y=y)

  def test_op_translation_is_used(self):
    expr = M.math.add(L.x, L.y)
    x = arolla.dense_array([1, 2, 3])
    y = arolla.dense_array([4, 5, 6])
    mock_add = mock.MagicMock(wraps=lambda x, y: kd.add(kd.add(x, y), x))
    with mock.patch.object(
        koda_test_eval, '_get_eager_koda_op', return_value=mock_add
    ):
      res = koda_test_eval.eager_eval(expr, x=x, y=y)
    arolla.testing.assert_qvalue_allequal(res, x + y + x)
    mock_add.assert_called_once()
    (x_arg, y_arg), _ = mock_add.call_args
    self.assertIsInstance(x_arg, data_slice.DataSlice)
    self.assertIsInstance(y_arg, data_slice.DataSlice)
    arolla.testing.assert_qvalue_allequal(x_arg.as_dense_array(), x)
    arolla.testing.assert_qvalue_allequal(y_arg.as_dense_array(), y)

  def test_kd_op_mapping_flag(self):
    expr = (L.x - L.y) * L.x
    x = arolla.dense_array([1, 2, 3])
    y = arolla.dense_array([4, 5, 6])
    with flagsaver.flagsaver(
        extra_flags=[
            'kd_op_mapping:math.subtract:add',
            'kd_op_mapping:math.multiply:add',
        ]
    ):
      arolla.testing.assert_qvalue_allequal(
          koda_test_eval.eager_eval(expr, x=x, y=y), x + y + x
      )

  def test_kd_op_mapping_duplicate_arolla_op(self):
    expr = M.math.add(
        arolla.dense_array([1, 2, 3]), arolla.dense_array([4, 5, 6])
    )
    with flagsaver.flagsaver(
        extra_flags=['kd_op_mapping:math.add:add', 'kd_op_mapping:math.add:add']
    ):
      with self.assertRaisesWithLiteralMatch(
          ValueError, "duplicate arolla_op='math.add' found in `kd_op_mapping`"
      ):
        _ = koda_test_eval.eager_eval(expr)

  def test_kd_op_mapping_invalid_form(self):
    expr = M.math.add(
        arolla.dense_array([1, 2, 3]), arolla.dense_array([4, 5, 6])
    )
    with flagsaver.flagsaver(extra_flags=['kd_op_mapping:math.add::add']):
      with self.assertRaisesRegex(
          ValueError, "invalid `kd_op_mapping` value: 'math.add::add'"
      ):
        _ = koda_test_eval.eager_eval(expr)

  def test_missing_output_qtype(self):
    with self.assertRaisesRegex(
        ValueError, 'the output qtype could not be determined'
    ):
      _ = koda_test_eval.eager_eval(L.x)

  def test_inconsistent_qtype_annotations(self):
    with self.assertRaisesRegex(
        Exception,
        re.escape('inconsistent annotation.qtype(expr: INT32, qtype=FLOAT32)'),
    ):
      _ = koda_test_eval.eager_eval(
          M.annotation.qtype(L.x, arolla.FLOAT32), x=1
      )

  def test_output_qtype_casting(self):
    """Tests that the output qtype is used to cast the output."""
    x = arolla.optional_int32(1)
    y = arolla.optional_int32(2)
    implicit_koda_output = data_conversion.arolla_from_koda(
        kd.add(
            data_conversion.koda_from_arolla(x),
            data_conversion.koda_from_arolla(y),
        )
    )
    # _Not_ optional.
    arolla.testing.assert_qvalue_allequal(implicit_koda_output, arolla.int32(3))
    # Optional.
    arolla.testing.assert_qvalue_allequal(
        koda_test_eval.eager_eval(M.math.add(x, y)), arolla.optional_int32(3)
    )

  @parameterized.parameters(
      (arolla.array([1, 2, 3]), arolla.types.ArrayToScalarEdge(3)),
      (arolla.dense_array([1, 2, 3]), arolla.types.DenseArrayToScalarEdge(3)),
      (arolla.array([1, 2, 3]), arolla.types.ArrayEdge.from_sizes([2, 0, 1])),
      (
          arolla.dense_array([1, 2, 3]),
          arolla.types.DenseArrayEdge.from_sizes([2, 0, 1]),
      ),
      (arolla.array([1, 2, 3]), arolla.unspecified()),
      (arolla.dense_array([1, 2, 3]), arolla.unspecified()),
  )
  def test_eval_agg_into_op(self, x, into_edge):
    expr = M.math.sum(L.x, into=into_edge)
    arolla.testing.assert_qvalue_allequal(
        koda_test_eval.eager_eval(expr, x=x), arolla.eval(expr, x=x)
    )

  @parameterized.parameters(
      (arolla.array([1, 2, 3]), arolla.types.ArrayToScalarEdge(3)),
      (arolla.dense_array([1, 2, 3]), arolla.types.DenseArrayToScalarEdge(3)),
      (arolla.array([1, 2, 3]), arolla.types.ArrayEdge.from_sizes([2, 0, 1])),
      (
          arolla.dense_array([1, 2, 3]),
          arolla.types.DenseArrayEdge.from_sizes([2, 0, 1]),
      ),
      (arolla.array([1, 2, 3]), arolla.unspecified()),
      (arolla.dense_array([1, 2, 3]), arolla.unspecified()),
  )
  def test_eval_agg_over_op(self, x, over_edge):
    expr = M.array.cum_count(L.x, over=over_edge)
    arolla.testing.assert_qvalue_allequal(
        koda_test_eval.eager_eval(expr, x=x), arolla.eval(expr, x=x)
    )

  def test_eval_agg_over_op_unexpected_rank(self):

    @arolla.optools.add_to_registry()
    @arolla.optools.as_lambda_operator('kde.cum_count_incorrect_impl')
    def cum_count_incorrect_impl(x):
      return agg_sum_fake_for_test(x)

    x = arolla.array([1, 2, 3])
    over_edge = arolla.types.ArrayToScalarEdge(3)
    expr = M.array.cum_count(L.x, over=over_edge)
    with flagsaver.flagsaver(
        extra_flags=['kd_op_mapping:array.cum_count:cum_count_incorrect_impl']
    ):
      with self.assertRaisesRegex(
          ValueError, 'expected the output to have the same rank as the input'
      ):
        koda_test_eval.eager_eval(expr, x=x)

  def test_args_for_reshape_with_over(self):

    @arolla.optools.add_to_registry()
    @arolla.optools.as_py_function_operator(
        'kde.fake_ordinal_rank', qtype_inference_expr=P.x
    )
    def fake_ordinal_rank(x, tie_breaker, descending):
      if tie_breaker.qtype != arolla.UNSPECIFIED:
        tie_breaker = tie_breaker.as_arolla_value()
      flat_res = arolla.abc.invoke_op(
          M.array.ordinal_rank,
          (
              x.as_arolla_value(),
              tie_breaker,
              x.get_shape()[-1],
              descending.as_arolla_value(),
          ),
      )
      return data_slice.DataSlice.from_vals(flat_res).reshape(x.get_shape())

    x = arolla.array([1, 1, 3])
    tie_breaker = arolla.array([2, 1, 3])
    over_edge = arolla.types.ArrayToScalarEdge(3)

    with flagsaver.flagsaver(
        extra_flags=[
            'args_for_reshape:x',
            'args_for_reshape:tie_breaker',
            'kd_op_mapping:array.ordinal_rank:fake_ordinal_rank',
        ]
    ):
      expr = M.array.ordinal_rank(L.x, L.tie_breaker, over=over_edge)
      arolla.testing.assert_qvalue_allequal(
          koda_test_eval.eager_eval(expr, x=x, tie_breaker=tie_breaker),
          arolla.eval(expr, x=x, tie_breaker=tie_breaker),
      )

      arolla.testing.assert_qvalue_allequal(
          koda_test_eval.eager_eval(
              expr, x=x, tie_breaker=arolla.unspecified()
          ),
          arolla.eval(expr, x=x, tie_breaker=arolla.unspecified()),
      )

  def test_args_for_reshape_with_into(self):

    @arolla.optools.add_to_registry()
    @arolla.optools.as_py_function_operator(
        'kde.fake_correlation', qtype_inference_expr=P.x
    )
    def fake_correlation(x, y):
      flat_res = arolla.abc.invoke_op(
          M.math.correlation,
          (
              x.as_arolla_value(),
              y.as_arolla_value(),
              x.get_shape()[-1],
          ),
      )
      return data_slice.DataSlice.from_vals(flat_res)

    x = arolla.array([1.0, 1.0, 3.0])
    y = arolla.array([2.0, 1.0, 3.0])
    into_edge = arolla.types.ArrayEdge.from_sizes([2, 0, 1])

    with flagsaver.flagsaver(
        extra_flags=[
            'args_for_reshape:x',
            'args_for_reshape:y',
            'kd_op_mapping:math.correlation:fake_correlation',
        ]
    ):
      expr = M.math.correlation(L.x, L.y, into=into_edge)
      arolla.testing.assert_qvalue_allequal(
          koda_test_eval.eager_eval(expr, x=x, y=y),
          arolla.eval(expr, x=x, y=y),
      )

  def test_overriding_arolla_unspecified_arg_with_param_default_dataslice(
      self,
  ):
    y_default = 1

    @arolla.optools.add_to_registry()
    @arolla.optools.as_py_function_operator(
        'kde.simple_add_for_test',
        qtype_inference_expr=P.x,
    )
    def kde_simple_add_for_test(x, y=data_slice.DataSlice.from_vals(y_default)):
      return x + y

    with flagsaver.flagsaver(
        extra_flags=[
            'kd_op_mapping:arolla_simple_add_for_test:simple_add_for_test',
        ]
    ):
      expr = M.arolla_simple_add_for_test(L.x, L.y)

      arolla.testing.assert_qvalue_allequal(
          koda_test_eval.eager_eval(
              expr, x=(1 - y_default), y=arolla.unspecified()
          ),
          arolla.eval(expr, x=1, y=arolla.unspecified()),
      )

  def test_respects_koda_args_with_arolla_unspecified_default_values(
      self,
  ):

    y_default = 1

    @arolla.optools.add_to_registry()
    @arolla.optools.as_py_function_operator(
        'kde.another_simple_add_for_test',
        qtype_inference_expr=P.x,
    )
    def kde_another_simple_add_for_test(x, y=arolla.unspecified()):
      if y.qtype == arolla.UNSPECIFIED:
        return x + y_default
      return x + y

    with flagsaver.flagsaver(
        extra_flags=[
            'kd_op_mapping:arolla_simple_add_for_test:another_simple_add_for_test',
        ]
    ):
      expr = M.arolla_simple_add_for_test(L.x, L.y)

      arolla.testing.assert_qvalue_allequal(
          koda_test_eval.eager_eval(
              expr, x=(1 - y_default), y=arolla.unspecified()
          ),
          arolla.eval(expr, x=1, y=arolla.unspecified()),
      )


if __name__ == '__main__':
  absltest.main()
