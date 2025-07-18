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

from absl.testing import absltest
from arolla import arolla
from koladata import kd as user_facing_kd
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.functor.parallel import clib as _
from koladata.operators import bootstrap
from koladata.operators import iterables
from koladata.operators import koda_internal_iterables
from koladata.operators import koda_internal_parallel
from koladata.operators import math
from koladata.operators import optools
from koladata.operators import slices as slice_ops
from koladata.operators import tuple as tuple_ops
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import iterable_qvalue
from koladata.types import signature_utils

from koladata.functor.parallel import execution_config_pb2


_ORIGINAL_ARGUMENTS = (
    execution_config_pb2.ExecutionConfig.ArgumentTransformation.ORIGINAL_ARGUMENTS
)
_EXECUTOR = execution_config_pb2.ExecutionConfig.ArgumentTransformation.EXECUTOR
_EXECUTION_CONTEXT = (
    execution_config_pb2.ExecutionConfig.ArgumentTransformation.EXECUTION_CONTEXT
)
_NON_DETERMINISTIC_TOKEN = (
    execution_config_pb2.ExecutionConfig.ArgumentTransformation.NON_DETERMINISTIC_TOKEN
)
I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals


class KodaInternalParallelTransformTest(absltest.TestCase):

  def _test_eval_on_futures(self, fn, *, replacements, inputs, expected_output):
    context = expr_eval.eval(
        koda_internal_parallel.create_execution_context(
            koda_internal_parallel.get_eager_executor(),
            fns.obj(operator_replacements=replacements),
        )
    )
    transformed_fn = koda_internal_parallel.transform(context, fn)
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(
                transformed_fn(
                    *[koda_internal_parallel.as_future(x) for x in inputs],
                    return_type_as=koda_internal_parallel.as_future(
                        expected_output
                    ),
                )
            )
        ),
        expected_output,
    )

  def test_basic(self):
    self._test_eval_on_futures(
        functor_factories.trace_py_fn(lambda x, y: x + y),
        replacements=[],
        inputs=[1, 2],
        expected_output=ds(3),
    )

  def test_replacement(self):
    future_data_slice_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(qtypes.DATA_SLICE)
    )

    @optools.add_to_registry()
    @optools.as_py_function_operator(
        'koda_internal.parallel.transform_test.my_add',
        qtype_inference_expr=future_data_slice_qtype,
        deterministic=False,
    )
    def my_add(op_context, op_executor, x, y):
      testing.assert_equal(
          op_executor,
          expr_eval.eval(koda_internal_parallel.get_eager_executor()),
      )
      testing.assert_equal(
          op_context.qtype,
          expr_eval.eval(bootstrap.get_execution_context_qtype()),
      )
      x_value = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(x)
      )
      y_value = expr_eval.eval(
          koda_internal_parallel.get_future_value_for_testing(y)
      )
      return expr_eval.eval(koda_internal_parallel.as_future(x_value - y_value))

    self._test_eval_on_futures(
        functor_factories.trace_py_fn(lambda x, y: x + y * 2),
        replacements=[
            fns.obj(
                from_op='kd.add',
                to_op='koda_internal.parallel.transform_test.my_add',
                argument_transformation=fns.obj(
                    arguments=[
                        _EXECUTION_CONTEXT,
                        _EXECUTOR,
                        _ORIGINAL_ARGUMENTS,
                        _NON_DETERMINISTIC_TOKEN,
                    ],
                ),
            )
        ],
        inputs=[1, 2],
        expected_output=ds(-3),
    )

  def test_streams(self):
    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.parallel_chain',
    )
    def parallel_chain(executor, *streams, value_type_as=arolla.unspecified()):
      streams = arolla.optools.fix_trace_args(streams)
      return koda_internal_parallel.unwrap_future_to_stream(
          koda_internal_parallel.async_eval(
              executor,
              koda_internal_parallel.stream_chain,
              streams,
              value_type_as,
              optools.unified_non_deterministic_arg(),
          )
      )

    fn = functor_factories.trace_py_fn(
        lambda x, y: user_facing_kd.iterables.chain(x, y)  # pylint: disable=unnecessary-lambda
    )
    executor = expr_eval.eval(koda_internal_parallel.get_eager_executor())
    context = expr_eval.eval(
        koda_internal_parallel.create_execution_context(
            executor,
            fns.obj(
                operator_replacements=[
                    fns.obj(
                        from_op='kd.iterables.chain',
                        to_op='koda_internal.parallel.transform_test.parallel_chain',
                        argument_transformation=fns.obj(
                            arguments=[
                                _EXECUTOR,
                                _ORIGINAL_ARGUMENTS,
                                _NON_DETERMINISTIC_TOKEN,
                            ],
                        ),
                    ),
                    fns.obj(
                        from_op='core.make_tuple',
                        to_op='core.make_tuple',
                    ),
                ]
            ),
        )
    )
    transformed_fn = koda_internal_parallel.transform(context, fn)
    res_stream = expr_eval.eval(
        transformed_fn(
            koda_internal_parallel.stream_make(1, 2),
            koda_internal_parallel.stream_make(3),
            return_type_as=koda_internal_parallel.stream_make(),
        )
    )
    testing.assert_equal(
        arolla.tuple(*res_stream.read_all(timeout=0)),
        arolla.tuple(ds(1), ds(2), ds(3)),
    )

  def test_literal_preserving_argument(self):
    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.my_decode',
    )
    def my_decode(x):
      return koda_internal_parallel.as_future(arolla.M.strings.static_decode(x))

    self._test_eval_on_futures(
        functor_factories.expr_fn(arolla.M.strings.decode(b'123')),
        replacements=[
            fns.obj(
                from_op='strings.decode',
                to_op='koda_internal.parallel.transform_test.my_decode',
                argument_transformation=fns.obj(
                    keep_literal_argument_indices=[0],
                ),
            )
        ],
        inputs=[],
        expected_output=arolla.text('123'),
    )

  def test_literal_preserving_argument_non_literal(self):
    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.my_decode2',
    )
    def my_decode2(x):
      return koda_internal_parallel.as_future(arolla.M.strings.static_decode(x))

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a bytes literal, got x: FUTURE[BYTES]'),
    ):
      self._test_eval_on_futures(
          functor_factories.expr_fn(
              arolla.M.strings.decode(arolla.M.strings.join(b'123', b'456'))
          ),
          replacements=[
              fns.obj(
                  from_op='strings.decode',
                  to_op='koda_internal.parallel.transform_test.my_decode2',
                  argument_transformation=fns.obj(
                      keep_literal_argument_indices=[0],
                  ),
              )
          ],
          inputs=[],
          expected_output=arolla.text('123456'),
      )

  def test_multiple_literal_preserving_arguments(self):
    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.my_decode3',
    )
    def my_decode3(x, y, z):
      return tuple_ops.tuple_(x, y, z)

    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.my_decode4',
    )
    def my_decode4(executor, x, y, z):
      del executor  # Unused.
      return tuple_ops.tuple_(
          koda_internal_parallel.as_future(arolla.M.strings.static_decode(x)),
          y,
          koda_internal_parallel.as_future(arolla.M.strings.static_decode(z)),
      )

    context = expr_eval.eval(
        koda_internal_parallel.create_execution_context(
            koda_internal_parallel.get_eager_executor(),
            fns.obj(
                operator_replacements=[
                    fns.obj(
                        from_op=(
                            'koda_internal.parallel.transform_test.my_decode3'
                        ),
                        to_op=(
                            'koda_internal.parallel.transform_test.my_decode4'
                        ),
                        argument_transformation=fns.obj(
                            arguments=[_EXECUTOR, _ORIGINAL_ARGUMENTS],
                            keep_literal_argument_indices=[2, 0],
                        ),
                    )
                ]
            ),
        )
    )
    # Two of the arguments are the same literal, since the code must handle them
    # differently and it's a special case in the implementation.
    fn = functor_factories.expr_fn(
        my_decode3(
            arolla.bytes(b'123'), arolla.bytes(b'123'), arolla.bytes(b'789')
        )
    )
    transformed_fn = koda_internal_parallel.transform(context, fn)
    res = expr_eval.eval(
        transformed_fn(
            return_type_as=tuple_ops.tuple_(
                koda_internal_parallel.as_future(arolla.text('')),
                koda_internal_parallel.as_future(arolla.bytes(b'')),
                koda_internal_parallel.as_future(arolla.text('')),
            )
        )
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[0])
        ),
        arolla.text('123'),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[1])
        ),
        arolla.bytes(b'123'),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[2])
        ),
        arolla.text('789'),
    )

  def test_no_expr_functor(self):
    fn = functor_factories.expr_fn(ds(1))
    context = expr_eval.eval(
        koda_internal_parallel.create_execution_context(
            koda_internal_parallel.get_eager_executor(),
            fns.obj(operator_replacements=[]),
        )
    )
    transformed_fn = expr_eval.eval(
        koda_internal_parallel.transform(context, fn)
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(
                transformed_fn(
                    return_type_as=expr_eval.eval(
                        koda_internal_parallel.as_future(data_slice.DataSlice)
                    )
                )
            )
        ).no_bag(),
        ds(1),
    )

  def test_non_functor(self):
    context = expr_eval.eval(
        koda_internal_parallel.create_execution_context(
            koda_internal_parallel.get_eager_executor(),
            fns.obj(operator_replacements=[]),
        )
    )
    with self.assertRaisesRegex(
        ValueError,
        'functor must be a functor',
    ):
      expr_eval.eval(
          koda_internal_parallel.transform(I.context, I.fn),
          context=context,
          fn=ds(1),
      )

  def test_tuples_without_replacement(self):
    self._test_eval_on_futures(
        functor_factories.expr_fn(
            arolla.abc.bind_op(slice_ops.stack, I.args),
            signature=signature_utils.ARGS_KWARGS_SIGNATURE,
        ),
        replacements=[],
        inputs=[1, 2],
        expected_output=ds([1, 2]),
    )

  def test_tuples_with_replacement(self):
    future_data_slice_qtype = expr_eval.eval(
        koda_internal_parallel.get_future_qtype(qtypes.DATA_SLICE)
    )

    @optools.add_to_registry()
    @optools.as_py_function_operator(
        'koda_internal.parallel.transform_test.make_tuple',
        qtype_inference_expr=arolla.types.make_tuple_qtype(
            future_data_slice_qtype,
            arolla.types.make_tuple_qtype(
                future_data_slice_qtype, future_data_slice_qtype
            ),
        ),
    )
    def make_tuple(x, y):
      testing.assert_equal(
          x.qtype,
          arolla.types.make_tuple_qtype(
              future_data_slice_qtype, future_data_slice_qtype
          ),
      )
      testing.assert_equal(y.qtype, future_data_slice_qtype)
      return arolla.tuple(y, x)

    fn = functor_factories.trace_py_fn(
        lambda x, y: user_facing_kd.tuple(x, y)  # pylint: disable=unnecessary-lambda
    )
    context = expr_eval.eval(
        koda_internal_parallel.create_execution_context(
            koda_internal_parallel.get_eager_executor(),
            fns.obj(
                operator_replacements=[
                    fns.obj(
                        from_op='kd.tuple',
                        to_op=(
                            'koda_internal.parallel.transform_test.make_tuple'
                        ),
                    )
                ],
            ),
        )
    )
    transformed_fn = koda_internal_parallel.transform(context, fn)
    res = expr_eval.eval(
        transformed_fn(
            tuple_ops.tuple_(
                koda_internal_parallel.as_future(ds(1)),
                koda_internal_parallel.as_future(ds(2)),
            ),
            koda_internal_parallel.as_future(ds(3)),
            return_type_as=tuple_ops.tuple_(
                koda_internal_parallel.as_future(ds(0)),
                tuple_ops.tuple_(
                    koda_internal_parallel.as_future(ds(0)),
                    koda_internal_parallel.as_future(ds(0)),
                ),
            ),
        )
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[1][0])
        ),
        ds(1),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[1][1])
        ),
        ds(2),
    )
    testing.assert_equal(
        expr_eval.eval(
            koda_internal_parallel.get_future_value_for_testing(res[0])
        ),
        ds(3),
    )

  def test_default_value(self):
    self._test_eval_on_futures(
        functor_factories.trace_py_fn(lambda x, y=1: x + y),
        replacements=[],
        inputs=[2],
        expected_output=ds(3),
    )
    self._test_eval_on_futures(
        functor_factories.trace_py_fn(lambda x, y=1: x + y),
        replacements=[],
        inputs=[2, 3],
        expected_output=ds(5),
    )

  def test_default_value_bag_type(self):
    db = data_bag.DataBag.empty().freeze()
    self._test_eval_on_futures(
        functor_factories.trace_py_fn(lambda y=1: y),
        replacements=[],
        inputs=[db],
        expected_output=db,
    )

  def test_default_value_non_parallel_slice_passed(self):
    fn = functor_factories.trace_py_fn(lambda y=1: y)
    context = koda_internal_parallel.create_execution_context(
        koda_internal_parallel.get_eager_executor(), None
    )
    transformed_fn = expr_eval.eval(
        koda_internal_parallel.transform(context, fn)
    )
    future_slice = koda_internal_parallel.as_future(data_slice.DataSlice).eval()
    with self.assertRaisesRegex(
        ValueError,
        'a non-parallel data slice passed to a parallel functor',
    ):
      _ = expr_eval.eval(transformed_fn(ds(1), return_type_as=future_slice))
    with self.assertRaisesRegex(
        ValueError,
        'a non-parallel data slice passed to a parallel functor',
    ):
      _ = expr_eval.eval(
          transformed_fn(
              fns.new(x=fns.slice([1, 2])), return_type_as=future_slice
          )
      )

  def test_annotations_basic(self):
    context = expr_eval.eval(
        koda_internal_parallel.create_execution_context(
            koda_internal_parallel.get_eager_executor(),
            fns.obj(operator_replacements=[]),
        )
    )
    expr = arolla.M.annotation.export(
        arolla.M.annotation.export(math.add(1, 2), 'foo'), 'bar'
    )
    fn = functor_factories.expr_fn(expr)
    transformed_fn = expr_eval.eval(
        koda_internal_parallel.transform(context, fn)
    )
    testing.assert_equal(
        koda_internal_parallel.get_future_value_for_testing(
            transformed_fn(
                return_type_as=koda_internal_parallel.as_future(
                    data_slice.DataSlice
                ).eval()
            )
        ).eval(),
        ds(3),
    )
    # We don't want the annotation to be disconnected from the expr it
    # annotates, so that annotations like source_location keep functioning.
    # However, the whole thing gets embedded into a lambda operator
    # inside async_eval, so it's hard to write a test that is not fragile.
    # Checking the string representation seems to be the best we can do.
    self.assertIn(
        str(expr).replace("'", "\\'"),
        str(introspection.unpack_expr(transformed_fn.returns)),
    )

  def test_annotations_with_stream_replacement(self):
    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.stream_for_annotations',
    )
    def stream_for_annotations(elems, value_type_as):
      del value_type_as  # Unused.
      x = elems[0]
      y = elems[1]
      x_value = koda_internal_parallel.get_future_value_for_testing(x)
      y_value = koda_internal_parallel.get_future_value_for_testing(y)
      return koda_internal_parallel.stream_make(y_value, x_value)

    context = expr_eval.eval(
        koda_internal_parallel.create_execution_context(
            koda_internal_parallel.get_eager_executor(),
            fns.obj(
                operator_replacements=[
                    fns.obj(
                        from_op='kd.iterables.make',
                        to_op='koda_internal.parallel.transform_test.stream_for_annotations',
                    ),
                ],
            ),
        )
    )
    expr = arolla.M.annotation.export(
        arolla.M.annotation.export(iterables.make(1, 2), 'foo'), 'bar'
    )
    fn = functor_factories.expr_fn(expr)
    transformed_fn = expr_eval.eval(
        koda_internal_parallel.transform(context, fn)
    )
    testing.assert_equal(
        arolla.tuple(
            *transformed_fn(
                return_type_as=koda_internal_parallel.stream_make().eval()
            ).read_all(timeout=1.0)
        ),
        arolla.tuple(ds(2), ds(1)),
    )

  # The existence of this test does not mean this behavior is desired, it just
  # documents the current state.
  def test_qtype_annotation_with_stream_replacement_fails(self):

    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.empty_stream',
    )
    def empty_stream():
      return koda_internal_parallel.stream_make()

    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.empty_iterable',
    )
    def empty_iterable():
      return iterables.make()

    context = expr_eval.eval(
        koda_internal_parallel.create_execution_context(
            koda_internal_parallel.get_eager_executor(),
            fns.obj(
                operator_replacements=[
                    fns.obj(
                        from_op='koda_internal.parallel.transform_test.empty_iterable',
                        to_op=(
                            'koda_internal.parallel.transform_test.empty_stream'
                        ),
                    ),
                ],
            ),
        )
    )
    expr = arolla.M.annotation.qtype(
        empty_iterable(),
        koda_internal_iterables.get_iterable_qtype(qtypes.DATA_SLICE),
    )
    testing.assert_equal(expr_eval.eval(expr), iterable_qvalue.Iterable())
    fn = functor_factories.expr_fn(expr)
    transformed_fn = expr_eval.eval(
        koda_internal_parallel.transform(context, fn)
    )
    with self.assertRaisesRegex(ValueError, 'inconsistent qtype'):
      _ = transformed_fn(
          return_type_as=koda_internal_parallel.stream_make().eval()
      ).read_all(timeout=1.0)

  def test_qtype_signatures(self):
    execution_context_qtype = expr_eval.eval(
        bootstrap.get_execution_context_qtype()
    )
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.transform,
        [
            (
                execution_context_qtype,
                qtypes.DATA_SLICE,
                qtypes.NON_DETERMINISTIC_TOKEN,
                qtypes.DATA_SLICE,
            ),
        ],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES
        + (execution_context_qtype,),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(koda_internal_parallel.transform(I.context, I.fn))
    )


if __name__ == '__main__':
  absltest.main()
