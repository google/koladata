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
from koladata.functions import attrs
from koladata.functions import functions as fns
from koladata.functor import functor_factories
from koladata.functor import tracing_decorator
from koladata.functor.parallel import clib as _
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import iterable_qvalue
from koladata.types import signature_utils

from koladata.functor.parallel import transform_config_pb2


_ORIGINAL_ARGUMENTS = (
    transform_config_pb2.ParallelTransformConfigProto.ArgumentTransformation.ORIGINAL_ARGUMENTS
)
_EXECUTOR = (
    transform_config_pb2.ParallelTransformConfigProto.ArgumentTransformation.EXECUTOR
)
_NON_DETERMINISTIC_TOKEN = (
    transform_config_pb2.ParallelTransformConfigProto.ArgumentTransformation.NON_DETERMINISTIC_TOKEN
)
I = input_container.InputContainer('I')
V = input_container.InputContainer('V')
S = I.self
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
kde_internal = kde_operators.internal
kd = eager_op_utils.operators_container('kd')
_PARALLEL_CALL_REPLACEMENT_CONTEXT = expr_eval.eval(
    kde_internal.parallel.create_transform_config(
        fns.obj(
            operator_replacements=[
                fns.new(
                    from_op='kd.functor.call',
                    to_op='koda_internal.parallel._parallel_call',
                    argument_transformation=fns.new(
                        arguments=[
                            transform_config_pb2.ParallelTransformConfigProto.ArgumentTransformation.EXECUTOR,
                            transform_config_pb2.ParallelTransformConfigProto.ArgumentTransformation.ORIGINAL_ARGUMENTS,
                        ],
                        functor_argument_indices=[0],
                    ),
                ),
            ]
        ),
    )
)


class KodaInternalParallelTransformTest(absltest.TestCase):

  def _test_eval_on_futures(
      self,
      fn,
      *,
      replacements,
      inputs,
      expected_output,
      allow_runtime_transforms=False
  ):
    executor = kde_internal.parallel.get_eager_executor()
    config = expr_eval.eval(
        kde_internal.parallel.create_transform_config(
            fns.obj(
                operator_replacements=replacements,
                allow_runtime_transforms=allow_runtime_transforms,
            ),
        )
    )
    transformed_fn = kde_internal.parallel.transform(config, fn)
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(
                transformed_fn(
                    executor,
                    *[kde_internal.parallel.as_future(x) for x in inputs],
                    return_type_as=kde_internal.parallel.as_future(
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
        kde_internal.parallel.get_future_qtype(qtypes.DATA_SLICE)
    )

    @optools.add_to_registry()
    @optools.as_py_function_operator(
        'koda_internal.parallel.transform_test.my_add',
        qtype_inference_expr=future_data_slice_qtype,
        deterministic=False,
    )
    def my_add(op_executor, x, y):
      testing.assert_equal(
          op_executor,
          expr_eval.eval(kde_internal.parallel.get_eager_executor()),
      )
      x_value = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(x)
      )
      y_value = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(y)
      )
      return expr_eval.eval(kde_internal.parallel.as_future(x_value - y_value))

    self._test_eval_on_futures(
        functor_factories.trace_py_fn(lambda x, y: x + y * 2),
        replacements=[
            fns.obj(
                from_op='kd.math.add',
                to_op='koda_internal.parallel.transform_test.my_add',
                argument_transformation=fns.obj(
                    arguments=[
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
      return kde_internal.parallel.unwrap_future_to_stream(
          kde_internal.parallel.async_eval(
              executor,
              kde_internal.parallel.stream_chain,
              streams,
              value_type_as,
              optools.unified_non_deterministic_arg(),
          )
      )

    fn = functor_factories.trace_py_fn(
        lambda x, y: user_facing_kd.iterables.chain(x, y)  # pylint: disable=unnecessary-lambda
    )
    executor = expr_eval.eval(kde_internal.parallel.get_eager_executor())
    config = expr_eval.eval(
        kde_internal.parallel.create_transform_config(
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
    transformed_fn = kde_internal.parallel.transform(config, fn)
    res_stream = expr_eval.eval(
        transformed_fn(
            executor,
            kde_internal.parallel.stream_make(1, 2),
            kde_internal.parallel.stream_make(3),
            return_type_as=kde_internal.parallel.stream_make(),
        )
    )
    testing.assert_equal(
        arolla.tuple(*res_stream.read_all(timeout=0)),
        arolla.tuple(ds(1), ds(2), ds(3)),
    )

  def test_literal_preserving_argument(self):

    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.initial_decode',
    )
    def initial_decode(x):
      return arolla.M.strings.decode(x)

    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.my_decode',
    )
    def my_decode(x):
      return kde_internal.parallel.as_future(arolla.M.strings.static_decode(x))

    self._test_eval_on_futures(
        functor_factories.expr_fn(initial_decode(arolla.bytes(b'123'))),
        replacements=[
            fns.obj(
                from_op='koda_internal.parallel.transform_test.initial_decode',
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
        'koda_internal.parallel.transform_test.initial_decode2',
    )
    def initial_decode2(x):
      return arolla.M.strings.decode(x)

    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.my_decode2',
    )
    def my_decode2(x):
      return kde_internal.parallel.as_future(arolla.M.strings.static_decode(x))

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a bytes literal, got x: FUTURE[BYTES]'),
    ):
      self._test_eval_on_futures(
          functor_factories.expr_fn(
              initial_decode2(arolla.M.strings.join(b'123', b'456'))
          ),
          replacements=[
              fns.obj(
                  from_op=(
                      'koda_internal.parallel.transform_test.initial_decode2'
                  ),
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
      return kde.tuple(x, y, z)

    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.my_decode4',
    )
    def my_decode4(executor, x, y, z):
      del executor  # Unused.
      return kde.tuple(
          kde_internal.parallel.as_future(arolla.M.strings.static_decode(x)),
          y,
          kde_internal.parallel.as_future(arolla.M.strings.static_decode(z)),
      )

    config = expr_eval.eval(
        kde_internal.parallel.create_transform_config(
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
    transformed_fn = kde_internal.parallel.transform(config, fn)
    res = expr_eval.eval(
        transformed_fn(
            kde_internal.parallel.get_eager_executor(),
            return_type_as=kde.tuple(
                kde_internal.parallel.as_future(arolla.text('')),
                kde_internal.parallel.as_future(arolla.bytes(b'')),
                kde_internal.parallel.as_future(arolla.text('')),
            ),
        )
    )
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res[0])
        ),
        arolla.text('123'),
    )
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res[1])
        ),
        arolla.bytes(b'123'),
    )
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res[2])
        ),
        arolla.text('789'),
    )

  def test_no_expr_functor(self):
    fn = functor_factories.expr_fn(ds(1))
    config = expr_eval.eval(
        kde_internal.parallel.create_transform_config(
            fns.obj(operator_replacements=[]),
        )
    )
    transformed_fn = expr_eval.eval(kde_internal.parallel.transform(config, fn))
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(
                transformed_fn(
                    kde_internal.parallel.get_eager_executor(),
                    return_type_as=expr_eval.eval(
                        kde_internal.parallel.as_future(data_slice.DataSlice)
                    ),
                )
            )
        ).no_bag(),
        ds(1),
    )

  def test_non_functor(self):
    config = expr_eval.eval(
        kde_internal.parallel.create_transform_config(
            fns.obj(operator_replacements=[]),
        )
    )
    with self.assertRaisesRegex(
        ValueError,
        'functor must be a functor',
    ):
      expr_eval.eval(
          kde_internal.parallel.transform(config, ds(1)),
      )

  def test_tuples_without_replacement(self):
    self._test_eval_on_futures(
        functor_factories.expr_fn(
            arolla.abc.bind_op(kde.slices.stack, I.args),
            signature=signature_utils.ARGS_KWARGS_SIGNATURE,
        ),
        replacements=[],
        inputs=[1, 2],
        expected_output=ds([1, 2]),
    )

  def test_tuples_with_replacement(self):
    future_data_slice_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(qtypes.DATA_SLICE)
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
    config = expr_eval.eval(
        kde_internal.parallel.create_transform_config(
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
    transformed_fn = kde_internal.parallel.transform(config, fn)
    res = expr_eval.eval(
        transformed_fn(
            kde_internal.parallel.get_eager_executor(),
            kde.tuple(
                kde_internal.parallel.as_future(ds(1)),
                kde_internal.parallel.as_future(ds(2)),
            ),
            kde_internal.parallel.as_future(ds(3)),
            return_type_as=kde.tuple(
                kde_internal.parallel.as_future(ds(0)),
                kde.tuple(
                    kde_internal.parallel.as_future(ds(0)),
                    kde_internal.parallel.as_future(ds(0)),
                ),
            ),
        )
    )
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res[1][0])
        ),
        ds(1),
    )
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res[1][1])
        ),
        ds(2),
    )
    testing.assert_equal(
        expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(res[0])
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
    db = data_bag.DataBag.empty_mutable().freeze()
    self._test_eval_on_futures(
        functor_factories.trace_py_fn(lambda y=1: y),
        replacements=[],
        inputs=[db],
        expected_output=db,
    )

  def test_default_value_non_parallel_slice_passed(self):
    fn = functor_factories.trace_py_fn(lambda y=1: y)
    config = expr_eval.eval(kde_internal.parallel.create_transform_config(None))
    transformed_fn = expr_eval.eval(kde_internal.parallel.transform(config, fn))
    future_slice = kde_internal.parallel.as_future(data_slice.DataSlice).eval()
    with self.assertRaisesRegex(
        ValueError,
        'a non-parallel data slice passed to a parallel functor',
    ):
      _ = expr_eval.eval(
          transformed_fn(
              kde_internal.parallel.get_eager_executor(),
              ds(1),
              return_type_as=future_slice,
          )
      )
    with self.assertRaisesRegex(
        ValueError,
        'a non-parallel data slice passed to a parallel functor',
    ):
      _ = expr_eval.eval(
          transformed_fn(
              kde_internal.parallel.get_eager_executor(),
              fns.new(x=fns.slice([1, 2])),
              return_type_as=future_slice,
          )
      )

  def test_annotations_basic(self):
    config = expr_eval.eval(
        kde_internal.parallel.create_transform_config(
            fns.obj(operator_replacements=[]),
        )
    )
    expr = arolla.M.annotation.export(
        arolla.M.annotation.export(kde.math.add(1, 2), 'foo'), 'bar'
    )
    fn = functor_factories.expr_fn(expr)
    transformed_fn = expr_eval.eval(kde_internal.parallel.transform(config, fn))
    testing.assert_equal(
        kde_internal.parallel.get_future_value_for_testing(
            transformed_fn(
                kde_internal.parallel.get_eager_executor(),
                return_type_as=kde_internal.parallel.as_future(
                    data_slice.DataSlice
                ).eval(),
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
      x_value = kde_internal.parallel.get_future_value_for_testing(x)
      y_value = kde_internal.parallel.get_future_value_for_testing(y)
      return kde_internal.parallel.stream_make(y_value, x_value)

    config = expr_eval.eval(
        kde_internal.parallel.create_transform_config(
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
        arolla.M.annotation.export(kde.iterables.make(1, 2), 'foo'), 'bar'
    )
    fn = functor_factories.expr_fn(expr)
    transformed_fn = expr_eval.eval(kde_internal.parallel.transform(config, fn))
    testing.assert_equal(
        arolla.tuple(
            *transformed_fn(
                expr_eval.eval(kde_internal.parallel.get_eager_executor()),
                return_type_as=kde_internal.parallel.stream_make().eval(),
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
      return kde_internal.parallel.stream_make()

    @optools.add_to_registry()
    @optools.as_lambda_operator(
        'koda_internal.parallel.transform_test.empty_iterable',
    )
    def empty_iterable():
      return kde.iterables.make()

    config = expr_eval.eval(
        kde_internal.parallel.create_transform_config(
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
        kde_internal.iterables.get_iterable_qtype(qtypes.DATA_SLICE),
    )
    testing.assert_equal(expr_eval.eval(expr), iterable_qvalue.Iterable())
    fn = functor_factories.expr_fn(expr)
    transformed_fn = expr_eval.eval(kde_internal.parallel.transform(config, fn))
    with self.assertRaisesRegex(ValueError, 'inconsistent annotation.qtype'):
      _ = transformed_fn(
          expr_eval.eval(kde_internal.parallel.get_eager_executor()),
          return_type_as=kde_internal.parallel.stream_make().eval(),
      ).read_all(timeout=1.0)

  def test_nested_replacement(self):
    future_data_slice_qtype = expr_eval.eval(
        kde_internal.parallel.get_future_qtype(qtypes.DATA_SLICE)
    )

    @optools.add_to_registry()
    @optools.as_py_function_operator(
        'koda_internal.parallel.transform_test.my_op_with_nested_transform',
    )
    def my_op_with_nested_transform(x, fn1, y, fn2):  # pylint: disable=unused-argument
      self.fail('should not be called')

    @optools.add_to_registry()
    @optools.as_py_function_operator(
        'koda_internal.parallel.transform_test.replaced_op_with_nested_transform',
        qtype_inference_expr=future_data_slice_qtype,
    )
    def replaced_op_with_nested_transform(executor, x, fn1, y, fn2):
      testing.assert_equal(
          executor,
          expr_eval.eval(kde_internal.parallel.get_eager_executor()),
      )
      x_value = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(x)
      )
      y_value = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(y)
      )

      def inner_eval(fn, x):
        return expr_eval.eval(
            kde_internal.parallel.get_future_value_for_testing(
                fn(
                    executor,
                    kde_internal.parallel.as_future(x),
                    return_type_as=kde_internal.parallel.as_future(
                        data_slice.DataSlice
                    ),
                )
            )
        )

      res = kd.map_py(inner_eval, fn1, x_value)
      if fn2.qtype != arolla.UNSPECIFIED:
        res += kd.map_py(inner_eval, fn2, y_value)
      return expr_eval.eval(kde_internal.parallel.as_future(res))

    @optools.add_to_registry()
    @optools.as_py_function_operator(
        'koda_internal.parallel.transform_test.my_nested_transform_inner_op',
    )
    def my_nested_transform_inner_op(x):  # pylint: disable=unused-argument
      self.fail('should not be called')

    @optools.add_to_registry()
    @optools.as_py_function_operator(
        'koda_internal.parallel.transform_test.replaced_nested_transform_inner_op',
        qtype_inference_expr=future_data_slice_qtype,
    )
    def replaced_nested_transform_inner_op(x):
      x_value = expr_eval.eval(
          kde_internal.parallel.get_future_value_for_testing(x)
      )
      res = x_value * 10
      return expr_eval.eval(kde_internal.parallel.as_future(res))

    fn1 = functor_factories.expr_fn(my_nested_transform_inner_op(S))
    fn2 = functor_factories.expr_fn(S)
    replacements = [
        fns.obj(
            from_op='koda_internal.parallel.transform_test.my_op_with_nested_transform',
            to_op='koda_internal.parallel.transform_test.replaced_op_with_nested_transform',
            argument_transformation=fns.obj(
                arguments=[
                    _EXECUTOR,
                    _ORIGINAL_ARGUMENTS,
                ],
                functor_argument_indices=[1, 3],
            ),
        ),
        fns.obj(
            from_op='koda_internal.parallel.transform_test.my_nested_transform_inner_op',
            to_op='koda_internal.parallel.transform_test.replaced_nested_transform_inner_op',
        ),
    ]

    self._test_eval_on_futures(
        functor_factories.expr_fn(
            my_op_with_nested_transform(S.x, V.fn1, S.y, V.fn2),
            fn1=fn1,
            fn2=fn2,
        ),
        replacements=replacements,
        inputs=[fns.obj(x=1, y=2)],
        expected_output=ds(1 * 10 + 2),
    )

    self._test_eval_on_futures(
        functor_factories.expr_fn(
            my_op_with_nested_transform(S.x, fn1, S.y, arolla.unspecified()),
        ),
        replacements=replacements,
        inputs=[fns.obj(x=1, y=2)],
        expected_output=ds(1 * 10),
    )

    self._test_eval_on_futures(
        functor_factories.expr_fn(
            my_op_with_nested_transform(
                S.x,
                # Literal subexpressions are evaluated during transform.
                kde.slice([fn1, fn2]),
                S.y,
                arolla.unspecified(),
            ),
        ),
        replacements=replacements,
        inputs=[fns.obj(x=1, y=2)],
        expected_output=ds([10, 1]),
    )

    self._test_eval_on_futures(
        functor_factories.expr_fn(
            my_op_with_nested_transform(
                S.x,
                # Literal subexpressions are evaluated during transform.
                kde.slice([V.fn1, V.fn2]),
                S.y,
                arolla.unspecified(),
            ),
            fn1=fn1,
            fn2=fn2,
        ),
        replacements=replacements,
        inputs=[fns.obj(x=1, y=2)],
        expected_output=ds([10, 1]),
    )

    fn3 = functor_factories.expr_fn(S + V.foo)
    self._test_eval_on_futures(
        functor_factories.expr_fn(
            my_op_with_nested_transform(
                S.x,
                # Literal subexpressions are evaluated during transform.
                kde.slice([V.fn1, V.fn3.with_attrs(foo=5)]),
                S.y,
                arolla.unspecified(),
            ),
            fn1=fn1,
            fn3=fn3,
        ),
        replacements=replacements,
        inputs=[fns.obj(x=1, y=2)],
        expected_output=ds([10, 6]),
    )

    with self.assertRaisesRegex(ValueError, 'allow_runtime_transforms=True'):
      self._test_eval_on_futures(
          functor_factories.expr_fn(
              my_op_with_nested_transform(
                  S.x, kde.slice([fn1, S.fn]), S.y, arolla.unspecified()
              ),
          ),
          replacements=replacements,
          inputs=[fns.obj(x=1, y=2)],
          expected_output=ds([10, 1]),
      )

    self._test_eval_on_futures(
        functor_factories.expr_fn(
            my_op_with_nested_transform(
                S.x, fns.slice([fn1, fn2]), S.y, arolla.unspecified()
            ),
        ),
        replacements=replacements,
        inputs=[fns.obj(x=1, y=2)],
        expected_output=ds([10, 1]),
        allow_runtime_transforms=True,
    )

  def test_multiple_calls_to_subfunctor_transformed_only_once(self):

    @tracing_decorator.TraceAsFnDecorator()
    def inner_fn(x):
      return x + 1

    outer_fn = functor_factories.trace_py_fn(
        lambda x: inner_fn(inner_fn(inner_fn(x)))
    )
    transformed_fn = expr_eval.eval(
        kde_internal.parallel.transform(
            _PARALLEL_CALL_REPLACEMENT_CONTEXT, outer_fn
        )
    )
    self.assertEqual(
        [x for x in attrs.dir(transformed_fn) if x.startswith('_transformed_')],
        ['_transformed_inner_fn'],
    )
    res = expr_eval.eval(
        kde_internal.parallel.get_future_value_for_testing(
            transformed_fn(
                kde_internal.parallel.get_eager_executor(),
                kde_internal.parallel.as_future(1),
                return_type_as=kde_internal.parallel.as_future(
                    data_slice.DataSlice
                ).eval(),
            )
        )
    )
    testing.assert_equal(res, ds(4))

  def test_handles_transformed_name_collision(self):

    @tracing_decorator.TraceAsFnDecorator()
    def inner_fn(x):
      return x + 1

    outer_fn = functor_factories.trace_py_fn(
        lambda x: inner_fn(inner_fn(inner_fn(x)))
    )
    transformed_fn = expr_eval.eval(
        kde_internal.parallel.transform(
            _PARALLEL_CALL_REPLACEMENT_CONTEXT, outer_fn
        )
    )
    transformed_names = [
        x for x in attrs.dir(transformed_fn) if x.startswith('_transformed_')
    ]
    self.assertLen(transformed_names, 1)

    outer_fn2 = functor_factories.trace_py_fn(
        lambda x: (x + 2).with_name(transformed_names[0]) + inner_fn(x)
    )
    transformed_fn2 = expr_eval.eval(
        kde_internal.parallel.transform(
            _PARALLEL_CALL_REPLACEMENT_CONTEXT, outer_fn2
        )
    )
    res = expr_eval.eval(
        kde_internal.parallel.get_future_value_for_testing(
            transformed_fn2(
                kde_internal.parallel.get_eager_executor(),
                kde_internal.parallel.as_future(1),
                return_type_as=kde_internal.parallel.as_future(
                    data_slice.DataSlice
                ).eval(),
            )
        )
    )
    testing.assert_equal(res, ds(5))

  def test_qtype_signatures(self):
    parallel_transform_config_qtype = expr_eval.eval(
        kde_internal.parallel.get_transform_config_qtype()
    )
    arolla.testing.assert_qtype_signatures(
        kde_internal.parallel.transform,
        [
            (
                parallel_transform_config_qtype,
                qtypes.DATA_SLICE,
                qtypes.DATA_SLICE,
            ),
        ],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES
        + (parallel_transform_config_qtype,),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(kde_internal.parallel.transform(I.config, I.fn))
    )


if __name__ == '__main__':
  absltest.main()
