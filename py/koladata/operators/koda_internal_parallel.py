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

"""Internal operators for parallel execution."""

from arolla import arolla
from arolla.derived_qtype import derived_qtype
from google.protobuf import text_format
from koladata.base import py_functors_base_py_ext
from koladata.expr import input_container
from koladata.expr import introspection
from koladata.expr import py_expr_eval_py_ext
from koladata.operators import assertion
from koladata.operators import bootstrap
from koladata.operators import core
from koladata.operators import functor
from koladata.operators import iterables
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import koda_internal_functor
from koladata.operators import koda_internal_iterables
from koladata.operators import masking
from koladata.operators import optools
from koladata.operators import proto as proto_ops
from koladata.operators import qtype_utils
from koladata.operators import schema as schema_ops
from koladata.operators import slices
from koladata.operators import tuple as tuple_ops
from koladata.operators import view_overloads
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants
from koladata.types import signature_utils

from koladata.functor.parallel import execution_config_pb2


M = arolla.M | derived_qtype.M
P = arolla.P
constraints = arolla.optools.constraints

async_eval = arolla.abc.lookup_operator('koda_internal.parallel.async_eval')
get_default_executor = arolla.abc.lookup_operator(
    'koda_internal.parallel.get_default_executor'
)
get_eager_executor = arolla.abc.lookup_operator(
    'koda_internal.parallel.get_eager_executor'
)

EXECUTOR = M.qtype.qtype_of(get_eager_executor())

EMPTY_TUPLE = arolla.make_tuple_qtype()


def _replace_non_deterministic_leaf_with_param(expr):
  """Replaces non-deterministic leaf with a param, for use in DispatchOperator."""
  return arolla.abc.sub_by_fingerprint(
      expr,
      {
          py_boxing.NON_DETERMINISTIC_TOKEN_LEAF.fingerprint: (
              P.non_deterministic_leaf
          ),
      },
  )


@arolla.optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.make_executor',
    qtype_constraints=[
        constraints.expect_scalar_integer(P.thread_limit),
    ],
    qtype_inference_expr=EXECUTOR,
    custom_boxing_fn_name_per_parameter=dict(
        thread_limit=py_boxing.WITH_AROLLA_BOXING,
    ),
    deterministic=False,
)
def make_executor(thread_limit=0):
  """Returns a new executor.

  Note: The `thread_limit` limits the concurrency; however, the executor may
  have no dedicated threads, and the actual concurrency limit might be lower.

  Args:
    thread_limit: The number of threads to use. Must be non-negative; 0 means
      that the number of threads is selected automatically.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.streams.current_executor'])
@optools.as_backend_operator(
    'koda_internal.parallel.current_executor',
    qtype_inference_expr=EXECUTOR,
    deterministic=False,
)
def current_executor():
  """Returns the current executor.

  If the current computation is running on an executor, this operator
  returns it. If no executor is set for the current context, this operator
  returns an error.

  Note: For the convenience, in Python environments, the default executor
  (see `get_default_executor`) is implicitly set as the current executor.
  However, this might not be not the case for other environments.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.create_execution_context',
    qtype_inference_expr=bootstrap.get_execution_context_qtype(),
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.config),
    ],
)
def create_execution_context(config):  # pylint: disable=unused-argument
  """Creates an execution context with the given config.

  Args:
    config: A data slice with the configuration to be used. It must be a scalar
      with structure corresponding to the ExecutionConfig proto. Attributes
      which are not part of ExecutionConfig proto will be ignored.

  Returns:
    An execution context with the given config.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.parallel.get_future_qtype',
    qtype_inference_expr=arolla.QTYPE,
    qtype_constraints=[constraints.expect_qtype(P.value_qtype)],
)
def get_future_qtype(value_qtype):  # pylint: disable=unused-argument
  """Gets the future qtype for the given value qtype."""
  raise NotImplementedError('implemented in the backend')


# Since futures holding a value are immutable, this operator can be kept
# deterministic.
@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.as_future',
    qtype_inference_expr=get_future_qtype(P.arg),
    qtype_constraints=[
        (
            ~bootstrap.is_stream_qtype(P.arg),
            'as_future cannot be applied to a stream',
        ),
        (
            ~bootstrap.is_future_qtype(P.arg),
            'as_future cannot be applied to a future',
        ),
    ],
)
def as_future(arg):  # pylint: disable=unused-argument
  """Wraps the given argument in a future, if not already one."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.get_future_value_for_testing',
    qtype_inference_expr=M.qtype.get_value_qtype(P.arg),
    qtype_constraints=[
        qtype_utils.expect_future(P.arg),
    ],
)
def get_future_value_for_testing(arg):  # pylint: disable=unused-argument
  """Gets the value from the given future for testing purposes."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.unwrap_future_to_future',
    qtype_inference_expr=M.qtype.get_value_qtype(P.arg),
    qtype_constraints=[
        qtype_utils.expect_future(P.arg),
        (
            bootstrap.is_future_qtype(M.qtype.get_value_qtype(P.arg)),
            (
                'expected a future to a future, got'
                f' {arolla.optools.constraints.name_type_msg(P.arg)}'
            ),
        ),
    ],
)
def unwrap_future_to_future(arg):  # pylint: disable=unused-argument
  """Given future to future, returns future getting the value of inner future."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.unwrap_future_to_stream',
    qtype_inference_expr=M.qtype.get_value_qtype(P.arg),
    qtype_constraints=[
        qtype_utils.expect_future(P.arg),
        (
            bootstrap.is_stream_qtype(M.qtype.get_value_qtype(P.arg)),
            (
                'expected a future to a stream, got'
                f' {arolla.optools.constraints.name_type_msg(P.arg)}'
            ),
        ),
    ],
)
def unwrap_future_to_stream(arg):  # pylint: disable=unused-argument
  """Given future to stream, returns stream getting the values of inner stream."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.parallel.get_stream_qtype',
    qtype_inference_expr=arolla.QTYPE,
    qtype_constraints=[constraints.expect_qtype(P.value_qtype)],
)
def get_stream_qtype(value_qtype):  # pylint: disable=unused-argument
  """Gets the stream qtype for the given value qtype."""
  raise NotImplementedError('implemented in the backend')


_STREAM_CHAIN_QTYPE_INFERENCE_EXPR = M.qtype.conditional_qtype(
    P.value_type_as == arolla.UNSPECIFIED,
    M.qtype.conditional_qtype(
        P.streams == EMPTY_TUPLE,
        get_stream_qtype(qtypes.DATA_SLICE),
        M.qtype.get_field_qtype(P.streams, 0),
    ),
    get_stream_qtype(P.value_type_as),
)

_STREAM_CHAIN_QTYPE_CONSTRAINTS = (
    (
        M.seq.all(
            M.seq.map(
                bootstrap.is_stream_qtype,
                M.qtype.get_field_qtypes(P.streams),
            )
        ),
        'all inputs must be streams',
    ),
    (
        M.seq.all_equal(M.qtype.get_field_qtypes(P.streams)),
        'all input streams must have the same value type',
    ),
    (
        (P.value_type_as == arolla.UNSPECIFIED)
        | (P.streams == EMPTY_TUPLE)
        | (
            M.qtype.get_field_qtype(P.streams, 0)
            == get_stream_qtype(P.value_type_as)
        ),
        'input streams must be compatible with value_type_as',
    ),
)


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_chain',
    qtype_inference_expr=_STREAM_CHAIN_QTYPE_INFERENCE_EXPR,
    qtype_constraints=_STREAM_CHAIN_QTYPE_CONSTRAINTS,
    deterministic=False,
)
def stream_chain(*streams, value_type_as=arolla.unspecified()):
  """Creates a stream that chains the given streams, in the given order.

  The streams must all have the same value type. If value_type_as is
  specified, it must be the same as the value type of the streams, if any.

  Args:
    *streams: A list of streams to be chained (concatenated).
    value_type_as: A value that has the same type as the items in the streams.
      It is useful to specify this explicitly if the list of streams may be
      empty. If this is not specified and the list of streams is empty, the
      stream will have DATA_SLICE as the value type.

  Returns:
    A stream that chains the given streams in the given order.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_interleave',
    qtype_inference_expr=_STREAM_CHAIN_QTYPE_INFERENCE_EXPR,
    qtype_constraints=_STREAM_CHAIN_QTYPE_CONSTRAINTS,
    deterministic=False,
)
def stream_interleave(*streams, value_type_as=arolla.unspecified()):
  """Creates a stream that interleaves the given streams.

  The resulting stream has all items from all input streams, and the order of
  items from each stream is preserved. But the order of interleaving of
  different streams can be arbitrary.

  Having unspecified order allows the parallel execution to put the items into
  the result in the order they are computed, potentially increasing the amount
  of parallel processing done.

  The input streams must all have the same value type. If value_type_as is
  specified, it must be the same as the value type of the streams, if any.

  Args:
    *streams: Input streams.
    value_type_as: A value that has the same type as the items in the streams.
      It is useful to specify this explicitly if the list of streams may be
      empty. If this is not specified and the list of streams is empty, the
      resulting stream will have DATA_SLICE as the value type.

  Returns:
    A stream that interleaves the input streams in an unspecified order.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_chain_from_stream',
    qtype_inference_expr=M.qtype.get_value_qtype(P.stream_of_streams),
    qtype_constraints=[
        qtype_utils.expect_stream(P.stream_of_streams),
        (
            bootstrap.is_stream_qtype(
                M.qtype.get_value_qtype(P.stream_of_streams)
            ),
            (
                'expected a stream of streams, got'
                f' {constraints.name_type_msg(P.stream_of_streams)}'
            ),
        ),
    ],
    deterministic=False,
)
def stream_chain_from_stream(stream_of_streams):
  """Creates a stream that chains the given streams.

  The resulting stream has all items from the first sub-stream, then all items
  from the second sub-stream, and so on.

  Example:
      ```
      parallel.stream_chain_from_stream(
          parallel.stream_make(
              parallel.stream_make(1, 2, 3),
              parallel.stream_make(4),
              parallel.stream_make(5, 6),
          )
      )
      ```
      result: A stream with items [1, 2, 3, 4, 5, 6].

  Args:
    stream_of_streams: A stream of input streams.

  Returns:
    A stream that chains the input streams.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_interleave_from_stream',
    qtype_inference_expr=M.qtype.get_value_qtype(P.stream_of_streams),
    qtype_constraints=[
        qtype_utils.expect_stream(P.stream_of_streams),
        (
            bootstrap.is_stream_qtype(
                M.qtype.get_value_qtype(P.stream_of_streams)
            ),
            (
                'expected a stream of streams, got'
                f' {constraints.name_type_msg(P.stream_of_streams)}'
            ),
        ),
    ],
    deterministic=False,
)
def stream_interleave_from_stream(stream_of_streams):
  """Creates a stream that interleaves the given streams.

  The resulting stream has all items from all input streams, and the order of
  items from each stream is preserved. But the order of interleaving of
  different streams can be arbitrary.

  Having unspecified order allows the parallel execution to put the items into
  the result in the order they are computed, potentially increasing the amount
  of parallel processing done.

  Args:
    stream_of_streams: A stream of input streams.

  Returns:
    A stream that interleaves the input streams in an unspecified order.
  """
  raise NotImplementedError('implemented in the backend')


# Since the stream returned by this operator is immutable, this operator can be
# kept deterministic.
@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_make',
    qtype_constraints=[
        (
            M.seq.all_equal(M.qtype.get_field_qtypes(P.items)),
            'all items must have the same type',
        ),
        (
            (P.value_type_as == arolla.UNSPECIFIED)
            | (P.items == EMPTY_TUPLE)
            | (P.value_type_as == M.qtype.get_field_qtype(P.items, 0)),
            'items must be compatible with value_type_as',
        ),
    ],
    qtype_inference_expr=get_stream_qtype(
        M.qtype.conditional_qtype(
            P.value_type_as == arolla.UNSPECIFIED,
            M.qtype.conditional_qtype(
                P.items == EMPTY_TUPLE,
                qtypes.DATA_SLICE,
                M.qtype.get_field_qtype(P.items, 0),
            ),
            P.value_type_as,
        )
    ),
)
def stream_make(*items, value_type_as=arolla.unspecified()):
  """Creates a stream from the given items, in the given order.

  The items must all have the same type (for example data slice, or data bag).
  However, in case of data slices, the items can have different shapes or
  schemas.

  Args:
    *items: Items to be put into the stream.
    value_type_as: A value that has the same type as the items. It is useful to
      specify this explicitly if the list of items may be empty. If this is not
      specified and the list of items is empty, the iterable will have data
      slice as the value type.

  Returns:
    A stream with the given items.
  """
  raise NotImplementedError('implemented in the backend')


# Since the stream returned by this operator is immutable, this operator can be
# kept deterministic.
@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_from_iterable',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.iterable),
    ],
    qtype_inference_expr=get_stream_qtype(M.qtype.get_value_qtype(P.iterable)),
)
def stream_from_iterable(iterable):
  """Creates a stream from the given iterable.

  Args:
    iterable: The iterable with the items to be put into the stream.

  Returns:
    A stream with the items from the given iterable, in the same order.
  """
  raise NotImplementedError('implemented in the backend')


@optools.as_lambda_operator(
    'koda_internal.parallel._internal_as_parallel',
    qtype_constraints=[
        (
            ~bootstrap.is_future_qtype(P.outer_arg),
            'as_parallel cannot be applied to a future',
        ),
        (
            ~bootstrap.is_stream_qtype(P.outer_arg),
            'as_parallel cannot be applied to a stream',
        ),
    ],
)
def _internal_as_parallel(outer_arg, outer_self_op):
  """Implementation helper for as_parallel."""
  # We prefix the arguments with "outer_" here to avoid conflict with the
  # names in DispatchOperator.
  return arolla.types.DispatchOperator(
      'arg, self_op',
      tuple_case=arolla.types.DispatchCase(
          M.core.map_tuple(
              P.self_op,
              P.arg,
              P.self_op,
          ),
          condition=M.qtype.is_tuple_qtype(P.arg),
      ),
      namedtuple_case=arolla.types.DispatchCase(
          M.core.apply_varargs(
              M.namedtuple.make,
              M.qtype.get_field_names(M.qtype.qtype_of(P.arg)),
              M.core.map_tuple(
                  P.self_op,
                  M.derived_qtype.upcast(M.qtype.qtype_of(P.arg), P.arg),
                  P.self_op,
              ),
          ),
          condition=M.qtype.is_namedtuple_qtype(P.arg),
      ),
      iterable_case=arolla.types.DispatchCase(
          stream_from_iterable(P.arg),
          condition=bootstrap.is_iterable_qtype(P.arg),
      ),
      non_deterministic_token_case=arolla.types.DispatchCase(
          P.arg,
          condition=P.arg == qtypes.NON_DETERMINISTIC_TOKEN,
      ),
      default=as_future(P.arg),
  )(outer_arg, outer_self_op)


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.as_parallel',
)
def as_parallel(arg):
  """Given a value, return the parallel version of it.

  In the automatic parallel computation world, we use the following rules:

  For a type X, we define parallel_type[X] as:
  - stream[Y] if X is iterable[Y].
  - tuple[parallel_type[Y1], parallel_type[Y2], ...] when X
    is tuple[Y1, Y2, ...]
  - namedtuple[foo=parallel_type[Y1], bar=parallel_type[Y2], ...] when X
    is namedtuple[foo=Y1, bar=Y2, ...]
  - non_deterministic_token if X is non_deterministic_token.
  - an error if X is a future or a stream.
  - future[X] otherwise.
  Note that for tuples/namedtuples, the above rule is applied recursively.

  If an operator takes an argument of type X, the parallel version must take
  parallel_type[X] for this argument.
  If an operator returns type Y, the parallel version must return
  parallel_type[Y].

  Args:
    arg: The value to get the parallel version of.

  Returns:
    The parallel version of the given value.
  """
  return _internal_as_parallel(arg, _internal_as_parallel)


@arolla.optools.as_lambda_operator(
    'koda_internal.parallel._internal_namedtuple_with_names_from',
)
def _internal_namedtuple_with_names_from(example_namedtuple, *args):
  """Creates a namedtuple with the same field names as the given example.

  This helper is needed so that we can evaluate M.namedtuple.make
  via async_eval, since it requires the field names to be statically
  computable.

  Args:
    example_namedtuple: The example namedtuple to get the field names from.
    *args: The field values for the new namedtuple.

  Returns:
    A namedtuple with the same field names as the given example and the given
    field values.
  """
  args = arolla.optools.fix_trace_args(args)
  return M.core.apply_varargs(
      M.namedtuple.make,
      M.qtype.get_field_names(M.qtype.qtype_of(example_namedtuple)),
      args,
  )


# TODO: Also create a recursive version of this.
@arolla.optools.as_lambda_operator(
    'koda_internal.parallel._nonrecursive_is_parallel_qtype',
)
def _nonrecursive_is_parallel_qtype(arg):
  """A relaxed check if the given argument is a parallel type.

  This does not recurse into tuples/namedtuples.

  Args:
    arg: The argument QType to check.

  Returns:
    present if the argument could be a parallel type, missing if we are certain
    that the argument is not a parallel type.
  """
  return (
      (
          bootstrap.is_future_qtype(arg)
          & ~M.qtype.is_tuple_qtype(M.qtype.get_value_qtype(arg))
          & ~M.qtype.is_namedtuple_qtype(M.qtype.get_value_qtype(arg))
          & (M.qtype.get_value_qtype(arg) != qtypes.NON_DETERMINISTIC_TOKEN)
      )
      | bootstrap.is_stream_qtype(arg)
      | (arg == qtypes.NON_DETERMINISTIC_TOKEN)
      | M.qtype.is_tuple_qtype(arg)
      | M.qtype.is_namedtuple_qtype(arg)
  )


@optools.as_lambda_operator(
    'koda_internal.parallel._internal_future_from_parallel',
    # Note that we do not check the tuple/namedtuple contents here,
    # it will be checked in the recursive call.
    qtype_constraints=[
        qtype_utils.expect_executor(P.outer_executor),
        (
            _nonrecursive_is_parallel_qtype(P.outer_arg)
            & ~bootstrap.is_stream_qtype(P.outer_arg),
            (
                'future_from_parallel can only be applied to a parallel'
                ' non-stream type, got'
                f' {constraints.name_type_msg(P.outer_arg)}'
            ),
        ),
    ],
)
def _internal_future_from_parallel(outer_arg, outer_executor, outer_self_op):
  """Implementation helper for future_from_parallel."""
  # We prefix the arguments with "outer_" here to avoid conflict with the
  # names in DispatchOperator.
  return arolla.types.DispatchOperator(
      'arg, executor, self_op',
      tuple_case=arolla.types.DispatchCase(
          M.core.apply_varargs(
              async_eval,
              P.executor,
              tuple_ops.tuple_,
              M.core.map_tuple(
                  P.self_op,
                  P.arg,
                  P.executor,
                  P.self_op,
              ),
          ),
          condition=M.qtype.is_tuple_qtype(P.arg),
      ),
      namedtuple_case=arolla.types.DispatchCase(
          M.core.apply_varargs(
              async_eval,
              P.executor,
              _internal_namedtuple_with_names_from,
              P.arg,
              M.core.map_tuple(
                  P.self_op,
                  M.derived_qtype.upcast(M.qtype.qtype_of(P.arg), P.arg),
                  P.executor,
                  P.self_op,
              ),
          ),
          condition=M.qtype.is_namedtuple_qtype(P.arg),
      ),
      non_deterministic_token_case=arolla.types.DispatchCase(
          as_future(P.arg),
          condition=P.arg == qtypes.NON_DETERMINISTIC_TOKEN,
      ),
      future_case=arolla.types.DispatchCase(
          P.arg,
          condition=bootstrap.is_future_qtype(P.arg),
      ),
  )(outer_arg, outer_executor, outer_self_op)


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.future_from_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
    ],
)
def future_from_parallel(executor, arg):
  """Given a value of a parallel type (see as_parallel), return a future.

  The result of the future will be the eager value corresponding to the parallel
  value. In case the parallel value involves streams, this will raise.

  The transformation happening here is creating a future to tuple/namedtuple
  instead of a tuple/namedtuple of futures.

  Args:
    executor: The executor to use to create tuples/namedtuples.
    arg: The value to convert to the future form.

  Returns:
    A future with the eager value corresponding to the given parallel value.
  """
  return _internal_future_from_parallel(
      arg, executor, _internal_future_from_parallel
  )


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.async_unpack_tuple',
    qtype_constraints=[
        qtype_utils.expect_future(P.future),
        (
            M.qtype.is_tuple_qtype(M.qtype.get_value_qtype(P.future))
            | M.qtype.is_namedtuple_qtype(M.qtype.get_value_qtype(P.future)),
            'input must be a future to a tuple or a namedtuple',
        ),
    ],
    qtype_inference_expr=M.qtype.make_tuple_qtype(
        M.seq.map(
            get_future_qtype,
            M.qtype.get_field_qtypes(M.qtype.get_value_qtype(P.future)),
        )
    ),
)
def async_unpack_tuple(future):  # pylint: disable=unused-argument
  """Given a future to tuple/namedtuple, returns tuple of futures to its fields."""
  raise NotImplementedError('implemented in the backend')


@optools.as_lambda_operator(
    'koda_internal.parallel._internal_parallel_from_future',
    qtype_constraints=[
        qtype_utils.expect_future(P.outer_arg),
    ],
)
def _internal_parallel_from_future(outer_arg, outer_executor, outer_self_op):
  """Implementation helper for parallel_from_future."""
  # We prefix the arguments with "outer_" here to avoid conflict with the
  # names in DispatchOperator.
  return arolla.types.DispatchOperator(
      'arg, executor, self_op, new_non_deterministic_token',
      tuple_case=arolla.types.DispatchCase(
          M.core.map_tuple(
              P.self_op,
              async_unpack_tuple(P.arg),
              P.executor,
              P.self_op,
              P.new_non_deterministic_token,
          ),
          condition=M.qtype.is_tuple_qtype(M.qtype.get_value_qtype(P.arg)),
      ),
      namedtuple_case=arolla.types.DispatchCase(
          M.core.apply_varargs(
              M.namedtuple.make,
              M.qtype.get_field_names(
                  M.qtype.get_value_qtype(M.qtype.qtype_of(P.arg))
              ),
              M.core.map_tuple(
                  P.self_op,
                  async_unpack_tuple(P.arg),
                  P.executor,
                  P.self_op,
                  P.new_non_deterministic_token,
              ),
          ),
          condition=M.qtype.is_namedtuple_qtype(M.qtype.get_value_qtype(P.arg)),
      ),
      non_deterministic_token_case=arolla.types.DispatchCase(
          P.new_non_deterministic_token,
          condition=M.qtype.get_value_qtype(P.arg)
          == qtypes.NON_DETERMINISTIC_TOKEN,
      ),
      iterable_case=arolla.types.DispatchCase(
          unwrap_future_to_stream(
              async_eval(
                  P.executor,
                  stream_from_iterable,
                  P.arg,
              )
          ),
          condition=bootstrap.is_iterable_qtype(M.qtype.get_value_qtype(P.arg)),
      ),
      default=P.arg,
  )(
      outer_arg,
      outer_executor,
      outer_self_op,
      optools.unified_non_deterministic_arg(),
  )


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.parallel_from_future',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_future(P.arg),
    ],
)
def parallel_from_future(executor, arg):
  """Given a future, convert it to a corresponding parallel value.

  More specifically, if the future has a tuple/namedtuple value, convert it to
  a tuple/namedtuple of futures. If the input is a future to the
  non-deterministic token, returns (a new) non-deterministic token. If one
  of the values in the future is an iterable, converts it to a stream (which
  will therefore have all values available at once).

  Args:
    executor: The executor to use for conversions that require it.
    arg: The value to convert from the future form.

  Returns:
    A value of a parallel type (see as_parallel) corresponding to the given
    future.
  """
  return _internal_parallel_from_future(
      arg, executor, _internal_parallel_from_future
  )


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_map',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream),
        qtype_utils.expect_data_slice(P.fn),
    ],
    qtype_inference_expr=get_stream_qtype(P.value_type_as),
    deterministic=False,
)
def stream_map(executor, stream, fn, *, value_type_as=data_slice.DataSlice):
  """Returns a new stream by applying `fn` to each item in the input stream.

  For each item of the input `stream`, the `fn` is called. The single
  resulting item from each call is then written into the new output stream.

  Args:
    executor: An executor for scheduling asynchronous operations.
    stream: The input stream.
    fn: The function to be executed for each item of the input stream. It will
      receive an item as the positional argument and its result must be of the
      same type as `value_type_as`.
    value_type_as: The type to use as value type of the resulting stream.

  Returns:
    The resulting stream.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_map_unordered',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream),
        qtype_utils.expect_data_slice(P.fn),
    ],
    qtype_inference_expr=get_stream_qtype(P.value_type_as),
    deterministic=False,
)
def stream_map_unordered(
    executor, stream, fn, *, value_type_as=data_slice.DataSlice
):
  """Returns a new stream by applying `fn` to each item in the input `stream`.

  For each item of the input `stream`, the `fn` is called. The single
  resulting item from each call is then written into the new output stream.

  IMPORTANT: The order of the items in the resulting stream is not guaranteed.

  Args:
    executor: An executor for scheduling asynchronous operations.
    stream: The input stream.
    fn: The function to be executed for each item of the input stream. It will
      receive an item as the positional argument and its result must be of the
      same type as `value_type_as`.
    value_type_as: The type to use as value type of the resulting stream.

  Returns:
    The resulting stream.
  """
  raise NotImplementedError('implemented in the backend')


# This operator is tested via koda_internal_parallel_transform_test.py.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._get_value_or_parallel_default',
)
def _get_value_or_parallel_default(
    outer_value, outer_default_value_marker, outer_default_value
):
  """An implementation helper for transform.

  If outer_value is a DataSlice, asserts that it is the same as
  outer_default_value_marker and returns the parallel version of
  outer_default_value. Otherwise returns outer_value unchanged.

  Since DataSlice is not a valid parallel type, there is no way for a
  legitimate argument to a parallel functor to be a DataSlice except when
  it is the default value marker.

  All parameters are prefixed with "outer_" to avoid conflicts with the
  names in DispatchOperator.

  Args:
    outer_value: The value to check.
    outer_default_value_marker: The default value marker.
    outer_default_value: The default value to use when we see the default value
      marker.

  Returns:
    The parallel version of outer_default_value if outer_value is
    outer_default_value_marker, otherwise outer_value.
  """
  return arolla.types.DispatchOperator(
      'value, default_value_marker, default_value',
      data_slice_case=arolla.types.DispatchCase(
          assertion.with_assertion(
              as_parallel(P.default_value),
              # We cast to OBJECT and have kd.all to have a proper error message
              # instead of a schema mismatch or a shape mismatch.
              (slices.get_ndim(P.value) == 0)
              & masking.all_(
                  schema_ops.with_schema(P.value, schema_constants.OBJECT)
                  == schema_ops.with_schema(
                      P.default_value_marker, schema_constants.OBJECT
                  )
              ),
              'a non-parallel data slice passed to a parallel functor',
          ),
          condition=P.value == qtypes.DATA_SLICE,
      ),
      default=P.value,
  )(outer_value, outer_default_value_marker, outer_default_value)


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.transform',
    qtype_inference_expr=qtypes.DATA_SLICE,
    qtype_constraints=[
        qtype_utils.expect_execution_context(P.context),
        qtype_utils.expect_data_slice(P.fn),
    ],
    deterministic=False,
)
def transform(context, fn):  # pylint: disable=unused-argument
  """Transforms the given functor to a parallel version.

  The transformed functor will expect all arguments in the parallel form
  (futures/streams/tuples thereof), and will also expect an additional
  first positional-only argument, the executor. It will return a parallel
  version of the result.

  Args:
    context: The execution context to use for the transformation.
    fn: The functor to transform.

  Returns:
    The transformed functor.
  """
  raise NotImplementedError('implemented in the backend')


@optools.as_lambda_operator(
    'koda_internal.parallel._internal_unwrap_future_to_parallel',
    qtype_constraints=[
        qtype_utils.expect_future(P.outer_arg),
        (
            _nonrecursive_is_parallel_qtype(
                M.qtype.get_value_qtype(P.outer_arg)
            ),
            (
                'input must be a future to a parallel type, got'
                f' {constraints.name_type_msg(P.outer_arg)}'
            ),
        ),
    ],
)
def _internal_unwrap_future_to_parallel(outer_arg, outer_self_op):
  """Implementation helper for unwrap_future_to_parallel."""
  # We prefix the arguments with "outer_" here to avoid conflict with the
  # names in DispatchOperator.
  return arolla.types.DispatchOperator(
      'arg, self_op, new_non_deterministic_token',
      tuple_case=arolla.types.DispatchCase(
          M.core.map_tuple(
              P.self_op,
              async_unpack_tuple(P.arg),
              P.self_op,
              P.new_non_deterministic_token,
          ),
          condition=M.qtype.is_tuple_qtype(M.qtype.get_value_qtype(P.arg)),
      ),
      namedtuple_case=arolla.types.DispatchCase(
          M.core.apply_varargs(
              M.namedtuple.make,
              M.qtype.get_field_names(
                  M.qtype.get_value_qtype(M.qtype.qtype_of(P.arg))
              ),
              M.core.map_tuple(
                  P.self_op,
                  async_unpack_tuple(P.arg),
                  P.self_op,
                  P.new_non_deterministic_token,
              ),
          ),
          condition=M.qtype.is_namedtuple_qtype(M.qtype.get_value_qtype(P.arg)),
      ),
      non_deterministic_token_case=arolla.types.DispatchCase(
          P.new_non_deterministic_token,
          condition=(
              M.qtype.get_value_qtype(P.arg) == qtypes.NON_DETERMINISTIC_TOKEN
          ),
      ),
      future_case=arolla.types.DispatchCase(
          unwrap_future_to_future(P.arg),
          condition=bootstrap.is_future_qtype(M.qtype.get_value_qtype(P.arg)),
      ),
      stream_case=arolla.types.DispatchCase(
          unwrap_future_to_stream(P.arg),
          condition=bootstrap.is_stream_qtype(M.qtype.get_value_qtype(P.arg)),
      ),
  )(outer_arg, outer_self_op, optools.unified_non_deterministic_arg())


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.unwrap_future_to_parallel',
    qtype_constraints=[
        qtype_utils.expect_future(P.arg),
    ],
)
def unwrap_future_to_parallel(arg):
  """Given a future to a parallel type, returns a copy of the inner value."""
  return _internal_unwrap_future_to_parallel(
      arg, _internal_unwrap_future_to_parallel
  )


@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_call_impl',
)
def _parallel_call_impl(executor, context, fn, parallel_args):
  transformed_fn = transform(context, fn)
  args = parallel_args[0]
  return_type_as = parallel_args[1]
  kwargs = parallel_args[2]
  return arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
      functor.call,
      transformed_fn,
      args=M.core.concat_tuples(M.core.make_tuple(executor), args),
      return_type_as=return_type_as,
      kwargs=kwargs,
      **optools.unified_non_deterministic_kwarg(),
  )


def _expect_future_data_slice(arg):
  return (
      arg == get_future_qtype(qtypes.DATA_SLICE),
      (
          'expected a future to a data slice, got'
          f' {constraints.name_type_msg(arg)}'
      ),
  )


def _expect_future_data_slice_or_unspecified(arg):
  return (
      (arg == get_future_qtype(qtypes.DATA_SLICE))
      | (arg == arolla.UNSPECIFIED),
      (
          'expected a future to a data slice or unspecified, got'
          f' {constraints.name_type_msg(arg)}'
      ),
  )


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.parallel_call',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_execution_context(P.context),
        _expect_future_data_slice(P.fn),
        (
            # TODO: get rid of "nonrecursive" here.
            M.seq.all(
                M.seq.map(
                    _nonrecursive_is_parallel_qtype,
                    M.qtype.get_field_qtypes(P.args),
                )
            ),
            (
                'args must have parallel types, got'
                f' {constraints.name_type_msg(P.args)}'
            ),
        ),
        (
            # TODO: get rid of "nonrecursive" here.
            M.seq.all(
                M.seq.map(
                    _nonrecursive_is_parallel_qtype,
                    M.qtype.get_field_qtypes(P.kwargs),
                )
            ),
            (
                'kwargs must have parallel types, got'
                f' {constraints.name_type_msg(P.kwargs)}'
            ),
        ),
        (
            # TODO: get rid of "nonrecursive" here.
            _nonrecursive_is_parallel_qtype(P.return_type_as)
            | (P.return_type_as == arolla.UNSPECIFIED),
            (
                'return_type_as must have a parallel type, got'
                f' {constraints.name_type_msg(P.return_type_as)}'
            ),
        ),
    ],
)
def parallel_call(
    executor,
    context,
    fn,
    *args,
    return_type_as=arolla.unspecified(),
    **kwargs,
):
  """Calls the given functor via the parallel evaluation.

  This operator is intented to be used as a parallel version of `functor.call`,
  therefore all inputs except `context` must have parallel types
  (futures/streams/tuples thereof).
  See execution_config.proto for more details about parallel types.

  Note that the default values specified in the signature here are not used
  when using this operator as a parallel version of `functor.call`, only when
  this operator is called directly.

  By default, we will wrap each variable expr of the provided functor into an
  async_eval, meaning that we'd wait for all inputs to be ready,
  then compute the output, which will therefore take parallel form: a future,
  a stream, or a tuple/namedtuple thereof. If any of the inputs to a variable
  expr is an iterable (stream in the parallel version), then the default
  conversion will fail, since we do not want to wait for all elements of the
  stream to be available by default. However, if the output of a particular
  variable expr is an iterable but none of the inputs are, then we will convert
  the result to a stream after the asynchronous evaluation of the variable expr.

  However, custom behavior overrides can be added for particular operators via
  `context`. In particular, `context` is expected to contain custom behavior
  for all operators that take an iterable as input.

  Example:
    parallel_call(
        context, as_future(kd.fn(I.x + I.y)), x=as_future(2), y=as_future(3))
    # returns a future equivalent to as_future(5)

  Args:
    executor: The executor to use for computations.
    context: The execution context to use for the parallel call.
    fn: The future with the functor to be called, typically created via kd.fn().
    *args: The parallel versions of the positional arguments to pass to the
      call.
    return_type_as: The return type of the parallel call is expected to be the
      same as the return type of this expression. In most cases, this will be a
      literal of the corresponding type. This needs to be specified if the
      functor does not return a DataSlice, in other words if the parallel
      version does not return a future to a DataSlice.
    **kwargs: The parallel versions of the keyword arguments to pass to the
      call.

  Returns:
    The parallel value containing the result of the call.
  """
  args, kwargs = arolla.optools.fix_trace_args_kwargs(args, kwargs)
  return_type_as = M.core.default_if_unspecified(
      return_type_as,
      as_future(data_slice.DataSlice),
  )

  # We need async_eval here since `fn` is a future. `async_eval` will wait on
  # any argument that is a future before exeucting the async operator, but all
  # other arguments are passed as is, including tuples of futures. So we wrap
  # the rest into a tuple so that async_eval does not wait on the
  # args/kwargs/return_type_as futures since the transformed parallel functor
  # expects parallel arguments for those.
  return unwrap_future_to_parallel(
      async_eval(
          executor,
          _parallel_call_impl,
          executor,
          context,
          fn,
          (args, return_type_as, kwargs),
          optools.unified_non_deterministic_arg(),
      )
  )


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.parallel_call_fn_returning_stream',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        _expect_future_data_slice(P.fn),
        # TODO: share the same constraints with parallel_call.
    ],
)
def parallel_call_fn_returning_stream(
    executor,
    fn,
    *args,
    return_type_as=arolla.unspecified(),
    **kwargs,
):
  """Calls the functor returning a stream via the parallel evaluation.

  This operator is intented to be used as a parallel version of
  `functor.call_fn_returning_stream_when_parallel`,
  therefore all inputs except `context` must have parallel types
  (futures/streams/tuples thereof).
  See execution_config.proto for more details about parallel types.

  Args:
    executor: The executor to use for computations.
    fn: The future with the functor to be called, typically created via kd.fn().
      The functor must return a stream when called.
    *args: The parallel versions of the positional arguments to pass to the
      call.
    return_type_as: The return type of the parallel call is expected to be the
      same as the return type of this expression. In most cases, this will be a
      literal of the corresponding type. This needs to be specified if the
      functor does not return a DataSlice, in other words if the parallel
      version does not return a future to a DataSlice.
    **kwargs: The parallel versions of the keyword arguments to pass to the
      call.

  Returns:
    The parallel value containing the result of the call.
  """
  args, kwargs = arolla.optools.fix_trace_args_kwargs(args, kwargs)
  return_type_as = M.core.default_if_unspecified(
      return_type_as,
      stream_make(),
  )

  return unwrap_future_to_stream(
      async_eval(
          executor,
          functor.call,
          fn,
          future_from_parallel(executor, args),
          return_type_as,
          future_from_parallel(executor, kwargs),
          optools.unified_non_deterministic_arg(),
      )
  )


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.stream_flat_map_chain',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream),
        qtype_utils.expect_data_slice(P.fn),
    ],
)
def stream_flat_map_chain(
    executor, stream, fn, *, value_type_as=data_slice.DataSlice
):
  """Executes flat maps over the given stream.

  `fn` is called for each item in the input stream, and it must return a new
  stream. The streams returned by `fn` are then chained to produce the final
  result.

  Example:
      ```
      parallel.stream_flat_map_interleaved(
          parallel.get_default_executor(),
          parallel.stream_make(1, 10),
          lambda x: parallel.stream_make(x, x * 2, x * 3),
      )
      ```
      result: A stream with items [1, 2, 3, 10, 20, 30].

  Args:
    executor: An executor for scheduling asynchronous operations.
    stream: The stream to iterate over.
    fn: The function to be executed for each item in the stream. It will receive
      the stream item as the positional argument and must return a stream of
      values compatible with value_type_as.
    value_type_as: The type to use as element type of the resulting stream.

  Returns:
    The resulting interleaved results of `fn` calls.
  """
  return stream_chain_from_stream(
      stream_map(
          executor,
          stream,
          fn,
          value_type_as=stream_make(value_type_as=value_type_as),
      )
  )


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.stream_flat_map_interleaved',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream),
        qtype_utils.expect_data_slice(P.fn),
    ],
)
def stream_flat_map_interleaved(
    executor, stream, fn, *, value_type_as=data_slice.DataSlice
):
  """Executes flat maps over the given stream.

  `fn` is called for each item in the input stream, and it must return a new
  stream. The streams returned by `fn` are then interleaved to produce the final
  result. Note that while the internal order of items within each stream
  returned by `fn` is preserved, the overall order of items from different
  streams is not guaranteed.

  Example:
      ```
      parallel.stream_flat_map_interleaved(
          parallel.get_default_executor(),
          parallel.stream_make(1, 10),
          lambda x: parallel.stream_make(x, x * 2, x * 3),
      )
      ```
      result: A stream with items {1, 2, 3, 10, 20, 30}. While the relative
        order within {1, 2, 3} and {10, 20, 30} is guaranteed, the overall order
        of items is unspecified. For instance, the following orderings are both
        possible:
         * [1, 10, 2, 20, 3, 30]
         * [10, 20, 30, 1, 2, 3]

  Args:
    executor: An executor for scheduling asynchronous operations.
    stream: The stream to iterate over.
    fn: The function to be executed for each item in the stream. It will receive
      the stream item as the positional argument and must return a stream of
      values compatible with value_type_as.
    value_type_as: The type to use as element type of the resulting stream.

  Returns:
    The resulting interleaved results of `fn` calls.
  """
  return stream_interleave_from_stream(
      stream_map_unordered(
          executor,
          stream,
          fn,
          value_type_as=stream_make(value_type_as=value_type_as),
      )
  )


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_reduce',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_data_slice(P.fn),
        qtype_utils.expect_stream(P.stream),
    ],
    qtype_inference_expr=get_stream_qtype(P.initial_value),
    deterministic=False,
)
def stream_reduce(executor, fn, stream, initial_value):
  """Reduces a stream by iteratively applying a functor `fn`.

  This operator applies `fn` sequentially to an accumulating value and each
  item of the `stream`. The process begins with `initial_value`, then follows
  this pattern:

           value_0 = initial_value
           value_1 = fn(value_0, stream[0])
           value_2 = fn(value_1, stream[1])
                  ...

  The result of the reduction is the final computed value.

  Args:
    executor: The executor to use for computations.
    fn: A binary function that takes two positional arguments -- the current
      accumulating value and the next item from the stream -- and returns a new
      value. It's expected to return a value of the same type as
      `initial_value`.
    stream: The input stream.
    initial_value: The initial value.

  Returns:
    A stream with a single item containing the final result of the reduction.
  """
  raise NotImplementedError('implemented in the backend')


@arolla.optools.as_backend_operator(
    'koda_internal.parallel._stream_while_returns',
    qtype_inference_expr=get_stream_qtype(P.initial_returns),
)
def _stream_while_returns(
    executor,
    condition_fn,
    body_fn,
    initial_returns,
    initial_state,
    non_deterministic,
):
  """(internal) Repeatedly applies a body functor while a condition is met.

  Each iteration, the operator passes current state variables (including
  `returns`) as keyword arguments to `condition_fn` and `body_fn`. The loop
  continues if `condition_fn` returns `present`. State variables are then
  updated from `body_fn`'s namedtuple return value.

  Args:
    executor: The executor to use for computations.
    condition_fn: A functor that accepts state variables as keyword arguments
      and returns a MASK data-item, either directly or as a single-item stream.
      A `present` value indicates the loop should continue; `missing` indicates
      it should stop.
    body_fn: A functor that accepts state variables as keyword arguments and
      returns a namedtuple (see `kd.namedtuple`) containing updated values for a
      subset of the state variables. These updated values must retain their
      original types.
    initial_returns: The initial value for the `returns` state variable.
    initial_state: Initial values for state variables.
    non_deterministic: Non-deterministic token.

  Returns:
    A single-item stream with the final value of the `returns` state variable.
  """
  raise NotImplementedError('implemented in the backend')


@arolla.optools.as_backend_operator(
    'koda_internal.parallel._stream_while_yields',
    qtype_inference_expr=get_stream_qtype(P.initial_yields),
)
def _stream_while_yields(
    executor,
    condition_fn,
    body_fn,
    yields_param_name,
    initial_yields,
    initial_state,
    non_deterministic,
):
  """(internal) Repeatedly applies a body functor while a condition is met.

  Each iteration, the operator passes current state variables (excluding
  `yields`) as keyword arguments to `condition_fn` and `body_fn`. The loop
  continues if `condition_fn` returns `present`. State variables are then
  updated from `body_fn`'s namedtuple return value. If `body_fn` also returns
  a `yields` variable, these individual values are appended to form
  the resulting stream.

  Args:
    executor: The executor to use for computations.
    condition_fn: A functor that accepts state variables (excluding `yields`) as
      keyword arguments and returns a MASK data-item, either directly or as a
      single-item stream. A `present` value indicates the loop should continue;
      `missing` indicates it should stop.
    body_fn: A functor that accepts state variables (excluding `yields`) as
      keyword arguments and returns a namedtuple (see `kd.namedtuple`)
      containing updated values for a subset of the state variables. These
      updated values must retain their original types.
    yields_param_name: Name of the "yield" state variable.
    initial_yields: The initial value for the "yields' state variable.
    initial_state: Initial values for state variables.
    non_deterministic: Non-deterministic token.

  Returns:
    A stream formed by concatenating the initial `yields` with all
    subsequent `yields` values produced by the `body_fn`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.stream_while',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_data_slice(P.condition_fn),
        qtype_utils.expect_data_slice(P.body_fn),
        qtype_utils.expect_stream_or_unspecified(P.yields),
        qtype_utils.expect_stream_or_unspecified(P.yields_interleaved),
        (
            (
                ((1 & (P.returns != arolla.UNSPECIFIED)) | 0)
                + ((1 & (P.yields != arolla.UNSPECIFIED)) | 0)
                + ((1 & (P.yields_interleaved != arolla.UNSPECIFIED)) | 0)
            )
            == 1,
            (
                'exactly one of `returns`, `yields`, or `yields_interleaved`'
                ' must be specified'
            ),
        ),
    ],
    deterministic=False,
)
def stream_while(
    executor,
    condition_fn,
    body_fn,
    *,
    returns=arolla.unspecified(),
    yields=arolla.unspecified(),
    yields_interleaved=arolla.unspecified(),
    **initial_state,
):
  """Repeatedly applies a body functor while a condition is met.

  Each iteration, the operator passes current state variables (including
  `returns`, if specified) as keyword arguments to `condition_fn` and `body_fn`.
  The loop continues if `condition_fn` returns `present`. State variables are
  then updated from `body_fn`'s namedtuple return value.

  This operator always returns a stream, with the concrete behaviour
  depending on whether `returns`, `yields`, or `yields_interleaved` was
  specified (exactly one of them must be specified):

  - `returns`: a single-item stream with the final value of the `returns` state
    variable;

  - `yields`: a stream created by chaining the initial `yields` stream with any
    subsequent streams produced by the `body_fn`;

  - `yields_interleaved`: the same as for `yields`, but instead of being chained
    the streams are interleaved.

  Args:
    executor: The executor to use for computations.
    condition_fn: A functor that accepts state variables (including `returns`,
      if specified) as keyword arguments and returns a MASK data-item, either
      directly or as a single-item stream. A `present` value indicates the loop
      should continue; `missing` indicates it should stop.
    body_fn: A functor that accepts state variables *including `returns`, if
      specified) as keyword arguments and returns a namedtuple (see
      `kd.namedtuple`) containing updated values for a subset of the state
      variables. These updated values must retain their original types.
    returns: If present, the initial value of the 'returns' state variable.
    yields: If present, the initial value of the 'yields' state variable.
    yields_interleaved: If present, the initial value of the
      `yields_interleaved` state variable.
    **initial_state: Initial values for state variables.

  Returns:
    If `returns` is a state variable, the value of `returns` when the loop
    ended. Otherwise, a stream combining the values of `yields` or
    `yields_interleaved` from each body invocation.
  """
  initial_state = arolla.optools.fix_trace_kwargs(initial_state)
  return arolla.types.DispatchOperator(
      'executor, condition_fn, body_fn, returns, yields, yields_interleaved,'
      ' initial_state, non_deterministic',
      returns_case=arolla.types.DispatchCase(
          _stream_while_returns(
              P.executor,
              P.condition_fn,
              P.body_fn,
              P.returns,
              P.initial_state,
              P.non_deterministic,
          ),
          condition=(P.returns != arolla.UNSPECIFIED),
      ),
      yields_case=arolla.types.DispatchCase(
          arolla.abc.bind_op(
              stream_chain_from_stream,
              _stream_while_yields(
                  P.executor,
                  P.condition_fn,
                  P.body_fn,
                  arolla.text('yields'),
                  P.yields,
                  P.initial_state,
                  P.non_deterministic,
              ),
              P.non_deterministic,
          ),
          condition=(P.yields != arolla.UNSPECIFIED),
      ),
      yields_interleaved_case=arolla.types.DispatchCase(
          arolla.abc.bind_op(
              stream_interleave_from_stream,
              _stream_while_yields(
                  P.executor,
                  P.condition_fn,
                  P.body_fn,
                  arolla.text('yields_interleaved'),
                  P.yields_interleaved,
                  P.initial_state,
                  P.non_deterministic,
              ),
              P.non_deterministic,
          ),
          condition=(P.yields_interleaved != arolla.UNSPECIFIED),
      ),
  )(
      executor,
      condition_fn,
      body_fn,
      returns,
      yields,
      yields_interleaved,
      initial_state,
      optools.unified_non_deterministic_arg(),
  )


@arolla.optools.as_backend_operator(
    'koda_internal.parallel._stream_for_returns',
    qtype_inference_expr=get_stream_qtype(P.initial_returns),
)
def _stream_for_returns(
    executor,
    stream,
    body_fn,
    finalize_fn,
    condition_fn,
    initial_returns,
    initial_state,
    non_deterministic,
):
  """(internal) Executes a loop over the given iterable."""
  raise NotImplementedError('implemented in the backend')


@arolla.optools.as_backend_operator(
    'koda_internal.parallel._stream_for_yields',
    qtype_inference_expr=get_stream_qtype(P.initial_yields),
)
def _stream_for_yields(
    executor,
    stream,
    body_fn,
    finalize_fn,
    condition_fn,
    yields_param_name,
    initial_yields,
    initial_state,
    non_deterministic,
):
  """(internal) Executes a loop over the given iterable."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.stream_for',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream),
        qtype_utils.expect_data_slice(P.body_fn),
        qtype_utils.expect_data_slice_or_unspecified(P.finalize_fn),
        qtype_utils.expect_data_slice_or_unspecified(P.condition_fn),
        (
            (
                ((1 & (P.returns != arolla.UNSPECIFIED)) | 0)
                + ((1 & (P.yields != arolla.UNSPECIFIED)) | 0)
                + ((1 & (P.yields_interleaved != arolla.UNSPECIFIED)) | 0)
            )
            == 1,
            (
                'exactly one of `returns`, `yields`, or `yields_interleaved`'
                ' must be specified'
            ),
        ),
    ],
    deterministic=False,
)
def stream_for(
    executor,
    stream,
    body_fn,
    *,
    finalize_fn=arolla.unspecified(),
    condition_fn=arolla.unspecified(),
    returns=arolla.unspecified(),
    yields=arolla.unspecified(),
    yields_interleaved=arolla.unspecified(),
    **initial_state,
):
  """Executes a loop over the given stream.

  Exactly one of `returns`, `yields`, `yields_interleaved` must be specified,
  and that dictates what this operator returns.

  When `returns` is specified, it is one more variable added to `initial_state`,
  and the value of that variable at the end of the loop is returned in a single-
  item stream.

  When `yields` is specified, it must be an stream, and the value
  passed there, as well as the values set to this variable in each
  stream of the loop, are chained to get the resulting stream.

  When `yields_interleaved` is specified, the behavior is the same as `yields`,
  but the values are interleaved instead of chained.

  The behavior of the loop is equivalent to the following pseudocode (with
  a simplification that `stream` is an `iterable`):

    state = initial_state  # Also add `returns` to it if specified.
    while condition_fn(state):
      item = next(iterable)
      if item == <end-of-iterable>:
        upd = finalize_fn(**state)
      else:
        upd = body_fn(item, **state)
      if yields/yields_interleaved is specified:
        yield the corresponding data from upd, and remove it from upd.
      state.update(upd)
      if item == <end-of-iterable>:
        break
    if returns is specified:
      yield state['returns']

  Args:
    executor: The executor to use for computations.
    stream: The stream to iterate over.
    body_fn: The function to be executed for each item in the stream. It will
      receive the stream item as the positional argument, and the loop variables
      as keyword arguments (excluding `yields`/`yields_interleaved` if those are
      specified), and must return a namedtuple with the new values for some or
      all loop variables (including `yields`/`yields_interleaved` if those are
      specified).
    finalize_fn: The function to be executed when the stream is exhausted. It
      will receive the same arguments as `body_fn` except the positional
      argument, and must return the same namedtuple. If not specified, the state
      at the end will be the same as the state after processing the last item.
      Note that finalize_fn is not called if condition_fn ever returns false.
    condition_fn: The function to be executed to determine whether to continue
      the loop. It will receive the loop variables as keyword arguments, and
      must return a MASK scalar. Can be used to terminate the loop early without
      processing all items in the stream. If not specified, the loop will
      continue until the stream is exhausted.
    returns: The loop variable that holds the return value of the loop.
    yields: The loop variables that holds the values to yield at each iteration,
      to be chained together.
    yields_interleaved: The loop variables that holds the values to yield at
      each iteration, to be interleaved.
    **initial_state: The initial state of the loop variables.

  Returns:
    Either a stream with a single returns value or a stream of yielded values.
  """
  initial_state = arolla.optools.fix_trace_kwargs(initial_state)
  return arolla.types.DispatchOperator(
      'executor, stream, body_fn, finalize_fn, condition_fn, returns, yields,'
      ' yields_interleaved, initial_state, non_deterministic',
      returns_case=arolla.types.DispatchCase(
          _stream_for_returns(
              P.executor,
              P.stream,
              P.body_fn,
              P.finalize_fn,
              P.condition_fn,
              P.returns,
              P.initial_state,
              P.non_deterministic,
          ),
          condition=(P.returns != arolla.UNSPECIFIED),
      ),
      yields_case=arolla.types.DispatchCase(
          arolla.abc.bind_op(
              stream_chain_from_stream,
              _stream_for_yields(
                  P.executor,
                  P.stream,
                  P.body_fn,
                  P.finalize_fn,
                  P.condition_fn,
                  arolla.text('yields'),
                  P.yields,
                  P.initial_state,
                  P.non_deterministic,
              ),
              P.non_deterministic,
          ),
          condition=(P.yields != arolla.UNSPECIFIED),
      ),
      yields_interleaved_case=arolla.types.DispatchCase(
          arolla.abc.bind_op(
              stream_interleave_from_stream,
              _stream_for_yields(
                  P.executor,
                  P.stream,
                  P.body_fn,
                  P.finalize_fn,
                  P.condition_fn,
                  arolla.text('yields_interleaved'),
                  P.yields_interleaved,
                  P.initial_state,
                  P.non_deterministic,
              ),
              P.non_deterministic,
          ),
          condition=(P.yields_interleaved != arolla.UNSPECIFIED),
      ),
  )(
      executor,
      stream,
      body_fn,
      finalize_fn,
      condition_fn,
      returns,
      yields,
      yields_interleaved,
      initial_state,
      optools.unified_non_deterministic_arg(),
  )


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_call',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_data_slice(P.fn),
    ],
    qtype_inference_expr=M.qtype.conditional_qtype(
        bootstrap.is_stream_qtype(P.return_type_as),
        P.return_type_as,
        get_stream_qtype(P.return_type_as),
    ),
    deterministic=False,
)
def stream_call(
    executor, fn, *args, return_type_as=data_slice.DataSlice, **kwargs
):
  """Calls a functor on the given executor and yields the result(s) as a stream.

  For stream arguments tagged with `stream_await`, `stream_call` first
  awaits the corresponding input streams. Each of these streams is expected to
  yield exactly one item, which is then passed as the argument to the functor
  `fn`. If a labeled stream is empty or yields more than one item, it is
  considered an error.

  The `return_type_as` parameter specifies the return type of the functor `fn`.
  Unless the return type is already a stream, the result of `stream_call` is
  a `STREAM[return_type]` storing a single value returned by the functor.
  However, if `return_type_as` is a stream, the result of `stream_call` is
  of the same stream type, holding the same items as the stream returned by
  the functor.

  IMPORTANT: The current implementation supports the case when `return_type_as`
  is non-stream while the functor returns a stream, in which case
  `stream_call` works the same way as if `return_type_as` were a stream.
  This behaviour isn't part of the official API and is subject to change. If it
  becomes critical for your application's functionality or blocks you from using
  this operator, please contact us!

  Args:
    executor: The executor to use for computations.
    fn: The functor to be called, typically created via kd.fn().
    *args: The positional arguments to pass to the call. The stream arguments
      tagged with `stream_await` will be awaited before the call, and expected
      to yield exactly one item.
    return_type_as: The return type of the functor `fn` call.
    **kwargs: The keyword arguments to pass to the call. Scalars will be
      auto-boxed to DataItems.

  Returns:
    If the return type of the functor (as specified by `return_type_as`) is
    a non-stream type, the result of `stream_call` is a single-item stream
    with the functor's return value. Otherwise, the result is a stream of
    the same type as `return_type_as`, containing the same items as the stream
    returned by the functor.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.streams.await_'])
@optools.as_lambda_operator('koda_internal.parallel.await')
def stream_await(arg):
  """Indicates to kd.streams.call that the argument should be awaited.

  This operator acts as a marker. When the returned value is passed to
  `kd.streams.call`, it signals that `kd.streams.call` should await
  the underlying stream to yield a single item. This single item is then
  passed to the functor.

  Importantly, `stream_await` itself does not perform any awaiting or blocking.

  If the input `arg` is not a stream, this operators returns `arg` unchanged.

  Note: `kd.streams.call` expects an awaited stream to yield exactly one item.
  Producing zero or more than one item from an awaited stream will result in
  an error during the `kd.streams.call` evaluation.

  Args:
    arg: The input argument (the operator has effect only if `arg` is a stream).

  Returns:
    If `arg` was a stream, it gets labeled with 'AWAIT'. If `arg` was not
    a stream, `arg` is returned without modification.
  """
  return arolla.types.DispatchOperator(
      'arg',
      stream_case=arolla.types.DispatchCase(
          M.derived_qtype.downcast(
              M.derived_qtype.get_labeled_qtype(
                  M.qtype.qtype_of(P.arg),
                  'AWAIT',
              ),
              P.arg,
          ),
          condition=bootstrap.is_stream_qtype(P.arg),
      ),
      default=P.arg,
  )(arg)


@optools.add_to_registry(aliases=['kd.streams.unsafe_blocking_await'])
@optools.as_backend_operator(
    'koda_internal.parallel.unsafe_blocking_await',
    qtype_constraints=[
        qtype_utils.expect_stream(P.stream),
    ],
    qtype_inference_expr=M.qtype.get_value_qtype(P.stream),
)
def unsafe_blocking_await(stream):
  """Blocks until the given stream yields a single item.

  IMPORTANT: This operator is inherently unsafe and should be used with extreme
  caution. It's primarily intended for transitional periods when migrating
  a complex, synchronous computation to a concurrent model, enabling incremental
  changes instead of a complete migration in one step.

  The main danger stems from its blocking nature: it blocks the calling thread
  until the stream is ready. However, if the task responsible for filling
  the stream is also scheduled on the same executor, and all executor threads
  become blocked, that task may never execute, leading to a deadlock.

  While seemingly acceptable initially, prolonged or widespread use of this
  operator will eventually cause deadlocks, requiring a non-trivial refactoring
  of your computation.

  BEGIN-GOOGLE-INTERNAL
  Note: While this operator is relatively safe to use with fibers, it's still
  NOT recommended for permanent use.
  END-GOOGLE-INTERNAL

  Args:
    stream: A single-item input stream.

  Returns:
    The single item from the stream.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_from_future',
    qtype_constraints=[
        qtype_utils.expect_future(P.future),
    ],
    qtype_inference_expr=get_stream_qtype(M.qtype.get_value_qtype(P.future)),
)
def stream_from_future(future):
  """Creates a stream from the given future. It has 1 element or an error."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.future_from_single_value_stream',
    qtype_constraints=[
        qtype_utils.expect_stream(P.stream),
    ],
    qtype_inference_expr=get_future_qtype(M.qtype.get_value_qtype(P.stream)),
)
def future_from_single_value_stream(stream):
  """Creates a future from the given stream.

  The stream must have exactly one value, otherwise an error is raised.

  Args:
    stream: The input stream.

  Returns:
    A future with the single value from the stream.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.future_iterable_from_stream',
    qtype_constraints=[
        qtype_utils.expect_stream(P.stream),
    ],
    qtype_inference_expr=get_future_qtype(
        koda_internal_iterables.get_iterable_qtype(
            M.qtype.get_value_qtype(P.stream)
        )
    ),
)
def future_iterable_from_stream(stream):
  """Creates a future to an iterable from the given stream.

  Args:
    stream: The input stream.

  Returns:
    A future to an iterable with the values from the stream, in the same order.
  """
  raise NotImplementedError('implemented in the backend')


# qtype constraints for everything except executor are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_get_item',
    qtype_constraints=[
        qtype_utils.expect_executor(P.outer_executor),
    ],
)
def _parallel_get_item(outer_executor, outer_x, outer_key):
  """The version of get_item that works directly on parallel types.

  If x is a tuple/namedtuple, gets the corresponding item directly.
  Otherwise uses async_eval to call eager get_item asynchronously.

  Args:
    outer_executor: The executor to use for asynchronous operations.
    outer_x: The tuple/namedtuple/DataSlice to get the item from.
    outer_key: The key to get the item with.

  Returns:
    The item from x with the given key.
  """
  return arolla.types.DispatchOperator(
      'executor, x, key',
      direct_case=arolla.types.DispatchCase(
          view_overloads.get_item(P.x, P.key),
          condition=M.qtype.is_tuple_qtype(P.x)
          | M.qtype.is_namedtuple_qtype(P.x),
      ),
      # Since we handled tuples above, the remaining cases are all working
      # on data slices, so we can use async_eval directly without
      # parallel_from_future etc. This also makes this work both with key
      # as a future and key as a literal, which is required for our replacement.
      default=async_eval(P.executor, view_overloads.get_item, P.x, P.key),
  )(outer_executor, outer_x, outer_key)


@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_if_impl',
)
def _parallel_if_impl(executor, context, cond, yes_fn, no_fn, parallel_args):
  """Implementation helper for _parallel_if."""
  args = parallel_args[0]
  return_type_as = parallel_args[1]
  kwargs = parallel_args[2]
  cond = assertion.with_assertion(
      cond,
      (slices.get_ndim(cond) == 0)
      & (schema_ops.get_schema(cond) == schema_constants.MASK),
      'the condition in kd.if_ must be a MASK scalar',
  )
  return arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
      parallel_call,
      executor,
      context,
      # If M.core.where worked on non-array types, we could take futures
      # to yes_fn/no_fn here and avoid unwrapping and re-wrapping. But it
      # should not matter too much.
      as_future(masking.cond(cond, yes_fn, no_fn)),
      args=args,
      return_type_as=return_type_as,
      kwargs=kwargs,
      **optools.unified_non_deterministic_kwarg(),
  )


# qtype constraints for everything except executor are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_if',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_execution_context(P.context),
    ],
)
def _parallel_if(
    executor,
    context,
    cond,
    yes_fn,
    no_fn,
    args,
    return_type_as,
    kwargs,
):
  """The parallel version of kd.if_."""
  return unwrap_future_to_parallel(
      async_eval(
          executor,
          _parallel_if_impl,
          executor,
          context,
          cond,
          yes_fn,
          no_fn,
          (args, return_type_as, kwargs),
          optools.unified_non_deterministic_arg(),
      )
  )


# Since the stream returned by this operator is immutable, this operator can be
# kept deterministic.
@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.empty_stream_like',
    qtype_constraints=[
        qtype_utils.expect_stream(P.stream),
    ],
    qtype_inference_expr=P.stream,
)
def empty_stream_like(stream):  # pylint: disable=unused-argument
  """Returns an empty stream with the same value type as the given stream."""
  raise NotImplementedError('implemented in the backend')


# This operator takes executor as the second argument because we want to
# pass it to core.map_tuple.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._single_element_stream_from_parallel',
)
def _single_element_stream_from_parallel(arg, executor):
  """Composes stream_from_future and future_from_parallel."""
  return stream_from_future(future_from_parallel(executor, arg))


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._empty_streams_from_value_type_as',
)
def _empty_streams_from_value_type_as(executor, value_type_as):
  """Returns a tuple of streams corresponding to the given value_type_as.

  If value_type_as is future[unspecified], returns an empty tuple.
  Otherwise value_type_as is expected to be a valid parallel value,
  which we convert to a future and return a tuple with a single stream
  of the same type as that future.

  Args:
    executor: The executor to use for asynchronous operations.
    value_type_as: The parallel value derived from the value_type_as argument to
      an iterable operator.

  Returns:
    A tuple of streams corresponding to the given value_type_as.
  """
  return arolla.types.DispatchOperator(
      'inner_executor, inner_value_type_as',
      unspecified_case=arolla.types.DispatchCase(
          tuple_ops.tuple_(),
          condition=bootstrap.is_future_qtype(P.inner_value_type_as)
          & (
              M.qtype.get_value_qtype(P.inner_value_type_as)
              == arolla.UNSPECIFIED
          ),
      ),
      default=tuple_ops.tuple_(
          empty_stream_like(
              _single_element_stream_from_parallel(
                  P.inner_value_type_as, P.inner_executor
              )
          )
      ),
  )(executor, value_type_as)


# qtype constraints for everything except executor are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_stream_chain',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
    ],
)
def _parallel_stream_chain(executor, streams, value_type_as):
  """The parallel version of iterables.chain."""
  streams = M.core.concat_tuples(
      streams, _empty_streams_from_value_type_as(executor, value_type_as)
  )
  return arolla.abc.bind_op(
      stream_chain,
      streams,
      **optools.unified_non_deterministic_kwarg(),
  )


# qtype constraints for everything except executor are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_stream_interleave',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
    ],
)
def _parallel_stream_interleave(executor, streams, value_type_as):
  """The parallel version of iterables.interleave."""
  streams = M.core.concat_tuples(
      streams, _empty_streams_from_value_type_as(executor, value_type_as)
  )
  return arolla.abc.bind_op(
      stream_interleave,
      streams,
      **optools.unified_non_deterministic_kwarg(),
  )


# qtype constraints for everything except executor are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_stream_make',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
    ],
)
def _parallel_stream_make(executor, items, value_type_as):
  """The parallel version of stream_make."""
  item_streams = M.core.map_tuple(
      _single_element_stream_from_parallel,
      items,
      executor,
  )
  return _parallel_stream_chain(executor, item_streams, value_type_as)


# qtype constraints for everything except executor are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_stream_make_unordered',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
    ],
)
def _parallel_stream_make_unordered(executor, items, value_type_as):
  """The parallel version of iterables.make_unordered."""
  item_streams = M.core.map_tuple(
      _single_element_stream_from_parallel,
      items,
      executor,
  )
  return _parallel_stream_interleave(executor, item_streams, value_type_as)


def _create_as_parallel_wrapping_fn():
  """Creates a functor wrapping the argument in as_parallel before forwarding."""
  kind_enum = signature_utils.ParameterKind
  V = input_container.InputContainer('V')  # pylint: disable=invalid-name
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(
          V.fn(V.executor, as_parallel(I.x), return_type_as=V.return_type_as)
      ),
      signature_utils.signature([
          signature_utils.parameter('x', kind_enum.POSITIONAL_ONLY),
      ]),
  )


_AS_PARALLEL_WRAPPING_FN = _create_as_parallel_wrapping_fn()


@optools.as_lambda_operator(
    'koda_internal.parallel._internal_parallel_stream_flat_map_chain',
)
def _internal_parallel_stream_flat_map_chain(
    executor, context, fn, parallel_args
):
  """The parallel version of iterables.flat_map_chain."""
  stream = parallel_args[0]
  value_type_as = parallel_args[1]
  stream_type_as = _single_element_stream_from_parallel(value_type_as, executor)
  transformed_fn = transform(context, fn)
  wrapped_fn = core.clone(
      _AS_PARALLEL_WRAPPING_FN,
      fn=transformed_fn,
      executor=koda_internal_functor.pack_as_literal(executor),
      return_type_as=koda_internal_functor.pack_as_literal(stream_type_as),
  )
  return stream_chain_from_stream(
      stream_map(
          executor,
          stream,
          wrapped_fn,
          value_type_as=stream_type_as,
      )
  )


# qtype constraints for everything except context are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_stream_flat_map_chain',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_execution_context(P.context),
    ],
)
def _parallel_stream_flat_map_chain(
    executor, context, stream, fn, value_type_as
):
  """The parallel version of iterables.flat_map_chain."""
  return unwrap_future_to_parallel(
      async_eval(
          executor,
          _internal_parallel_stream_flat_map_chain,
          executor,
          context,
          fn,
          (stream, value_type_as),
          optools.unified_non_deterministic_arg(),
      )
  )


@optools.as_lambda_operator(
    'koda_internal.parallel._internal_parallel_stream_flat_map_interleaved',
)
def _internal_parallel_stream_flat_map_interleaved(
    executor, context, fn, parallel_args
):
  """The parallel version of iterables.flat_map_interleave."""
  stream = parallel_args[0]
  value_type_as = parallel_args[1]
  stream_type_as = _single_element_stream_from_parallel(value_type_as, executor)
  transformed_fn = transform(context, fn)
  wrapped_fn = core.clone(
      _AS_PARALLEL_WRAPPING_FN,
      fn=transformed_fn,
      executor=koda_internal_functor.pack_as_literal(executor),
      return_type_as=koda_internal_functor.pack_as_literal(stream_type_as),
  )
  return stream_interleave_from_stream(
      stream_map_unordered(
          executor,
          stream,
          wrapped_fn,
          value_type_as=stream_type_as,
      )
  )


# qtype constraints for everything except context are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_stream_flat_map_interleaved',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_execution_context(P.context),
    ],
)
def _parallel_stream_flat_map_interleaved(
    executor, context, stream, fn, value_type_as
):
  """The parallel version of iterables.flat_map_interleaved."""
  return unwrap_future_to_parallel(
      async_eval(
          executor,
          _internal_parallel_stream_flat_map_interleaved,
          executor,
          context,
          fn,
          (stream, value_type_as),
          optools.unified_non_deterministic_arg(),
      )
  )


def _create_second_argument_as_parallel_wrapping_fn():
  """Creates a functor wrapping second argument in as_parallel before forwarding."""
  kind_enum = signature_utils.ParameterKind
  V = input_container.InputContainer('V')  # pylint: disable=invalid-name
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(
          V.fn(
              V.executor, I.x, as_parallel(I.y), return_type_as=V.return_type_as
          )
      ),
      signature_utils.signature([
          signature_utils.parameter('x', kind_enum.POSITIONAL_ONLY),
          signature_utils.parameter('y', kind_enum.POSITIONAL_ONLY),
      ]),
  )


_SECOND_ARGUMENT_AS_PARALLEL_WRAPPING_FN = (
    _create_second_argument_as_parallel_wrapping_fn()
)


@optools.as_lambda_operator(
    'koda_internal.parallel._internal_parallel_stream_reduce',
)
def _internal_parallel_stream_reduce(executor, context, fn, parallel_args):
  """Implementation helper for _parallel_stream_reduce."""
  items = parallel_args[0]
  initial_value = parallel_args[1]
  transformed_fn = transform(context, fn)
  wrapped_fn = core.clone(
      _SECOND_ARGUMENT_AS_PARALLEL_WRAPPING_FN,
      fn=transformed_fn,
      executor=koda_internal_functor.pack_as_literal(executor),
      return_type_as=koda_internal_functor.pack_as_literal(initial_value),
  )
  return unwrap_future_to_parallel(
      future_from_single_value_stream(
          stream_reduce(
              executor,
              wrapped_fn,
              items,
              initial_value,
          )
      )
  )


# qtype constraints for everything except context are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_stream_reduce',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_execution_context(P.context),
    ],
)
def _parallel_stream_reduce(executor, context, fn, items, initial_value):
  """The parallel version of functor.reduce."""
  return unwrap_future_to_parallel(
      async_eval(
          executor,
          _internal_parallel_stream_reduce,
          executor,
          context,
          fn,
          (items, initial_value),
          optools.unified_non_deterministic_arg(),
      )
  )


# qtype constraints for everything except executor are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_stream_reduce_concat',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
    ],
)
def _parallel_stream_reduce_concat(executor, items, initial_value, ndim):
  """The parallel version of iterables.reduce_concat."""
  return async_eval(
      executor,
      iterables.reduce_concat,
      future_iterable_from_stream(items),
      initial_value,
      ndim,
  )


# qtype constraints for everything except executor are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_stream_reduce_updated_bag',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
    ],
)
def _parallel_stream_reduce_updated_bag(executor, items, initial_value):
  """The parallel version of iterables.reduce_updated_bag."""
  return async_eval(
      executor,
      iterables.reduce_updated_bag,
      future_iterable_from_stream(items),
      initial_value,
  )


def _create_loop_condition_wrapping_fn():
  """Adapts a while condition functor to be called in parallel mode.

  We prepend the executor to the arguments of the condition functor (to support
  the condition functor in the parallel form), and convert the result from a
  future to a stream (to adapt from the parallel loop APIs to the stream loop
  APIs).

  Returns:
    A functor that requires the following variables to be set before evaluation:
    - fn: The transformed-to-parallel functor to be called.
    - executor: The executor to be used for the functor call.
  """
  V = input_container.InputContainer('V')  # pylint: disable=invalid-name
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(
          stream_from_future(
              arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
                  functor.call,
                  V.fn,
                  args=M.core.concat_tuples(
                      M.core.make_tuple(V.executor), I.args
                  ),
                  kwargs=I.kwargs,
                  return_type_as=as_future(None),
                  **optools.unified_non_deterministic_kwarg(),
              )
          )
      ),
      signature_utils.ARGS_KWARGS_SIGNATURE,
  )


_LOOP_CONDITION_WRAPPING_FN = _create_loop_condition_wrapping_fn()


def _create_while_body_wrapping_fn():
  """Adapts a while body functor to be called in parallel mode.

  We prepend the executor to the arguments of the body functor (to support
  the condition functor in the parallel form).

  Returns:
    A functor that requires the following variables to be set before evaluation:
    - fn: The transformed-to-parallel functor to be called.
    - executor: The executor to be used for the functor call.
    - empty_yields_namedtuple: A namedtuple with empty yields/yields_interleaved
      streams corresponding to the expected return value from the body functor.
  """
  kind_enum = signature_utils.ParameterKind
  V = input_container.InputContainer('V')  # pylint: disable=invalid-name
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name
  # We use call_and_update_namedtuple instead of just call since we do not
  # know which fields will be in the return namedtuple, so we cannot provide
  # proper return_type_as.
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(
          arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
              functor.call_and_update_namedtuple,
              V.fn,
              args=M.core.make_tuple(V.executor),
              namedtuple_to_update=M.namedtuple.union(
                  I.kwargs,
                  V.empty_yields_namedtuple,
              ),
              kwargs=I.kwargs,
              **optools.unified_non_deterministic_kwarg(),
          ),
      ),
      signature_utils.signature([
          signature_utils.parameter('kwargs', kind_enum.VAR_KEYWORD),
      ]),
  )


_WHILE_BODY_WRAPPING_FN = _create_while_body_wrapping_fn()


def _empty_yields_namedtuple(outer_yields, outer_yields_interleaved):
  """Creates a namedtuple with empty yields/yields_namedtuple streams."""
  return arolla.types.DispatchOperator(
      'yields, yields_interleaved',
      yields_case=arolla.types.DispatchCase(
          M.namedtuple.make(
              yields=empty_stream_like(P.yields),
          ),
          condition=(P.yields != get_future_qtype(arolla.UNSPECIFIED)),
      ),
      yields_interleaved_case=arolla.types.DispatchCase(
          M.namedtuple.make(
              yields_interleaved=empty_stream_like(P.yields_interleaved),
          ),
          condition=(
              P.yields_interleaved != get_future_qtype(arolla.UNSPECIFIED)
          ),
      ),
      default=arolla.M.namedtuple.make(),
  )(outer_yields, outer_yields_interleaved)


@optools.as_lambda_operator(
    'koda_internal.parallel._internal_parallel_stream_while',
)
def _internal_parallel_stream_while(
    executor, context, condition_fn, body_fn, parallel_args
):
  """Implementation helper for _parallel_stream_while."""
  outer_returns = parallel_args[0]
  outer_yields = parallel_args[1]
  outer_yields_interleaved = parallel_args[2]
  outer_initial_state = parallel_args[3]
  outer_transformed_condition_fn = transform(context, condition_fn)
  outer_transformed_condition_fn = core.clone(
      _LOOP_CONDITION_WRAPPING_FN,
      executor=koda_internal_functor.pack_as_literal(executor),
      fn=outer_transformed_condition_fn,
  )
  outer_transformed_body_fn = transform(context, body_fn)
  empty_yields_namedtuple = _empty_yields_namedtuple(
      outer_yields, outer_yields_interleaved
  )
  outer_transformed_body_fn = core.clone(
      _WHILE_BODY_WRAPPING_FN,
      executor=koda_internal_functor.pack_as_literal(executor),
      fn=outer_transformed_body_fn,
      empty_yields_namedtuple=koda_internal_functor.pack_as_literal(
          empty_yields_namedtuple
      ),
  )
  outer_executor = executor
  return arolla.types.DispatchOperator(
      'executor, returns, yields, yields_interleaved, initial_state,'
      ' transformed_condition_fn, transformed_body_fn, non_deterministic_leaf',
      returns_case=arolla.types.DispatchCase(
          _replace_non_deterministic_leaf_with_param(
              unwrap_future_to_parallel(
                  future_from_single_value_stream(
                      _stream_while_returns(
                          P.executor,
                          P.transformed_condition_fn,
                          P.transformed_body_fn,
                          P.returns,
                          P.initial_state,
                          optools.unified_non_deterministic_arg(),
                      )
                  )
              )
          ),
          condition=P.returns != get_future_qtype(arolla.UNSPECIFIED),
      ),
      yields_case=arolla.types.DispatchCase(
          _replace_non_deterministic_leaf_with_param(
              stream_chain_from_stream(
                  _stream_while_yields(
                      P.executor,
                      P.transformed_condition_fn,
                      P.transformed_body_fn,
                      arolla.text('yields'),
                      P.yields,
                      P.initial_state,
                      optools.unified_non_deterministic_arg(),
                  )
              )
          ),
          condition=P.yields != get_future_qtype(arolla.UNSPECIFIED),
      ),
      yields_interleaved_case=arolla.types.DispatchCase(
          _replace_non_deterministic_leaf_with_param(
              stream_interleave_from_stream(
                  _stream_while_yields(
                      P.executor,
                      P.transformed_condition_fn,
                      P.transformed_body_fn,
                      arolla.text('yields_interleaved'),
                      P.yields_interleaved,
                      P.initial_state,
                      optools.unified_non_deterministic_arg(),
                  )
              )
          ),
          condition=P.yields_interleaved
          != get_future_qtype(arolla.UNSPECIFIED),
      ),
  )(
      outer_executor,
      outer_returns,
      outer_yields,
      outer_yields_interleaved,
      outer_initial_state,
      outer_transformed_condition_fn,
      outer_transformed_body_fn,
      py_boxing.NON_DETERMINISTIC_TOKEN_LEAF,
  )


# qtype constraints for everything except context are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_stream_while',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_execution_context(P.context),
    ],
)
def _parallel_stream_while(
    executor,
    context,
    condition_fn,
    body_fn,
    returns,
    yields,
    yields_interleaved,
    initial_state,
):
  """The parallel version of functor.while_."""
  return unwrap_future_to_parallel(
      async_eval(
          executor,
          _internal_parallel_stream_while,
          executor,
          context,
          condition_fn,
          body_fn,
          (returns, yields, yields_interleaved, initial_state),
          optools.unified_non_deterministic_arg(),
      )
  )


def _create_for_body_wrapping_fn():
  """Adapts a for body functor to be called in parallel mode.

  We prepend the executor to the arguments of the body functor (to support
  the condition functor in the parallel form), and convert the positional
  `x` argument to the parallel form.

  Returns:
    A functor that requires the following variables to be set before evaluation:
    - fn: The transformed-to-parallel functor to be called.
    - executor: The executor to be used for the functor call.
    - empty_yields_namedtuple: A namedtuple with empty yields/yields_interleaved
      streams corresponding to the expected return value from the body functor.
  """
  kind_enum = signature_utils.ParameterKind
  V = input_container.InputContainer('V')  # pylint: disable=invalid-name
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name
  # We use call_and_update_namedtuple instead of just call since we do not
  # know which fields will be in the return namedtuple, so we cannot provide
  # proper return_type_as.
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(
          arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
              functor.call_and_update_namedtuple,
              V.fn,
              args=M.core.make_tuple(V.executor, as_parallel(I.x)),
              namedtuple_to_update=M.namedtuple.union(
                  I.kwargs,
                  V.empty_yields_namedtuple,
              ),
              kwargs=I.kwargs,
              **optools.unified_non_deterministic_kwarg(),
          ),
      ),
      signature_utils.signature([
          signature_utils.parameter('x', kind_enum.POSITIONAL_ONLY),
          signature_utils.parameter('kwargs', kind_enum.VAR_KEYWORD),
      ]),
  )


_FOR_BODY_WRAPPING_FN = _create_for_body_wrapping_fn()


def _create_for_finalize_wrapping_fn():
  """Adapts a for finalize functor to be called in parallel mode.

  We prepend the executor to the arguments of the finalize functor (to support
  the condition functor in the parallel form).

  Returns:
    A functor that requires the following variables to be set before evaluation:
    - fn: The transformed-to-parallel functor to be called.
    - executor: The executor to be used for the functor call.
    - empty_yields_namedtuple: A namedtuple with empty yields/yields_interleaved
      streams corresponding to the expected return value from the body functor.
  """
  kind_enum = signature_utils.ParameterKind
  V = input_container.InputContainer('V')  # pylint: disable=invalid-name
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name
  # We use call_and_update_namedtuple instead of just call since we do not
  # know which fields will be in the return namedtuple, so we cannot provide
  # proper return_type_as.
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(
          arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
              functor.call_and_update_namedtuple,
              V.fn,
              args=M.core.make_tuple(V.executor),
              namedtuple_to_update=M.namedtuple.union(
                  I.kwargs,
                  V.empty_yields_namedtuple,
              ),
              kwargs=I.kwargs,
              **optools.unified_non_deterministic_kwarg(),
          ),
      ),
      signature_utils.signature([
          signature_utils.parameter('kwargs', kind_enum.VAR_KEYWORD),
      ]),
  )


_FOR_FINALIZE_WRAPPING_FN = _create_for_finalize_wrapping_fn()


@optools.as_lambda_operator(
    'koda_internal.parallel._internal_parallel_stream_for',
)
def _internal_parallel_stream_for(
    executor, context, body_fn, finalize_fn, condition_fn, parallel_args
):
  """Implementation helper for _parallel_stream_while."""
  outer_stream = parallel_args[0]
  outer_returns = parallel_args[1]
  outer_yields = parallel_args[2]
  outer_yields_interleaved = parallel_args[3]
  outer_initial_state = parallel_args[4]
  empty_yields_namedtuple = _empty_yields_namedtuple(
      outer_yields, outer_yields_interleaved
  )
  outer_transformed_body_fn = core.clone(
      _FOR_BODY_WRAPPING_FN,
      executor=koda_internal_functor.pack_as_literal(executor),
      fn=transform(context, body_fn),
      empty_yields_namedtuple=koda_internal_functor.pack_as_literal(
          empty_yields_namedtuple
      ),
  )
  outer_transformed_finalize_fn = arolla.types.DispatchOperator(
      'executor, context, empty_yields_namedtuple, finalize_fn,'
      ' non_deterministic_leaf',
      unspecified_case=arolla.types.DispatchCase(
          P.finalize_fn,
          condition=P.finalize_fn == arolla.UNSPECIFIED,
      ),
      default=_replace_non_deterministic_leaf_with_param(
          core.clone(
              _FOR_FINALIZE_WRAPPING_FN,
              executor=koda_internal_functor.pack_as_literal(P.executor),
              fn=transform(P.context, P.finalize_fn),
              empty_yields_namedtuple=koda_internal_functor.pack_as_literal(
                  P.empty_yields_namedtuple
              ),
          )
      ),
  )(
      executor,
      context,
      empty_yields_namedtuple,
      finalize_fn,
      py_boxing.NON_DETERMINISTIC_TOKEN_LEAF,
  )
  outer_transformed_condition_fn = arolla.types.DispatchOperator(
      'executor, context, condition_fn, non_deterministic_leaf',
      unspecified_case=arolla.types.DispatchCase(
          P.condition_fn,
          condition=P.condition_fn == arolla.UNSPECIFIED,
      ),
      default=_replace_non_deterministic_leaf_with_param(
          core.clone(
              _LOOP_CONDITION_WRAPPING_FN,
              executor=koda_internal_functor.pack_as_literal(P.executor),
              fn=transform(
                  P.context,
                  P.condition_fn,
              ),
          )
      ),
  )(executor, context, condition_fn, py_boxing.NON_DETERMINISTIC_TOKEN_LEAF)
  outer_executor = executor
  return arolla.types.DispatchOperator(
      'executor, stream, returns, yields, yields_interleaved, initial_state,'
      ' transformed_body_fn, transformed_finalize_fn, transformed_condition_fn,'
      ' non_deterministic_leaf',
      returns_case=arolla.types.DispatchCase(
          _replace_non_deterministic_leaf_with_param(
              unwrap_future_to_parallel(
                  future_from_single_value_stream(
                      _stream_for_returns(
                          P.executor,
                          P.stream,
                          P.transformed_body_fn,
                          P.transformed_finalize_fn,
                          P.transformed_condition_fn,
                          P.returns,
                          P.initial_state,
                          optools.unified_non_deterministic_arg(),
                      )
                  )
              )
          ),
          condition=P.returns != get_future_qtype(arolla.UNSPECIFIED),
      ),
      yields_case=arolla.types.DispatchCase(
          _replace_non_deterministic_leaf_with_param(
              stream_chain_from_stream(
                  _stream_for_yields(
                      P.executor,
                      P.stream,
                      P.transformed_body_fn,
                      P.transformed_finalize_fn,
                      P.transformed_condition_fn,
                      arolla.text('yields'),
                      P.yields,
                      P.initial_state,
                      optools.unified_non_deterministic_arg(),
                  ),
              )
          ),
          condition=P.yields != get_future_qtype(arolla.UNSPECIFIED),
      ),
      yields_interleaved_case=arolla.types.DispatchCase(
          _replace_non_deterministic_leaf_with_param(
              stream_interleave_from_stream(
                  _stream_for_yields(
                      P.executor,
                      P.stream,
                      P.transformed_body_fn,
                      P.transformed_finalize_fn,
                      P.transformed_condition_fn,
                      arolla.text('yields_interleaved'),
                      P.yields_interleaved,
                      P.initial_state,
                      optools.unified_non_deterministic_arg(),
                  ),
              )
          ),
          condition=P.yields_interleaved
          != get_future_qtype(arolla.UNSPECIFIED),
      ),
  )(
      outer_executor,
      outer_stream,
      outer_returns,
      outer_yields,
      outer_yields_interleaved,
      outer_initial_state,
      outer_transformed_body_fn,
      outer_transformed_finalize_fn,
      outer_transformed_condition_fn,
      py_boxing.NON_DETERMINISTIC_TOKEN_LEAF,
  )


# qtype constraints for everything except context are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_stream_for',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_execution_context(P.context),
    ],
)
def _parallel_stream_for(
    executor,
    context,
    stream,
    body_fn,
    finalize_fn,
    condition_fn,
    returns,
    yields,
    yields_interleaved,
    initial_state,
):
  """The parallel version of functor.for_."""
  return unwrap_future_to_parallel(
      async_eval(
          executor,
          _internal_parallel_stream_for,
          executor,
          context,
          body_fn,
          finalize_fn,
          condition_fn,
          (stream, returns, yields, yields_interleaved, initial_state),
          optools.unified_non_deterministic_arg(),
      )
  )


def _create_transform_fn_template():
  """Creates a functor template calling transform operator.

  Before calling the resulting functor, one needs to add a "context" variable
  to it, which should evaluate to the execution context to use.

  Returns:
    A functor that takes a single positional argument, which is a functor to
    be transformed to parallel, and returns its parallel version.
  """
  kind_enum = signature_utils.ParameterKind
  V = input_container.InputContainer('V')  # pylint: disable=invalid-name
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(transform(V.context, I.fn)),
      signature_utils.signature([
          signature_utils.parameter('fn', kind_enum.POSITIONAL_ONLY),
      ]),
  )


_TRANSFORM_FN_TEMPLATE = _create_transform_fn_template()


def _create_transform_fn(context):
  """Returns an expression creating functor from _TRANSFORM_FN_TEMPLATE."""
  return core.clone(
      _TRANSFORM_FN_TEMPLATE,
      context=koda_internal_functor.pack_as_literal(context),
  )


def _create_pointwise_invoke_fn_template():
  """Creates a functor template calling a functor at a given index.

  Before calling the resulting functor, one needs to add four variables to it:
  - `fn` should evaluate to a flat (1D) slice of functors in a parallel version
    (taking and returning futures to DataSlices).
  - `args` should evaluate to a tuple of flat (1D) slices (positional
    arguments).
  - `kwargs` should evaluate to a namedtuple of flat (1D) slices (keyword
    arguments).
  - `executor` should evaluate to the executor to use for converting DataItem
    to DataSlice at the end (see Returns section for details).

  `fn` and the items of `args` and `kwargs` must have the same size.

  Returns:
    A functor that takes a single positional argument `idx`, which is expected
    to be a DataItem, calls `fn[idx]` passing it futures containing `arg[idx]`
    for arg in `args` and `k=v[idx]` for k, v in the items of `kwargs`, takes
    the result which is expected to be a future to a DataItem, and returns
    a future to a 1D DataSlice with that item and size 1.
  """
  kind_enum = signature_utils.ParameterKind
  V = input_container.InputContainer('V')  # pylint: disable=invalid-name
  I = input_container.InputContainer('I')  # pylint: disable=invalid-name
  # Note that "fn" is expected to be a "transformed to parallel" functor, so
  # its inputs must be futures and it returns a future.
  fn = slices.take(V.fn, I.idx)
  args = M.core.map_tuple(slices.take, V.args, I.idx)
  args = M.core.map_tuple(as_future, args)
  kwargs = M.derived_qtype.upcast(M.qtype.qtype_of(V.kwargs), V.kwargs)
  kwargs = M.core.map_tuple(slices.take, kwargs, I.idx)
  kwargs = M.core.map_tuple(as_future, kwargs)
  kwargs = M.core.apply_varargs(
      M.namedtuple.make,
      M.qtype.get_field_names(M.qtype.qtype_of(V.kwargs)),
      kwargs,
  )
  res = arolla.abc.bind_op(  # pytype: disable=wrong-arg-types
      functor.call,
      fn,
      args=M.core.concat_tuples(M.core.make_tuple(V.executor), args),
      return_type_as=as_future(None),
      kwargs=kwargs,
      **optools.unified_non_deterministic_kwarg(),
  )

  @optools.as_lambda_operator('_verify_data_item_and_make_1d')
  def _verify_data_item_and_make_1d(res):
    res = assertion.with_assertion(
        res,
        slices.get_ndim(res) == 0,
        'the functor in kd.map must evaluate to a DataItem',
    )
    return res.repeat(1)

  res = async_eval(V.executor, _verify_data_item_and_make_1d, res)
  res = stream_from_future(res)
  return py_functors_base_py_ext.create_functor(
      introspection.pack_expr(res),
      signature_utils.signature([
          signature_utils.parameter('idx', kind_enum.POSITIONAL_ONLY),
      ]),
  )


_POINTWISE_INVOKE_FN_TEMPLATE = _create_pointwise_invoke_fn_template()


def _create_pointwise_invoke_fn(executor, fn, args, kwargs):
  """Returns an expression creating functor from _POINTWISE_INVOKE_FN_TEMPLATE."""
  return core.clone(
      _POINTWISE_INVOKE_FN_TEMPLATE,
      executor=koda_internal_functor.pack_as_literal(executor),
      fn=koda_internal_functor.pack_as_literal(fn),
      args=koda_internal_functor.pack_as_literal(args),
      kwargs=koda_internal_functor.pack_as_literal(kwargs),
  )


@optools.as_lambda_operator(
    'koda_internal.parallel._internal_parallel_stream_map',
)
def _internal_parallel_stream_map(
    executor,
    context,
    fn,
    args,
    include_missing,
    kwargs,
):
  """Implementation helper for _parallel_stream_map."""
  unique_fns = slices.unique(fn.flatten())
  transform_fn = _create_transform_fn(context)
  unique_transformed_fns = functor.map_(transform_fn, unique_fns)
  transformed_fns = slices.translate(fn, unique_fns, unique_transformed_fns)
  kwargs_tuple = M.derived_qtype.upcast(M.qtype.qtype_of(kwargs), kwargs)
  aligned = M.core.apply_varargs(
      slices.align,
      M.core.concat_tuples(
          M.core.make_tuple(transformed_fns), args, kwargs_tuple
      ),
  )
  aligned_has = M.core.map_tuple(masking.has, aligned)
  transformed_fns = tuple_ops.get_nth(aligned, 0)
  mask_to_call = masking.cond(
      include_missing == slices.bool_(True),
      masking.has(transformed_fns),
      M.core.reduce_tuple(masking.apply_mask, aligned_has),
  )
  transformed_fns = transformed_fns.flatten()
  aligned_args_kwargs = M.core.slice_tuple(aligned, 1, -1)
  args = M.core.slice_tuple(
      aligned_args_kwargs, 0, M.qtype.get_field_count(M.qtype.qtype_of(args))
  )
  kwargs_tuple = M.core.slice_tuple(
      aligned_args_kwargs,
      M.qtype.get_field_count(M.qtype.qtype_of(args)),
      -1,
  )

  @optools.as_lambda_operator('flatten')
  def flatten(x):
    return jagged_shape_ops.flatten(x)

  args = M.core.map_tuple(flatten, args)
  kwargs_tuple = M.core.map_tuple(flatten, kwargs_tuple)
  kwargs = M.derived_qtype.downcast(M.qtype.qtype_of(kwargs), kwargs_tuple)
  indices = slices.index(mask_to_call.flatten()).select_present()
  indices_stream = stream_from_iterable(iterables.from_1d_slice(indices))
  invoke_fn = _create_pointwise_invoke_fn(
      executor, transformed_fns, args, kwargs
  )
  res_stream = stream_chain_from_stream(
      stream_map(
          executor,
          indices_stream,
          invoke_fn,
          value_type_as=stream_make(),
      )
  )
  res_future = _parallel_stream_reduce_concat(
      executor, res_stream, as_future(slices.slice_([])), ndim=as_future(1)
  )

  @optools.as_lambda_operator('restore_shape')
  def restore_shape(res, mask_to_call):
    return jagged_shape_ops.reshape(
        slices.inverse_select(res, mask_to_call.flatten()),
        jagged_shape_ops.get_shape(mask_to_call),
    )

  return async_eval(
      executor,
      restore_shape,
      res_future,
      mask_to_call,
  )


# qtype constraints for everything except context are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_stream_map',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_execution_context(P.context),
    ],
)
def _parallel_stream_map(
    executor,
    context,
    fn,
    args,
    include_missing,
    kwargs,
):
  """The parallel version of functor.map."""
  return unwrap_future_to_future(
      async_eval(
          executor,
          _internal_parallel_stream_map,
          executor,
          context,
          fn,
          future_from_parallel(executor, args),
          include_missing,
          future_from_parallel(executor, kwargs),
          optools.unified_non_deterministic_arg(),
      )
  )


@optools.as_lambda_operator(
    'koda_internal.parallel._internal_parallel_with_assertion',
)
def _internal_parallel_with_assertion(
    x_as_tuple,
    condition,
    message_or_fn,
    args,
):
  """Implementation helper for _parallel_with_assertion."""
  return arolla.abc.bind_op(
      assertion.with_assertion,
      x=x_as_tuple[0],
      condition=condition,
      message_or_fn=message_or_fn,
      args=args,
  )


# qtype constraints for everything except executor are omitted in favor of
# the implicit constraints from the lambda body, to avoid duplication.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel._parallel_with_assertion',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
    ],
)
def _parallel_with_assertion(executor, x, condition, message_or_fn, args):
  """The parallel version of assertion.with_assertion."""
  return unwrap_future_to_parallel(
      async_eval(
          executor,
          _internal_parallel_with_assertion,
          # We wrap x into a tuple to avoid waiting for it if it is a future.
          (x,),
          condition,
          message_or_fn,
          # We do not support streams in args for now.
          future_from_parallel(executor, args),
      )
  )


_DEFAULT_EXECUTION_CONFIG_TEXTPROTO = """
  operator_replacements {
    from_op: "core.make_tuple"
    to_op: "core.make_tuple"
  }
  operator_replacements {
    from_op: "kd.tuple"
    to_op: "kd.tuple"
  }
  operator_replacements {
    from_op: "core.get_nth"
    to_op: "core.get_nth"
    argument_transformation {
      keep_literal_argument_indices: 1
    }
  }
  operator_replacements {
    from_op: "kd.tuples.get_nth"
    to_op: "kd.tuples.get_nth"
    argument_transformation {
      keep_literal_argument_indices: 1
    }
  }
  operator_replacements {
    from_op: "koda_internal.view.get_item"
    to_op: "koda_internal.parallel._parallel_get_item"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      keep_literal_argument_indices: 1
    }
  }
  operator_replacements {
    from_op: "namedtuple.make"
    to_op: "namedtuple.make"
    argument_transformation {
      keep_literal_argument_indices: 0
    }
  }
  operator_replacements {
    from_op: "kd.namedtuple"
    to_op: "kd.namedtuple"
  }
  operator_replacements {
    from_op: "namedtuple.get_field"
    to_op: "namedtuple.get_field"
    argument_transformation {
      keep_literal_argument_indices: 1
    }
  }
  operator_replacements {
    from_op: "kd.tuples.get_namedtuple_field"
    to_op: "kd.tuples.get_namedtuple_field"
    argument_transformation {
      keep_literal_argument_indices: 1
    }
  }
  operator_replacements {
    from_op: "koda_internal.non_deterministic"
    to_op: "koda_internal.non_deterministic"
    argument_transformation {
      keep_literal_argument_indices: 1
    }
  }
  operator_replacements {
    from_op: "kd.functor.call"
    to_op: "koda_internal.parallel.parallel_call"
    argument_transformation {
      arguments: EXECUTOR
      arguments: EXECUTION_CONTEXT
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.functor.call_fn_returning_stream_when_parallel"
    to_op: "koda_internal.parallel.parallel_call_fn_returning_stream"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.functor.if_"
    to_op: "koda_internal.parallel._parallel_if"
    argument_transformation {
      arguments: EXECUTOR
      arguments: EXECUTION_CONTEXT
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.iterables.make"
    to_op: "koda_internal.parallel._parallel_stream_make"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      arguments: NON_DETERMINISTIC_TOKEN
    }
  }
  operator_replacements {
    from_op: "kd.iterables.make_unordered"
    to_op: "koda_internal.parallel._parallel_stream_make_unordered"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.iterables.chain"
    to_op: "koda_internal.parallel._parallel_stream_chain"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      arguments: NON_DETERMINISTIC_TOKEN
    }
  }
  operator_replacements {
    from_op: "kd.iterables.interleave"
    to_op: "koda_internal.parallel._parallel_stream_interleave"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.functor.flat_map_chain"
    to_op: "koda_internal.parallel._parallel_stream_flat_map_chain"
    argument_transformation {
      arguments: EXECUTOR
      arguments: EXECUTION_CONTEXT
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.functor.flat_map_interleaved"
    to_op: "koda_internal.parallel._parallel_stream_flat_map_interleaved"
    argument_transformation {
      arguments: EXECUTOR
      arguments: EXECUTION_CONTEXT
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.functor.reduce"
    to_op: "koda_internal.parallel._parallel_stream_reduce"
    argument_transformation {
      arguments: EXECUTOR
      arguments: EXECUTION_CONTEXT
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.iterables.reduce_concat"
    to_op: "koda_internal.parallel._parallel_stream_reduce_concat"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.iterables.reduce_updated_bag"
    to_op: "koda_internal.parallel._parallel_stream_reduce_updated_bag"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.functor.while_"
    to_op: "koda_internal.parallel._parallel_stream_while"
    argument_transformation {
      arguments: EXECUTOR
      arguments: EXECUTION_CONTEXT
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.functor.for_"
    to_op: "koda_internal.parallel._parallel_stream_for"
    argument_transformation {
      arguments: EXECUTOR
      arguments: EXECUTION_CONTEXT
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.functor.map"
    to_op: "koda_internal.parallel._parallel_stream_map"
    argument_transformation {
      arguments: EXECUTOR
      arguments: EXECUTION_CONTEXT
      arguments: ORIGINAL_ARGUMENTS
    }
  }
  operator_replacements {
    from_op: "kd.assertion.with_assertion"
    to_op: "koda_internal.parallel._parallel_with_assertion"
    argument_transformation {
      arguments: EXECUTOR
      arguments: ORIGINAL_ARGUMENTS
      arguments: NON_DETERMINISTIC_TOKEN
    }
  }
"""

_DEFAULT_EXECUTION_CONFIG = py_expr_eval_py_ext.eval_expr(
    proto_ops.from_proto_bytes(
        text_format.Parse(
            _DEFAULT_EXECUTION_CONFIG_TEXTPROTO,
            execution_config_pb2.ExecutionConfig(),
        ).SerializeToString(),
        'koladata.functor.parallel.ExecutionConfig',
    )
)


# This is a lambda operator with a literal inside so that it's also available in
# C++.
@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.get_default_execution_config'
)
def get_default_execution_config():
  """Returns the default execution config for parallel computation."""
  return literal_operator.literal(_DEFAULT_EXECUTION_CONFIG)


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.get_default_execution_context'
)
def get_default_execution_context():
  """Returns the default execution config for parallel computation."""
  return create_execution_context(get_default_execution_config())
