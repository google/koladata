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

"""Internal operators for parallel execution."""

from arolla import arolla
from koladata.operators import bootstrap
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import tuple as tuple_ops
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes


M = arolla.M
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


@arolla.optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.make_asio_executor',
    qtype_constraints=[
        constraints.expect_scalar_integer(P.num_threads),
    ],
    qtype_inference_expr=EXECUTOR,
    custom_boxing_fn_name_per_parameter=dict(
        num_threads=py_boxing.WITH_AROLLA_BOXING,
    ),
    deterministic=False,
)
def make_asio_executor(num_threads=0):
  """Returns a new executor based on boost::asio::thread_pool.

  Args:
    num_threads: The number of threads to use. Must be non-negative; 0 means
      that the number of threads is selected automatically.
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
@optools.add_to_registry(view=None)
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


@optools.add_to_registry(view=None)
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


@optools.add_to_registry(view=None)
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


@optools.add_to_registry(view=None)
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
    qtype_inference_expr=M.qtype.get_value_qtype(P.sstream),
    qtype_constraints=[
        qtype_utils.expect_stream(P.sstream),
        (
            bootstrap.is_stream_qtype(M.qtype.get_value_qtype(P.sstream)),
            (
                'expected a stream of streams, got'
                f' {constraints.name_type_msg(P.sstream)}'
            ),
        ),
    ],
    deterministic=False,
)
def stream_chain_from_stream(sstream):
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
    sstream: A stream of input streams.

  Returns:
    A stream that chains the input streams.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_interleave_from_stream',
    qtype_inference_expr=M.qtype.get_value_qtype(P.sstream),
    qtype_constraints=[
        qtype_utils.expect_stream(P.sstream),
        (
            bootstrap.is_stream_qtype(M.qtype.get_value_qtype(P.sstream)),
            (
                'expected a stream of streams, got'
                f' {constraints.name_type_msg(P.sstream)}'
            ),
        ),
    ],
    deterministic=False,
)
def stream_interleave_from_stream(sstream):
  """Creates a stream that interleaves the given streams.

  The resulting stream has all items from all input streams, and the order of
  items from each stream is preserved. But the order of interleaving of
  different streams can be arbitrary.

  Having unspecified order allows the parallel execution to put the items into
  the result in the order they are computed, potentially increasing the amount
  of parallel processing done.

  Args:
    sstream: A stream of input streams.

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
    'koda_internal.parallel._internal_make_namedtuple_with_names_from',
)
def _internal_make_namedtuple_with_names_from(example_namedtuple, *args):
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


@optools.as_lambda_operator(
    'koda_internal.parallel._internal_future_from_parallel',
    # Note that we do not check the tuple/namedtuple contents here,
    # it will be checked in the recursive call.
    qtype_constraints=[
        qtype_utils.expect_executor(P.outer_executor),
        (
            (
                bootstrap.is_future_qtype(P.outer_arg)
                & ~M.qtype.is_tuple_qtype(M.qtype.get_value_qtype(P.outer_arg))
                & ~M.qtype.is_namedtuple_qtype(
                    M.qtype.get_value_qtype(P.outer_arg)
                )
                & (
                    M.qtype.get_value_qtype(P.outer_arg)
                    != qtypes.NON_DETERMINISTIC_TOKEN
                )
            )
            | (P.outer_arg == qtypes.NON_DETERMINISTIC_TOKEN)
            | M.qtype.is_tuple_qtype(P.outer_arg)
            | M.qtype.is_namedtuple_qtype(P.outer_arg),
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
              tuple_ops.make_tuple,
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
              _internal_make_namedtuple_with_names_from,
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
def future_from_parallel(executor, arg):  # pylint: disable=unused-argument
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
def _internal_parallel_from_future(outer_arg, outer_self_op):
  """Implementation helper for parallel_from_future."""
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
          condition=M.qtype.get_value_qtype(P.arg)
          == qtypes.NON_DETERMINISTIC_TOKEN,
      ),
      default=P.arg,
  )(outer_arg, outer_self_op, optools.unified_non_deterministic_arg())


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.parallel_from_future',
    qtype_constraints=[
        qtype_utils.expect_future(P.arg),
    ],
)
def parallel_from_future(arg):  # pylint: disable=unused-argument
  """Given a future, convert it to a corresponding parallel value.

  More specifically, if the future has a tuple/namedtuple value, convert it to
  a tuple/namedtuple of futures. If the input is a future to the
  non-deterministic token, returns (a new) non-deterministic token.

  Args:
    arg: The value to convert from the future form.

  Returns:
    A value of a parallel type (see as_parallel) corresponding to the given
    future.
  """
  return _internal_parallel_from_future(arg, _internal_parallel_from_future)


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_map',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream),
        qtype_utils.expect_data_slice(P.body_fn),
    ],
    qtype_inference_expr=get_stream_qtype(P.value_type_as),
    deterministic=False,
)
def stream_map(
    executor, stream, body_fn, *, value_type_as=data_slice.DataSlice
):
  """Returns a new stream by applying body_fn to each item in the input stream.

  For each item of the input `stream`, the `body_fn` is called. The single
  resulting item from each call is then written into the new output stream.

  Args:
    executor: An executor for scheduling asynchronous operations.
    stream: The input stream.
    body_fn: The function to be executed for each item of the input stream. It
      will receive an item as the positional argument and its result must be of
      the same type as `value_type_as`.
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


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.parallel.stream_flat_map_chain',
    qtype_constraints=(
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream),
        qtype_utils.expect_data_slice(P.fn),
    ),
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
    qtype_constraints=(
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream),
        qtype_utils.expect_data_slice(P.fn),
    ),
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
        order within {1, 2, 3} and {10, 20, 30} is guarnteed, the overall order
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
    executor: The executor to use for asynchronous operations (if any).
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
