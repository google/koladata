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
# TODO: disallow creating futures to streams in this operator,
# once we have streams, since passing a stream to an operator expecting a future
# is much more likely to be a bug.
@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.parallel.as_future',
    qtype_inference_expr=M.qtype.conditional_qtype(
        bootstrap.is_future_qtype(P.arg),
        P.arg,
        get_future_qtype(P.arg),
    ),
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
    An stream that chains the given streams, in the given order.
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
    A stream that interleaves the input streams, in unspecified order.
  """
  raise NotImplementedError('implemented in the backend')


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
    deterministic=False,
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


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.parallel.stream_from_iterable',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.iterable),
    ],
    qtype_inference_expr=get_stream_qtype(M.qtype.get_value_qtype(P.iterable)),
    deterministic=False,
)
def stream_from_iterable(iterable):
  """Creates a stream from the given iterable.

  Args:
    iterable: The iterable with the items to be put into the stream.

  Returns:
    A stream with the items from the given iterable, in the same order.
  """
  raise NotImplementedError('implemented in the backend')
