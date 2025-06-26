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

"""Internal operators working with iterables that are not exposed to the user.

An iterable is a derived type from an arolla Sequence, which is intended to be
used to represent streams that need streaming processing in multithreaded
evaluation.
"""

from arolla import arolla
from arolla.derived_qtype import derived_qtype
from koladata.operators import arolla_bridge
from koladata.operators import jagged_shape
from koladata.operators import math
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import random
from koladata.operators import schema
from koladata.operators import slices
from koladata.types import qtypes

P = arolla.P


# This is registered in C++ since it has type-inference-time evaluation,
# which is needed for from_sequence to work since the type argument
# of downcast must be evaluatable at type inference time.
get_iterable_qtype = arolla.abc.lookup_operator(
    'koda_internal.iterables.get_iterable_qtype'
)


@optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'koda_internal.iterables.from_sequence',
    qtype_constraints=[
        arolla.optools.constraints.expect_sequence(P.x),
    ],
)
def from_sequence(x):
  """Converts a sequence to an iterable."""
  return derived_qtype.M.downcast(
      get_iterable_qtype(
          arolla.M.qtype.get_value_qtype(arolla.M.qtype.qtype_of(x))
      ),
      x,
  )


@optools.add_to_registry(view=None)
@optools.as_lambda_operator(
    'koda_internal.iterables.to_sequence',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.x),
    ],
)
def to_sequence(x):
  """Converts an iterable to a sequence."""
  return derived_qtype.M.upcast(arolla.M.qtype.qtype_of(x), x)


@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.iterables.sequence_from_1d_slice',
    qtype_inference_expr=arolla.M.qtype.make_sequence_qtype(qtypes.DATA_SLICE),
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def sequence_from_1d_slice(x):  # pylint: disable=unused-argument
  """Creates an arolla Sequence of DataItems from a 1D DataSlice."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'koda_internal.iterables.sequence_to_1d_slice',
    qtype_inference_expr=qtypes.DATA_SLICE,
    qtype_constraints=[(
        P.x == arolla.M.qtype.make_sequence_qtype(qtypes.DATA_SLICE),
        (
            'expected a sequence of DataItems, got'
            f' {arolla.optools.constraints.name_type_msg(P.x)}'
        ),
    )],
)
def sequence_to_1d_slice(x):  # pylint: disable=unused-argument
  """Creates an 1D DataSlice from an arolla Sequence of DataItems."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.iterables.shuffle',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.x),
    ],
)
def shuffle(x):
  """Shuffles the items in the iterable.

  This operation is intentionally non-deterministic, and should be used to
  obtain a random order in an iterable in cases where in the parallel
  execution environment the order can be arbitrary.

  Args:
    x: The iterable to shuffle.

  Returns:
    A shuffled iterable.
  """
  input_seq = to_sequence(x)
  size = arolla.M.seq.size(input_seq)
  shape = jagged_shape.new(arolla_bridge.to_data_slice(size))
  # We use Koda rather than Arolla random to pick up the "nondeterministic
  # default seed" behavior.
  random_values = random.randint_shaped(shape)
  random_order = slices.ordinal_rank(random_values)
  random_order_seq = sequence_from_1d_slice(random_order)
  random_order_seq = arolla.M.seq.map(
      arolla_bridge.to_arolla_int64, random_order_seq
  )
  shuffled_seq = arolla.M.seq.map(
      arolla.M.seq.at,
      # Copying the sequence is cheap-ish since it has a shared_ptr inside.
      arolla.M.seq.repeat(input_seq, size),
      random_order_seq,
  )
  return from_sequence(shuffled_seq)


@optools.add_to_registry(view=None)
@optools.as_backend_operator(
    'koda_internal.iterables.sequence_chain',
    qtype_inference_expr=arolla.M.qtype.get_value_qtype(P.sequences),
    qtype_constraints=[
        arolla.optools.constraints.expect_sequence(P.sequences),
        (
            arolla.M.qtype.is_sequence_qtype(
                arolla.M.qtype.get_value_qtype(P.sequences)
            ),
            'expected a sequence of sequences',
        ),
    ],
)
def sequence_chain(sequences):  # pylint: disable=unused-argument
  """Chains the given sequences into one."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(view=None)
@optools.as_lambda_operator(
    'koda_internal.iterables.sequence_interleave',
    qtype_constraints=[
        arolla.optools.constraints.expect_sequence(P.sequences),
        (
            arolla.M.qtype.is_sequence_qtype(
                arolla.M.qtype.get_value_qtype(P.sequences)
            ),
            'expected a sequence of sequences',
        ),
    ],
)
def sequence_interleave(sequences):
  """Interleaves the given sequences into one randomly.

  This operation is intentionally non-deterministic, and should be used to
  obtain a random interleaving order in cases where in the parallel
  execution environment the order can be arbitrary.

  Args:
    sequences: A sequence of sequences to interleave.

  Returns:
    An interleaved sequence.
  """
  chain_res = sequence_chain(sequences)
  sizes_seq = arolla.M.seq.map(
      arolla.M.seq.size,
      sequences,
  )
  sizes_item_seq = arolla.M.seq.map(
      # We need a lambda wrapper to always use a default value for the 'shape'
      # argument.
      optools.as_lambda_operator('koda_internal.iterables._to_data_slice')(
          lambda x: arolla_bridge.to_data_slice(x)  # pylint: disable=unnecessary-lambda
      ),
      sizes_seq,
  )
  # Here and below comments show a possible value of each slice for the
  # case where we interleave an iterable of size 3 and an iterable of size 2,
  # before the corresponding statement.
  # [3, 2]
  sizes = schema.to_int64(sequence_to_1d_slice(sizes_item_seq))
  # [0, 0, 0, 1, 1]
  indices = slices.repeat(slices.index(sizes), sizes).flatten()
  random_values = random.randint_shaped_as(indices)
  # [1, 0, 0, 1, 0]
  shuffled_indices = slices.sort(indices, sort_by=random_values)
  # [[0, 3], [1, 2, 4]]
  grouped_indices = slices.group_by_indices(shuffled_indices)
  # [[0, 1], [0, 1, 2]]
  grouped_index_in_group = slices.index(grouped_indices)
  # [0, 0, 1, 1, 2]
  index_in_group = slices.take(
      grouped_index_in_group.flatten(),
      slices.inverse_mapping(grouped_indices.flatten()),
  )
  # [3, 0, 1, 4, 2]
  overall_index = (
      slices.take(math.cum_sum(sizes) - sizes, shuffled_indices)
      + index_in_group
  )
  overall_index_seq = sequence_from_1d_slice(overall_index)
  overall_index_seq = arolla.M.seq.map(
      arolla_bridge.to_arolla_int64, overall_index_seq
  )
  return arolla.M.seq.map(
      arolla.M.seq.at,
      # A sequence has a shared_ptr inside, so copying it is cheap.
      arolla.M.seq.repeat(chain_res, arolla.M.seq.size(chain_res)),
      overall_index_seq,
  )


@optools.add_to_registry()
@optools.as_lambda_operator(
    'koda_internal.iterables.empty_as',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.iterable),
    ],
)
def empty_as(iterable):
  """Returns an empty iterable of the same type as `iterable`."""
  return from_sequence(arolla.M.seq.slice(to_sequence(iterable), 0, 0))
