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

"""Internal operators working with iterables that are not exposed to the user.

An iterable is a derived type from an arolla Sequence, which is intended to be
used to represent streams that need streaming processing in multithreaded
evaluation.
"""

from arolla import arolla
from koladata.operators import arolla_bridge
from koladata.operators import jagged_shape
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import random
from koladata.operators import slices
from koladata.types import qtypes

P = arolla.P


# This is registered in C++ since it has type-inference-time evaluation,
# which is needed for from_sequence to work since the type argument
# of downcast must be evaluatable at type inference time.
get_iterable_qtype = arolla.abc.lookup_operator(
    'koda_internal.iterables.get_iterable_qtype'
)


@optools.add_to_registry(view=None)
@arolla.optools.as_backend_operator(
    'koda_internal.iterables.is_iterable_qtype',
    qtype_inference_expr=arolla.OPTIONAL_UNIT,
    qtype_constraints=[arolla.optools.constraints.expect_qtype(P.qtype)],
)
def is_iterable_qtype(qtype):  # pylint: disable=unused-argument
  """Checks if the given qtype is an iterable qtype."""
  raise NotImplementedError('implemented in the backend')


def _expect_iterable(param):
  return (
      is_iterable_qtype(param),
      (
          'expected an iterable type, got'
          f' {arolla.optools.constraints.name_type_msg(param)}'
      ),
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
  return arolla.M.derived_qtype.downcast(
      get_iterable_qtype(
          arolla.M.qtype.get_value_qtype(arolla.M.qtype.qtype_of(x))
      ),
      x,
  )


@optools.add_to_registry(view=None)
@optools.as_lambda_operator(
    'koda_internal.iterables.to_sequence',
    qtype_constraints=[
        _expect_iterable(P.x),
    ],
)
def to_sequence(x):
  """Converts an iterable to a sequence."""
  return arolla.M.derived_qtype.upcast(
      arolla.M.qtype.qtype_of(x),
      x,
  )


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
@optools.as_lambda_operator(
    'koda_internal.iterables.shuffle',
    qtype_constraints=[
        _expect_iterable(P.x),
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
    qtype_inference_expr=arolla.M.qtype.get_field_qtype(P.sequences, 0),
    qtype_constraints=[
        (
            arolla.M.qtype.get_field_count(P.sequences) > 0,
            'must have at least one sequence',
        ),
        (
            arolla.M.seq.all(
                arolla.M.seq.map(
                    arolla.M.qtype.is_sequence_qtype,
                    arolla.M.qtype.get_field_qtypes(P.sequences),
                )
            ),
            'all inputs must be sequences',
        ),
        (
            arolla.M.seq.all_equal(
                arolla.M.qtype.get_field_qtypes(P.sequences)
            ),
            'all inputs must have the same type',
        ),
    ],
)
def sequence_chain(*sequences):  # pylint: disable=unused-argument
  """Chains the given sequences into one."""
  raise NotImplementedError('implemented in the backend')
