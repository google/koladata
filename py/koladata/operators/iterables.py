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

"""Operators working with iterables.

An iterable is a derived type from an arolla Sequence, which is intended to be
used to represent streams that need streaming processing in multithreaded
evaluation.
"""

from arolla import arolla
from koladata.operators import bootstrap
from koladata.operators import koda_internal_iterables
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import data_slice
from koladata.types import qtypes

P = arolla.P
M = arolla.M


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.iterables.make',
)
def make(*items, value_type_as=arolla.unspecified()):
  """Creates an iterable from the provided items, in the given order.

  The items must all have the same type (for example data slice, or data bag).
  However, in case of data slices, the items can have different shapes or
  schemas.

  Args:
    *items: Items to be put into the iterable.
    value_type_as: A value that has the same type as the items. It is useful to
      specify this explicitly if the list of items may be empty. If this is not
      specified and the list of items is empty, the iterable will have data
      slice as the value type.

  Returns:
    An iterable with the given items.
  """
  items = arolla.optools.fix_trace_args(items)
  seq = arolla.types.DispatchOperator(
      'items, value_type_as',
      empty_items_unspecified_value_type_as_case=arolla.types.DispatchCase(
          M.seq.slice(M.seq.make(data_slice.DataSlice.from_vals(None)), 0, 0),
          condition=(M.qtype.get_field_count(P.items) == 0)
          & (P.value_type_as == arolla.UNSPECIFIED),
      ),
      unspecified_value_type_as_case=arolla.types.DispatchCase(
          M.core.apply_varargs(M.seq.make, P.items),
          condition=(M.qtype.get_field_count(P.items) > 0)
          & (P.value_type_as == arolla.UNSPECIFIED),
      ),
      # We add value_type_as to the sequence and then remove it via seq.slice,
      # so that it is properly handled in type deduction and validation logic.
      default=M.seq.slice(
          M.core.apply_varargs(M.seq.make, P.value_type_as, P.items),
          1,
          arolla.int64(2**63 - 1),
      ),
  )(items, value_type_as)
  return koda_internal_iterables.from_sequence(seq)


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.iterables.make_unordered',
)
def make_unordered(*items, value_type_as=arolla.unspecified()):
  """Creates an iterable from the provided items, in an arbitrary order.

  Having unspecified order allows the parallel execution to put the items into
  the iterable in the order they are computed, potentially increasing the amount
  of parallel processing done.

  When used with the non-parallel evaluation, we intentionally randomize the
  order to prevent user code from depending on the order, and avoid
  discrepancies when switching to parallel evaluation.

  Args:
    *items: Items to be put into the iterable.
    value_type_as: A value that has the same type as the items. It is useful to
      specify this explicitly if the list of items may be empty. If this is not
      specified and the list of items is empty, the iterable will have data
      slice as the value type.

  Returns:
    An iterable with the given items, in an arbitrary order.
  """
  items = arolla.optools.fix_trace_args(items)
  ordered_seq = arolla.abc.bind_op(
      make,
      items=items,
      value_type_as=value_type_as,
  )
  return koda_internal_iterables.shuffle(ordered_seq)


@arolla.optools.as_lambda_operator(
    'koda_internal.iterables._iterable_type_matches_value_type'
)
def _iterable_type_matches_value_type(iterable_type, value_type):
  """Checks if the iterable matches the value_type_as."""
  return (value_type == arolla.UNSPECIFIED) | (
      iterable_type == koda_internal_iterables.get_iterable_qtype(value_type)
  )


_ITERABLES_CHAIN_QTYPE_CONSTRAINTS = (
    (
        arolla.M.seq.all(
            arolla.M.seq.map(
                bootstrap.is_iterable_qtype,
                arolla.M.qtype.get_field_qtypes(P.iterables),
            )
        ),
        'all inputs must be iterables',
    ),
    (
        arolla.M.seq.all_equal(arolla.M.qtype.get_field_qtypes(P.iterables)),
        'all given iterables must have the same value type',
    ),
    (
        arolla.M.seq.all(
            arolla.M.seq.map(
                _iterable_type_matches_value_type,
                arolla.M.qtype.get_field_qtypes(P.iterables),
                arolla.M.seq.repeat(
                    P.value_type_as,
                    arolla.M.qtype.get_field_count(P.iterables),
                ),
            )
        ),
        (
            'when value_type_as is specified, all iterables must have that'
            ' value type'
        ),
    ),
)


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.iterables.chain',
    qtype_constraints=_ITERABLES_CHAIN_QTYPE_CONSTRAINTS,
)
def chain(*iterables, value_type_as=arolla.unspecified()):
  """Creates an iterable that chains the given iterables, in the given order.

  The iterables must all have the same value type. If value_type_as is
  specified, it must be the same as the value type of the iterables, if any.

  Args:
    *iterables: A list of iterables to be chained (concatenated).
    value_type_as: A value that has the same type as the iterables. It is useful
      to specify this explicitly if the list of iterables may be empty. If this
      is not specified and the list of iterables is empty, the iterable will
      have DataSlice as the value type.

  Returns:
    An iterable that chains the given iterables, in the given order.
  """
  iterables = arolla.optools.fix_trace_args(iterables)
  return arolla.types.DispatchOperator(
      'iterables, value_type_as',
      empty_iterables_case=arolla.types.DispatchCase(
          make(value_type_as=P.value_type_as),
          condition=(M.qtype.get_field_count(P.iterables) == 0),
      ),
      # The compatibility of value_type_as with the iterables was checked
      # in qtype_constraints.
      default=koda_internal_iterables.from_sequence(
          koda_internal_iterables.sequence_chain(
              M.seq.map(
                  koda_internal_iterables.to_sequence,
                  M.core.apply_varargs(M.seq.make, P.iterables),
              )
          )
      ),
  )(iterables, value_type_as)


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.iterables.interleave',
    qtype_constraints=_ITERABLES_CHAIN_QTYPE_CONSTRAINTS,
)
def interleave(*iterables, value_type_as=arolla.unspecified()):
  """Creates an iterable that interleaves the given iterables.

  The resulting iterable has all items from all input iterables, and the order
  within each iterable is preserved. But the order of interleaving of different
  iterables can be arbitrary.

  Having unspecified order allows the parallel execution to put the items into
  the result in the order they are computed, potentially increasing the amount
  of parallel processing done.

  The iterables must all have the same value type. If value_type_as is
  specified, it must be the same as the value type of the iterables, if any.

  Args:
    *iterables: A list of iterables to be interleaved.
    value_type_as: A value that has the same type as the iterables. It is useful
      to specify this explicitly if the list of iterables may be empty. If this
      is not specified and the list of iterables is empty, the iterable will
      have DataSlice as the value type.

  Returns:
    An iterable that interleaves the given iterables, in arbitrary order.
  """
  iterables = arolla.optools.fix_trace_args(iterables)
  return arolla.types.DispatchOperator(
      'iterables, value_type_as, non_deterministic',
      empty_iterables_case=arolla.types.DispatchCase(
          make(value_type_as=P.value_type_as),
          condition=(M.qtype.get_field_count(P.iterables) == 0),
      ),
      # The compatibility of value_type_as with the iterables was checked
      # in qtype_constraints.
      default=koda_internal_iterables.from_sequence(
          arolla.abc.bind_op(
              koda_internal_iterables.sequence_interleave,
              M.seq.map(
                  koda_internal_iterables.to_sequence,
                  M.core.apply_varargs(M.seq.make, P.iterables),
              ),
              P.non_deterministic,
          )
      ),
  )(iterables, value_type_as, optools.unified_non_deterministic_arg())


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.iterables.reduce_concat',
    qtype_inference_expr=qtypes.DATA_SLICE,
    qtype_constraints=[
        qtype_utils.expect_iterable(P.items),
        (
            M.qtype.get_value_qtype(P.items) == qtypes.DATA_SLICE,
            (
                'expected an iterable of DATA_SLICE, got'
                f' {arolla.optools.constraints.name_type_msg(P.items)}'
            ),
        ),
        qtype_utils.expect_data_slice(P.initial_value),
        qtype_utils.expect_data_slice(P.ndim),
    ],
)
def reduce_concat(items, initial_value, ndim=1):  # pylint: disable=unused-argument
  """Concatenates the values of the given iterable.

  This operator is a concrete case of the more general kd.functor.reduce, which
  exists to speed up such concatenation from O(N^2) that the general reduce
  would provide to O(N). See the docstring of kd.concat for more details about
  the concatenation semantics.

  Args:
    items: An iterable of data slices to be concatenated.
    initial_value: The initial value to be concatenated before items.
    ndim: The number of last dimensions to concatenate.

  Returns:
    The concatenated data slice.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.iterables.reduce_updated_bag',
    qtype_inference_expr=qtypes.DATA_BAG,
    qtype_constraints=[
        qtype_utils.expect_iterable(P.items),
        (
            M.qtype.get_value_qtype(P.items) == qtypes.DATA_BAG,
            (
                'expected an iterable of DATA_BAG, got'
                f' {arolla.optools.constraints.name_type_msg(P.items)}'
            ),
        ),
        qtype_utils.expect_data_bag(P.initial_value),
    ],
)
def reduce_updated_bag(items, initial_value):  # pylint: disable=unused-argument
  """Merges the bags from the given iterable into one.

  This operator is a concrete case of the more general kd.functor.reduce, which
  exists to speed up such merging from O(N^2) that the general reduce
  would provide to O(N). See the docstring of kd.updated_bag for more details
  about the merging semantics.

  Args:
    items: An iterable of data bags to be merged.
    initial_value: The data bag to be merged with the items. Note that the items
      will be merged as updates to this bag, meaning that they will take
      precedence over the initial_value on conflicts.

  Returns:
    The merged data bag.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.iterables.from_1d_slice',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.slice_),
    ],
)
def from_1d_slice(slice_):
  """Converts a 1D DataSlice to a Koda iterable of DataItems.

  Args:
    slice_: A 1D DataSlice to be converted to an iterable.

  Returns:
    A Koda iterable of DataItems, in the order of the slice. All returned
    DataItems point to the same DataBag as the input DataSlice.
  """
  return koda_internal_iterables.from_sequence(
      koda_internal_iterables.sequence_from_1d_slice(slice_)
  )
