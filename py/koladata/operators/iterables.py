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

"""Operators working with iterables.

An iterable is a derived type from an arolla Sequence, which is intended to be
used to represent streams that need streaming processing in multithreaded
evaluation.
"""

from arolla import arolla
from koladata.operators import koda_internal_iterables
from koladata.operators import optools
from koladata.types import data_slice

P = arolla.P
M = arolla.M


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.iterables.make',
)
def make(*items, value_type_as=arolla.unspecified()):
  """Creates an iterable from the given list of items, in the given order.

  The items must all have the same type (for example data slice, or data bag).
  However, in case of data slices, the items can have different shapes or
  schemas.

  Args:
    *items: A list of items to be put into the iterable.
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
          condition=M.qtype.is_tuple_qtype(P.items)
          & (M.qtype.get_field_count(P.items) > 0)
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
