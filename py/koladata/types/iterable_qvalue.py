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

"""QValue specialisation for the iterable type."""

from __future__ import annotations

from typing import Any, Iterator

from arolla import arolla
from koladata.operators import koda_internal_iterables
from koladata.types import py_boxing
from koladata.types import qtypes


_SEQUENCE_TO_ITERABLE_EXPR = koda_internal_iterables.from_sequence(arolla.L.arg)
_ITERABLE_TO_SEQUENCE_EXPR = koda_internal_iterables.to_sequence(arolla.L.arg)


class Iterable(arolla.QValue):
  """QValue specialization for sequence qtype.

  It intentionally provides only __iter__ method, and not __len__ for example,
  since the users are expected to use iterables in such a way that they do not
  need to know the length in advance, since they can represent streams in
  streaming workflows.
  """

  def __new__(cls, *values: Any, value_type_as: Any = None) -> Iterable:
    """Constructs a iterable qvalue.

    Args:
      *values: Values.
      value_type_as: The value to infer the type of the iterable items from.
        Must be specified if the values may be empty and they are not
        DataSlices.

    Returns:
      An iterable with the given values.
    """
    if value_type_as is None:
      if values:
        value_qtype = None
      else:
        value_qtype = qtypes.DATA_SLICE
    else:
      value_qtype = py_boxing.as_qvalue(value_type_as).qtype
    seq = arolla.types.Sequence(
        *[py_boxing.as_qvalue(x) for x in values], value_qtype=value_qtype
    )
    return arolla.eval(_SEQUENCE_TO_ITERABLE_EXPR, arg=seq)

  def __iter__(self) -> Iterator[arolla.QValue]:
    seq = arolla.eval(_ITERABLE_TO_SEQUENCE_EXPR, arg=self)
    return iter(seq)


# Register qvalue specializations for iterable types.
arolla.abc.register_qvalue_specialization(
    '::koladata::iterables::IterableQType', Iterable
)
