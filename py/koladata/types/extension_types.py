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

"""Helper functions for working with extension types."""

import functools
from typing import Any

from arolla import arolla
from arolla.derived_qtype import derived_qtype
from koladata.types import data_slice
from koladata.types import qtypes


M = arolla.M | derived_qtype.M


@functools.lru_cache
def _get_downcast_expr(qtype):
  return M.derived_qtype.downcast(qtype, arolla.L.x)

_UPCAST_EXPR = M.derived_qtype.upcast(M.qtype.qtype_of(arolla.L.x), arolla.L.x)


def _is_extension_type(qtype: arolla.QType) -> bool:
  """Checks if the given qtype is a Koda extension type."""
  return (
      arolla.abc.invoke_op('derived_qtype.get_qtype_label', (qtype,)) != ''  # pylint: disable=g-explicit-bool-comparison
  ) and (
      arolla.abc.invoke_op('qtype.decay_derived_qtype', (qtype,))
      == qtypes.DATA_SLICE
  )


def is_koda_extension(x: Any) -> bool:
  """Returns True iff the given object is instance of a Koda extension type."""
  if not isinstance(x, arolla.QValue):
    return False
  return _is_extension_type(x.qtype)


def wrap(x: data_slice.DataSlice, qtype: arolla.QType) -> Any:
  """Wraps a DataSlice into an instance of the given extension type."""
  if not isinstance(x, data_slice.DataSlice):
    raise ValueError(f'expected a DataSlice, got: {type(x)}')
  if not _is_extension_type(qtype):
    raise ValueError(f'expected an extension type, got: {qtype}')
  return arolla.eval(_get_downcast_expr(qtype), x=x)


def unwrap(x: Any) -> data_slice.DataSlice:
  """Unwraps an extension type into the underlying DataSlice."""
  if not is_koda_extension(x):
    raise ValueError(f'expected an extension type, got: {type(x)}')
  return arolla.eval(_UPCAST_EXPR, x=x)
