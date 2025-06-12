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

"""Internal operators for working with functors."""

from koladata.operators import optools
from koladata.types import qtypes


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.functor.pack_as_literal',
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def pack_as_literal(arg):  # pylint: disable=unused-argument
  """Packs the given value as a quoted expr literal."""
  raise NotImplementedError('implemented in the backend')
