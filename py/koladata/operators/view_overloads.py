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

"""Overloadable operators for expression view methods."""

from koladata.operators import op_repr
from koladata.operators import optools


@optools.add_to_registry_as_overloadable(
    'koda_internal.view.get_item', repr_fn=op_repr.get_item_repr
)
def get_item(x, key):  # pylint: disable=unused-argument
  """Returns `x[key]`."""
  raise NotImplementedError('overloadable operator')
