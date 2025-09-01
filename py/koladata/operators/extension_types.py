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

"""Extension type operators."""

from arolla import arolla
from arolla.derived_qtype import derived_qtype
from koladata.operators import optools
from koladata.types import schema_constants


M = arolla.M | derived_qtype.M
P = arolla.P
INT64 = schema_constants.INT64
constraints = arolla.optools.constraints


@optools.add_to_registry(view=None)  # Provided by the QType.
@optools.as_lambda_operator('kd.extension_types.dynamic_cast')
def dynamic_cast(value, qtype):
  """Up-, down-, and side-casts `value` to `qtype`."""
  up = M.derived_qtype.upcast(M.qtype.qtype_of(value), value)
  down = M.derived_qtype.downcast(qtype, up)
  # TODO: Remove in favor of supporting QType inference directly in
  # M.derived_qtype.downcast.
  return M.annotation.qtype(down, qtype)
