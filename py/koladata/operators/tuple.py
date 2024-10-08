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

"""Tuple operators."""

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.expr import view
from koladata.operators import optools
from koladata.types import schema_constants

M = arolla.OperatorsContainer(jagged_shape)
P = arolla.P
MASK = schema_constants.MASK
constraints = arolla.optools.constraints


@optools.add_to_registry(aliases=['kde.make_tuple'], view=view.KodaTupleView)
@optools.as_lambda_operator(
    'kde.tuple.make_tuple',
)
def make_tuple(*args):
  """Returns a tuple-like object containing the given `*args`."""
  # Somewhat confusingly, lambda operator *args are a length-1 Python tuple
  # containing the Arolla tuple of the actual args, so we need to unpack here.
  args_tuple, = args
  return args_tuple
