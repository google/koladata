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

"""Bitwise Koda operators."""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils

P = arolla.P


@optools.add_to_registry(
    aliases=['kd.bitwise_and'],
)
@optools.as_backend_operator(
    'kd.bitwise.bitwise_and',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def bitwise_and(x, y):  # pylint: disable=unused-argument
  """Computes pointwise bitwise x & y."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(
    aliases=['kd.bitwise_or'],
)
@optools.as_backend_operator(
    'kd.bitwise.bitwise_or',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def bitwise_or(x, y):  # pylint: disable=unused-argument
  """Computes pointwise bitwise x | y."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(
    aliases=['kd.bitwise_xor'],
)
@optools.as_backend_operator(
    'kd.bitwise.bitwise_xor',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.y),
    ],
)
def bitwise_xor(x, y):  # pylint: disable=unused-argument
  """Computes pointwise bitwise x ^ y."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(
    aliases=['kd.bitwise_invert'],
)
@optools.as_backend_operator(
    'kd.bitwise.invert',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
    ],
)
def invert(x):  # pylint: disable=unused-argument
  """Computes pointwise bitwise ~x."""
  raise NotImplementedError('implemented in the backend')
