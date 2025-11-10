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

from arolla import arolla as _arolla
from koladata.operators import optools as _optools
from koladata.operators import qtype_utils as _qtype_utils

_P = _arolla.P


@_optools.add_to_registry(
    aliases=['kd.bitwise_and'], via_cc_operator_package=True
)
@_optools.as_backend_operator(
    'kd.bitwise.bitwise_and',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.x),
        _qtype_utils.expect_data_slice(_P.y),
    ],
)
def bitwise_and(x, y):  # pylint: disable=unused-argument
  """Computes pointwise bitwise x & y."""
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.bitwise_or'], via_cc_operator_package=True
)
@_optools.as_backend_operator(
    'kd.bitwise.bitwise_or',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.x),
        _qtype_utils.expect_data_slice(_P.y),
    ],
)
def bitwise_or(x, y):  # pylint: disable=unused-argument
  """Computes pointwise bitwise x | y."""
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.bitwise_xor'], via_cc_operator_package=True
)
@_optools.as_backend_operator(
    'kd.bitwise.bitwise_xor',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.x),
        _qtype_utils.expect_data_slice(_P.y),
    ],
)
def bitwise_xor(x, y):  # pylint: disable=unused-argument
  """Computes pointwise bitwise x ^ y."""
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.bitwise_invert'], via_cc_operator_package=True
)
@_optools.as_backend_operator(
    'kd.bitwise.invert',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.x),
    ],
)
def invert(x):  # pylint: disable=unused-argument
  """Computes pointwise bitwise ~x."""
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.bitwise_count'], via_cc_operator_package=True
)
@_optools.as_backend_operator(
    'kd.bitwise.count',
    qtype_constraints=[
        _qtype_utils.expect_data_slice(_P.x),
    ],
)
def count(x):  # pylint: disable=unused-argument
  """Computes the number of bits set to 1 in the given input."""
  raise NotImplementedError('implemented in the backend')
