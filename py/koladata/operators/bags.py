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

"""DataSlice operators working with DataBags."""

from arolla import arolla as _arolla
from arolla.jagged_shape import jagged_shape as _jagged_shape
from koladata.operators import optools as _optools
from koladata.operators import qtype_utils as _qtype_utils
from koladata.operators import view_overloads as _
from koladata.types import py_boxing as _py_boxing
from koladata.types import qtypes as _qtypes
from koladata.types import schema_constants as _schema_constants

_M = _arolla.M | _jagged_shape.M
_P = _arolla.P
_MASK = _schema_constants.MASK
_constraints = _arolla.optools.constraints


@_optools.add_to_registry(aliases=['kd.bag'], via_cc_operator_package=True)
@_optools.as_backend_operator(
    'kd.bags.new',
    qtype_inference_expr=_qtypes.DATA_BAG,
    deterministic=False,
)
def new():
  """Returns an empty DataBag."""
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.is_null_bag'], via_cc_operator_package=True
)
@_optools.as_backend_operator(
    'kd.bags.is_null_bag',
    qtype_constraints=[_qtype_utils.expect_data_bag(_P.bag)],
    qtype_inference_expr=_qtypes.DATA_SLICE,
)
def is_null_bag(bag):  # pylint: disable=unused-argument
  """Returns `present` if DataBag `bag` is a NullDataBag."""
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.enriched_bag'], via_cc_operator_package=True
)
@_arolla.optools.as_backend_operator(
    'kd.bags.enriched',
    qtype_constraints=[
        _qtype_utils.expect_data_bag_args(_P.bags),
    ],
    qtype_inference_expr=_qtypes.DATA_BAG,
    experimental_aux_policy=_py_boxing.DEFAULT_BOXING_POLICY,
)
def enriched(*bags):  # pylint: disable=unused-argument
  """Creates a new immutable DataBag enriched by `bags`.

   It adds `bags` as fallbacks rather than merging the underlying data thus
   the cost is O(1).

   Databags earlier in the list have higher priority.
   `enriched_bag(bag1, bag2, bag3)` is equivalent to
   `enriched_bag(enriched_bag(bag1, bag2), bag3)`, and so on for additional
   DataBag args.

  Args:
    *bags: DataBag(s) for enriching.

  Returns:
    An immutable DataBag enriched by `bags`.
  """
  raise NotImplementedError('implemented in the backend')


@_optools.add_to_registry(
    aliases=['kd.updated_bag'], via_cc_operator_package=True
)
@_arolla.optools.as_backend_operator(
    'kd.bags.updated',
    qtype_constraints=[
        _qtype_utils.expect_data_bag_args(_P.bags),
    ],
    qtype_inference_expr=_qtypes.DATA_BAG,
    experimental_aux_policy=_py_boxing.DEFAULT_BOXING_POLICY,
)
def updated(*bags):  # pylint: disable=unused-argument
  """Creates a new immutable DataBag updated by `bags`.

   It adds `bags` as fallbacks rather than merging the underlying data thus
   the cost is O(1).

   Databags later in the list have higher priority.
   `updated_bag(bag1, bag2, bag3)` is equivalent to
   `updated_bag(bag1, updated_bag(bag2, bag3))`, and so on for additional
   DataBag args.

  Args:
    *bags: DataBag(s) for updating.

  Returns:
    An immutable DataBag updated by `bags`.
  """
  raise NotImplementedError('implemented in the backend')
