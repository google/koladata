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

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import view_overloads as _
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants

M = arolla.M | jagged_shape.M
P = arolla.P
MASK = schema_constants.MASK
constraints = arolla.optools.constraints


@optools.add_to_registry(aliases=['kd.bag'])
@optools.as_backend_operator(
    'kd.bags.new',
    qtype_inference_expr=qtypes.DATA_BAG,
    deterministic=False,
)
def bag():
  """Returns an empty DataBag."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.enriched_bag'])
@arolla.optools.as_backend_operator(
    'kd.bags.enriched',
    qtype_constraints=[
        qtype_utils.expect_data_bag_args(P.bags),
    ],
    qtype_inference_expr=qtypes.DATA_BAG,
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def enriched_bag(*bags):  # pylint: disable=unused-argument
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


@optools.add_to_registry(aliases=['kd.updated_bag'])
@arolla.optools.as_backend_operator(
    'kd.bags.updated',
    qtype_constraints=[
        qtype_utils.expect_data_bag_args(P.bags),
    ],
    qtype_inference_expr=qtypes.DATA_BAG,
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def updated_bag(*bags):  # pylint: disable=unused-argument
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
