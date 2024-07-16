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

"""SchemaItem."""

from arolla import arolla
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import operator_lookup


_op_impl_lookup = operator_lookup.OperatorLookup()


@data_slice.register_reserved_class_method_names
class SchemaItem(data_item.DataItem):
  """SchemaItem is a DataItem representing a Koda Schema."""

  def get_nofollowed_schema(self) -> data_item.DataItem:
    return arolla.abc.aux_eval_op(_op_impl_lookup.get_nofollowed_schema, self)

  def __call__(self, *args, **kwargs):
    """Schema DataItem can be used as Entity creator."""
    if self.db is None:
      raise ValueError(
          'only SchemaItem with DataBags can be used for creating Entities'
      )
    if self.is_dict_schema():
      return data_bag.DataBag.empty().dict(*args, **kwargs, schema=self)
    return data_bag.DataBag.empty().new(*args, **kwargs, schema=self)


arolla.abc.register_qvalue_specialization(
    '::koladata::python::SchemaItem', SchemaItem
)
