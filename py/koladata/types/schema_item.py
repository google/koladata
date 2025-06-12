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

"""SchemaItem."""

from arolla import arolla
from koladata.expr import py_expr_eval_py_ext as _py_expr_eval_py_ext
from koladata.types import data_item
from koladata.types import data_slice


_eval_op = _py_expr_eval_py_ext.eval_op


@data_slice.register_reserved_class_method_names
class SchemaItem(data_item.DataItem):
  """SchemaItem is a DataItem representing a Koda Schema."""

  def get_nofollowed_schema(self) -> data_item.DataItem:
    return _eval_op('kd.get_nofollowed_schema', self)

  def new(self, **attrs) -> data_slice.DataSlice:
    """Returns a new Entity with this Schema."""
    if self.get_bag() is None:
      raise ValueError(
          'only SchemaItem with DataBags can be used for creating Entities'
      )
    if any(isinstance(attr, arolla.Expr) for attr in attrs.values()):
      return arolla.abc.aux_bind_op('kd.new', schema=self, **attrs)
    return _eval_op('kd.new', schema=self, **attrs)

  # TODO: Deprecate when all usage is updated.
  def __call__(self, *args, **kwargs):
    """Schema DataItem can be used as Entity creator."""
    raise ValueError(
        'creating Entities through Schema.__call__ is deprecated; '
        'please use kd.new(schema=Schema, ...) or Schema.new(...) instead; '
        f'creating with {self}'
    )


arolla.abc.register_qvalue_specialization(
    '::koladata::python::SchemaItem', SchemaItem
)
