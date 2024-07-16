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

"""Entry point to Koda API for users."""

import types as _py_types

from arolla import arolla as _arolla
from koladata.expr import expr_eval as _expr_eval
from koladata.expr import input_container as _input_container
from koladata.functions import functions as _functions
from koladata.operators import eager_op_utils as _eager_op_utils
from koladata.operators import kde_operators as _kde_operators
from koladata.testing import testing as _testing
from koladata.types import data_bag as _data_bag
from koladata.types import data_item as _data_item
from koladata.types import data_slice as _data_slice
from koladata.types import dict_item as _dict_item
from koladata.types import list_item as _list_item
from koladata.types import literal_operator as _literal_operator
from koladata.types import schema_constants as _schema_constants
from koladata.types import schema_item as _schema_item


_HAS_DYNAMIC_ATTRIBUTES = True


### Used as type annotations in user's code.
types = _py_types.ModuleType('types')
types.DataBag = _data_bag.DataBag
types.DataItem = _data_item.DataItem
types.DataSlice = _data_slice.DataSlice
types.ListItem = _list_item.ListItem
types.DictItem = _dict_item.DictItem
types.SchemaItem = _schema_item.SchemaItem

exceptions = _py_types.ModuleType('exceptions')
exceptions.KodaError = _exceptions.KodaError


### Eager operators / functions from operators.
def _InitOpsAndContainers(op_container):
  kd_ops = _eager_op_utils.operators_container(op_container)
  for op_or_container_name in dir(kd_ops):
    globals()[op_or_container_name] = getattr(kd_ops, op_or_container_name)


_InitOpsAndContainers('kde')


### Public functions.

# Creating DataItem / DataSlice.
item = _data_item.DataItem.from_vals
slice = _data_slice.DataSlice.from_vals  # pylint: disable=redefined-builtin


# Impure functions (kd.bag, kd.list, kd.new, kd.set_attr, ...).
def _LoadImpureFunctions():
  for fn_name in dir(_functions):
    if not fn_name.startswith('_'):
      globals()[fn_name] = getattr(_functions, fn_name)


_LoadImpureFunctions()


### Expr-related functions, Input/Variable containers and operator container.
I = _input_container.InputContainer('I')
V = _input_container.InputContainer('V')
eval = _expr_eval.eval  # pylint: disable=redefined-builtin
kde = _kde_operators.kde
literal = _literal_operator.literal


### Koda constants.

# Primitive schemas.
INT32 = _schema_constants.INT32
INT64 = _schema_constants.INT64
FLOAT32 = _schema_constants.FLOAT32
FLOAT64 = _schema_constants.FLOAT64
BOOLEAN = _schema_constants.BOOLEAN
MASK = _schema_constants.MASK
BYTES = _schema_constants.BYTES
TEXT = _schema_constants.TEXT
EXPR = _schema_constants.EXPR

# Special purpose schemas.
ANY = _schema_constants.ANY
ITEMID = _schema_constants.ITEMID
OBJECT = _schema_constants.OBJECT
SCHEMA = _schema_constants.SCHEMA
NONE = _schema_constants.NONE

# Mask constants.
missing = item(None, _schema_constants.MASK)
present = item(_arolla.present(), _schema_constants.MASK)


### Public submodules.

testing = _testing

__all__ = [api for api in globals().keys() if not api.startswith('_')]


def __dir__():  # pylint: disable=invalid-name
  return __all__
