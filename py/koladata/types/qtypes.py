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

"""Koda QTypes."""

from arolla import arolla
from koladata.types import data_bag_py_ext as _data_bag_py_ext
from koladata.types import data_slice_py_ext as _data_slice_py_ext
from koladata.types import ellipsis as _ellipsis
from koladata.types import jagged_shape as _jagged_shape


DATA_SLICE = _data_slice_py_ext.DataSlice.from_vals(None).qtype
DATA_BAG = _data_bag_py_ext.DataBag.empty().qtype
JAGGED_SHAPE = _jagged_shape.JAGGED_SHAPE
ELLIPSIS = _ellipsis.ELLIPSIS
NON_DETERMINISTIC_TOKEN = arolla.abc.bind_op(
    'koda_internal.non_deterministic',
    arolla.unit(), arolla.literal(arolla.int64(0)),
).qtype
EXECUTOR = arolla.abc.bind_op('koda_internal.parallel.get_eager_executor').qtype
