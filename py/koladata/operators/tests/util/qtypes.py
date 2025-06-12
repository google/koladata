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

"""QType-related test util."""

from arolla import arolla
from koladata.types import qtypes

DATA_BAG = qtypes.DATA_BAG
DATA_SLICE = qtypes.DATA_SLICE
JAGGED_SHAPE = qtypes.JAGGED_SHAPE
NON_DETERMINISTIC_TOKEN = qtypes.NON_DETERMINISTIC_TOKEN


EMPTY_TUPLE = arolla.make_tuple_qtype()

TUPLES_OF_DATA_BAGS = (
    EMPTY_TUPLE,
    arolla.make_tuple_qtype(DATA_BAG),
    arolla.make_tuple_qtype(DATA_BAG, DATA_BAG),
)

TUPLES_OF_DATA_SLICES = (
    EMPTY_TUPLE,
    arolla.make_tuple_qtype(DATA_SLICE),
    arolla.make_tuple_qtype(DATA_SLICE, DATA_SLICE),
)


EMPTY_NAMEDTUPLE = arolla.make_namedtuple_qtype()

NAMEDTUPLES_OF_DATA_BAGS = (
    EMPTY_NAMEDTUPLE,
    arolla.make_namedtuple_qtype(kwarg_0=DATA_BAG),
    arolla.make_namedtuple_qtype(kwarg_0=DATA_BAG, kwarg_1=DATA_BAG),
)

NAMEDTUPLES_OF_DATA_SLICES = (
    EMPTY_NAMEDTUPLE,
    arolla.make_namedtuple_qtype(kwarg_0=DATA_SLICE),
    arolla.make_namedtuple_qtype(kwarg_0=DATA_SLICE, kwarg_1=DATA_SLICE),
)


DETECT_SIGNATURES_QTYPES = tuple(
    dict.fromkeys(
        arolla.testing.DETECT_SIGNATURES_DEFAULT_QTYPES
        + (
            JAGGED_SHAPE,
            DATA_BAG,
            DATA_SLICE,
            qtypes.EXECUTOR,
            qtypes.NON_DETERMINISTIC_TOKEN,
            EMPTY_TUPLE,
            EMPTY_NAMEDTUPLE,
            *TUPLES_OF_DATA_BAGS,
            *NAMEDTUPLES_OF_DATA_BAGS,
            *TUPLES_OF_DATA_SLICES,
            *NAMEDTUPLES_OF_DATA_SLICES,
        )
    )
)
