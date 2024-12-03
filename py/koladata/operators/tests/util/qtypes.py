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

"""QType-related test util."""

from arolla import arolla
from koladata.types import qtypes


DETECT_SIGNATURES_QTYPES = arolla.testing.DETECT_SIGNATURES_DEFAULT_QTYPES + (
    qtypes.JAGGED_SHAPE,
    qtypes.DATA_BAG,
    qtypes.DATA_SLICE,
    qtypes.NON_DETERMINISTIC_TOKEN,
)
