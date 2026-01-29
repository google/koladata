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

"""Annotation Koda operators."""

from arolla import arolla
from koladata.operators import optools

# NOTE: Implemented in C++.
source_location = arolla.abc.lookup_operator('kd.annotation.source_location')
with_name = arolla.abc.lookup_operator('kd.annotation.with_name')

optools.add_to_registry('kd.with_name', via_cc_operator_package=True)(with_name)
