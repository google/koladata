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

"""The public API for Koda View.

This API is highly experimental and is subject to change without notice.
"""

import types as _py_types
from koladata.ext.view import view as _view

align = _view.align
view = _view.view
map = _view.map_  # pylint: disable=redefined-builtin

types = _py_types.SimpleNamespace(
    View=_view.View,
)
