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

"""A deprecated version of koladata/kd_ext.py.

TODO: Remove this file.
"""

import typing as _typing
import warnings as _warnings
from koladata import kd_ext as _kd_ext


def __getattr__(name: str) -> _typing.Any:  # pylint: disable=invalid-name
  _warnings.warn(
      'You are importing kd_ext from a deprecated location. Please import'
      ' kd_ext via `from koladata import kd_ext`.',
      RuntimeWarning,
  )
  return getattr(_kd_ext, name)
