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

"""Initializes Koda extension operators.

Do not use directly, use `from koladata import kd_ext` instead.
"""

from koladata import kd
from koladata.ext.contrib import cc_operators_py_clib as _

# The Expr operators are coming from :cc_operators package, importing this
# module in order to register Koda-specific parts like view and repr.
from koladata.ext.contrib import operators as _  # pylint: disable=g-bad-import-order

kde_ext = kd.optools.make_operators_container('kd_ext', 'kd_ext.contrib').kd_ext
