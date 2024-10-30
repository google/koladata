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

"""User-facing module for Koda functor related APIs."""

from koladata.functor import functor_factories as _functor_factories
from koladata.functor import signature_utils   # pylint: disable=unused-import
from koladata.operators import eager_op_utils as _eager_op_utils

_kd = _eager_op_utils.operators_container('kde')

fn = _functor_factories.fn
call = _kd.call
trace_py_fn = _functor_factories.trace_py_fn
py_fn = _functor_factories.py_fn
bind = _functor_factories.bind
as_fn = _functor_factories.as_fn
get_signature = _functor_factories.get_signature
allow_arbitrary_unused_inputs = _functor_factories.allow_arbitrary_unused_inputs
