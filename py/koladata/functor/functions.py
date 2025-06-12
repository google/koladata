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

"""Eager-only functions related to functors.

These definitions are injected into "kd.", therefore this module is tested
in kd_test.py.
"""

import types as _py_types

from koladata.functor import functor_factories as _functor_factories
from koladata.functor import tracing_decorator as _tracing_decorator
from koladata.types import signature_utils as _signature_utils


functor = _py_types.SimpleNamespace(
    fn=_functor_factories.fn,
    expr_fn=_functor_factories.expr_fn,
    trace_py_fn=_functor_factories.trace_py_fn,
    py_fn=_functor_factories.py_fn,
    bind=_functor_factories.bind,
    is_fn=_functor_factories.is_fn,
    fstr_fn=_functor_factories.fstr_fn,
    map_py_fn=_functor_factories.map_py_fn,
    get_signature=_functor_factories.get_signature,
    allow_arbitrary_unused_inputs=_functor_factories.allow_arbitrary_unused_inputs,
    trace_as_fn=_tracing_decorator.TraceAsFnDecorator,
    signature_utils=_signature_utils,
    TypeTracingConfig=_tracing_decorator.TypeTracingConfig,
)

# These become top-level "kd." functions.
fn = _functor_factories.fn
trace_py_fn = _functor_factories.trace_py_fn
py_fn = _functor_factories.py_fn
is_fn = _functor_factories.is_fn
bind = _functor_factories.bind
trace_as_fn = _tracing_decorator.TraceAsFnDecorator
