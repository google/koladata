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

"""Internal operators that are not exposed to the user."""

from arolla import arolla
from koladata.operators import optools


@optools.add_to_registry()
@optools.as_backend_operator(
    'koda_internal.non_deterministic_identity',
    qtype_inference_expr=arolla.P.x,
    deterministic=False,
)
def non_deterministic_identity(x, /):  # pylint: disable=unused-argument
  """Returns the argument; acts as a barrier to literal folding.

  The intended purpose is to manually prevent literal folding for computations
  that are known to be non-deterministic but might be incorrectly interpreted
  as deterministic by the compiler.

  A principal example:
    ```python
    counter = 0

    def fn():
      nonlocal counter
      counter += 1
      return arolla.int32(counter)

    expr = arolla.M.py.call(
        koda_internal.non_deterministic_identity(fn), arolla.INT32)
    x1 = kd.eval(expr)
    x2 = kd.eval(expr)
    ```

  In this example, `fn` is clearly non-deterministic due to its dependence on
  the `counter` variable. However, the compiler cannot deduce this and may
  replace it with a literal value computed during compilation. The use of
  `non_deterministic_identity(fn)` ensures that this does not happen.

  Args:
    x: The argument to return.

  Returns:
    The argument.
  """
  raise NotImplementedError('implemented in the backend')
