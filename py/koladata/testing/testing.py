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

"""A front-end module for kd.testing.*."""

from koladata.operators.tests.util import qtypes as _test_qtypes
from koladata.testing import test_utils as _test_utils

assert_equal = _test_utils.assert_equal
assert_equivalent = _test_utils.assert_equivalent
assert_allclose = _test_utils.assert_allclose
assert_dicts_keys_equal = _test_utils.assert_dicts_keys_equal
assert_dicts_values_equal = _test_utils.assert_dicts_values_equal
assert_dicts_equal = _test_utils.assert_dicts_equal
assert_nested_lists_equal = _test_utils.assert_nested_lists_equal
assert_unordered_equal = _test_utils.assert_unordered_equal
assert_non_deterministic_exprs_equal = (
    _test_utils.assert_non_deterministic_exprs_equal
)
assert_traced_exprs_equal = _test_utils.assert_traced_exprs_equal
assert_traced_non_deterministic_exprs_equal = (
    _test_utils.assert_traced_non_deterministic_exprs_equal
)

DETECT_SIGNATURES_QTYPES = _test_qtypes.DETECT_SIGNATURES_QTYPES
