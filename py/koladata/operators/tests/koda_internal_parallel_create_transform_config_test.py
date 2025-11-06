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

from absl.testing import absltest
from arolla import arolla
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import bootstrap
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
koda_internal_parallel = kde_operators.internal.parallel


# We do not have a Python API for fetching the config from the context,
# so that part is tested in the C++ tests, here just do small sanity
# checks.
class KodaInternalParallelCreateTransformConfigTest(absltest.TestCase):

  def test_can_run(self):
    _ = arolla.eval(koda_internal_parallel.create_transform_config(None))

  def test_config_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'config_src must be a scalar, got rank 1',
    ):
      _ = arolla.eval(
          koda_internal_parallel.create_transform_config(ds([None]))
      )

  def test_qtype_signatures(self):
    parallel_transform_config_qtype = arolla.eval(
        bootstrap.get_transform_config_qtype()
    )
    arolla.testing.assert_qtype_signatures(
        koda_internal_parallel.create_transform_config,
        [(qtypes.DATA_SLICE, parallel_transform_config_qtype)],
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES
        + (parallel_transform_config_qtype,),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(koda_internal_parallel.create_transform_config(I.x))
    )


if __name__ == '__main__':
  absltest.main()
