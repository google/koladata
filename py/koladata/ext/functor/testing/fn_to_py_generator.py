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

"""Generator tool to convert Koda functors to Python code."""

import ast
import importlib
import os

from absl import app
from absl import flags

from koladata import kd_ext

from pyink import mode as pyink_mode
from pyink import format_str as pyink_format_str

_DEFAULT_PYINK_MODE = pyink_mode.Mode(
    line_length=80,
    preview=True,
    unstable=True,
    is_pyink=True,
    pyink_indentation=2,
)

_MODEL_MODULE = flags.DEFINE_string(
    'model_module',
    None,
    'Fully qualified module name containing the MODEL.',
    required=True,
)
_OUTPUT_FILE = flags.DEFINE_string(
    'output_file',
    None,
    'Path to write the generated Python code.',
    required=True,
)


def _format_code(code: str) -> str:
  """Format Python code using pyink-compatible settings."""
  return pyink_format_str(code, mode=_DEFAULT_PYINK_MODE)


def main(argv):
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')

  # Dynamically import the model module
  try:
    model_module = importlib.import_module(_MODEL_MODULE.value)
  except ImportError as e:
    raise ValueError(
        f'Could not import module {_MODEL_MODULE.value}: {e}'
    ) from e

  if not hasattr(model_module, 'MODEL'):
    raise ValueError(
        f'Module {_MODEL_MODULE.value} does not have a MODEL attribute.'
    )

  functor = model_module.MODEL

  # Generate AST
  module_ast = kd_ext.functor.to_py(functor, name='top')  # pyrefly: ignore[missing-attribute]
  generated_code = ast.unparse(module_ast) + '\n'
  generated_code = _format_code(generated_code)

  # Write to output file
  os.makedirs(os.path.dirname(_OUTPUT_FILE.value), exist_ok=True)
  with open(_OUTPUT_FILE.value, 'w') as f:
    f.write(generated_code)


if __name__ == '__main__':
  app.run(main)
