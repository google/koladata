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

"""Public API for kd_ext.tqdm — tqdm progress reporting integration."""

from koladata.ext.tqdm import context as _context
from koladata.ext.tqdm import tqdm as _tqdm
from koladata.ext.tqdm import types as _types
from koladata.ext.tqdm.proto import progress_pb2 as _progress_pb2

tqdm = _tqdm.tqdm
trange = _tqdm.trange
using_reporter = _context.using_reporter
get_reporter = _context.get_reporter
ProgressRecord = _progress_pb2.ProgressRecord
ProgressReporter = _types.ProgressReporter
ProgressState = _types.ProgressState
TqdmConfig = _types.TqdmConfig
