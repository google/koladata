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

"""JSON stream processing operators."""

from arolla import arolla
from koladata.operators import koda_internal_parallel
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import streams
from koladata.operators import view_overloads as _
from koladata.types import qtypes

M = arolla.M
P = arolla.P


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_backend_operator(
    'kd.json_stream._salvage_stream',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
        qtype_utils.expect_data_slice(P.allow_nan),
        qtype_utils.expect_data_slice(P.ensure_ascii),
        qtype_utils.expect_data_slice(P.max_depth),
    ],
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _salvage_stream(
    executor,  # pylint: disable=unused-argument
    stream,  # pylint: disable=unused-argument
    allow_nan,  # pylint: disable=unused-argument
    ensure_ascii,  # pylint: disable=unused-argument
    max_depth,  # pylint: disable=unused-argument
):
  """Backend operator for salvage."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._salvage_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
        qtype_utils.expect_future(P.allow_nan),
        qtype_utils.expect_future(P.ensure_ascii),
        qtype_utils.expect_future(P.max_depth),
    ],
)
def _salvage_parallel(executor, stream, allow_nan, ensure_ascii, max_depth):
  """Parallel operator for salvage."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _salvage_stream,
          executor,
          stream,
          allow_nan,
          ensure_ascii,
          max_depth,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.salvage',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
        qtype_utils.expect_data_slice(P.allow_nan),
        qtype_utils.expect_data_slice(P.ensure_ascii),
        qtype_utils.expect_data_slice(P.max_depth),
    ],
)
def salvage(
    input_chunks,  # pylint: disable=unused-argument
    /,
    *,
    allow_nan=False,
    ensure_ascii=False,
    max_depth=100,
):
  r"""Normalizes a chunked string containing JSON-like syntax to JSON.

  This operator tries its best to interpret the input chunks as JSON, while
  minimially delaying the output chunks. In parallel mode, this means that the
  output stream is minimally delayed, which is much more useful.

  Basic guarantees:
  - The (concatenated) output is always a sequence of '\n'-newline-separated
    valid JSON values.
  - If the (concatenated) input is a sequence of ASCII-whitespace-separated
    valid JSON values with container nesting depth at most `max_depth`, the
    output is JSON-value-equivalent to the input.
    - Strings are equivalent by sequence of represented unicode code points,
      and numbers are equivalent by numeric value with unlimited precision.

  Supports the following additional syntax, to tolerate "variant" JSON:
  - All of JSON5 according to https://spec.json5.org/
    - Non-decimal integer literal magnitudes (ignoring sign) are 64 bit.
    - Decimal number literals use unlimited precision.
  - Additional syntax from Python (not covered by JSON5):
    - Line comments starting with a single hash character `#`.
    - False, True, and None (as true, false, and null).
    - Tuple literals (interpreted as arrays).
    - '''...''' and \"""...\""" triple-quoted strings.
    - \a string escape interpreted as U+0007.
    - \o \oo \ooo octal string escapes.
    - \UXXXXXXXX 32-bit hexadecimal string escapes.
    - u"" and b"" string prefixes (accepted and ignored).
    - Underscores in numeric literals (like 123_456).
    - Octal (0o) and binary (0b) integer literals.
      - Magnitudes (ignoring sign) are 64 bit.
    - l and L integer suffixes (accepted and ignored).
  - Additional syntax from JavaScript (not covered by Python/JSON5):
    - \u{...} variable-length hexadecimal string escapes.
    - n integer suffix (accepted and ignored).

  All other input is handled in an implementation-defined way and is subject
  to change in future versions.

  Example:

    ```
    {a: True, `b`: '''ship
    it \u{1f60a}''', 100: [-0x200, +300e10000,]}   0o100007
       false
    ```
    ->
    ```
    {"a":true,"b":"ship\nit 🚀","100":[-512,300e10000]}
    32775
    false
    ```

  Args:
    input_chunks: An iterable of STRING DataItems. The input is the
      concatenation of these string chunks. Missing DataItems are treated as
      empty strings.
    allow_nan: A BOOLEAN DataItem. If true, like in python `json.dumps`, the
      non-standard JSON number literals `NaN` and `Infinity` and `-Infinity` are
      allowed in the output.
    ensure_ascii: A BOOLEAN DataItem. If `True`, the output will contain only
      ASCII-range characters. If `False` (the default), non-ASCII code points in
      output JSON strings will use UTF-8 instead of JSON escape sequences.
    max_depth: A present INT32 or INT64 DataItem. If the input contains nested
      containers deeper than `max_depth`, the output is no longer guaranteed to
      match the input value, even if the input is valid JSON. This is mainly a
      safeguard to prevent unbounded memory usage on large inputs.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _salvage_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
                  allow_nan,
                  ensure_ascii,
                  max_depth,
              )
          )
      )
  )
