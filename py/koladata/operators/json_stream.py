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

r"""JSON stream processing operators.

Some background that applies to all operators:

The operators in this module use "chunked string streams" as inputs and outputs.
A chunked string stream is a Koda ITERABLE or STREAM of STRING DataItems. The
logical value of a chunked string stream is the in-order concatenation of
the values (the "chunks") in the stream. Missing values in a chunked string
stream are tolerated but treated as empty strings.

The behavior of each operator in this module is specified only on the logical
values of the input and output chunked string streams: two input streams with
the same logical value but with different chunking will cause an operator to
produce the same output value (possibly with different chunking). Any input
chunking is allowed, and output chunking is implementation-defined and subject
to change in future versions.

All operators in this module are designed to "minimally delay" their output
chunked string streams, so that they are useful for real-time processing.
However, there are no hard guarantees about this unless otherwise specified.

Within the logical chunked string streams, these operators (with a couple of
exceptions) expect and output streams of whitespace-separated JSON values. The
chunk boundaries of a chunked string stream and the contained JSON value stream
are fully independent.

Except for kd.json_stream.salvage, these operators MUST be given chunked string
streams containing whitespace-separated valid JSON as input, otherwise their
behavior is unspecified. Consider using kd.json_stream.salvage to preprocess any
input data that isn't known to be valid for other operators.

Example:

  # A chunked string stream.
  kd.iterables.make('{"x"', ':"y"}\n', '"z"\n')

  # Its logical value.
  '{"x":"y"}\n"z"\n'

  # The stream of JSON values it contains, formatted more nicely for
  # documentation purposes.
  `{"x": "y"} "z"`

  # A plausible (but not guaranteed) output chunked string stream from
  # kd.json_stream.get_object_key_value(our_example_stream, key="x")
  kd.iterables.make('"y"\n', 'null\n')

  # The guaranteed logical value of the output chunked string stream above.
  '"y"\nnull\n'
"""

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
@optools.as_lambda_operator('kd.json_stream.filter_json')
def filter_json(input_chunks, field_to_extract):
  """Extracts requested field from streamed JSON.

  It automatically fixes some errors in the input stream: replaces single quotes
  with double quotes, quotes unquoted keys and values, handles linebreaks in
  string literals. Also removes all spaces and linebreaks outside of string
  literals.

  Args:
    input_chunks: An iterable of STRING DataItems with JSON fragments.
    field_to_extract: JSONPath string (e.g. "$.docs[*].name"), specifies a field
      to extract from the input stream. Only subset of JSONPath features is
      supported. List index can be specified only as `[*]`.

  Returns:
    An iterable of STRING DataItems. Each value is a JSON corresponding
    to the given JSONPath.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              koda_internal_parallel.stream_filter_json(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
                  field_to_extract,
              )
          )
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator('kd.json_stream.stream_string_value')
def stream_string_value(input_chunks, field_to_extract):
  r"""Extracts requested string value from streamed JSON.

  Example
    input:
      [ {"id":1, "str":"some\nstring"}, {"id": 2, "str":"another string"} ]
    field_to_extract: $[*].str
    output:
      some
      string

  Note that if there are several values matching `field_to_extract`, only
  the first one is used.

  Args:
    input_chunks: An iterable of STRING DataItems with JSON fragments.
    field_to_extract: JSONPath string (e.g. "$.docs[*].name"), specifies a field
      to extract from the input stream. Only subset of JSONPath features is
      supported. List index can be specified only as `[*]`.

  Returns:
    An iterable of STRING DataItems.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              koda_internal_parallel.stream_string_from_json(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
                  field_to_extract,
              )
          )
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._filter_json_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
        qtype_utils.expect_future(P.field_to_extract),
    ],
)
def _filter_json_parallel(executor, stream, field_to_extract):
  """Parallel operator for filter_json."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          koda_internal_parallel.stream_filter_json,
          executor,
          stream,
          field_to_extract,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._stream_string_value_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
        qtype_utils.expect_future(P.field_to_extract),
    ],
)
def _stream_string_value_parallel(executor, stream, field_to_extract):
  """Parallel operator for stream_string_value."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          koda_internal_parallel.stream_string_from_json,
          executor,
          stream,
          field_to_extract,
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._chunk_values_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _chunk_values_stream(executor, stream):
  """Backend operator for chunk_values."""
  del executor, stream
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._chunk_values_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
    ],
)
def _chunk_values_parallel(executor, stream):
  """Parallel operator for chunk_values."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor, _chunk_values_stream, executor, stream
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.chunk_values',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
    ],
)
def chunk_values(input_chunks, /):
  r"""Aligns chunk boundaries to top-level JSON values in the stream.

  Each output chunk will contain exactly one top-level JSON value followed by
  exactly one '\n' character, without changing the logical JSON content.
  Whitespace outside of strings is removed.

  For example, if the input chunked string stream has the logical value:

    `1 [2,3] {"x":4}`

  Then the output will have chunks:

    `1\n` `[2,3]\n` `{"x":4}\n`

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.

  Returns:
    An iterable of present STRING DataItems. Each chunk contains exactly one
    top-level JSON value followed by a newline.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _chunk_values_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
              )
          )
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._explode_array_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _explode_array_stream(executor, stream):
  """Backend operator for explode."""
  del executor, stream
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._explode_array_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
    ],
)
def _explode_array_parallel(executor, stream):
  """Parallel operator for explode."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _explode_array_stream,
          executor,
          stream,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.explode_array',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
    ],
)
def explode_array(input_chunks, /):
  r"""Extracts contents of all top-level JSON arrays as separate values.

  Skips any top-level values that are not arrays. If there are multiple
  top-level arrays in the input, their contents are concatenated in the output.

  For example:

    `[1] 2 [3, [4, 5]]`
    ->
    `1 3 [4,5]`

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _explode_array_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
              )
          )
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._get_array_nth_value_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _get_array_nth_value_stream(executor, stream, n):
  """Backend operator for get_array_nth_value."""
  del executor, stream, n
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._get_array_nth_value_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
        qtype_utils.expect_future(P.n),
    ],
)
def _get_array_nth_value_parallel(executor, stream, n):
  """Parallel operator for get_array_nth_value."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _get_array_nth_value_stream,
          executor,
          stream,
          n,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.get_array_nth_value',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
        qtype_utils.expect_data_slice(P.n),
    ],
)
def get_array_nth_value(input_chunks, /, *, n):
  r"""Extracts the `n`th element from all top-level JSON arrays.

  Returns `null` if a top-level JSON value is not an array or does not have an
  `n`th element.

  For example (with n=0):

    `[1, 2, 3] 4 [[5]] []`
    ->
    `1 null [5] null`

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.
    n: A present INT32 or INT64 DataItem representing The 0-indexed offset of
      the array element to extract.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _get_array_nth_value_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
                  n,
              )
          )
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._get_object_key_value_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _get_object_key_value_stream(executor, stream, key):
  """Backend operator for get_object_key_value."""
  del executor, stream, key
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._get_object_key_value_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
        qtype_utils.expect_future(P.key),
    ],
)
def _get_object_key_value_parallel(executor, stream, key):
  """Parallel operator for get_object_key_value."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _get_object_key_value_stream,
          executor,
          stream,
          key,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.get_object_key_value',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
        qtype_utils.expect_data_slice(P.key),
    ],
)
def get_object_key_value(input_chunks, /, *, key):
  r"""Extracts the value for a specific key from each top-level JSON object.

  For each top-level JSON value that is an object, extracts the first value
  associated with the given key. If the key appears multiple times, only the
  first value is emitted. If the key is not found or the top-level value is not
  an object, emits `null`.

  For example (with key='x'):

    `{"x":1,"y":2} [3] {"y":4}`
    ->
    `1 null null`

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.
    key: A present STRING DataItem. The object key to look up.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _get_object_key_value_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
                  key,
              )
          )
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._get_object_key_values_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _get_object_key_values_stream(executor, stream, key):
  """Backend operator for get_object_key_values."""
  del executor, stream, key
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._get_object_key_values_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
        qtype_utils.expect_future(P.key),
    ],
)
def _get_object_key_values_parallel(executor, stream, key):
  """Parallel operator for get_object_key_values."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _get_object_key_values_stream,
          executor,
          stream,
          key,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.get_object_key_values',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
        qtype_utils.expect_data_slice(P.key),
    ],
)
def get_object_key_values(input_chunks, /, *, key):
  r"""Extracts all values for a specific key from each top-level JSON object.

  For each top-level JSON value that is an object, extracts all values
  associated with the given key as an array. If a top-level value is not an
  object, or the key is not found, the result for that top-level value is an
  empty array.

  For example (with key='x'):

    `{"x":1,"x":2} [3] {"y":4}`
    ->
    `[1,2] [] []`

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.
    key: A present STRING DataItem. The object key to look up.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _get_object_key_values_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
                  key,
              )
          )
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._head_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _head_stream(executor, stream, n):
  """Backend operator for head."""
  del executor, stream, n
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._head_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
        qtype_utils.expect_future(P.n),
    ],
)
def _head_parallel(executor, stream, n):
  """Parallel operator for head."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _head_stream,
          executor,
          stream,
          n,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.head',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
        qtype_utils.expect_data_slice(P.n),
    ],
)
def head(input_chunks, /, *, n=1):
  r"""Keeps only the first `n` top-level JSON values from the stream.

  Unnecessary whitespace is removed and newlines are inserted after each top-
  level value.

  For example (with n=2):

    `1 [2,3] {"x":4}`
    ->
    `1 [2,3]`

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.
    n: A present INT32 or INT64 DataItem. The number of top-level values to
      keep.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _head_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
                  n,
              )
          )
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._implode_array_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _implode_array_stream(executor, stream):
  """Backend operator for implode_array."""
  del executor, stream
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._implode_array_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
    ],
)
def _implode_array_parallel(executor, stream):
  """Parallel operator for implode_array."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _implode_array_stream,
          executor,
          stream,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.implode_array',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
    ],
)
def implode_array(input_chunks, /):
  r"""Converts a stream of JSON values into a single JSON array.

  For example:

    `1 [2,3] null {"x":4}`
    ->
    `[1,[2,3],null,{"x":4}]`

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _implode_array_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
              )
          )
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._prettify_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _prettify_stream(executor, stream, indent_string):
  """Backend operator for prettify."""
  del executor, stream, indent_string
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._prettify_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
        qtype_utils.expect_future(P.indent_string),
    ],
)
def _prettify_parallel(executor, stream, indent_string):
  """Parallel operator for prettify."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _prettify_stream,
          executor,
          stream,
          indent_string,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.prettify',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
        qtype_utils.expect_data_slice(P.indent_string),
    ],
)
def prettify(input_chunks, /, *, indent_string='  '):
  r"""Adds indents and newlines to a stream of valid JSON values.

  Equivalent to python `json.dumps(..., indent=indent_string)`, to make large
  JSON values more human-readable. Empty containers `[]` and `{}` do not have
  whitespace inserted into them, matching python `json.dumps`.

  For example (with indent_string='  '):

    ```
    {"x": [], "y": [1, 2, 3]}
    ```
    ->
    ```json
    {
      "x": [],
      "y": [
        1,
        2,
        3,
      ]
    }
    ```

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.
    indent_string: A present STRING DataItem. The character sequence to use as a
      single indent. Must be valid UTF-8.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _prettify_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
                  indent_string,
              )
          )
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._quote_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _quote_stream(executor, stream):
  """Backend operator for quote."""
  del executor, stream
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._quote_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
    ],
)
def _quote_parallel(executor, stream):
  """Parallel operator for quote."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _quote_stream,
          executor,
          stream,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.quote',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
    ],
)
def quote(input_chunks, /):
  r"""Packs an input stream of arbitrary UTF-8 into a single JSON string.

  Only necessary escapes are applied.

  For example:

    `hello "world"`
    ->
    `"hello \"world\""`

  See the module docstring for more details about the input and output format.
  The input is a chunked string stream but is not required to contain valid
  JSON.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _quote_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
              )
          )
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._salvage_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _salvage_stream(executor, stream, allow_nan, ensure_ascii, max_depth):
  """Backend operator for salvage."""
  del executor, stream, allow_nan, ensure_ascii, max_depth
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
    input_chunks, /, *, allow_nan=False, ensure_ascii=False, max_depth=100
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
    - Single-triple-quoted and double-triple-quoted strings.
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

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.
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


@optools.as_backend_operator(
    'kd.json_stream._select_nonempty_arrays_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _select_nonempty_arrays_stream(executor, stream):
  """Backend operator for select_nonempty_arrays."""
  del executor, stream
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._select_nonempty_arrays_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
    ],
)
def _select_nonempty_arrays_parallel(executor, stream):
  """Parallel operator for select_nonempty_arrays."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _select_nonempty_arrays_stream,
          executor,
          stream,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.select_nonempty_arrays',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
    ],
)
def select_nonempty_arrays(input_chunks, /):
  r"""Keeps top-level JSON values that are arrays with at least one element.

  For example:

    `[1] [] 2 [3,4]`
    ->
    `[1] [3,4]`

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _select_nonempty_arrays_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
              )
          )
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._select_nonempty_objects_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _select_nonempty_objects_stream(executor, stream):
  """Backend operator for select_nonempty_objects."""
  del executor, stream
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._select_nonempty_objects_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
    ],
)
def _select_nonempty_objects_parallel(executor, stream):
  """Parallel operator for select_nonempty_objects."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _select_nonempty_objects_stream,
          executor,
          stream,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.select_nonempty_objects',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
    ],
)
def select_nonempty_objects(input_chunks, /):
  r"""Keeps top-level JSON values that are objects with at least one item.

  For example:

    `{"x":1} {} 2 {"y":3}`
    ->
    `{"x":1} {"y":3}`

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _select_nonempty_objects_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
              )
          )
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._select_nonnull_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _select_nonnull_stream(executor, stream):
  """Backend operator for select_nonnull."""
  del executor, stream
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._select_nonnull_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
    ],
)
def _select_nonnull_parallel(executor, stream):
  """Parallel operator for select_nonnull."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _select_nonnull_stream,
          executor,
          stream,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.select_nonnull',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
    ],
)
def select_nonnull(input_chunks, /):
  r"""Keeps top-level JSON values that are not the value `null`.

  For example:

    `1 null [2] null "x"`
    ->
    `1 [2] "x"`

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _select_nonnull_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
              )
          )
      )
  )


@optools.as_backend_operator(
    'kd.json_stream._unquote_stream',
    qtype_inference_expr=streams.get_stream_qtype(qtypes.DATA_SLICE),
)
def _unquote_stream(executor, stream):
  """Backend operator for unquote."""
  del executor, stream
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream._unquote_parallel',
    qtype_constraints=[
        qtype_utils.expect_executor(P.executor),
        qtype_utils.expect_stream(P.stream, qtypes.DATA_SLICE),
    ],
)
def _unquote_parallel(executor, stream):
  """Parallel operator for unquote."""
  return koda_internal_parallel.unwrap_future_to_stream(
      koda_internal_parallel.async_eval(
          executor,
          _unquote_stream,
          executor,
          stream,
      )
  )


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_lambda_operator(
    'kd.json_stream.unquote',
    qtype_constraints=[
        qtype_utils.expect_iterable(P.input_chunks),
    ],
)
def unquote(input_chunks, /):
  r"""Extracts the contents of all JSON strings from the input.

  This applies to *all* strings, including ones nested inside of top-level
  containers, and object keys.

  If there are multiple strings in the input, the contents are concatenated
  in the result.

  For example:

    `"hello" 123 "world"`
    ->
    `helloworld`

    `{"x": {"y": "z"}}, "w"`
    ->
    `xyzw`

  See the module docstring for more details about the input and output format.

  Args:
    input_chunks: An iterable of STRING DataItems. The logical input is the
      concatenation of these string chunks.

  Returns:
    An iterable of present STRING DataItems. The output is the concatenation of
    these string chunks.
  """
  return koda_internal_parallel.unsafe_blocking_wait(
      koda_internal_parallel.stream_from_future(
          koda_internal_parallel.future_iterable_from_stream(
              _unquote_stream(
                  streams.get_eager_executor(),
                  koda_internal_parallel.stream_from_iterable(input_chunks),
              )
          )
      )
  )
