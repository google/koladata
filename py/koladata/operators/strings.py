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

"""String Koda operators."""

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import op_repr
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants


M = arolla.OperatorsContainer(jagged_shape)
P = arolla.P
constraints = arolla.optools.constraints


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings._agg_join',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.sep),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _agg_join(x, sep):  # pylint: disable=unused-argument
  """Returns a DataSlice of strings joined on last dimension."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kde.strings.agg_join',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.sep),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_join(
    x, sep=data_slice.DataSlice.from_vals(None), ndim=arolla.unspecified()
):
  """Returns a DataSlice of strings joined on last ndim dimensions.

  Example:
    ds = kd.slice([['el', 'psy', 'congroo'], ['a', 'b', 'c']))
    kd.agg_join(ds, ' ')  # -> kd.slice(['el psy congroo', 'a b c'])
    kd.agg_join(ds, ' ', ndim=2)  # -> kd.slice('el psy congroo a b c')

  Args:
    x: Text or bytes DataSlice
    sep: If specified, will join by the specified string, otherwise will be
      empty string.
    ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
      <= get_ndim(x).
  """
  return _agg_join(jagged_shape_ops.flatten_last_ndim(x, ndim), sep)


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.contains',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.substr),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def contains(s, substr):  # pylint: disable=unused-argument,redefined-outer-name
  """Returns present iff `s` contains `substr`.

  Examples:
    kd.strings.constains(kd.slice(['Hello', 'Goodbye']), 'lo')
      # -> kd.slice([kd.present, kd.missing])
    kd.strings.contains(
      kd.slice([b'Hello', b'Goodbye']),
      kd.slice([b'lo', b'Go']))
      # -> kd.slice([kd.present, kd.present])

  Args:
    s: The strings to consider. Must have schema TEXT or BYTES.
    substr: The substrings to look for in `s`. Must have the same schema as `s`.

  Returns:
    The DataSlice of present/missing values with schema MASK.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.count',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.substr),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def count(s, substr):  # pylint: disable=unused-argument,redefined-outer-name
  """Counts the number of occurrences of `substr` in `s`.

  Examples:
    kd.strings.count(kd.slice(['Hello', 'Goodbye']), 'l')
      # -> kd.slice([2, 0])
    kd.strings.count(
      kd.slice([b'Hello', b'Goodbye']),
      kd.slice([b'Hell', b'o']))
      # -> kd.slice([1, 2])

  Args:
    s: The strings to consider.
    substr: The substrings to count in `s`. Must have the same schema as `s`.

  Returns:
    The DataSlice of INT32 counts.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.find',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.substr),
        qtype_utils.expect_data_slice(P.start),
        qtype_utils.expect_data_slice(P.end),
        qtype_utils.expect_data_slice(P.failure_value),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
# pylint: disable=unused-argument,redefined-outer-name
def find(
    s,
    substr,
    start=data_slice.DataSlice.from_vals(0, schema_constants.INT64),
    end=data_slice.DataSlice.from_vals(None, schema_constants.INT64),
    failure_value=data_slice.DataSlice.from_vals(None, schema_constants.INT64),
):  # pylint: enable=unused-argument,redefined-outer-name
  """Returns the offset of the first occurrence of `substr` in `s`.

  Searches within the offset range of `[start, end)`. If nothing is found,
  returns `failure_value`.

  The units of `start`, `end`, and the return value are all byte offsets if `s`
  is `BYTES` and codepoint offsets if `s` is `TEXT`.

  Args:
   s: (TEXT or BYTES) Strings to search in.
   substr: (TEXT or BYTES)  Strings to search for in `s`. Should have the same
     dtype as `s`.
   start: (optional int) Offset to start the search. Defaults to 0.
   end: (optional int) Offset to stop the search.
   failure_value: (optional int) Reported if `substr` is not found in `s`.

  Returns:
    The offset of the first occurrence of `substr` in `s`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.printf',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.fmt),
        qtype_utils.expect_data_slice_args(P.args),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def printf(fmt, *args):  # pylint: disable=unused-argument
  """Formats strings according to printf-style (C++) format strings.

  See absl::StrFormat documentation for the format string details.

  Example:
    kd.strings.printf(kd.slice(['Hello %s!', 'Goodbye %s!']), 'World')
      # -> kd.slice(['Hello World!', 'Goodbye World!'])
    kd.strings.printf('%v + %v = %v', 1, 2, 3)  # -> kd.slice('1 + 2 = 3')

  Args:
    fmt: Format string (Text or Bytes).
    *args: Arguments to format (primitive types compatible with `fmt`).

  Returns:
    The formatted string.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(repr_fn=op_repr.full_signature_repr)
@optools.as_backend_operator(
    'kde.strings.format',
    aux_policy=py_boxing.FULL_SIGNATURE_POLICY,
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.fmt),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def format_(
    fmt=py_boxing.positional_only(), kwargs=py_boxing.var_keyword()
):  # pylint: disable=unused-argument
  """Formats strings according to python str.format style.

  Format support is slightly different from Python:
  1. {x:v} is equivalent to {x} and supported for all types as default string
     format.
  2. Only float and integers support other format specifiers.
    E.g., {x:.1f} and {x:04d}.
  3. If format is missing type specifier `f` or `d` at the end, we are
     adding it automatically based on the type of the argument.

  Note: only keyword arguments are supported.

  Examples:
    kd.strings.format(kd.slice(['Hello {n}!', 'Goodbye {n}!']), n='World')
      # -> kd.slice(['Hello World!', 'Goodbye World!'])
    kd.strings.format('{a} + {b} = {c}', a=1, b=2, c=3)
      # -> kd.slice('1 + 2 = 3')
    kd.strings.format(
        '{a} + {b} = {c}',
        a=kd.slice([1, 2]),
        b=kd.slice([2, 3]),
        c=kd.slice([3, 5]))
      # -> kd.slice(['1 + 2 = 3', '2 + 3 = 5'])
    kd.strings.format(
        '({a:03} + {b:e}) * {c:.2f} ='
        ' {a:02d} * {c:3d} + {b:07.3f} * {c:08.4f}'
        a=5, b=5.7, c=75)
      # -> kd.slice(
      #        '(005 + 5.700000e+00) * 75.00 = 05 *  75 + 005.700 * 075.0000')

  Args:
    fmt: Format string (Text or Bytes).
    kwargs: Arguments to format.

  Returns:
    The formatted string.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.fstr'])
@optools.as_lambda_operator(
    'kde.strings.fstr',
    aux_policy=py_boxing.FSTR_POLICY,
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.fmt),
    ],
)
def fstr(fmt):
  """Transforms Koda f-string into an expression.

  f-string must be created via Python f-string syntax. It must contain at least
  one formatted DataSlice or Expression.
  Each DataSlice/Expression must have custom format specification,
  e.g. `{ds:s}` or `{expr:.2f}`.
  Find more about format specification in kde.strings.format docs.

  NOTE: `{ds:s}` can be used for any type to achieve default string conversion.

  Examples:
    greeting_expr = kde.fstr(f'Hello, {I.countries:s}!')

    countries = kd.slice(['USA', 'Schweiz'])
    kd.eval(greeting_expr, countries=countries)
      # -> kd.slice(['Hello, USA!', 'Hello, Schweiz!'])

    local_greetings = ds(['Hello', 'Gruezi'])
    # Data slice is interpreted as literal.
    local_greeting_expr = kde.fstr(
        f'{local_greetings:s}, {I.countries:s}!'
    )
    kd.eval(local_greeting_expr, countries=countries)
      # -> kd.slice(['Hello, USA!', 'Gruezi, Schweiz!'])

    price_expr = kde.fstr(
        f'Lunch price in {I.countries:s} is {I.prices:.2f} {I.currencies:s}.')
    kd.eval(price_expr,
            countries=countries,
            prices=kd.slice([35.5, 49.2]),
            currencies=kd.slice(['USD', 'CHF']))
      # -> kd.slice(['Lunch price in USA is 35.50 USD.',
                     'Lunch price in Schweiz is 49.20 CHF.'])

  Args:
    fmt: f-string to evaluate.

  Returns:
    Expr that formats provided f-string.
  """
  return fmt


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings._test_only_format_wrapper',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.fmt),
        qtype_utils.expect_data_slice(P.arg_names),
        qtype_utils.expect_data_slice_args(P.args),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def _test_only_format_wrapper(fmt, arg_names, *args):  # pylint: disable=unused-argument
  """Test only wrapper for format with Arolla signature."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.join',
    qtype_constraints=[
        (
            M.qtype.get_field_count(P.args) > 0,
            'expected at least one argument',
        ),
        qtype_utils.expect_data_slice_args(P.args),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def join(*args):  # pylint: disable=unused-argument
  """Concatenates the given strings.

  Examples:
    kd.strings.join(kd.slice(['Hello ', 'Goodbye ']), 'World')
      # -> kd.slice(['Hello World', 'Goodbye World'])
    kd.strings.join(kd.slice([b'foo']), kd.slice([b' ']), kd.slice([b'bar']))
      # -> kd.slice([b'foo bar'])

  Args:
    *args: The inputs to concatenate in the given order.

  Returns:
    The string concatenation of all the inputs.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.length',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def length(x):  # pylint: disable=unused-argument
  """Returns a DataSlice of lengths in bytes for Byte or codepoints for Text.

  For example,
    kd.strings.length(kd.slice(['abc', None, ''])) -> kd.slice([3, None, 0])
    kd.strings.length(kd.slice([b'abc', None, b''])) -> kd.slice([3, None, 0])
    kd.strings.length(kd.item('你好')) -> kd.item(2)
    kd.strings.length(kd.item('你好'.encode())) -> kd.item(6)

  Note that the result DataSlice always has INT32 schema.

  Args:
    x: Text or Bytes DataSlice.

  Returns:
    A DataSlice of lengths.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.lower',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def lower(x):  # pylint: disable=unused-argument
  """Returns a DataSlice with the lowercase version of each string in the input.

  For example,
    kd.strings.lower(kd.slice(['AbC', None, ''])) -> kd.slice(['abc', None, ''])
    kd.strings.lower(kd.item('FOO')) -> kd.item('foo')

  Note that the result DataSlice always has TEXT schema.

  Args:
    x: Text DataSlice.

  Returns:
    A Text DataSlice of lowercase strings.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.lstrip',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.chars),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def lstrip(s, chars=data_slice.DataSlice.from_vals(None)):
  r"""Strips whitespaces or the specified characters from the left side of `s`.

  If `chars` is missing, then whitespaces are removed.
  If `chars` is present, then it will strip all leading characters from `s`
  that are present in the `chars` set.

  Examples:
    kd.strings.lstrip(kd.slice(['   spacious   ', '\t text \n']))
      # -> kd.slice(['spacious   ', 'text \n'])
    kd.strings.lstrip(kd.slice(['www.example.com']), kd.slice(['cmowz.']))
      # -> kd.slice(['example.com'])
    kd.strings.lstrip(kd.slice([['#... Section 3.1 Issue #32 ...'], ['# ...']]),
        kd.slice('.#! '))
      # -> kd.slice([['Section 3.1 Issue #32 ...'], ['']])

  Args:
    s: (TEXT or BYTES) Original string.
    chars (Optional TEXT or BYTES, the same as `s`): The set of chars to remove.

  Returns:
    Stripped string.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.regex_extract',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.text),
        qtype_utils.expect_data_slice(P.regex),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def regex_extract(text, regex):  # pylint: disable=unused-argument
  """Extracts a substring from `text` with the capturing group of `regex`.

  Regular expression matches are partial, which means `regex` is matched against
  a substring of `text`.
  For full matches, where the whole string must match a pattern, please enclose
  the pattern in `^` and `$` characters.
  The pattern must contain exactly one capturing group.

  Examples:
    kd.strings.regex_extract(kd.item('foo'), kd.item('f(.)'))
      # kd.item('o')
    kd.strings.regex_extract(kd.item('foobar'), kd.item('o(..)'))
      # kd.item('ob')
    kd.strings.regex_extract(kd.item('foobar'), kd.item('^o(..)$'))
      # kd.item(None).with_schema(kd.TEXT)
    kd.strings.regex_extract(kd.item('foobar'), kd.item('^.o(..)a.$'))
      # kd.item('ob')
    kd.strings.regex_extract(kd.item('foobar'), kd.item('.*(b.*r)$'))
      # kd.item('bar')
    kd.strings.regex_extract(kd.slice(['abcd', None, '']), kd.slice('b(.*)'))
      # -> kd.slice(['cd', None, None])

  Args:
    text: (TEXT) A string.
    regex: (TEXT) A scalar string that represents a regular expression (RE2
      syntax) with exactly one capturing group.

  Returns:
    For the first partial match of `regex` and `text`, returns the substring of
    `text` that matches the capturing group of `regex`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.regex_match',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.text),
        qtype_utils.expect_data_slice(P.regex),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def regex_match(text, regex):  # pylint: disable=unused-argument
  """Returns `present` if `text` matches the regular expression `regex`.

  Matches are partial, which means a substring of `text` matches the pattern.
  For full matches, where the whole string must match a pattern, please enclose
  the pattern in `^` and `$` characters.

  Examples:
    kd.strings.regex_match(kd.item('foo'), kd.item('oo'))
      # -> kd.present
    kd.strings.regex_match(kd.item('foo'), '^oo$')
      # -> kd.missing
    kd.strings.regex_match(kd.item('foo), '^foo$')
      # -> kd.present
    kd.strings.regex_match(kd.slice(['abc', None, '']), 'b')
      # -> kd.slice([kd.present, kd.missing, kd.missing])
    kd.strings.regex_match(kd.slice(['abcd', None, '']), kd.slice('b.d'))
      # -> kd.slice([kd.present, kd.missing, kd.missing])

  Args:
    text: (TEXT) A string.
    regex: (TEXT) A scalar string that represents a regular expression (RE2
      syntax).

  Returns:
    `present` if `text` matches `regex`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.replace',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.old),
        qtype_utils.expect_data_slice(P.new),
        qtype_utils.expect_data_slice(P.max_subs),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
# pylint: disable=unused-argument
def replace(
    s,
    old,
    new,
    max_subs=data_slice.DataSlice.from_vals(None, schema_constants.INT32),
):  # pylint: enable=unused-argument
  """Replaces up to `max_subs` occurrences of `old` within `s` with `new`.

  If `max_subs` is missing or negative, then there is no limit on the number of
  substitutions. If it is zero, then `s` is returned unchanged.

  If the search string is empty, the original string is fenced with the
  replacement string, for example: replace("ab", "", "-") returns "-a-b-". That
  behavior is similar to Python's string replace.

  Args:
   s: (TEXT or BYTES) Original string.
   old: (TEXT or BYTES, the same as `s`) String to replace.
   new: (TEXT or BYTES, the same as `s`) Replacement string.
   max_subs: (optional INT32) Max number of substitutions. If unspecified or
     negative, then there is no limit on the number of substitutions.

  Returns:
    String with applied substitutions.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.rfind',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.substr),
        qtype_utils.expect_data_slice(P.start),
        qtype_utils.expect_data_slice(P.end),
        qtype_utils.expect_data_slice(P.failure_value),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
# pylint: disable=unused-argument,redefined-outer-name
def rfind(
    s,
    substr,
    start=data_slice.DataSlice.from_vals(0, schema_constants.INT64),
    end=data_slice.DataSlice.from_vals(None, schema_constants.INT64),
    failure_value=data_slice.DataSlice.from_vals(None, schema_constants.INT64),
):  # pylint: enable=unused-argument,redefined-outer-name
  """Returns the offset of the last occurrence of `substr` in `s`.

  Searches within the offset range of `[start, end)`. If nothing is found,
  returns `failure_value`.

  The units of `start`, `end`, and the return value are all byte offsets if `s`
  is `BYTES` and codepoint offsets if `s` is `TEXT`.

  Args:
   s: (TEXT or BYTES) Strings to search in.
   substr: (TEXT or BYTES)  Strings to search for in `s`. Should have the same
     dtype as `s`.
   start: (optional int) Offset to start the search. Defaults to 0.
   end: (optional int) Offset to stop the search.
   failure_value: (optional int) Reported if `substr` is not found in `s`.

  Returns:
    The offset of the last occurrence of `substr` in `s`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.rstrip',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.chars),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def rstrip(s, chars=data_slice.DataSlice.from_vals(None)):
  r"""Strips whitespaces or the specified characters from the right side of `s`.

  If `chars` is missing, then whitespaces are removed.
  If `chars` is present, then it will strip all tailing characters from `s` that
  are present in the `chars` set.

  Examples:
    kd.strings.rstrip(kd.slice(['   spacious   ', '\t text \n']))
      # -> kd.slice(['   spacious', '\t text'])
    kd.strings.rstrip(kd.slice(['www.example.com']), kd.slice(['cmowz.']))
      # -> kd.slice(['www.example'])
    kd.strings.rstrip(kd.slice([['#... Section 3.1 Issue #32 ...'], ['# ...']]),
        kd.slice('.#! '))
      # -> kd.slice([['#... Section 3.1 Issue #32'], ['']])

  Args:
    s: (TEXT or BYTES) Original string.
    chars (Optional TEXT or BYTES, the same as `s`): The set of chars to remove.

  Returns:
    Stripped string.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.split'])
@optools.as_backend_operator(
    'kde.strings.split',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.sep),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def split(x, sep=data_slice.DataSlice.from_vals(None)):
  """Returns x split by the provided separator.

  Example:
    ds = kd.slice(['Hello world!', 'Goodbye world!'])
    kd.split(ds)  # -> kd.slice([['Hello', 'world!'], ['Goodbye', 'world!']])

  Args:
    x: DataSlice: (can be text or bytes)
    sep: If specified, will split by the specified string not omitting empty
      strings, otherwise will split by whitespaces while omitting empty strings.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.strip',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.chars),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def strip(s, chars=data_slice.DataSlice.from_vals(None)):
  r"""Strips whitespaces or the specified characters from both sides of `s`.

  If `chars` is missing, then whitespaces are removed.
  If `chars` is present, then it will strip all leading and tailing characters
  from `s` that are present in the `chars` set.

  Examples:
    kd.strings.strip(kd.slice(['   spacious   ', '\t text \n']))
      # -> kd.slice(['spacious', 'text'])
    kd.strings.strip(kd.slice(['www.example.com']), kd.slice(['cmowz.']))
      # -> kd.slice(['example'])
    kd.strings.strip(kd.slice([['#... Section 3.1 Issue #32 ...'], ['# ...']]),
        kd.slice('.#! '))
      # -> kd.slice([['Section 3.1 Issue #32'], ['']])

  Args:
    s: (TEXT or BYTES) Original string.
    chars (Optional TEXT or BYTES, the same as `s`): The set of chars to remove.

  Returns:
    Stripped string.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kde.substr'])
@optools.as_backend_operator(
    'kde.strings.substr',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.start),
        qtype_utils.expect_data_slice(P.end),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
# pylint: disable=unused-argument
def substr(
    x,
    start=data_slice.DataSlice.from_vals(0, schema_constants.INT64),
    end=data_slice.DataSlice.from_vals(None, schema_constants.INT64),
):
  # pylint: enable=unused-argument
  """Returns a DataSlice of substrings with indices [start, end).

  The usual Python rules apply:
    * A negative index is computed from the end of the string.
    * An empty range yields an empty string, for example when start >= end and
      both are positive.

  The result is broadcasted to the common shape of all inputs.

  Examples:
    ds = kd.slice([['Hello World!', 'Ciao bella'], ['Dolly!']])
    kd.substr(ds)         # -> kd.slice([['Hello World!', 'Ciao bella'],
                                         ['Dolly!']])
    kd.substr(ds, 5)      # -> kd.slice([[' World!', 'bella'], ['!']])
    kd.substr(ds, -2)     # -> kd.slice([['d!', 'la'], ['y!']])
    kd.substr(ds, 1, 5)   # -> kd.slice([['ello', 'iao '], ['olly']])
    kd.substr(ds, 5, -1)  # -> kd.slice([[' World', 'bell'], ['']])
    kd.substr(ds, 4, 100) # -> kd.slice([['o World!', ' bella'], ['y!']])
    kd.substr(ds, -1, -2) # -> kd.slice([['', ''], ['']])
    kd.substr(ds, -2, -1) # -> kd.slice([['d', 'l'], ['y']])

    # Start and end may also be multidimensional.
    ds = kd.slice('Hello World!')
    start = kd.slice([1, 2])
    end = kd.slice([[2, 3], [4]])
    kd.substr(ds, start, end) # -> kd.slice([['e', 'el'], ['ll']])

  Args:
    x: Text or Bytes DataSlice. If text, then `start` and `end` are codepoint
      offsets. If bytes, then `start` and `end` are byte offsets.
    start: The start index of the substring. Inclusive. Assumed to be 0 if
      unspecified.
    end: The end index of the substring. Exclusive. Assumed to be the length of
      the string if unspecified.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kde.strings.upper',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def upper(x):  # pylint: disable=unused-argument
  """Returns a DataSlice with the uppercase version of each string in the input.

  For example,
    kd.strings.upper(kd.slice(['abc', None, ''])) -> kd.slice(['ABC', None, ''])
    kd.strings.upper(kd.item('foo')) -> kd.item('FOO')

  Note that the result DataSlice always has TEXT schema.

  Args:
    x: Text DataSlice.

  Returns:
    A Text DataSlice of uppercase strings.
  """
  raise NotImplementedError('implemented in the backend')
