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

"""String Koda operators."""

from arolla import arolla
from arolla.jagged_shape import jagged_shape
from koladata.fstring import fstring as _fstring
from koladata.operators import arolla_bridge
from koladata.operators import assertion
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators import masking as masking_ops
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import slices
from koladata.types import data_slice
from koladata.types import py_boxing
from koladata.types import qtypes
from koladata.types import schema_constants


M = arolla.M | jagged_shape.M
P = arolla.P
constraints = arolla.optools.constraints


@optools.as_backend_operator('kd.strings._agg_join')
def _agg_join(x, sep):  # pylint: disable=unused-argument
  """Returns a DataSlice of strings joined on last dimension."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.strings.agg_join',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.sep),
        qtype_utils.expect_data_slice_or_unspecified(P.ndim),
    ],
)
def agg_join(x, sep=None, ndim=arolla.unspecified()):
  """Returns a DataSlice of strings joined on last ndim dimensions.

  Example:
    ds = kd.slice([['el', 'psy', 'congroo'], ['a', 'b', 'c']))
    kd.agg_join(ds, ' ')  # -> kd.slice(['el psy congroo', 'a b c'])
    kd.agg_join(ds, ' ', ndim=2)  # -> kd.slice('el psy congroo a b c')

  Args:
    x: String or bytes DataSlice
    sep: If specified, will join by the specified string, otherwise will be
      empty string.
    ndim: The number of dimensions to compute indices over. Requires 0 <= ndim
      <= get_ndim(x).
  """
  return _agg_join(jagged_shape_ops.flatten_last_ndim(x, ndim), sep)


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.contains',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.substr),
    ],
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
    s: The strings to consider. Must have schema STRING or BYTES.
    substr: The substrings to look for in `s`. Must have the same schema as `s`.

  Returns:
    The DataSlice of present/missing values with schema MASK.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.count',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.substr),
    ],
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
    'kd.strings.find',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.substr),
        qtype_utils.expect_data_slice(P.start),
        qtype_utils.expect_data_slice(P.end),
    ],
)
def find(
    s,
    substr,
    start=data_slice.DataSlice.from_vals(0, schema_constants.INT64),
    end=data_slice.DataSlice.from_vals(None, schema_constants.INT64),
):  # pylint: disable=unused-argument,redefined-outer-name
  """Returns the offset of the first occurrence of `substr` in `s`.

  The units of `start`, `end`, and the return value are all byte offsets if `s`
  is `BYTES` and codepoint offsets if `s` is `STRING`.

  Args:
   s: (STRING or BYTES) Strings to search in.
   substr: (STRING or BYTES) Strings to search for in `s`. Should have the same
     dtype as `s`.
   start: (optional int) Offset to start the search, defaults to 0.
   end: (optional int) Offset to stop the search, defaults to end of the string.

  Returns:
    The offset of the last occurrence of `substr` in `s`, or missing if there
    are no occurrences.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'kd.strings.printf',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.fmt),
        qtype_utils.expect_data_slice_args(P.args),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def printf(fmt, *args):  # pylint: disable=unused-argument
  """Formats strings according to printf-style (C++) format strings.

  See absl::StrFormat documentation for the format string details.

  Example:
    kd.strings.printf(kd.slice(['Hello %s!', 'Goodbye %s!']), 'World')
      # -> kd.slice(['Hello World!', 'Goodbye World!'])
    kd.strings.printf('%v + %v = %v', 1, 2, 3)  # -> kd.slice('1 + 2 = 3')

  Args:
    fmt: Format string (String or Bytes).
    *args: Arguments to format (primitive types compatible with `fmt`).

  Returns:
    The formatted string.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.format'])
@optools.as_backend_operator(
    'kd.strings.format',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.fmt),
        qtype_utils.expect_data_slice_kwargs(P.kwargs),
    ],
)
def format_(fmt, /, **kwargs):  # pylint: disable=unused-argument
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
    fmt: Format string (String or Bytes).
    **kwargs: Arguments to format.

  Returns:
    The formatted string.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.fstr'])
@arolla.optools.as_lambda_operator(
    'kd.strings.fstr',
    experimental_aux_policy='koladata_adhoc_binding_policy[kd.strings.fstr]',
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
  Find more about format specification in kd.strings.format docs.

  NOTE: `{ds:s}` can be used for any type to achieve default string conversion.

  Examples:
    greeting_expr = kd.lazy.fstr(f'Hello, {I.countries:s}!')

    countries = kd.slice(['USA', 'Schweiz'])
    kd.eval(greeting_expr, countries=countries)
      # -> kd.slice(['Hello, USA!', 'Hello, Schweiz!'])

    local_greetings = ds(['Hello', 'Gruezi'])
    # Data slice is interpreted as literal.
    local_greeting_expr = kd.lazy.fstr(
        f'{local_greetings:s}, {I.countries:s}!'
    )
    kd.eval(local_greeting_expr, countries=countries)
      # -> kd.slice(['Hello, USA!', 'Gruezi, Schweiz!'])

    price_expr = kd.lazy.fstr(
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


def _fstr_bind_args(fstr, /):  # pylint: disable=redefined-outer-name
  """Argument binding policy for Koda fstr operator.

  This policy takes f-string with base64 encoded Expressions and converts
  to the format expression. This single expression is passed as argument to the
  identity kd.strings.fstr operator.

  Example (actual expression may be different):
    kd.fstr(f'Hello {I.x:s}') ->
    kd.fstr(kd.format('Hello {x:s}', x=I.x))

  Args:
    fstr: F-string with base64 encoded Expressions.

  Returns:
    Tuple with the input for fstr operator.
  """
  if not isinstance(fstr, str):
    raise TypeError(f'expected a string, got {fstr}')
  return (_fstring.fstr_expr(fstr),)


arolla.abc.register_adhoc_aux_binding_policy(
    fstr, _fstr_bind_args, make_literal_fn=py_boxing.literal
)


@optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'kd.strings._test_only_format_wrapper',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.fmt),
        qtype_utils.expect_data_slice(P.arg_names),
        qtype_utils.expect_data_slice_args(P.args),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
)
def _test_only_format_wrapper(fmt, arg_names, *args):  # pylint: disable=unused-argument
  """Test only wrapper for format with Arolla signature."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'kd.strings.join',
    qtype_constraints=[
        (
            M.qtype.get_field_count(P.args) > 0,
            'expected at least one argument',
        ),
        qtype_utils.expect_data_slice_args(P.args),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
    experimental_aux_policy=py_boxing.DEFAULT_BOXING_POLICY,
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
    'kd.strings.length', qtype_constraints=[qtype_utils.expect_data_slice(P.x)]
)
def length(x):  # pylint: disable=unused-argument
  """Returns a DataSlice of lengths in bytes for Byte or codepoints for String.

  For example,
    kd.strings.length(kd.slice(['abc', None, ''])) -> kd.slice([3, None, 0])
    kd.strings.length(kd.slice([b'abc', None, b''])) -> kd.slice([3, None, 0])
    kd.strings.length(kd.item('你好')) -> kd.item(2)
    kd.strings.length(kd.item('你好'.encode())) -> kd.item(6)

  Note that the result DataSlice always has INT32 schema.

  Args:
    x: String or Bytes DataSlice.

  Returns:
    A DataSlice of lengths.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.lower', qtype_constraints=[qtype_utils.expect_data_slice(P.x)]
)
def lower(x):  # pylint: disable=unused-argument
  """Returns a DataSlice with the lowercase version of each string in the input.

  For example,
    kd.strings.lower(kd.slice(['AbC', None, ''])) -> kd.slice(['abc', None, ''])
    kd.strings.lower(kd.item('FOO')) -> kd.item('foo')

  Note that the result DataSlice always has STRING schema.

  Args:
    x: String DataSlice.

  Returns:
    A String DataSlice of lowercase strings.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.lstrip',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.chars),
    ],
)
def lstrip(s, chars=None):
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
    s: (STRING or BYTES) Original string.
    chars: (Optional STRING or BYTES, the same as `s`): The set of chars to
      remove.

  Returns:
    Stripped string.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.regex_extract',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.text),
        qtype_utils.expect_data_slice(P.regex),
    ],
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
      # kd.item(None).with_schema(kd.STRING)
    kd.strings.regex_extract(kd.item('foobar'), kd.item('^.o(..)a.$'))
      # kd.item('ob')
    kd.strings.regex_extract(kd.item('foobar'), kd.item('.*(b.*r)$'))
      # kd.item('bar')
    kd.strings.regex_extract(kd.slice(['abcd', None, '']), kd.slice('b(.*)'))
      # -> kd.slice(['cd', None, None])

  Args:
    text: (STRING) A string.
    regex: (STRING) A scalar string that represents a regular expression (RE2
      syntax) with exactly one capturing group.

  Returns:
    For the first partial match of `regex` and `text`, returns the substring of
    `text` that matches the capturing group of `regex`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.regex_match',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.text),
        qtype_utils.expect_data_slice(P.regex),
    ],
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
    text: (STRING) A string.
    regex: (STRING) A scalar string that represents a regular expression (RE2
      syntax).

  Returns:
    `present` if `text` matches `regex`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.strings.regex_find_all',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.text),
        qtype_utils.expect_data_slice(P.regex),
    ],
)
def regex_find_all(text, regex):  # pylint: disable=unused-argument
  """Returns the captured groups of all matches of `regex` in `text`.

  The strings in `text` are scanned left-to-right to find all non-overlapping
  matches of `regex`. The order of the matches is preserved in the result. For
  each match, the substring matched by each capturing group of `regex` is
  recorded. For each item of `text`, the result contains a 2-dimensional value,
  where the first dimension captures the number of matches, and the second
  dimension captures the captured groups.

  Examples:
    # No capturing groups, but two matches:
    kd.strings.regex_find_all(kd.item('foo'), kd.item('o'))
      # -> kd.slice([[], []])
    # One capturing group, three matches:
    kd.strings.regex_find_all(kd.item('foo'), kd.item('(.)'))
      # -> kd.slice([['f'], ['o'], ['o']])
    # Two capturing groups:
    kd.strings.regex_find_all(
        kd.slice(['fooz', 'bar', '', None]),
        kd.item('(.)(.)')
    )
      # -> kd.slice([[['f', 'o'], ['o', 'z']], [['b', 'a']], [], []])
    # Get information about the entire substring of each non-overlapping match
    # by enclosing the pattern in additional parentheses:
    kd.strings.regex_find_all(
        kd.slice([['fool', 'solo'], ['bar', 'boat']]),
        kd.item('((.*)o)')
    )
      # -> kd.slice([[[['foo', 'fo']], [['solo', 'sol']]], [[], [['bo', 'b']]]])

  Args:
    text: (STRING) A string.
    regex: (STRING) A scalar string that represents a regular expression (RE2
      syntax).

  Returns:
    A DataSlice where each item of `text` is associated with a 2-dimensional
    representation of its matches' captured groups.
  """
  text = assertion.assert_primitive('text', text, schema_constants.STRING)
  regex = assertion.assert_present_scalar(
      'regex', regex, schema_constants.STRING
  )
  arolla_text = arolla_bridge.to_arolla_dense_array_text(text)
  arolla_regex = arolla_bridge.to_arolla_text(regex)
  result_tuple = M.strings.findall_regex(arolla_text, arolla_regex)
  get_nth = arolla.abc.lookup_operator('core.get_nth')
  flat_res = get_nth(result_tuple, 0)
  value_edge = get_nth(result_tuple, 1)
  group_edge = get_nth(result_tuple, 2)
  shape = arolla_bridge.from_arolla_jagged_shape(
      M.jagged.add_dims(
          arolla_bridge.to_arolla_jagged_shape(
              jagged_shape_ops.get_shape(text)
          ),
          value_edge,
          group_edge,
      ),
  )
  return (
      arolla_bridge.to_data_slice(flat_res)
      .reshape(shape)
      .with_schema(text.get_schema())
  )


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.strings.regex_replace_all',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.text),
        qtype_utils.expect_data_slice(P.regex),
        qtype_utils.expect_data_slice(P.replacement),
    ],
)
def regex_replace_all(text, regex, replacement):
  r"""Replaces all non-overlapping matches of `regex` in `text`.

  Examples:
    # Basic with match:
    kd.strings.regex_replace_all(
        kd.item('banana'),
        kd.item('ana'),
        kd.item('ono')
    )  # -> kd.item('bonona')
    # Basic with no match:
    kd.strings.regex_replace_all(
        kd.item('banana'),
        kd.item('x'),
        kd.item('a')
    )  # -> kd.item('banana')
    # Reference the first capturing group in the replacement:
    kd.strings.regex_replace_all(
        kd.item('banana'),
        kd.item('a(.)a'),
        kd.item(r'o\1\1o')
    )  # -> kd.item('bonnona')
    # Reference the whole match in the replacement with \0:
    kd.strings.regex_replace_all(
       kd.item('abcd'),
       kd.item('(.)(.)'),
       kd.item(r'\2\1\0')
    )  # -> kd.item('baabdccd')
    # With broadcasting:
    kd.strings.regex_replace_all(
        kd.item('foopo'),
        kd.item('o'),
        kd.slice(['a', 'e']),
    )  # -> kd.slice(['faapa', 'feepe'])
    # With missing values:
    kd.strings.regex_replace_all(
        kd.slice(['foobor', 'foo', None, 'bar']),
        kd.item('o(.)'),
        kd.slice([r'\0x\1', 'ly', 'a', 'o']),
    )  # -> kd.slice(['fooxoborxr', 'fly', None, 'bar'])

  Args:
    text: (STRING) A string.
    regex: (STRING) A scalar string that represents a regular expression (RE2
      syntax).
    replacement: (STRING) A string that should replace each match.
      Backslash-escaped digits (\1 to \9) can be used to reference the text that
      matched the corresponding capturing group from the pattern, while \0
      refers to the entire match. Replacements are not subject to re-matching.
      Since it only replaces non-overlapping matches, replacing "ana" within
      "banana" makes only one replacement, not two.

  Returns:
    The text string where the replacements have been made.
  """
  text = assertion.assert_primitive('text', text, schema_constants.STRING)
  replacement = assertion.assert_primitive(
      'replacement', replacement, schema_constants.STRING
  )
  regex = assertion.assert_present_scalar(
      'regex', regex, schema_constants.STRING
  )
  text_a, replacement_a = slices.align(text, replacement)
  text_da = arolla_bridge.to_arolla_dense_array_text(text_a)
  replacement_da = arolla_bridge.to_arolla_dense_array_text(replacement_a)
  regex_t = arolla_bridge.to_arolla_text(regex)
  res_da = M.strings.replace_all_regex(text_da, regex_t, replacement_da)
  return (
      arolla_bridge.to_data_slice(res_da)
      .reshape(text_a.get_shape())
      .with_schema(text.get_schema())
  )


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.replace',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.old),
        qtype_utils.expect_data_slice(P.new),
        qtype_utils.expect_data_slice(P.max_subs),
    ],
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
   s: (STRING or BYTES) Original string.
   old: (STRING or BYTES, the same as `s`) String to replace.
   new: (STRING or BYTES, the same as `s`) Replacement string.
   max_subs: (optional INT32) Max number of substitutions. If unspecified or
     negative, then there is no limit on the number of substitutions.

  Returns:
    String with applied substitutions.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.rfind',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.substr),
        qtype_utils.expect_data_slice(P.start),
        qtype_utils.expect_data_slice(P.end),
    ],
)
# pylint: disable=unused-argument,redefined-outer-name
def rfind(
    s,
    substr,
    start=data_slice.DataSlice.from_vals(0, schema_constants.INT64),
    end=data_slice.DataSlice.from_vals(None, schema_constants.INT64),
):  # pylint: enable=unused-argument,redefined-outer-name
  """Returns the offset of the last occurrence of `substr` in `s`.

  The units of `start`, `end`, and the return value are all byte offsets if `s`
  is `BYTES` and codepoint offsets if `s` is `STRING`.

  Args:
   s: (STRING or BYTES) Strings to search in.
   substr: (STRING or BYTES) Strings to search for in `s`. Should have the same
     dtype as `s`.
   start: (optional int) Offset to start the search, defaults to 0.
   end: (optional int) Offset to stop the search, defaults to end of the string.

  Returns:
    The offset of the last occurrence of `substr` in `s`, or missing if there
    are no occurrences.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.rstrip',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.chars),
    ],
)
def rstrip(s, chars=None):
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
    s: (STRING or BYTES) Original string.
    chars (Optional STRING or BYTES, the same as `s`): The set of chars to
      remove.

  Returns:
    Stripped string.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.split',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.sep),
    ],
)
def split(x, sep=None):
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
    'kd.strings.strip',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.s),
        qtype_utils.expect_data_slice(P.chars),
    ],
)
def strip(s, chars=None):
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
    s: (STRING or BYTES) Original string.
    chars (Optional STRING or BYTES, the same as `s`): The set of chars to
      remove.

  Returns:
    Stripped string.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.substr',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.start),
        qtype_utils.expect_data_slice(P.end),
    ],
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
    'kd.strings.upper', qtype_constraints=[qtype_utils.expect_data_slice(P.x)]
)
def upper(x):  # pylint: disable=unused-argument
  """Returns a DataSlice with the uppercase version of each string in the input.

  For example,
    kd.strings.upper(kd.slice(['abc', None, ''])) -> kd.slice(['ABC', None, ''])
    kd.strings.upper(kd.item('foo')) -> kd.item('FOO')

  Note that the result DataSlice always has STRING schema.

  Args:
    x: String DataSlice.

  Returns:
    A String DataSlice of uppercase strings.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.decode', qtype_constraints=[qtype_utils.expect_data_slice(P.x)]
)
def decode(x):  # pylint: disable=unused-argument
  """Decodes `x` as STRING using UTF-8 decoding."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.encode', qtype_constraints=[qtype_utils.expect_data_slice(P.x)]
)
def encode(x):  # pylint: disable=unused-argument
  """Encodes `x` as BYTES using UTF-8 encoding."""
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_backend_operator('kd.strings._decode_base64')
def _decode_base64(x, missing_if_invalid):  # pylint: disable=unused-argument
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry()
@optools.as_lambda_operator(
    'kd.strings.decode_base64',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice_or_unspecified(P.on_invalid),
    ],
)
def decode_base64(x, /, *, on_invalid=arolla.unspecified()):
  """Decodes BYTES from `x` using base64 encoding (RFC 4648 section 4).

  The input strings may either have no padding, or must have the correct amount
  of padding. ASCII whitespace characters anywhere in the string are ignored.

  Args:
    x: DataSlice of STRING or BYTES containing base64-encoded strings.
    on_invalid: If unspecified (the default), any invalid base64 strings in `x`
      will cause an error. Otherwise, this must be a DataSlice broadcastable to
      `x` with a schema compatible with BYTES, and will be used in the result
      wherever the input string was not valid base64.

  Returns:
    DataSlice of BYTES.
  """
  return arolla.types.DispatchOperator(
      'x, on_invalid',
      error_if_invalid_case=arolla.types.DispatchCase(
          _decode_base64(P.x, arolla.boolean(False)),
          condition=P.on_invalid == arolla.UNSPECIFIED,
      ),
      default=masking_ops.coalesce(
          _decode_base64(P.x, arolla.boolean(True)),
          masking_ops.apply_mask(P.on_invalid, masking_ops.has(P.x)),
      ),
  )(x, on_invalid)


@optools.add_to_registry()
@optools.as_backend_operator(
    'kd.strings.encode_base64',
    qtype_constraints=[qtype_utils.expect_data_slice(P.x)],
)
def encode_base64(x):  # pylint: disable=unused-argument
  """Encodes BYTES `x` using base64 encoding (RFC 4648 section 4), with padding.

  Args:
    x: DataSlice of BYTES to encode.

  Returns:
    DataSlice of STRING.
  """
  raise NotImplementedError('implemented in the backend')
