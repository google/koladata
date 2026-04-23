<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.type_checking API

Utilities to annotatate functions with type checking.





### `kd.type_checking.check_inputs(**kw_constraints: TypeConstraint)` {#kd.type_checking.check_inputs}
Aliases:

- [kd.check_inputs](../kd.md#kd.check_inputs)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Decorator factory for adding runtime input type checking to Koda functions.

Resulting decorators will check the schemas of DataSlice inputs of
a function at runtime, and raise TypeError in case of mismatch.

Decorated functions will preserve the original function&#39;s signature and
docstring.

Decorated functions can be traced using `kd.fn` and the inputs to the
resulting functor will be wrapped in kd.assertion.with_assertion nodes that
match the assertions of the eager version.

Example for primitive schemas:

  @kd.check_inputs(hours=kd.INT32, minutes=kd.INT32)
  @kd.check_output(kd.STRING)
  def timestamp(hours, minutes):
    return kd.str(hours) + &#39;:&#39; + kd.str(minutes)

  timestamp(ds([10, 10, 10]), kd.ds([15, 30, 45]))  # Does not raise.
  timestamp(ds([10, 10, 10]), kd.ds([15.35, 30.12, 45.1]))  # raises TypeError

Example for complex schemas:

  Doc = kd.schema.named_schema(&#39;Doc&#39;, doc_id=kd.INT64, score=kd.FLOAT32)

  Query = kd.schema.named_schema(
      &#39;Query&#39;,
      query_text=kd.STRING,
      query_id=kd.INT32,
      docs=kd.list_schema(Doc),
  )

  @kd.check_inputs(query=Query)
  @kd.check_output(Doc)
  def get_docs(query):
    return query.docs[:]

Example for an argument that should not be an Expr at tracing time:
  @kd.check_inputs(x=kd.static_when_tracing(kd.INT32))
  def f(x):
    return x


Args:
  **kw_constraints: mapping of parameter names to type constraints. Names must
    match parameter names in the decorated function. Arguments for the given
    parameters must be DataSlices/DataItems that match the given type
    constraint(in particular, for SchemaItems, they must have the
    corresponding schema).

Returns:
  A decorator that can be used to type annotate a function that accepts
  DataSlices/DataItem inputs.</code></pre>

### `kd.type_checking.check_output(constraint: TypeConstraint)` {#kd.type_checking.check_output}
Aliases:

- [kd.check_output](../kd.md#kd.check_output)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Decorator factory for adding runtime output type checking to Koda functions.

Resulting decorators will check the schema of the DataSlice output of
a function at runtime, and raise TypeError in case of mismatch.

Decorated functions will preserve the original function&#39;s signature and
docstring.

Decorated functions can be traced using `kd.fn` and the output of the
resulting functor will be wrapped in a kd.assertion.with_assertion node that
match the assertion of the eager version.

Example for primitive schemas:

  @kd.check_inputs(hours=kd.INT32, minutes=kd.INT32)
  @kd.check_output(kd.STRING)
  def timestamp(hours, minutes):
    return kd.to_str(hours) + &#39;:&#39; + kd.to_str(minutes)

  timestamp(ds([10, 10, 10]), kd.ds([15, 30, 45]))  # Does not raise.
  timestamp(ds([10, 10, 10]), kd.ds([15.35, 30.12, 45.1]))  # raises TypeError

Example for complex schemas:

  Doc = kd.schema.named_schema(&#39;Doc&#39;, doc_id=kd.INT64, score=kd.FLOAT32)

  Query = kd.schema.named_schema(
      &#39;Query&#39;,
      query_text=kd.STRING,
      query_id=kd.INT32,
      docs=kd.list_schema(Doc),
  )

  @kd.check_inputs(query=Query)
  @kd.check_output(Doc)
  def get_docs(query):
    return query.docs[:]

Args:
  constraint: A type constraint for the output. Output of the decorated
    function must be a DataSlice/DataItem that matches the constraint(in
    particular, for SchemaItems, they must have the corresponding schema).

Returns:
  A decorator that can be used to annotate a function returning a
  DataSlice/DataItem.</code></pre>

### `kd.type_checking.duck_dict(key_constraint: TypeConstraint, value_constraint: TypeConstraint)` {#kd.type_checking.duck_dict}
Aliases:

- [kd.duck_dict](../kd.md#kd.duck_dict)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a duck dict constraint to be used in kd.check_inputs/output.

A duck_dict constraint will assert a DataSlice is a dict, checking the
key_constraint on the keys and value_constraint on the values. Use it if you
need to nest duck type constraints in dict constraints.

Example:
  @kd.check_inputs(mapping=kd.duck_dict(kd.STRING,
      kd.duck_type(doc_id=kd.INT64, score=kd.FLOAT32)))
  def f(query):
    pass

Args:
  key_constraint:  TypeConstraint representing the constraint to be
    checked on the keys of the dict.
  value_constraint:  TypeConstraint representing the constraint to be
    checked on the values of the dict.

Returns:
  A duck type constraint to be used in kd.check_inputs or kd.check_output.</code></pre>

### `kd.type_checking.duck_list(item_constraint: TypeConstraint)` {#kd.type_checking.duck_list}
Aliases:

- [kd.duck_list](../kd.md#kd.duck_list)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a duck list constraint to be used in kd.check_inputs/output.

A duck_list constraint will assert a DataSlice is a list, checking the
item_constraint on the items. Use it if you need to nest
duck type constraints in list constraints.

Example:
  @kd.check_inputs(query=kd.duck_type(docs=kd.duck_list(
      kd.duck_type(doc_id=kd.INT64, score=kd.FLOAT32)
  )))
  def f(query):
    pass

Args:
  item_constraint:  TypeConstraint representing the constraint to be
    checked on the items of the list.

Returns:
  A duck type constraint to be used in kd.check_inputs or kd.check_output.</code></pre>

### `kd.type_checking.duck_type(**kwargs: TypeConstraint)` {#kd.type_checking.duck_type}
Aliases:

- [kd.duck_type](../kd.md#kd.duck_type)

<pre class="no-copy"><code class="lang-text no-auto-prettify">Creates a duck type constraint to be used in kd.check_inputs/output.

A duck type constraint will assert that the DataSlice input/output of a
function has (at least) a certain set of attributes, as well as to specify
recursive constraints for those attributes.

Example:
  @kd.check_inputs(query=kd.duck_type(query_text=kd.STRING,
     docs=kd.duck_type()))
  def f(query):
    pass

  Checks that the DataSlice input parameter `query` has a STRING attribute
  `query_text`, and an attribute `docs` of any schema. `query` may also have
  additional unspecified attributes.

Args:
  **kwargs: mapping of attribute names to constraints. They can be any other
    kind of TypeConstraint. To assert only the presence of an
    attribute, without specifying additional constraints on that attribute,
    pass an empty duck type for that attribute.

Returns:
  A duck type constraint to be used in kd.check_inputs or kd.check_output.</code></pre>

### `kd.type_checking.functor(signature: inspect.Signature | None = None) -> _FunctorType` {#kd.type_checking.functor}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Returns constraint that an argument is a functor.

Optionally checking signature.

Args:
  signature: An optional `inspect.Signature` to check against the functor&#39;s
    signature. If None, only checks that the argument is a functor.</code></pre>

### `kd.type_checking.static_when_tracing(base_type: TypeConstraint | None = None) -> _StaticWhenTraced` {#kd.type_checking.static_when_tracing}
Aliases:

- [kd.static_when_tracing](../kd.md#kd.static_when_tracing)

<pre class="no-copy"><code class="lang-text no-auto-prettify">A constraint that the argument is static when tracing.

It is used to check that the argument is not an expression during tracing to
prevent a common mistake.

Examples:
- combined with checking the type:
  @type_checking.check_inputs(value=kd.static_when_tracing(kd.INT32))
- without checking the type:
  @type_checking.check_inputs(pick_a=kd.static_when_tracing())

Args:
  base_type: (optional)The base type to check against. If not specified, only
    checks that the argument is a static when tracing.

Returns:
  A constraint that the argument is a static when tracing.</code></pre>

