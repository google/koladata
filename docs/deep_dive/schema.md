<!-- go/markdown-->

# Technical Deep Dive: Schema

This guide offers a deep dive into `Schemas` and is part of a
[Koda Technical Deep Dive](overview.md) series.

* TOC
{:toc}

## Schema

Koda Schemas describe the type of content of a DataSlice. They dictate behavior
for attribute lookup/setting, clone/extract operations, etc. The associated type
promotion rules dictate the behavior of operators involving different but
compatible types.

The schema of a DataSlice can be accessed through `ds.get_schema()` and can be
changed through `kd.cast_to(ds, schema)` which casts `ds` to `schema` and
changes the underlying data if needed, or `ds.with_schema(schema)` which
*reinterprets* `ds` as `schema` without changing the underlying data.

### The Different Schemas

The existing Koda schemas, and their meaning, are listed below. Note that all
schemas allow missing values, so each item in e.g. an `INT32` DataSlice is
either a proper 32-bit integer or missing.

| Schema        | Meaning                                                      |
| ------------- | ------------------------------------------------------------ |
| `NONE`        | The DataSlice holds no values and the type is unspecified. A |
:               : DataSlice with this schema can *never* hold values.          :
| `INT32`       | 32-bit integer.                                              |
| `INT64`       | 64-bit integer.                                              |
| `FLOAT32`     | 32-bit floating point value.                                 |
| `FLOAT64`     | 64-bit floating point value.                                 |
| `BOOLEAN`     | Boolean value.                                               |
| `MASK`        | Mask value.                                                  |
| `BYTES`       | Bytestring value.                                            |
| `STRING`      | UTF-8 encoded text value.                                    |
| `EXPR`        | Quoted Koda Expression value.                                |
| `ITEMID`      | ItemId value, interpreted as an id. Attribute access and     |
:               : similar is disabled.                                         :
| Struct schema | ItemId value. The schema is dynamically allocated and is in  |
:               : itself an ItemId with associated attributes. Includes List,  :
:               : Dict, and Entities.                                          :
| `SCHEMA`      | Schema value. Either as a fixed schema type or as an         |
:               : allocated struct schema.                                     :
| `OBJECT`      | Primitive or ItemId values. Each value includes information  |
:               : about its own type\: primitives have a corresponding         :
:               : primitive schema and ItemIds have an associated Entity       :
:               : `__schema__` attribute giving the schema of the stored       :
:               : value. This schema allows the DataSlice to contain mixed     :
:               : (heterogeneous) values.                                      :

#### Struct Schemas

A DataSlice with a Struct schema holds ItemId values, representing Entities,
Lists or Dicts. The Struct schema is in itself an ItemId with associated
attributes. An attribute `'a'` on the schema gives the schema of attribute `'a'`
of the value. For example, the entity `SCHEMA(x=INT32)` indicates that the
values accessible through the attribute `'x'` are `INT32`. Note that a DataSlice
with an Struct schema have homogeneously typed values.

Entity schemas created through `kd.new` are, unlike all other schemas,
dynamically allocated and associated with values in a DataBag. Like
`kd.new(x=1)` is dynamically allocated, so is `kd.new(x=1).get_schema()`:

```py
e1 = kd.new(x=1)
e2 = kd.new(x=1)

assert e1.get_schema() != e2.get_schema()  # Comparison through ItemId.
```

Entity schemas can have their attributes altered:

```py
schema = kd.new(x=1).get_schema()
schema.with_attr('y', kd.FLOAT32)  # -> ENTITY(x=INT32, y=FLOAT32)
```

List and Dict schemas have universally-unique ItemIds, and their attributes
cannot be altered:

```py
l1 = kd.list([1, 2])
l2 = kd.list([1, 2])

assert l1.get_schema() == l2.get_schema()

l3 = kd.list([1, 2, 3, 4])

assert l1.get_schema() == l3.get_schema()
```

Entity schemas can also be created to be universally unique:

```py
assert kd.uu_schema(x=kd.INT32) == kd.uu_schema(x=kd.INT32)
assert kd.uu(x=1).get_schema() == kd.uu(x=1).get_schema()
```

#### OBJECT

The object schema is worth discussing in more detail. It has the unique property
among the schemas that it doesn't indicate the type of the object, but rather
that the type of each value should be inferred from the value itself. For
primitive values, the schema of each value is simply the corresponding schema.
For example, `int32{1}` has schema `INT32` and `bool{True}` has schema `BOOL`.
For ItemIds, the schema is stored in the `__schema__` attribute. For example:

```py
o1 = kd.obj(x=1)
o1.get_schema()  # OBJECT
o1.get_obj_schema()  # Looks up the `__schema__`: IMPLICIT_ENTITY(x=INT32)
```

Since each value carries its own schema (either implicitly or in the
`__schema__` attribute), it is possible for them to differ. `OBJECT` DataSlices
are the only ones capable of holding heterogeneous data. For example:

```py
ds = kd.slice([1, 'abc', kd.obj(x=1)])
ds.get_schema()  # OBJECT
ds.get_obj_schema()  # [INT32, STRING, IMPLICIT_ENTITY(x=INT32)]
```

Both DataSlices with a struct schema or `OBJECT` schema can hold structured
data. The key difference is that in the case of DataSlices with a struct schema,
the schema of each value is entirely defined by the schema of the DataSlice,
while in the case of `OBJECT`, it is local to each value. Because of this,
DataSlices with struct schemas are both more strict and faster for common
operations such as attribute access:

```py
mask = kd.slice([kd.present, None] * 50_000)
# Filtered objects and entities.
objects = kd.obj(x=kd.slice([1] * 100_000)) & mask
entities = kd.new(x=kd.slice([1] * 100_000)) & mask

# We can fill `objects` with anything, as the common schema is `OBJECT`.
objects | 2.0  # [Obj(x=1), 2.0, Obj(x=1), ...]

# But there is no common schema of `<entities.get_schema(), FLOAT32>`.
entities | 2.0  # -> Exception.

# Attribute access is faster for `entities` as the schema can be taken directly
# from the DataSlice instead of a collection of schema attributes.
_ = objects.x  # -> 1.2ms
_ = entities.x  # -> 322µs

# Fetching available attribute names is much faster for `entities` as it is
# given by the DataSlice schema instead of a collection of schema attributes.
_ = kd.dir(objects)  # -> 1.36ms
_ = kd.dir(entities)  # -> 1.25µs
```

#### Implicit and Explicit Entity Schemas

Entity schemas created explicitly using schema creation APIs or as a by-product
of `kd.new(**kwargs)` are called explicit entity schemas. Those created
implicitly as a by-product of `kd.obj(**kwargs)` are called implicit schemas.

Explicit entity schemas and implicit entity schemas differ by how they handle
schema conflicts during assignment. Attributes of an explicit entity schema
cannot be overridden unless `overwrite_schema=True` is set while attributes of
an implicit entity schema can be overridden by default.

```py
entity = kd.new(a=1)
# Fail as schemas are not compatible
# entity.with_attrs(a='2')
entity = entity.with_attrs(a='2', overwrite_schema=True)
entity.get_schema()  # ENTITY(a=STRING)

obj = kd.obj(a=1)
obj = obj.with_attrs(a='2')
obj.get_obj_schema()  # IMPLICIT_ENTITY(a=STRING)
```

The motivation behind this is that an explicit entity schema can be used by
multiple entities while an implicit schema cannot. Thus overriding schema
attributes of an explicit schema without `overwrite_schema=True` is dangerous.
For example,

```py
entities = kd.new(a=kd.slice([1, 2]))
# Only update the first item
# We want to assign it to '3' rather than 3 by mistake
# Imagine the following line succeeds without overwrite_schema=True
upd = kd.attrs(entities.S[0], a='3', overwrite_schema=True)
entities = entities.updated(upd)
# Fails because one value is 2 but schema is STRING
entities.a
```

However, it is not a problem for an implicit schema and allowing direct
overrides makes the code more concise.

```py
objs = kd.obj(a=kd.slice([1, 2]))
objs.a  # DataSlice([1, 2], schema: INT32, ndims: 1, size: 2)
upd = kd.attrs(objs.S[0], a='3')
objs = objs.updated(upd)
# It is fine as objects have different implicit schemas
objs.a  # DataSlice(['3', 2], schema: OBJECT, ndims: 1, size: 2)
```

NOTE: Adding new attributes is allowed for both explicit and implicit entity
schemas.

```py
entity = kd.new(a=1)
entity = entity.with_attrs(b='2')
entity.get_schema()  # ENTITY(a=INT32, b=STRING)

obj = kd.obj(a=1)
obj = obj.with_attrs(b='2')
obj.get_obj_schema()  # IMPLICIT_ENTITY(a=INT32, b=STRING)
```

### Type Promotion

Koda has well-defined rules for dealing with values with differing schemas that
are consistently applied across the library. The type promotion rules affect the
behavior of operators (especially those with several inputs), boxing of values
into DataSlices, and attribute assignment to name a few. These rules mainly
dictate *implicit* casting behavior, which are safe casts applied without user
intervention. The rules for *explicit* casting are more relaxed and may be
unsafe, and therefore require explicit calls from the user.

#### Implicit Casting

Implicit casting, or type promotion, plays a role in many parts of Koda. Values
are casted implicitly without user intervention to make for a smooth experience.
For example, the following examples show the type promotion rules in action:

*   `kd.math.add(kd.int32(1), kd.int64(2))` results in `kd.int64(3)`.
*   `kd.schema.new_schema(x=kd.INT64).new(x=kd.int32(1)).x` results in
    `kd.int64(1)`.
*   `kd.slice([1, 2.0])` results in a `FLOAT32` DataSlice `kd.float32([1.0,
    2.0])`.

The output schema of the `kd.math.add` example is the *common schema* of the
input schemas `<INT32, INT64> = INT64`. The type promotion rules dictate the
common schema for a collection of input schemas, and casts are done to the
common schema and are applied implicitly. Because of this, these rules should be
consistent and result in safe (not raise) and efficient casts. Note that not all
combinations of schemas have a common schema, for example `<ITEMID, INT32>`, and
an error is instead raised when computing it.

Koda's type promotion rules are defined through the following (partial)
[type promotion lattice](https://jax.readthedocs.io/en/latest/jep/9407-type-promotion.html#stepping-back-tables-and-lattices).
The supremum between two nodes in the lattice is the common schema. If one
doesn't exist, the common schema doesn't either.

```dot
digraph {
  node [fontsize=11, margin=0]
  "NONE" [width=1]

  "NONE" -> "Struct schema 1"
  "NONE" -> "Struct schema 2"
  "NONE" -> "..."
  "NONE" -> "ITEMID"
  "NONE" -> "SCHEMA"
  "NONE" -> "INT32"
  "NONE" -> "MASK"
  "NONE" -> "BOOL"
  "NONE" -> "BYTES"
  "NONE" -> "STRING"
  "NONE" -> "EXPR"
  "INT32" -> "INT64"
  "INT64" -> "FLOAT32"
  "FLOAT32" -> "FLOAT64"
  "FLOAT64" -> "OBJECT"
  "MASK" -> "OBJECT"
  "BOOL" -> "OBJECT"
  "BYTES" -> "OBJECT"
  "STRING" -> "OBJECT"
  "EXPR" -> "OBJECT"
}
```

For example, the common schema of `<INT32, INT64> = INT64` because that's the
supremum node. Similarly, `<INT32, FLOAT64> = FLOAT64` and `<INT32, MASK> =
OBJECT` while `<INT32, ITEMID>` is not defined.

Because the rules are defined through a (partial) type promotion lattice, they
are associative and commutative, meaning that the common schema of `<A, B, C> =
<<A, B>, C> = <A, <B, C>> = <<B, A>, C>, ...`. Additionally, each edge in the
lattice, and thereby the type promotion rules themselves, adheres to the
following criteria:

*   The resulting casts are safe and will not raise.
    *   `<INT32, INT64> = INT64` is an example of a safe cast since all 32-bit
        integers can safely be represented as 64-bit integers. The converse is
        not true.
*   Conservative promotion into wider types.
    *   Wider types are in general slower, use more memory, and are not as well
        supported in some workflows and on some architectures. Overly-eager
        promotion into wide types increases the risk of infeasible amounts of
        memory being used without the user intending to, limiting the usability
        of Koda as a whole.
*   Avoid unnecessary precision loss (`1.544 -> 1.5`) and, more importantly,
    loss of magnitude (`2**45` -> `INF`).
    *   `INT64` -> `FLOAT32` is a compromise between user convenience and
        preciseness. `FLOAT32` can represent the magnitude of `INT64`, but not
        its full range (with more pronounced precision loss for larger values).
    *   Floats are preferred over integers, similar to other libraries as well
        as Python.
    *   It avoids overly eager promotions to 64-bit values, such as `INT32` ->
        `FLOAT64` (necessary to represent the full range of values), which would
        leave `INT64` promotion unspecified, and would lead to potential
        performance degradation.
*   Avoid unnecessary use of mixed types through `OBJECT`.
    *   Some operators cannot handle mixed data, it is slower to work with, and
        it’s harder to reason about.

Note that differing struct schemas have no common schema. As such, type
promotion of nested types, such as `<LIST[NONE], LIST[INT32]>` is not supported.

For attribute assignments, such as
`kd.schema.new_schema(x=kd.INT64).new(x=kd.int32(1))`, implicit casting is done
to accommodate the differing types. As the attribute schema is fixed to `INT64`,
we require that for an assignment of a value with schema `X`, `<INT64, X> =
INT64`[^narrowing]. In such cases, we say that `X` is implicitly castable to
`INT64`. In fact, operations defined on some schema `Y` are therefore also
defined on all schemas that are implicitly castable to it.

[^narrowing]: In reality, we require that the *narrowed schema* of the input is
    implicitly castable to `INT64`. See the [Narrowing](#narrowing)
    section for more details.

Most of the time, these rules can be ignored as implicit casting will be done
without additional inputs from users. However, the following helpers are also
available:

*   [`kd.schema.agg_common_schema`](/koladata/g3doc/api_reference.md#kd.schema.agg_common_schema):
    Computes the common schema across the last `ndim` dimensions of a DataSlice.
*   [`kd.schema.common_schema`](/koladata/g3doc/api_reference.md#kd.schema.common_schema):
    Computes the common schema of the entire DataSlice.
*   [`kd.schema.cast_to_implicit`](/koladata/g3doc/api_reference.md#kd.schema.cast_to_implicit):
    Casts the input to a provided schema if allowed by the type promotion rules.

#### Explicit Casting

As a complement to implicit casting, it's possible to *explicitly* cast between
types through more relaxed rules. As the casting is done directly by the user,
it is not required that the cast is safe. For example, explicitly casting to
`INT32` is supported for e.g. `FLOAT32` in addition to all implicitly castable
inputs. However, this may fail if the value is too large.

Here are some cases where explicit casting is done:

*   [`kd.schema.cast_to`](/koladata/g3doc/api_reference.md#kd.schema.cast_to):
    Casts the input to the provided schema using relaxed casting rules (that may
    fail).
*   [`kd.int64`](/koladata/g3doc/api_reference.md#kd.int64):
    Converts the input to INT64 with relaxed casting rules (that may fail).
*   `kd.slice(data, schema=kd.INT64)`: Boxes the `data` to an INT64 slice with
    relaxed casting rules (that may fail).

Explicit casting between Entities is not supported. Instead,
`ds.with_schema(new_entity_schema)` can be used as an (unsafe) alternative to
*reinterpret cast* `ds` to the `new_entity_schema`.

#### Narrowing

The `OBJECT` schema is special since it doesn't indicate the type of the data it
holds, but rather indicates that the type is provided by each value itself. To
systematically support attribute assignment of `OBJECT` values, such as
`kd.schema.new_schema(x=kd.INT32).new(x=kd.item(1, schema=kd.OBJECT))`, Koda has
the concept of schema *narrowing*.

For all other schemas apart from `OBJECT`, the narrowed schema of a DataSlice
`ds` is simply the schema `ds.get_schema()`. For `OBJECT`, the narrowed schema
is the common schema of all elements, or `OBJECT` if none exists.

For attribute assignment to an attribute with schema `X`, we therefore require
that the new value has a *narrowed* schema that is implicitly castable to `X`.
That is, the data itself must be safely castable to `X`, even though the value
schema may not be implicitly castable to `X` in case of `OBJECT`. Note that
attribute assignment is only one example out of several where schema narrowing
is used:

```py
schema = kd.schema.new_schema(x=kd.INT32)
# The input is narrowed to `kd.item(1, schema=kd.INT32)` before being assigned.
schema.new(x=kd.item(1, schema=kd.OBJECT))  # DataItem(1, schema: INT32)

str_list = kd.list(['foo', 'bar'])
# The index is narrowed to `kd.item(0, schema=kd.INT64)`.
str_list[kd.item(0, schema=kd.OBJECT)]  # DataItem('foo', schema: STRING)

ints = kd.slice([[1, 2], [3]])
# The `ndim` is narrowed to `kd.item(2, schema=kd.INT64)`.
kd.agg_sum(ints, ndim=kd.item(2, schema=kd.OBJECT))  # DataItem(6, schema=INT32)
```

Here are some additional helpers to perform narrowing:

*   [`kd.schema.cast_to_narrow`](/koladata/g3doc/api_reference.md#kd.schema.cast_to_narrow):
    Casts the input to the provided schema by first computing the narrowed
    schema of the input and then casting to the provided schema using implicit
    casting rules.

### Boxing

Conversion from Python values to corresponding Koda values, which we refer to as
*boxing*, by default makes use of the type promotion rules seen above. In case a
`schema` is supplied, the result is instead produced using explicit casting
rules. Boxing through `kd.slice` can be split into two parts:

1.  A local decision for each "scalar" value of the input.
1.  A global decision for the entire resulting DataSlice.

Boxing for `kd.from_py` works in a similar manner, but with a slight variation
to accommodate how lists and other input types are treated.

#### Scalar DataItem Boxing

Scalar values are boxed according to the (incomplete) rules below:

*   If the value has a specified numeric type and width, e.g. np.int32, the
    DataItem should have the same numeric type and width, e.g. `INT32`.
*   If the value is a Python float, we attempt to wrap it into a `FLOAT32`, and
    fall back to `FLOAT64` if the magnitude of the value is too large (the
    magnitude cannot fit into `FLOAT32`).
*   If the value is a Python integer, we attempt to wrap it into an `INT32`, and
    fall back to `INT64` if the magnitude of the value is too large. If the
    value exceeds the limits of `INT64`, the result is the input as an `INT64`
    value modulo `MAX_INT64`.
*   Python booleans are converted to `BOOL`.
*   Python bytes are converted to `BYTES`, and str is converted to `STRING`.
*   Arolla types are converted into their corresponding schema if such exists,
    or fails otherwise.
*   Missing values (None) are treated as `NONE`.

The least obvious of these rules is the rules for Python numericals, where the
schema of the DataItem depends on the value itself, not just its type. While
this may cause confusion, it is motivated by practical user benefits and
existing users that rely on this behavior. The choice of wrapping as `INT32`
rather than always using `INT64` is motivated by the desire to avoid overly
eager use of wide types.

Treating None as `NONE`, which is the “minimum” schema, effectively marks its
type as unknown. This allows e.g. `LHS + None` to defer its schema to the `LHS`,
irrespective of its schema. This also allows us to compute the schema of
`kd.slice([...])` by the common type of its inputs.

#### DataSlice Boxing

Given a list of values, the schema of the DataSlice is given by the common
schema of the values in the list when viewed as DataItems. Example:
`kd.slice([1, 2.0]).get_schema()` -> `FLOAT32` since `<INT32, FLOAT32> =
FLOAT32`.

`kd.slice([])` has a `NONE` schema, which corresponds to the behavior of
`DataItem(None)` and has the property that `kd.concat(kd.slice(x[:i]),
kd.slice(x[i:])) == kd.slice(x)` for `0 <= i <= slice.size()`.

### Relation to Arolla

Arolla powers the underlying computations of Koda, and Koda operators are
implemented using Arolla. Arolla uses QTypes to enable compilation, to dispatch
to underlying computations, and to add type safety since the QType can flow
through the expression. Due to the dynamic nature of Koda, schemas are *not*
reflected in the QType system, and instead a single `DATA_SLICE` QType is used
throughout. The benefit is that the schema is allowed to be dynamic, and to be
treated as data inside of a DataBag. The downside is that the benefits that
Arolla can provide, mainly with respect to type safety at compilation time,
cannot be used. As such, Koda provides optional runtime checks
([`kd.check_inputs`](/koladata/g3doc/api_reference.md#kd.check_inputs)
and
[`kd.check_output`](/koladata/g3doc/api_reference.md#kd.check_output))
that provide type safety.
