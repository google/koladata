 <!-- go/markdown -->

# Koda Overview

This guide gives a quick 60-minute overview of **Koda**, geared mainly for new
users. See the [10 Minute Introduction](10_min_intro.md) for a short
introduction walking through an example, and the
[Koda Fundamentals](fundamentals.md) guide for a more detailed introduction.

Also see [Koda Cheatsheet](cheatsheet.md) for quick references.

* TOC
{:toc}

## Why Koda

### Koda Distinguishing Features

*   **Vectorization for complex data**: supports vectorized operations with C++
    grade performance not only for tables and arrays of primitives, but also for
    nested dicts, protos, structs, and graphs.
*   **Immutability**: enables modifications and keeping multiple, slightly
    varied versions of data without duplicating memory.
*   **Modular data**: data can be efficiently (usually for O(1)) split and
    joined, overlaid or enriched, and the data modules can be not just tables,
    columns and rows, but anything - from single attributes to proto updates to
    graph modifications.
*   **Computational graphs, lazy evaluation and serving support**: utilizes
    computational graphs that can be introspected and modified, and enable
    optimized data processing and serving.

### Why to Use Koda

*   **Interactivity in Colab**: transform interactively training data, design
    decision-making logic and evaluation flows (scoring, ranking, metrics etc.),
    work with models and more, where your data is tables, protos, structs,
    graphs etc.
*   **What-if experiments**: change input data, change evaluation
    logic, and get insights instantly.
*   **Zoom in on your data**: utilize **data views, updates, overlays,
    versions**, including when working with **large data sources**, and use only
    parts of the data needed at the moment.
*   **Performance**: computation is vectorized and performed in optimized C++.
*   **Evaluate in distributed environment or serve in production**: convert
    evaluation logic into **computational graphs**
    (ASTs) that can be introspected, optimized then evaluated in distributed
    environment or served in production.

## DataSlices and Items

Koda enables **vectorization** by utilizing *DataSlices*. DataSlices are arrays
with **partition trees**. They are stored and manipulated as **jagged arrays**
(irregular multi-dimensional arrays). Such partition trees are called
**JaggedShape**.

For example, the following DataSlice has 2 dimensions and 5 items. The first
dimension has 2 items and the second dimension has 5 items partitioned as `[3,
2]`.

```py {.pycon-doctest}
>>> from koladata import kd
>>> ds = kd.slice([["one", "two", "three"], ["four", "five"]])
>>> ds.get_ndim()
DataItem(2, schema: INT64)
>>> ds.get_shape()
JaggedShape(2, [3, 2])
```

Conceptually, it can be thought as partition tree + flattened array as shown in
the graph below.

```dot
digraph {
  Root -> "dim_1:0"
  Root -> "dim_1:1"
  "dim_1:0" -> "dim_2:0"
  "dim_1:0" -> "dim_2:1"
  "dim_1:0" -> "dim_2:2"
  "dim_2:0 bis" [label = "dim_2:0"]
  "dim_2:1 bis" [label = "dim_2:1"]
  "dim_1:1" -> "dim_2:0 bis"
  "dim_1:1" -> "dim_2:1 bis"
  "dim_2:0" -> one
  "dim_2:1" -> two
  "dim_2:2" -> three
  "dim_2:0 bis" -> four
  "dim_2:1 bis" -> five

  subgraph cluster_x {
    graph [style="dashed", label="Partition tree"]
    Root;"dim_1:0";"dim_1:1";"dim_2:0";"dim_2:1";"dim_2:2";"dim_2:0 bis";"dim_2:1 bis";
  }

  subgraph cluster_y {
    graph [style="dashed", label="Flattened array"]
    one;two;three;four;five
  }
}
```

**Items** are the elements of a DataSlice, and can be primitives (e.g. integers
or strings), or more complex data structures (e.g. lists, dicts and entities).

A zero-dimensional DataSlice is a scalar item. It is called a DataItem.

```py {.pycon-doctest}
>>> kd.item(1, schema=kd.FLOAT32)
DataItem(1.0, schema: FLOAT32)
>>> kd.item(1, schema=kd.FLOAT32).to_py()
1.0
>>> kd.list([10, 20, 30, 40])[2]
DataItem(30, schema: INT32, bag_id...)
>>> kd.dict({1: 'a', 2: 'b'})[2]
DataItem('b', schema: STRING, bag_id...)
>>> kd.from_py([{'a': [1, 2, 3], 'b': [4, 5, 6]}, {'a': 3, 'b': 4}])
DataItem(List[Dict{...'a'=List[1, 2, 3]...}, Dict{...'a'=3...}], schema: OBJECT, bag_id...)
>>> d = kd.to_py(kd.from_py({'a': [1, 2, 3], 'b': [4, 5, 6]}))
>>> dict(sorted(d.items()))
{'a': [1, 2, 3], 'b': [4, 5, 6]}
>>> kd.slice([kd.list([1, 2, 3]), kd.list([4, 5])])  # DataSlice of lists
DataSlice([List[1, 2, 3], List[4, 5]], schema: LIST[INT32], ...)
>>> kd.slice([kd.dict({'a':1, 'b':2}), kd.dict({'c':3})])  # DataSlice of dicts
DataSlice([Dict{...'a'=1...}, Dict{'c'=3}], schema: DICT{STRING, INT32}, ...)
```

The DataSlice `kd.slice([kd.list([1, 2, 3]), kd.list([4, 5])])` is different
from the DataSlice `kd.slice([[1, 2, 3], [4, 5]])`, as the following example
shows. However, they can be converted from/to each other as we will see later.

```py {.pycon-doctest}
>>> l1 = kd.list([1, 2, 3])
>>> l2 = kd.list([4, 5])
>>> list_ds = kd.slice([l1, l2])
>>> list_ds.get_ndim()
DataItem(1, schema: INT64)
>>> list_ds.get_size()
DataItem(2, schema: INT64)
>>> int_ds = kd.slice([[1, 2, 3], [4, 5]])
>>> int_ds.get_ndim()
DataItem(2, schema: INT64)
>>> int_ds.get_size()
DataItem(5, schema: INT64)
```

DataSlices have different mechanisms around accessing and broadcasting compared
to tensors and nested lists. That is, they specialize in **aggregation** from
inner dimensions into outer ones.

```py {.pycon-doctest}
>>> ds = kd.slice([[1, 2, 3], [4, 5]])

# Use .S for indexing
>>> ds.S[1, 0]
DataItem(4,...)

>>> ds.S[..., :2]
DataSlice([[1, 2], [4, 5]], schema: INT32...)

# Use .take for getting items in the last dimension
>>> ds.take(1)  # [2, 5] - 1st item in the last dimension
DataSlice([2, 5], schema: INT32, ...)

# Use .L to work with DataSlices as with python lists
>>> ds.L[0]
DataSlice([1, 2, 3], schema: INT32, ...)
>>> [int(y) for x in ds.L for y in x.L]
[1, 2, 3, 4, 5]
>>> kd.slice([5, 6]).expand_to(ds)
DataSlice([[5, 5, 5], [6, 6]], schema: INT32,...)
>>> ds + kd.slice([6, 7]) + kd.item(8)
DataSlice([[15, 16, 17], [19, 20]], schema: INT32,...)
>>> kd.agg_max(ds)
DataSlice([3, 5], schema: INT32, ...)
>>> kd.map_py(lambda x, y: x*y, ds, 2)
DataSlice([[2, 4, 6], [8, 10]], schema: INT32...)
>>> ds = kd.slice([4, 3, 4, 2, 2, 1, 4, 1, 2])
>>> kd.group_by(ds)
DataSlice([[4, 4, 4], [3], [2, 2, 2], [1, 1]], schema: INT32...)
>>> kd.group_by(ds).take(0)
DataSlice([4, 3, 2, 1], schema: INT32...)
>>> kd.unique(ds)  # the same as above
DataSlice([4, 3, 2, 1], schema: INT32...)

# Group_by can be used to swap dimensions, which can be used to transpose the
# final dimension of a slice.
>>> ds = kd.slice([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
>>> kd.group_by(ds.flatten(-2), kd.index(kd.present_shaped_as(ds)).flatten(-2))
DataSlice([[1, 4, 7], [2, 5, 8], [3, 6, 9]], schema: INT32...)
```

## Primitives

Koda supports INT32, INT64, FLOAT32, FLOAT64, STRING, BYTES, BOOLEAN, MASK as
primitive types.

```py {.pycon-doctest}
>>> kd.int32(1)
DataItem(1, schema: INT32)
>>> kd.int64([2, 3])
DataSlice([2, 3], schema: INT64...)
>>> kd.float32([[1., 2.], [3.]])
DataSlice([[1.0, 2.0], [3.0]], schema: FLOAT32,...)
>>> kd.str('string')
DataItem('string', schema: STRING)
>>> kd.bytes(b'bytes')
DataItem(b'bytes', schema: BYTES)
>>> kd.bool(True)
DataItem(True, schema: BOOLEAN)

# There is no native MASK type in Python, thus we have them directly in kd
>>> kd.present
DataItem(present, schema: MASK)
>>> kd.missing
DataItem(missing, schema: MASK)
```

## Entities, Schemas, ItemIds and Universally-Unique ItemIds

To work with structured data, Koda utilizes a concept of **entities** that are
identified by **ItemIds** (128-bit ids) and can have attributes, which can be
primitives or other entities. **Entities** can have **schema** assigned, and
multiple entities can share the same schema, which improves performance for
vectorized operations.

```py {.pycon-doctest}
# kd.new creates new entities and assigns schemas to them
>>> kd.new(x=1, y=2, schema='Point')
DataItem(Entity(x=1, y=2), schema: Point(x=INT32, y=INT32),...)

# Can also explicitly create schema with attributes before using.
>>> my_schema = kd.named_schema('Point', x=kd.INT32, y=kd.INT32)
>>> x = kd.new(x=1, y=2, schema=my_schema)
>>> x.get_schema() == my_schema # Yes, i.e. a present mask.
DataItem(present, schema: MASK)

# When converting from py, can specify schema
>>> kd.from_py({'x': 1, 'y': 2}, schema=my_schema)
DataItem(Entity(x=1, y=2), schema: Point(x=INT32, y=INT32),...)

# It's possible to create nested entities
>>> x = kd.new(a=1, b=kd.new(c=3, schema='Inner'), schema='Outer')
>>> x
DataItem(Entity(a=1, b=Entity(c=3)), schema: Outer(a=INT32, b=Inner(c=INT32)),...)

```

Entities are **immutable** by default, and it's possible to create multiple
slightly different versions with O(1) cost.

```py {.pycon-doctest}
>>> x = x.with_attrs(d=4)  # add an attribute
>>> x = x.updated(kd.attrs(x.b, c=5))  # update nested attribute 'c'

# Entities can be cloned or deep cloned
>>> x = kd.new(a=1, b=kd.new(c=3, schema='Inner'), schema='Outer')
>>> x1 = x.clone(a=2)
>>> x1.get_itemid() != x.get_itemid()
DataItem(present, schema: MASK)

>>> x1.b.get_itemid() == x.b.get_itemid()
DataItem(present, schema: MASK)

>>> x2 = x.deep_clone(a=2)
>>> x2.b.get_itemid() != x.b.get_itemid()
DataItem(present, schema: MASK)
```

Both entities and schemas can be dynamically **allocated** or be
**universally-unique** (i.e. have the same ItemIds across processes/machines).

```py {.pycon-doctest}
# Instead of specifying schemas, can auto-allocate them
>>> x = kd.new(a=1, b=kd.new(c=3))

# Entities with auto-allocated schemas cannot be mixed together in vectorized ops
>>> kd.new(x=1).get_schema() != kd.new(x=1).get_schema()
DataItem(present, schema: MASK)

# Auto-allocated schemas can be cast to have the same schema
>>> x, y = kd.new(a=1), kd.new(b=2)  # two entities with different schemas
>>> kd.slice([x, y.with_schema(x.get_schema())])
DataSlice([Entity(a=1), Entity():...], schema: ENTITY(a=INT32), ...)

# Universally unique entities can be used similarly to named tuples
>>> kd.uu(x=1, y=kd.uu(z=3))
DataItem(Entity(x=1, y=Entity(z=3)), schema: ENTITY(x=INT32, y=ENTITY(z=INT32)), bag_id:...)

>>> kd.dict({kd.uu(x=1, y=2): 10, kd.uu(x=2, y=3): 20})[kd.uu(x=1, y=2)]
DataItem(10, schema: INT32, bag_id:...)

# Dynamically allocated entities have different ids
>>> kd.new(x=1, y=2).get_itemid() != kd.new(x=1, y=2).get_itemid()
DataItem(present, schema: MASK)

# Universally-uniquely allocated entities have always the same ids
>>> kd.uu(x=1, y=2).get_itemid() == kd.uu(x=1, y=2).get_itemid()
DataItem(present, schema: MASK)

# Can encode itemid's into strings
>>> kd.encode_itemid(kd.new(x=1, y=2))  # always different, as ids are allocated
DataItem('...', schema: STRING)

>>> kd.encode_itemid(kd.uu(x=1, y=2))  # always the same
DataItem('07...', schema: STRING)
```

## DataSlices of Structured Data, Explosion/Implosion and Attribute Access

When working with DataSlices of structured data (entities, lists, and dicts),
the operation is applied to **all the items** in the DataSlice
**simultaneously**.

```py {.pycon-doctest}
# Root
# ├── dim_1:0
# │   ├── dim_2:0 -> kd.new(x=1, y=20, schema='Point')
# │   └── dim_2:1 -> kd.new(x=2, y=30, schema='Point')
# └── dim_1:1
#     ├── dim_2:0 -> kd.new(x=3, y=40, schema='Point')
#     ├── dim_2:1 -> kd.new(x=4, y=50, schema='Point')
#     └── dim_2:2 -> kd.new(x=5, y=60, schema='Point')
>>> kd.slice([
...     [
...       kd.new(x=1, y=20, schema='Point'),
...       kd.new(x=2, y=30, schema='Point')
...     ],
...     [
...       kd.new(x=3, y=40, schema='Point'),
...       kd.new(x=4, y=50, schema='Point'),
...       kd.new(x=5, y=60, schema='Point')
...     ]
... ])
DataSlice([
      [Entity(x=1, y=20), Entity(x=2, y=30)],
      [Entity(x=3, y=40), Entity(x=4, y=50), Entity(x=5, y=60)],
    ], schema: Point(x=INT32, y=INT32),...)

# Root
# ├── dim_1:0 -> kd.list([20, 30])
# └── dim_1:1 -> kd.list([40, 50, 60])
>>> kd.slice([kd.list([20, 30]), kd.list([40, 50, 60])])
DataSlice([List[20, 30], List[40, 50, 60]], schema: LIST[INT32],...)

# Root
# ├── dim_1:0
# │   ├── dim_2:0 -> kd.dict({'a': 1,'b': 2})
# │   └── dim_2:1 -> kd.dict({'b': 3,'c': 4})
# └── dim_1:1
#     └── dim_2:0 -> kd.dict({'a': 5,'b': 6,'c': 7})
>>> kd.slice([[kd.dict({'a': 1,'b': 2}), kd.dict({'b': 3,'c': 4})],
...  [kd.dict({'a': 5,'b': 6,'c': 7})]])
DataSlice([[Dict{...'b'=2...}, Dict{...'b'=3...}], [Dict{...'c'=7...}]], schema: DICT{STRING, INT32},...)
```

As a result of **attribute access** of a **DataSlice of entities**, a new
DataSlice is returned, which contains attributes of every corresponding entity
in the original DataSlice.

```py {.pycon-doctest}
>>> a = kd.slice([kd.new(x=1, schema='Foo'),
...               kd.new(x=2, schema='Foo'),
...               kd.new(x=3, schema='Foo')])
>>> a
DataSlice([Entity(x=1), Entity(x=2), Entity(x=3)], schema: Foo(x=INT32),...)

>>> a.x
DataSlice([1, 2, 3], schema: INT32,...)

>>> a2 = kd.new(x=kd.slice([1, 2, 3]), schema='Foo')  # The same as above, but more compact
>>> kd.testing.assert_equivalent(a, a2)

>>> b = kd.slice([kd.new(x=1, schema='Foo'),
...               kd.new(schema='Foo'),
...               kd.new(x=3, schema='Foo')])
>>> b
DataSlice([Entity(x=1), Entity():..., Entity(x=3)], schema: Foo(x=INT32),...)

>>> b.maybe('x')  # only the first and third ones have an attribute 'x'
DataSlice([1, None, 3], schema: INT32,...)
```

When accessing a **single element** of a **DataSlice of lists** or a **key** of
a **DataSlice of dicts**, a new DataSlice is returned with the corresponding
values in the original lists and dicts.

```py {.pycon-doctest}
>>> a = kd.slice([kd.list([1, 2, 3]), kd.list([4, 5])])

# Access 1st item in each list
>>> a[1]  # [2, 5] == [list0[1], list1[1]]
DataSlice([2, 5], schema: INT32,...)

>>> a = kd.slice([kd.dict({'a': 1, 'b': 2}), kd.dict({'b': 3, 'c': 4})])
>>> a['c']  # [None, 4] == [dict0['c'], dict1['c']]
DataSlice([None, 4], schema: INT32, ...)

```

A common operation is **explosion** of DataSlices of lists, when we return a new
DataSlice with an **extra dimension**, where the innermost dimension is composed
of the values of the original lists.

```py {.pycon-doctest}
>>> a = kd.slice([kd.list([1, 2, 3]), kd.list([4, 5])])

# Access 1st item in each list
>>> a[1]  # [2, 5] == [list0[1], list1[1]]
DataSlice([2, 5], schema: INT32,...)

# "Explosion": add another dimension to the DataSlice
# That is, a 1-dim DataSlice of lists becomes a 2-dim DataSlice
>>> a[:]
DataSlice([[1, 2, 3], [4, 5]], schema: INT32...)

# "Explosion" of the first two items in each list
>>> a[:2]
DataSlice([[1, 2], [4, 5]], schema: INT32,...)

>>> a[:].get_ndim() == a.get_ndim() + 1  # explosion adds one dimension
DataItem(present, schema: MASK)
```

An opposite operation is **implosion**, when we return a DataSlice of lists with
one fewer dimension, where each list contains the values of the innermost
dimension of the original DataSlice.

```py {.pycon-doctest}
# Implode replaces the last dimension with lists
>>> a = kd.slice([[1, 2, 3], [4, 5]])
>>> kd.implode(a)
DataSlice([List[1, 2, 3], List[4, 5]], schema: LIST[INT32],...)

>>> kd.implode(a)[:]# == a
DataSlice([[1, 2, 3], [4, 5]], schema: INT32,...)
```

Getting all keys or values of a DataSlice of dicts will return a DataSlice with
one more dimension.

```py {.pycon-doctest}
>>> a = kd.slice([kd.dict({'a': 1, 'b': 2}), kd.dict({'b': 3, 'c': 4})])

>>> a.get_keys()
DataSlice([['a', 'b'], ['b', 'c']], schema: STRING,...)

>>> a.get_values()
DataSlice([[1, 2], [3, 4]], schema: INT32,...)

# shortcut for get_value
>>> a[:] # [[1, 2], [3, 4]]
DataSlice([[1, 2], [3, 4]], schema: INT32,...)

# note, get_keys() doesn't guarantee to preserve the order, but we can sort before lookup
>>> a[kd.sort(a.get_keys())]
DataSlice([[1, 2], [3, 4]], schema: INT32,...)

>>> a.get_keys().get_ndim() == a.get_ndim() + 1  # the keys DataSlice has one more dimension
DataItem(present, schema: MASK)
```

Here is an example that puts everything together.

```py {.pycon-doctest}
>>> a = kd.from_py([{'x': 1}, {'x': 3}], dict_as_obj=True)
>>> b = kd.from_py([{'y': 2}, {'y': 4}])

>>> a[:].x + b[:]['y']
DataSlice([3, 7], schema: OBJECT, present: 2/2)

>>> kd.zip(kd.agg_sum(a[:].x), kd.agg_sum(b[:]['y']))
DataSlice([4, 6], schema: OBJECT, present: 2/2)
```

## Objects

To make possible mixing different primitives or entities/lists/dicts with
different schemas in a single DataSlice, Koda uses **objects**, which store
their schema in their data.

There are two main kinds of objects in Koda:

*   Primitives, such as integers and strings.
*   Objects that can have attributes and that use a special attribute to store
    their schemas. They are **similar to Python objects** that store their
    classes in the `__class__` attribute.

```py {.pycon-doctest}
>>> kd.obj(x=2, y=kd.obj(z=3))
DataItem(Obj(x=2, y=Obj(z=3)), schema: OBJECT, bag_id:...)

>>> x = kd.uuobj(x=2, y=kd.uuobj(z=3))  # universally unique (always the same id)
>>> kd.encode_itemid(x)  # always the same id
DataItem('07...', schema: STRING)

>>> x = kd.from_py([{'a': 1, 'b': 2}, {'c': 3, 'd': 4}], dict_as_obj=True)
>>> x[0]
DataItem(Obj(a=1, b=2), schema: OBJECT,...)

>>> x[:].maybe('a')
DataSlice([1, None], schema: OBJECT,...)

# Mix objects with different schemas
>>> kd.slice([kd.obj(1), kd.obj("hello"), kd.obj(kd.list([1, 2, 3]))])
DataSlice([1, 'hello', List[1, 2, 3]], schema: OBJECT,...)
>>> kd.slice([kd.obj(x=1, y=2), kd.obj(x="hello", y="world"), kd.obj(1)])
DataSlice([Obj(x=1, y=2), Obj(x='hello', y='world'), 1], schema: OBJECT,...)
>>> kd.obj(x=1).get_schema()
DataItem(OBJECT, schema: SCHEMA, bag_id:...)
>>> kd.obj(x=1).get_schema() == kd.obj(1).get_schema()
DataItem(present, schema: MASK)

# Get per-item schemas stored in every object
>>> kd.obj(x=1).get_obj_schema() # IMPLICIT_SCHEMA(x=INT32)
DataItem(IMPLICIT_ENTITY(x=INT32), schema: SCHEMA, bag_id:...)
>>> kd.obj(x=1).get_obj_schema() != kd.obj(1).get_obj_schema()  # yes, different actual schemas
DataItem(present, schema: MASK)
>>> kd.slice([kd.obj(x=1,y=2), kd.obj(x="hello", y="world"), kd.obj(1)]).get_obj_schema()
DataSlice([IMPLICIT_ENTITY(x=INT32, y=INT32), IMPLICIT_ENTITY(x=STRING, y=STRING), INT32], schema: SCHEMA...)
```

Similar to entities, objects can be modified with a cost of O(1), cloned or deep
cloned.

```py {.pycon-doctest}
>>> x = kd.obj(x=2, y=kd.obj(z=3))
>>> x = x.with_attrs(a=4)  # add attribute
>>> x = x.updated(kd.attrs(x.y, z=5))  # update nested attribute

>>> x1 = x.clone(z=4)
>>> x2 = x.deep_clone(z=5)
```

Entities and objects can be converted to each other.

```py {.pycon-doctest}
>>> x, y = kd.new(a=1), kd.new(b=2)
>>> kd.slice([kd.obj(x), kd.obj(y)])  # convert both entities to objects
DataSlice([Obj(a=1), Obj(b=2)], schema: OBJECT,...)

# Objects can be converted to entities
>>> my_schema = kd.named_schema('Point', x=kd.INT32, y=kd.INT32)
>>> a = kd.obj(x=1, y=2).with_schema(my_schema); a
DataItem(Entity(x=1, y=2), schema: Point(x=INT32, y=INT32),...)
>>> a2 = kd.from_py({'x': 1, 'y': 2}, dict_as_obj=True).with_schema(my_schema)  # the same as above
>>> kd.testing.assert_equivalent(a, a2)
```

Note: Compared to entities, objects have a higher **performance overhead**
during vectorized operations, as each object in a DataSlice has its own schema,
and different objects in the same DataSlice might have different sets of
attributes. For large data, the use of entities with explicit schemas is
recommended for faster execution.

Similar to entities, lists and dicts can be objects too.

```py {.pycon-doctest}
>>> l1 = kd.list([1, 2])
>>> l2 = kd.list(['3', '4'])
>>> l_objs = kd.slice([kd.obj(l1), kd.obj(l2)])
>>> l_objs[:]
DataSlice([[1, 2], ['3', '4']], schema: OBJECT,...)

>>> assert l_objs.get_schema() == kd.OBJECT
>>> l_objs.get_obj_schema()
DataSlice([LIST[INT32], LIST[STRING]]...)

>>> d1 = kd.dict({'a': 1})
>>> d2 = kd.dict({2: True})
>>> d_objs = kd.slice([kd.obj(d1), kd.obj(d2)])
>>> d_objs[:]
DataSlice([[1], [True]], schema: OBJECT, present: 2/2,...)

>>> assert d_objs.get_schema() == kd.OBJECT
>>> d_objs.get_obj_schema()
DataSlice([DICT{STRING, INT32}, DICT{INT32, BOOLEAN}], schema: SCHEMA,...)
```

Primitives are also objects. Their schemas are inferred from their values.

```py {.pycon-doctest}
>>> kd.obj(1)
DataItem(1, schema: OBJECT,...)
>>> kd.obj(kd.int64(1))
DataItem(int64{1}, schema: OBJECT,...)
>>> kd.obj('hello')
DataItem('hello', schema: OBJECT,...)

>>> assert kd.obj(1).get_schema() == kd.OBJECT
>>> assert kd.obj(1).get_obj_schema() == kd.INT32

# Dict values are objects
# No need to wrap them using kd.obj
>>> d = kd.dict({'a': 1, 'b': '2'})
>>> d.get_schema()
DataItem(DICT{STRING, OBJECT}, schema: SCHEMA...)
```

Another way to define custom structured data types in Koda is by using
[Extension Types](/koladata/g3doc/extension_types.md). These allow
you to create user-defined data structures which look like Python dataclasses
but integrate deeply with Koda's advanced features.

```py {.pycon-doctest}
>>> @kd.extension_type()
... class Point:
...   x: kd.FLOAT32
...   y: kd.FLOAT32
...   def norm(self):
...     return (self.x**2 + self.y**2)**0.5

>>> p = Point(x=3.0, y=4.0)
>>> p.norm()
DataItem(5.0, schema: FLOAT32)
```

## Sparsity and Masks

**Sparsity** is a first-class concept in Koda. Every item in a DataSlice can be
present or missing and all operators support missing values.

```py {.pycon-doctest}
>>> a = kd.slice([[1, None], [4]])
>>> b = kd.slice([[None, kd.obj(x=1)], [kd.obj(x=2)]])
>>> a + b.x
DataSlice([[None, None], [6]], schema: INT32, present: 1/3)
>>> kd.agg_any(kd.has(a))
DataSlice([present, present], schema: MASK, present: 2/2)
>>> kd.agg_all(kd.has(a))
DataSlice([missing, present], schema: MASK, present: 1/2)
```

**Masks** are used to represent present/missing state. They are also used in
comparison and logical operations.

```py {.pycon-doctest}
# Get the sparsity of a DataSlice
>>> kd.has(kd.slice([[1, None], [4]]))
DataSlice([[present, missing], [present]], schema: MASK, present: 2/3)
>>> kd.slice([1, None, 3, 4]) != 3
DataSlice([present, missing, missing, present], schema: MASK, present: 2/4)
>>> kd.slice([1, 2, 3, 4]) > 2
DataSlice([missing, missing, present, present], schema: MASK, present: 2/4)
```

Using masks instead of Booleans in comparison and logical operations is useful
because masks have a 2-valued logic. In the presence of missing values, the
Booleans have a 3-valued logic (over True, False, missing), which is more
complex and confusing.

Masks can be used to **filter** or **select** items in a DataSlice. The
difference is that filtering does not change the shape of the DataSlice and
filtered out items become missing, while selection changes the shape by only
keeping selected items in the resulting DataSlice.

```py {.pycon-doctest}
>>> x = kd.slice([1, 2, 3, 4])
>>> y = kd.slice([4, 5, 6, 7])

# To filter a DataSlice based on masks, use kd.apply_mask or & as shortcut
>>> kd.apply_mask(x, y >= 6)
DataSlice([None, None, 3, 4], schema: INT32, present: 2/4)
>>> x & (y >= 6)
DataSlice([None, None, 3, 4], schema: INT32, present: 2/4)

>>> a = kd.obj(x=kd.slice([1,2,3]))
>>> a.x >= 2
DataSlice([missing, present, present], schema: MASK, present: 2/3)
>>> a &= a.x >= 2
>>> a
DataSlice([None, Obj(x=2), Obj(x=3)], schema: OBJECT, present: 2/3,...)

# Can use 'select' to filter DataSlices
>>> a = kd.slice([kd.obj(x=1), kd.obj(x=2), kd.obj(x=3)])
>>> a1 = a.select(a.x >= 2); a1
DataSlice([Obj(x=2), Obj(x=3)], schema: OBJECT, present: 2/2,...)
>>> a2 = a.select(lambda u: u.x >= 2); # the same as above
>>> a3 = (a & (a.x >= 2)).select_present(); # the same as above
>>> kd.testing.assert_equal(a1, a2); kd.testing.assert_equal(a1, a3)

# Can update attributes only of selected entities/objects
>>> a.updated(kd.attrs(a1, y=a1.x*2))  # [Obj(x=1), Obj(x=2, y=4), Obj(x=3, y=6)]
DataSlice([Obj(x=1), Obj(x=2, y=4), Obj(x=3, y=6)], schema: OBJECT, present: 3/3,...)
```

**DataSlices** with compatible shapes can **coalesced** (i.e., missing items are
replaced by corresponding items of the other DataSlice).

```py {.pycon-doctest}
>>> kd.str(None)
DataItem(None, schema: STRING)
>>> kd.str(None) | 'hello'
DataItem('hello', schema: STRING)

>>> a = kd.coalesce(kd.slice([1, None, 3]), kd.slice([4,5,6])); a
DataSlice([1, 5, 3], schema: INT32,...)

>>> a2 = kd.slice([1, None, 3]) | kd.slice([4,5,6])  # the same as above
>>> kd.testing.assert_equal(a, a2)
```

## Immutable Workflows and Bags (Collections of Attributes)

### Immutability

Koda's data structures are **immutable**. However, Koda offers various efficient
ways to create modified copies of your data, including complex edits and joins.
These copies share the same underlying memory whenever possible, and many
operations, such as edits and joins, are performed in O(1) time. Immutability
means that modifying an entity/list/dict does *not* automatically modify the
original entity/list/dict, even if they share the same ItemId.

NOTE: Mutable APIs is available only in advanced, high-performance workflows,
but with trade-offs. They require a deeper understanding of Koda data model and
it is easier to make mistakes which can be hard to debug.

```py {.pycon-doctest}
>>> a = kd.new(x=2, y=kd.new(z=3)); a
DataItem(Entity(x=2, y=Entity(z=3)),...)

# update existing attribute and add a new attribute
>>> a1 = a.with_attrs(x=4, u=5); a1
DataItem(Entity(u=5, x=4, y=Entity(z=3)),...)

# a stays the same as it is immutable
>>> a
DataItem(Entity(x=2, y=Entity(z=3)),...)


>>> b = kd.dict({'a': 1, 'b': 2})

# dict keys order is non-deterministic
>>> to_sorted = lambda d: kd.zip(keys:=kd.sort(d.get_keys()), d[keys])

# update with a new key/value pair
>>> b1 = b.with_dict_update('a', 2); to_sorted(b1)
DataSlice([['a', 2], ['b', 2]],...)

# update with another dict
>>> b2 = b.with_dict_update(kd.dict({'a': 3, 'c': 4}))
>>> to_sorted(b2)
DataSlice([['a', 3], ['b', 2], ['c', 4]],...)

# b stays the same as it is immutable
>>> to_sorted(b)
DataSlice([['a', 1], ['b', 2]],...)


>>> c = kd.list([1, 2, 3])

# Create a new list with a distinct ItemId by concatenating two lists
>>> c1 = kd.concat_lists(c, kd.list([4, 5])); c1
DataItem(List[1, 2, 3, 4, 5],...)

# Or create a new list with a distinct ItemId by appending a DataSlice
>>> c2 = kd.appended_list(c, kd.slice([4, 5])); c2
DataItem(List[1, 2, 3, 4, 5],...)

# c stays the same as it is immutable
>>> c
DataItem(List[1, 2, 3],...)
```

### Bags

To support modifications and joins in an immutable environment, Koda utilizes
**bags**. Bags are collections of attributes. Each attribute within a bag is a
mapping: `(itemid, attribute_name) -> value`. All data structures (including
entities, dicts, and lists) are represented in this manner, and associated bags
are accessible via the `get_bag()` method. These mappings are stored using a
combination of hash maps and arrays. This hybrid approach enables fast,
vectorized performance for table-like data while supporting data with complex
structure and sparsity. Bags are merged for O(1) by utilizing a concept of
**fallbacks** (when we don't find a mapping in one bag, we look it up in the
other ones). Such a chain of fallback bags can be merged into a single bag when
higher lookup performance is required.

NOTE: Almost all data (e.g. entities, dicts, lists, objects, schemas) are stored
as attributes in bags.

```py {.pycon-doctest}
>>> a = kd.obj(x=2, y=kd.obj(z=3))

# Get the bag associated with a DataSlice
>>> db = a.get_bag()

# Get quick stats of a bag, use its repr
>>> db
DataBag ...
  2 Entities/Objects with 3 values in 3 attrs
  0 non empty Lists with 0 items
  0 non empty Dicts with 0 key/value entries
  2 schemas with 3 values...
Top attrs:
  z: 1 values
  y: 1 values
  x: 1 values

# Print out all attributes
>>> db.contents_repr()
DataBag...

# Print out only data attributes
>>> db.data_triples_repr()
DataBag...

# Print out only schema attributes
>>> db.schema_triples_repr()
SchemaBag...

# Get approximate size (e.g. number of attributes)
>>> db.get_approx_size()
8
```

<section class='zippy'>

Optional: Understand how entities are represented as attributes.

```
a = kd.obj(x=2, y=kd.obj(z=3))

a.get_bag()
# DataBag $b933:
#   2 Entities/Objects with 3 values in 3 attrs
#   0 non empty Lists with 0 items
#   0 non empty Dicts with 0 key/value entries
#   2 schemas with 3 values
#
# Top attrs:
#   z: 1 values
#   y: 1 values
#   x: 1 values


a.get_bag().contents_repr()
# DataBag $b933:
# $004QVkgdETIyelSHQw2OHs.get_obj_schema() => #6wL3PlezPQ4FnldO95VeD6
# $004QVkgdETIyelSHQw2OHs.z => 3
# $004QVkgdETIyelSHQw2OHt.get_obj_schema() => #6wK1lU2l9FsTJjliNOPBih
# $004QVkgdETIyelSHQw2OHt.x => 2
# $004QVkgdETIyelSHQw2OHt.y => $004QVkgdETIyelSHQw2OHs
#
# SchemaBag:
# #6wK1lU2l9FsTJjliNOPBih.x => INT32
# #6wK1lU2l9FsTJjliNOPBih.y => OBJECT
# #6wL3PlezPQ4FnldO95VeD6.z => INT32
```

</section>

<section class='zippy'>

Optional: Understand how dicts are represented as attributes.

```
b = kd.dict({'a': 1, 'b': 2})

b.get_bag()
# DataBag $4863:
#   0 Entities/Objects with 0 values in 0 attrs
#   0 non empty Lists with 0 items
#   1 non empty Dicts with 2 key/value entries
#   1 schemas with 2 values
#
# Top attrs:


b.get_bag().contents_repr()
# DataBag $4863:
# $0UGItpaGKCXaPsOxEscFOA['a'] => 1
# $0UGItpaGKCXaPsOxEscFOA['b'] => 2
#
# SchemaBag:
# #7QRy2BAblHHNytHxFmpaGL.get_key_schema() => STRING
# #7QRy2BAblHHNytHxFmpaGL.get_value_schema() => INT32
```

</section>

<section class='zippy'>

Optional: Understand how lists are represented as attributes.

```
c = kd.list([1, 2, 3])

c.get_bag()
# DataBag $10cc:
#  0 Entities/Objects with 0 values in 0 attrs
#  1 non empty Lists with 3 items
#  0 non empty Dicts with 0 key/value entries
#  1 schemas with 1 values
#
# Top attrs:


c.get_bag().contents_repr()
# DataBag $10cc:
# $0FAMhn8RmKvHXJvcKuKJq3[:] => [1, 2, 3]
#
# SchemaBag:
# #7QUhePdHCvsoCyAWAHwtzx.get_item_schema() => INT32
```

</section>

### Representing Updates as Bags

Instead of creating a modified object/dict/list directly, we typically create a
**bag** that contains the data **updates**. Updates are applied in O(1) time by
the fallback mechanism described above. Updates overwrite existing attributes or
add new ones.

```py {.pycon-doctest}
>>> a = kd.obj(x=2, y=kd.obj(z=3))

# update existing attribute and add a new attribute
>>> upd = kd.attrs(a, x=4, u=5)

# To see its contents, you can do
>>> upd.contents_repr()
DataBag...

>>> a.updated(upd)
DataItem(Obj(u=5, x=4, y=Obj(z=3)),...)

>>> b = kd.dict({'a': 1, 'b': 2})

# update with a new key/value pair
>>> upd = kd.dict_update(b, 'a', 2)
>>> b.updated(upd)
DataItem(Dict{'a'=2, 'b'=2},...)

# update with another dict
>>> upd = kd.dict_update(b, kd.dict({'a': 3, 'c': 4}))
>>> b2 = b.updated(upd); to_sorted(b2)
DataSlice([['a', 3], ['b', 2], ['c', 4]],...)

# Schemas are stored and can be updated in the same way
>>> a = kd.new(x=1, schema='MySchema')
>>> a.updated(kd.attrs(a.get_schema(), y=kd.INT32))  # update the schema
DataItem(Entity(x=1), schema: MySchema(x=INT32, y=INT32),...)
```

<section class='zippy'>

Optional: Understand how entity/object updates are represented as attributes.

```
a = kd.obj(x=2, y=kd.obj(z=3))
upd = kd.attrs(a, x=4, u=5)

# Only modification is stored as attributes in the update bag
upd
# DataBag $7de8:
#   1 Entities/Objects with 2 values in 2 attrs
#   0 non empty Lists with 0 items
#   0 non empty Dicts with 0 key/value entries
#   1 schemas with 2 values
#
# Top attrs:
#   x: 1 values
#   u: 1 values


upd.contents_repr()
# DataBag $7de8:
# $004QVkgdETIyelSHQw2OHz.get_obj_schema() => #6wGECIg4UYvAsDXKvqtJQC
# $004QVkgdETIyelSHQw2OHz.u => 5
# $004QVkgdETIyelSHQw2OHz.x => 4
#
# SchemaBag:
# #6wGECIg4UYvAsDXKvqtJQC.u => INT32
# #6wGECIg4UYvAsDXKvqtJQC.x => INT32
```

</section>

<section class='zippy'>

Optional: Understand how dict update is represented as attributes.

```
b = kd.dict({'a': 1, 'b': 2})
upd = kd.dict_update(b, kd.dict({'a': 3, 'c': 4}))

upd
# DataBag $7806:
#   0 Entities/Objects with 0 values in 0 attrs
#   0 non empty Lists with 0 items
#   1 non empty Dicts with 2 key/value entries
#  1 schemas with 2 values
#
# Top attrs:


upd.contents_repr()
# DataBag $7806:
# $0UGItpaGKCXaPsOxEscFOG['a'] => 3
# $0UGItpaGKCXaPsOxEscFOG['c'] => 4
#
# SchemaBag:
# #7QRy2BAblHHNytHxFmpaGL.get_key_schema() => STRING
# #7QRy2BAblHHNytHxFmpaGL.get_value_schema() => INT32
```

</section>

Here is a more complex example that puts everything together.

```py {.pycon-doctest}
>>> a = kd.obj(x=2, y=kd.obj(z=3), z=kd.dict({'a': 1, 'b': 2}), t=kd.list([1,2,3]))
>>> upd = kd.attrs(a, x=4, u=5)  # create data update
>>> a1 = a.updated(upd)
>>> a2 = a.with_attrs(x=4, u=5)  # a shortcut for above
>>> kd.testing.assert_equivalent(a1, a2)

>>> a2 = a.updated(kd.attrs(a.y, z=5))  # update a deep attribute a.y.z
>>> a3 = a.updated(kd.dict_update(a.z, 'b', 3))  # update dict
>>> a4 = a.updated(kd.dict_update(a.z, kd.dict({'b': 3, 'c': 4})))  # multi-update dict
>>> a5 = a.with_attrs(t=kd.concat_lists(a.t, kd.list([4, 5])))  # lists need to be updated as whole
```

Instead of being applied immediately, updates can be accumulated and applied
later.

```py {.pycon-doctest}
# Updates can be composed using << and >>, which defines what overwrites what
>>> upd = kd.attrs(a, x=3, y=4) << kd.attrs(a, x=5, u=6) # equivalent to kd.attrs(a, x=5, y=4, u=6)
>>> upd = kd.attrs(a, x=3, y=4) >> kd.attrs(a, x=5, u=6)  # equivalent to kd.attrs(a, x=3, y=4, u=6)

# Updates can be accumulated and applied later
>>> a = kd.obj(x=2, y=kd.obj(z=3))
>>> upd = kd.bag()  # empty update
>>> upd <<= kd.attrs(a, x=a.x + 1)
>>> upd <<= kd.attrs(a, x=a.updated(upd).x + 2)  # can use a.x with an update
>>> upd <<= kd.attrs(a, u=a.y.z + a.updated(upd).x)
>>> a6 = a.updated(upd); a6
DataItem(Obj(u=8, x=5, y=Obj(z=3)), schema: OBJECT,...)
```

All APIs and concepts mentioned above support vectorization using DataSlice.

```py {.pycon-doctest}
>>> a = kd.new(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6])); a
DataSlice([Entity(x=1, y=4), Entity(x=2, y=5), Entity(x=3, y=6)], schema: ENTITY(x=INT32, y=INT32),...)
>>> a.with_attrs(z=kd.slice([7, 8, 9]))
DataSlice([Entity(x=1, y=4, z=7), Entity(x=2, y=5, z=8), Entity(x=3, y=6, z=9)], schema: ENTITY(x=INT32, y=INT32, z=INT32),...)
>>> a.updated(kd.attrs(a, x=kd.slice([10, 11, 12])))
 DataSlice([Entity(x=10, y=4), Entity(x=11, y=5), Entity(x=12, y=6)], schema: ENTITY(x=INT32, y=INT32),...)

# Updates can target a subset of entities by utilizing sparsity
>>> a.updated(kd.attrs(a & (a.y >=5), z=kd.slice([7, 8, 9]))).z
DataSlice([None, 8, 9], schema: INT32,...)
```

### Enrichments

Data can be **enriched** with data from another source. Enrichment *only adds*
missing attributes without overwriting existing ones. This operation is also
O(1).

TIP: The key difference between update and enrichment is that update overrides
existing attributes while enrichment does not. Enrichment augments the
attributes using the fallback mechanism described above.

```py {.pycon-doctest}
>>> a = kd.obj(x=2, y=kd.obj(z=3))
>>> a_attrs = kd.attrs(a, x=1, u=5)

>>> a.updated(a_attrs)
DataItem(Obj(u=5, x=1, y=Obj(z=3)), schema: OBJECT,...)

>>> a.enriched(a_attrs)
DataItem(Obj(u=5, x=2, y=Obj(z=3)), schema: OBJECT,...)
```

### Extraction

Updates and enrichments merge multiple bags together. After enrichments and
updates, merged bags may contain attributes irrelevant to a specific object or
entity (e.g. enrichment with unrelated data). The `ds.extract_update()` method
allows extracting a bag containing only relevant attributes (those recursively
accessible from `ds`). `ds.extract()` is equivalent to
`ds.with_bag(ds.extract_update())`.

```py {.pycon-doctest}
>>> a = kd.obj(x=2, y=kd.obj(z=3), z=kd.dict({'a': 1, 'b': 2}), t=kd.list([1, 2, 3]))
>>> a.get_bag().get_approx_size()
20

>>> ay1 = a.y.with_attrs(z=4, zz=5)  # Modify ay1 on its own
>>> ay1.get_bag().get_approx_size()  # 25, as it contains all attributes from a
25

>>> extracted_bag = ay1.extract_update()
>>> extracted_bag.get_approx_size()  # 5, only it only contain ay1 attributes
5

>>> a.updated(extracted_bag)  # Apply the attributes from ay1 to a
DataItem(Obj(t=List[1, 2, 3], x=2, y=Obj(z=4, zz=5), z=Dict{...'b'=2...}), schema: OBJECT,...)

>>> a.enriched(extracted_bag)  # Instead, this augments the data without overwriting
DataItem(Obj(t=List[1, 2, 3], x=2, y=Obj(z=3, zz=5), z=Dict{...'b'=2...}), schema: OBJECT,...)

# If data is enriched with unrelated data, we can use extract to remove it

>>> b = kd.obj(x=1, y=2)
>>> a9 = a.enriched(kd.attrs(b, z=3))  # adding an unrelated attribute
>>> a9.extract()  # == a9 with inaccessible attributes removed
DataItem(Obj(t=List[1, 2, 3], x=2, y=Obj(z=3), z=Dict{...'b'=2...}), schema: OBJECT,...)
>>> kd.testing.assert_equivalent(a9.extract(), a9)
```

### Cloning

Cloning is another way to work in immutable way, but it allocates new ItemIds.
Thus, it is more expensive and data cannot be joined later.

```py {.pycon-doctest}
>>> a = kd.obj(x=2, y=kd.obj(z=3))
>>> assert(a.clone(x=3).get_itemid() != a.get_itemid())
>>> assert(a.with_attrs(x=3).get_itemid() == a.get_itemid())
>>> a.updated(a.clone(x=3).get_bag())  # doesn't overwrite x
DataItem(Obj(x=2, y=Obj(z=3)), schema: OBJECT, bag_id:...)

>>> a.updated(a.with_attrs(x=3).get_bag())  # overwrites x
DataItem(Obj(x=3, y=Obj(z=3)), schema: OBJECT, bag_id:...)
```

## Functors and Lazy Evaluation

Koda supports both eager computation and lazy evaluation similar to TF, JAX and
PyTorch.

Lazy evaluation has the following benefits:

*   **Separation of computation logic from evaluation**: same computation logic
    can be evaluated on different inputs
*   **Graph optimization**: computation is converted to a graph representation
    which can be further optimized
*   **Parallel computation**: graph can be analyzed and partitioned into
    sub-graphs which can be evaluated in parallel
*   **Distributed computation**: graph can be serialized and evaluated in remote
    workers
*   **Serving**: graph can be evaluated in a C++ production environment

Koda uses **tracing** and converts Python functions into Koda **functors**
representing computational graphs. Koda functors are special Koda objects that
can be used for evaluation or could be stored together with data.

As is the case in JAX, in order for tracing to work correctly, the Python
function can only contain Koda operators. Python control flow (i.e. `if`, `for`
and `while`) is executed only during tracing - the resulting functors will
depend on the Python control flow but will not include operators that mimic the
Python control flow. To trace a Python function, we wrap it with `kd.fn(py_fn)`.

```py {.pycon-doctest}
# Convert python functions into functors
>>> fn = kd.fn(lambda x, y: x+y, y=1)
>>> fn(kd.slice([1, 2, 3]))
DataSlice([2, 3, 4], schema: INT32,...)

>>> fn(kd.slice([1, 2, 3]), y=10)
DataSlice([11, 12, 13], schema: INT32,...)

# Functors can be used as normal Koda objects (assigned and stored)
>>> fns = kd.obj(fn1=fn, fn2=fn.bind(y=5))
>>> fns.fn2(kd.slice([1, 2, 3]))  # [6, 7, 8]
DataSlice([6, 7, 8], schema: INT32,...)

# Functors can be serialized
>>> x = kd.dumps(fn)
```

**kd.py_fn** can be used in interactive workflows to wrap python functions
without tracing, which can make debugging in certain situations easier.

```py {.pycon-doctest}
# kd.fn uses tracing, and kd.py_fn wraps a Python functions "as-is", which is
# useful because not everything can be traced
>>> fn = kd.py_fn(lambda x, y: x if kd.sum(x) > kd.sum(y) else y)
>>> fn(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5]))
DataSlice([4, 5], schema: INT32,...)
```

You can annotate functions that call other functors with **@kd.trace_as_fn**.
When such an annotated function is traced to produce a functor, then the inner
functors can be accessed as attributes.

```py {.pycon-doctest}
# functor_factory=kd.py_fn because the Python `while` cannot be traced properly.
>>> @kd.trace_as_fn(functor_factory=kd.py_fn)
... def my_op(x, y):
...   while (x > 0): y += x; x -= 1
...   return y

>>> @kd.trace_as_fn()
... def final(x, y, z): return my_op(my_op(x, y), z)

>>> fn = kd.fn(final)
>>> fn(2, 3, 4)  # my_op(2,3) => 3+(2+1) => 6; my_op(6, 4) => 4+(6+5+4+3+2+1) => 25
DataItem(25, schema: INT32)

>>> fn.final(2, 3, 4)  # the same as above
DataItem(25, schema: INT32)

>>> fn.final.my_op(2, 3)  # 6 - access of the deeper functor
DataItem(6, schema: INT32)

# "replace" the inner functor my_op
>>> fn.updated(kd.attrs(fn.final, my_op=kd.fn(lambda x, y: x * y)))(2, 3, 4)  # (2 * 3) * 4
DataItem(24, schema: INT32)
```

## Convenience Features

Koda provides a comprehensive list of convenience features including:

**String manipulations**

```py {.pycon-doctest}
>>> x, y = kd.slice([1, 2, 3]), kd.slice(["a", "b", "c"])
>>> kd.fstr(f"i{x:i}-{y:s}")
DataSlice(['i1-a', 'i2-b', 'i3-c'], schema: STRING,...)

>>> kd.strings.format("i{x}-{y}", x=x, y=y)  # the same as above
DataSlice(['i1-a', 'i2-b', 'i3-c'], schema: STRING,...)

>>> kd.strings.split(kd.slice(["a b", "c d e"]))
DataSlice([['a', 'b'], ['c', 'd', 'e']], schema: STRING,...)

>>> ds = kd.slice([['aa', 'bbb'], ['ccc', 'dd']])

>>> kd.strings.agg_join(ds, '-')
DataSlice(['aa-bbb', 'ccc-dd'], schema: STRING...)

>>> kd.strings.agg_join(ds, '-', ndim=2)
DataItem('aa-bbb-ccc-dd', schema: STRING)

>>> kd.strings.length(ds)
DataSlice([[2, 3], [3, 2]], schema: INT64...)
```

**Math operators**

```py {.pycon-doctest}
# Math
>>> x = kd.slice([[3., -1., 2.], [0.5, -0.7]])
>>> y = kd.slice([[1., 2., 0.5], [0.9, 0.3]])
>>> x * y
DataSlice([[3.0, -2.0, 1.0], [0.45, -0.21...]], schema: FLOAT32,...)

>>> kd.math.agg_mean(x)
DataSlice([1.333..., -0.0999...], schema: FLOAT32,...)

>>> kd.math.log10(x)
DataSlice([[0.477..., nan, 0.301...], [-0.301..., nan]], schema: FLOAT32,...)

>>> kd.math.pow(x,y)
DataSlice([[3.0, 1.0, 1.414...], [0.535..., nan]], schema: FLOAT32,...)
```

**Ranking**

```py {.pycon-doctest}
>>> x = kd.slice([[5., 4., 6., 4., 5.], [8., None, 2.]])
>>> kd.ordinal_rank(x)
DataSlice([[2, 0, 4, 1, 3], [1, None, 0]], schema: INT64,...)

>>> kd.ordinal_rank(x, descending=True)
DataSlice([[1, 3, 0, 4, 2], [0, None, 1]], schema: INT64,...)

>>> kd.dense_rank(x)
DataSlice([[1, 0, 2, 0, 1], [1, None, 0]], schema: INT64,...)
```

**Serialization**

```py {.pycon-doctest}
# DataSlice
>>> serialized_bytes = kd.dumps(ds)
>>> ds = kd.loads(serialized_bytes)

# Bag
>>> serialized_bytes = kd.dumps(db)
>>> db = kd.loads(serialized_bytes)
```

**Multi-threading**

```py {.pycon-doctest}
>>> def call_slow_fn(prompt):
...   return prompt + prompt
>>> kd.map_py(call_slow_fn, kd.slice(['hello', None, 'world']), max_threads=16)
DataSlice(['hellohello', None, 'worldworld'], schema: STRING,...)
```

## Mutable Workflows

Immutable workflows are recommended for most cases, but mutable data structures
can be useful in some situations. In particular, when a Koda data structure is
updated frequently, then a mutable version might be more efficient. An example
of this is a Koda data structure that implements a cache.

The `fork_bag` method returns a new mutable version of the underlying data bag
at the cost of **O(1)** (it uses copy-on-write and does not modify the original
bag), while `freeze_bag` returns an immutable version of the underlying data bag
(also for O(1)). The `extract` and `clone` methods extract and return immutable
pieces of a bag.

Please keep in mind that mutable workflows are not supported in tracing. They
consequently have more limited options for productionization.

```py {.pycon-doctest}
# Modify the same dict many times
>>> d = kd.dict()  # immutable
>>> d = d.fork_bag()  # mutable
>>> for i in range(100):
...   # Insert random x=>y mappings (10 at a time)
...   k = kd.random.randint_shaped(kd.shapes.new(10))
...   v = kd.random.randint_shaped(kd.shapes.new(10))
...   d[k] = v
>>> d = d.freeze_bag()  # immutable

>>> a = kd.obj(x=1, y=2).fork_bag()
>>> a.y = 3
>>> a.z = 4
>>> a = a.freeze_bag()
```

## Interoperability

Koda can easily interoperate with normal Python, Pandas, Numpy and Proto.

**From/to standard Python data structures**

```py {.pycon-doctest}
>>> kd.obj(x=1, y=2).x.to_py()
1

>>> kd.slice([[1, 2], [3, 4]]).to_py()
[[1, 2], [3, 4]]

>>> kd.obj(x=1, y=kd.obj(z=kd.obj(a=1))).to_py(max_depth=-1)
Obj(x=1, y=Obj(z=Obj(a=1)))

>>> kd.list([1, 2, 3]).to_py()
[1, 2, 3]

>>> kd.from_py([{'a': [1, 2, 3], 'b': [4, 5, 6]}, {'a': 3, 'b': 4}])
DataItem(List[Dict{...'b'=List[4, 5, 6]...}, Dict{...'b'=4...}], schema: OBJECT, bag_id:...)

>>> kd.to_py(kd.list([1, 2, 3]))
[1, 2, 3]

# Using `output_class`:

>>> import types as _py_types

>>> koda_obj = kd.obj(x=1, y=2)
>>> koda_obj.to_py(output_class=_py_types.SimpleNamespace)
namespace(...x=1...)

>>> import dataclasses
>>> @dataclasses.dataclass
... class Objxy:
...  x: int
...  y: int
>>> koda_obj.to_py(output_class=Objxy)
Objxy(x=1, y=2)

>>> kd.to_py(kd.list([1, 2, 3]), output_class=tuple[int,...])
(1, 2, 3)
```

**From/to Pandas DataFrames**

```py {.pycon-doctest}
>>> import pandas as pd
>>> from koladata.ext import pdkd

>>> pdkd.from_dataframe(pd.DataFrame(dict(x=[1, 2, 3], y=[10, 20, 30])))
DataSlice([Entity(x=1, y=10), Entity(x=2, y=20), Entity(x=3, y=30)], schema: ENTITY(x=INT64, y=INT64),...)

>>> pdkd.to_dataframe(kd.obj(x=kd.slice([1, 2, 3]), y=kd.slice([4, 5, 6])), cols=['x', 'y'])
   x  y
0  1  4
1  2  5
2  3  6
```

**From/to Numpy Arrays**

```py {.pycon-doctest}
>>> import numpy as np
>>> from koladata.ext import npkd

>>> npkd.to_array(kd.slice([1, 2, None, 3]))
array([1, 2, 0, 3], dtype=int32)

>>> npkd.from_array(np.array([1, 2, 0, 3]))
DataSlice([1, 2, 0, 3], schema: INT64,...)
```
