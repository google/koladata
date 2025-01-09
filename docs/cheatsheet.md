<!--* css: "//koladata/g3doc/cheatsheet.css" *-->
<!-- For the best formatting, please limit the line_max_char to 50. -->

# Koda Cheatsheet

## Koda Setup

<section>

### Import

```py
from koladata import kd

# Additional extension libraries if needed
# E.g. Numpy/Pandas conversions
from koladata import kd_ext
```

</section>

## Basic APIs

<section>

### Primitives and DataItem

```py
# Primitive dtypes
kd.INT32
kd.INT64
kd.FLOAT32
kd.FLOAT64
kd.STRING
kd.BYTES
kd.BOOLEAN
kd.MASK

# MASK type values
kd.present
kd.missing

# DataItem creation
i = kd.item(1)

kd.is_item(i)
kd.is_primitive(i)
kd.get_dtype(i) # kd.INT32

# DataItem creation with explicit dtypes
kd.int32(1)
kd.int64(2)
kd.float32(1.1)
kd.float64(2.2)
kd.str('a')
kd.bytes(b'a')
kd.bool(True)
kd.mask(None)

# Or use kd.item with explicit dtype
kd.item(1, kd.INT32)
kd.item(2, kd.INT64)

# kd.from_py is a universal converter
# Same as kd.item
kd.from_py(1)
```

</section>

<section>

### DataSlice

```py
ds = kd.slice([[1, 2], [3, None, 5]])
kd.is_slice(ds)

ds.get_size()
ds.get_ndim()
ds.get_shape()

ds.get_dtype() # kd.INT32

# DataSlice creation with explicit dtypes
kd.int32([[1, 2], [3, None, 5]])
kd.int64([[1, 2], [3, None, 5]])
kd.float32([[1., 2.],[3., None, 5.]])
kd.float64([[1., 2.],[3., None, 5.]])
kd.str(['a', None, 'b'])
kd.bytes([b'a', None, b'b'])
kd.bool([True, None, False])
kd.mask([kd.present, kd.missing])

# Or use kd.slice with explicit dtype
kd.slice([[1, 2], [3, None, 5]], kd.INT32)
kd.slice([[1, 2], [3, None, 5]], kd.INT64)

# kd.from_py is a universal converter
# Same as kd.slice
kd.from_py([[1, 2], [3, None, 5]])

ds = kd.slice([[1, 2], [3, None, 5]])
# Navigate DataSlice as "nested" lists
ds.L[1].L[2] # 5
# Items at idx in the first dims
ds.L[1] # [3, None, 5]

# Use in Python for-loop
for i in ds.L:
  for j in i.L:
    print(j)

# Subslice
ds.S[1, 2] # 5
# Take the third items in the last dimension
ds.S[2] # [None, 5]
ds.S[1:, :2] # [[3, None]]

# Get items at idx
ds.take(2) # [None, 5]
ds.take(kd.slice([1, 2])) # [2, 5]

# Expand the outermost dimension into a list
kd.to_pylist(ds)
# Convert to a nested Python list
ds.to_py()

# Reverse the order of items in the last dimension
kd.reverse(ds) # [[2, 1], [5, None, 3]]

# DataItem is 0-dim DataSlice
i = kd.slice(1) # same as kd.item(1)
i.get_ndim() # 0

# Visualize a DataSlice
ds.display()
```

</section>

<section>

### Entities

Entities can be thought of as instances of protos or C++ structs. That is, they
don't directly store their own schema. Instead, their schema is stored at
DataSlice level and all entities in a DataSlice share the same schema.

```py
# Entity creation with named schema
e = kd.new(x=1, y=2, schema='Point')
es = kd.new(x=kd.slice([1, 2, None]),
            y=kd.slice([4, None, 6]),
            schema='Point')

e.get_schema()
assert e.get_schema() == es.get_schema()

assert e.is_entity()

# Use an existing schema
s = kd.named_schema('Point', x=kd.INT32, y=kd.INT32)
e = kd.new(x=1, y=2, schema=s)

# When `schema=` is not provided, a new
# schema is created for each invocation
e1 = kd.new(x=1, y=2)
e2 = kd.new(x=1, y=2)
assert e1.get_schema() != e2.get_schema()

# Use provided itemids
itemid = kd.new_itemid()
e3 = kd.new(x=1, y=2, itemid=itemid)
e4 = kd.new(x=1, y=2, itemid=itemid)
assert e3.get_itemid() == e4.get_itemid()

# Get available attributes
kd.dir(e) # ['x', 'y']
# Or
# As all entities share the same schema,
# the intersection of attribute names is the same
e.get_attr_names(intersection=True) # ['x', 'y']
e.get_attr_names(intersection=False) # ['x', 'y']

# Access attribute
e.x # 1
e.get_attr('y') # 2
e.maybe('z') # None
e.get_attr('z', default=0) # 0
es.get_attr('x', default=0) # [1, 2, 0]

# Entities are immutable by default, modification is done
# by creating a new entity with updated attributes
e = kd.new(x=1, y=2, schema='Point')

# Update attributes
# Update a single attribute
e1 = e.with_attr('x', 3)
e1 = e.with_attr('z', 4)
# Also override schema
e1 = e.with_attr('y', 'a', update_schema=True)

# Update multiple attributes
e2 = e.with_attrs(z=4, x=3)
# Also override schema for 'y'
e2 = e.with_attrs(z=4, y='a', update_schema=True)

# Create an update and apply it separately
upd = kd.attrs(e, z=4, y=10)
e3 = e.updated(upd)

# Allows mixing multiple updates
e4 = e.updated(kd.attrs(e, z=4), kd.attrs(e, y=10))

# Update nested attributes
nested = kd.new(a=kd.new(c=kd.new(e=1), d=2), b=3)
nested = nested.updated(kd.attrs(nested.a.c, e=4),
                        kd.attrs(nested.a, d=5),
                        kd.attrs(nested, b=6))
```

</section>

<section>

### Lists

```py
# Create a list from a Python list
l1 = kd.list([1, 2, 3])
l2 = kd.list([[1, 2], [3], [4, 5]])

# Create multiple lists by imploding
# the last dimension of a DataSlice
l3 = kd.list(kd.slice([[1, 2], [3], [4, 5]]))

# l2 and l3 are different
kd.is_item(l2)
l2.get_size() # 1
l3.get_size() # 3

kd.is_list(l1)
kd.list_size(l1) # 3

# Use provided itemids
itemid = kd.new_listid()
l4 = kd.list([1, 2, 3], itemid=itemid)
l5 = kd.list([4, 5, 6], itemid=itemid)
assert l4.get_itemid() == l5.get_itemid()

# Python-like list operations
l1[0] # 1
kd.get_item(l1, 0) # 1
l1[1:] # [2, 3]

# Slice by a DataSlice
l1[kd.slice([0, 2])] # [1, 3]
l1[kd.slice([[2, 1], [0, None]])] # [[3, 2], [1, None]]

# Explode a list
l1[:] # [1, 2, 3]
kd.explode(l1) # [1, 2, 3]

kd.explode(l2, ndim=2)
# Explode all lists repeatedly
kd.explode(l2, ndim=-1)

# Filter out items
l6 = kd.list([1, 2, None, 4])
l6.select_items(lambda x: x >= 2) # [2, 4]

# Returns a DataSlice of lists concatenated from
# the list items of arguments.
kd.concat_lists(kd.list([1, 2]), kd.list([3, 4]))
```

</section>

<section>

### Dicts

```py
# Create a dict from a Python dict
d1 = kd.dict({'a': 1, 'b': 2})
d1 = kd.dict(kd.slice(['a', 'b'],
             kd.slice([1, 2]))) # Same as above

# Create multiple dicts
d2 = kd.dict(kd.slice([['a', 'b'], ['c']],
             kd.slice([[1, 2], [3]])))

kd.is_dict(d1)
d1.dict_size() # 2

# Use provided itemids
itemid = kd.new_dictid()
d3 = kd.dict({'a': 1, 'b': 2}, itemid=itemid)
d4 = kd.dict({'c': 3, 'd': 4}, itemid=itemid)
assert d3.get_itemid() == d4.get_itemid()

d1.get_keys() # ['a', 'b']
d1.get_values() # [1, 2]
d1['a']
d1.get_item('a') # Same as above

# Filter out keys/values
d1.select_keys(lambda k: k != 'b') # ['a']
d1.select_values(lambda v: v > 1) # [2]

# Dicts are immutable by default, modification is done
# by creating a new dict with updated key/values

# Update a key/value
d4 = d1.with_dict_update('c', 5)
# Update multiple key/values
another_dict = kd.dict({'a': 3, 'c': 5})
d5 = d1.with_dict_update(another_dict)
# Same as above
d5 = d1.with_dict_update(kd.slice(['a', 'c']),
                         kd.slice([3, 5]))

# Create an update and apply it separately
upd = d1.dict_update(another_dict)
d6 = d1.updated(upd)

# Allows mixing multiple updates
d7 = d1.updated(d1.dict_update('c', 5),
                d1.dict_update(another_dict))
```

</section>

<section>

### Objects

Objects can be thought of as Python objects. They directly store their own schema
as **schema** attribute similar to how Python objects store **class** attribute.
This allows objects in a DataSlice to have different schemas. Entities, Lists,
Dicts and primitives can be objects. Entities, Lists and Dicts store their own
schema as an internal `__schema__` attribute while primitives' schema is
determined by the type of their value.

```py
# Entity objects
o = kd.obj(x=1, y=2)
os = kd.obj(x=kd.slice([1, 2, None]),
            y=kd.slice([4, None, 6]))

os = kd.slice([kd.obj(x=1),
               kd.obj(y=2.0),
               kd.obj(x=1.0, y='a')])

os.get_schema() # kd.OBJECT
os.get_obj_schema()
# [IMPLICIT_SCHEMA(x=INT32),
#  IMPLICIT_SCHEMA(y=FLOAT32),
#  IMPLICIT_SCHEMA(x=INT32, y=STRING)]

# Use provided itemids
itemid = kd.new_itemid()
o1 = kd.obj(x=1, y=2, itemid=itemid)
o2 = kd.obj(x=1, y=2, itemid=itemid)
assert o1.get_itemid() == o2.get_itemid()

# Get available attributes
os1 = kd.slice([kd.obj(x=1), kd.obj(x=1.0, y='a')])
# Attributes present in all objects
kd.dir(os1) # ['x']
# Or
os1.get_attr_names(intersection=True) # ['x']
os1.get_attr_names(intersection=False) # ['x', 'y']

# Access attribute
o.x # 1
o.get_attr('y') # 2
o.maybe('z') # None
o.get_attr('z', default=0) # 0
os.get_attr('x', default=0) # [1, 0, 'a']

# Objects are immutable by default, modification is done
# by creating a new object with updated attributes
o = kd.obj(x=1, y=2)

# Update a single attribute
o1 = o.with_attr('x', 3)
o1 = o.with_attr('z', 4)
# Also override schema
# no update_schema=True is needed
o1 = o.with_attr('y', 'a')

# Update multiple attributes
o2 = o.with_attrs(z=4, x=3)
# Also override schema for 'y'
o2 = o.with_attrs(z=4, y='a')

# Create an update and apply it separately
upd = kd.attrs(o, z=4, y=10)
o3 = o.updated(upd)

# Allows mixing multiple updates
o4 = o.updated(kd.attrs(o, z=4), kd.attrs(o, y=10))

# Update nested attributes
nested = kd.obj(a=kd.obj(c=kd.obj(e=1), d=2), b=3)
nested = nested.updated(kd.attrs(nested.a.c, e=4),
                        kd.attrs(nested.a, d=5),
                        kd.attrs(nested, b=6))

# List and dict can be objects too
# To convert a list/dict to an object,
# use kd.obj()
l = kd.list([1, 2, 3])
l_obj = kd.obj(l)
l_obj[:] # [1, 2, 3]

d = kd.dict({'a': 1, 'b': 2})
d_obj = kd.obj(d)
d_obj.get_keys() # ['a', 'b']
d_obj['a'] # 1

# Convert an entity to an object
e = kd.new(x=1, y=2)
e_obj = kd.obj(e)

# Actually, we can pass primitive to kd.obj()
p_obj = kd.obj(1)
p_obj = kd.obj('a')

# An OBJECT Dataslice with entity, list,
# dict and primitive items
kd.slice([kd.obj(a=1), 1, kd.obj(kd.list([1, 2])),
          kd.obj(kd.dict({'a': 1}))])
```

</section>

<section>

### Subslicing DataSlices

Subslicing is an operation of getting part of the items in a DataSlice.
Sub-slicing slices **all** dimensions of a DataSlice or the last dimension when
only one dimension is specified. The API is called "subslice" because it slices
a DataSlice to create a sub-DataSlice.

See [kd.subslice](koda_v1_api_reference.md#kd.slices.subslice) APIs for more details.

```py
ds = kd.slice([[1, 2, 3], [4, 5]])

# Slice by indices
kd.subslice(ds, 1, 1)
kd.subslice(ds, 1, -1)

# Slice by range
kd.subslice(ds, 0, slice(1))
kd.subslice(ds, 0, slice(2, 4))

# 'S[]' as Syntactic sugar
ds.S[1, 1]
ds.S[0, :2]

# Multi-dimension
ds = kd.slice([[[1, 2, 3], [4, 5, 6]],
            [[7, 8, 9], [10, 11, 12]]])

ds.S[1] # [[2, 5], [8, 11]]
ds.S[1:, 0, :2] # [[7, 8]]
ds.S[0, :2] # [[1, 2], [7, 8]]
ds.S[kd.slice([[0, 1], [1]]), 1] # [[2, 5], [11]]

# Advanced slicing: slice by DataSlice
ds = kd.slice([[1, 2, 3], [4, 5]])
ds.S[kd.slice([0, 0]), 1:] # [[[2, 3], [2, 3]]]
ds.S[kd.slice([[0, 2], [0, 1]]),
        kd.slice([[0, 0], [1, 0]])]
# -> [[1, None], [2, 4]]

# Ellipse
ds = kd.slice([[[1, 2, 3], [4, 5]],
                [[6, 7]], [[8], [9, 10]]])
ds.S[1:, ...] # [[[6, 7]], [[8], [9, 10]]]
ds.S[1:, ..., :1] # [[[6]], [[8], [9]]]
ds.S[1:, 0, ..., :1] # [[6], [8]]
```

</section>

<section>

### Python-like Slicing

While subslicing provides an efficient way to slice all dimensions, it is
sometimes convenient to slice dimensions one by one similar to how to iterate
through a nested Python list. DataSlice provides a way to slice the **first**
dimension using `.L` notation. *L* stands for *Python list*.

```py
ds = kd.slice([[1, 2, 3], [4, 5]])

# Note that 'l' is a one-dim DataSlice
# and 'i' is a DataItem
for l in ds.L:
  print(type(l))
  for i in l.L:
    print(type(i))
    print(i)

# A Python list of one-dim DataSlice
ds.L[:]
# [DataSlice([1, 2, 3], ...),
#  DataSlice([4, 5], ...)]

ds.L[1] # DataSlice([4, 5], ...)
```

</section>

<section>

### Editable Containers

```py
x = kd.container()
x.a = 1
x.d = kd.container()
x.d.e = 4
x.d.f = kd.list([1, 2, 3])
```

</section>

<section>

### DataSlice Shape (a.k.a. Partition Tree)

```py
ds = kd.slice([[[1, 2], [3]], [[4, 5]]])
shape = ds.get_shape()
# -> JaggedShape(2, [2, 1], [2, 1, 2])

kd.shapes.ndim(shape) # 3
kd.shapes.size(shape) # 5

# Get the shape with N-1 dimensions
shape[:-1] # JaggedShape(1, 2, [2, 1])
# Get the shape with first dimension
shape[:2] # JaggedShape(1, 2)

# Create a new shape directly
shape1 = kd.shapes.new(2, [2, 1], [2, 1, 2])
assert shape1 == shape
```

</section>

<section>

### Changing shape of a DataSlice

```py
ds = kd.slice([[[1, 2], [3]], [[4, 5]]])
# Flatten all dimensions
ds.flatten() # [1, 2, 3, 4, 5]
# Flatten dimensions from the second to the end
ds.flatten(2)  # [[1, 2, 3], [4, 5]]
# Flatten dimensions from the second to the end
ds.flatten(-2) # [[1, 2, 3], [4, 5]]
# Flatten dimensions from the first to the third
ds.flatten(0, 2) # [[1, 2], [3], [4, 5]]
# When the start and end dimensions are the same,
# insert a new dimension
ds.flatten(2, 2) # [[[[1, 2]], [[3]]], [[[4, 5]]]]

# Reshape to the shape of another DataSlice
ds1 = kd.slice([1, 2, 3, 4, 5])
ds.reshape_as(ds1)
ds.reshape(ds1.get_shape())

ds2 = kd.slice([1, None, 3])
# Repeats values
ds2.repeat(2) # [[1, 1], [None, None], [3, 3]]
kd.repeat(ds2, 2) # same as above

# Repeats present values
kd.repeat_present(ds2, 2) # [[1, 1], [], [3, 3]]
```

</section>

<section>

### Broadcasting and Aligning

```py
# Expands x based on the shape of target
kd.slice([100, 200]).expand_to(
    kd.slice([[1,2],[3,4,5]]))
# Implodes last ndim dimensions into lists,
# expands the slice of lists, explodes the result
kd.slice([100, 200]).expand_to(
    kd.slice([[1,2],[3,4,5]]), ndim=1)

kd.is_expandable_to(x, target, ndim)
# Whether any of args is expandable to the other
kd.is_shape_compatible(x, y)

# Expands DataSlices to the same common shape
kd.align(kd.slice([[1, 2, 3], [4, 5]]),
         kd.slice('a'), kd.slice([1, 2]))
```

</section>

<section>

### ItemIds and UUIDs

```py
# A new ItemId is allocated when a new
# object/entity/list/dict/schema is created
o1 = kd.obj(x=1, y=2)
o2 = kd.obj(x=1, y=2)
assert o1.get_itemid() != o2.get_itemid()

# Get ItemId from object/entity/list/dict
itemid = o1.get_itemid()

# ItemId is a 128 integer
# Print out the ItemId in the base-62 format
str(itemid) # With Entity:$ prefix
str(kd.list().get_itemid()) # With List:$ prefix
str(kd.dict().get_itemid()) # With Dict:$ prefix
str(kd.schema.new_schema().get_itemid())
# With Schema:$ prefix

# Encode to/from base-62 number (as string).
str_id = kd.encode_itemid(itemid)
itemid1 = kd.decode_itemid(str_id)
assert itemid1 == itemid

# Convert ItemId back to the original
# object/entity/list/dict
kd.reify(itemid1, o1)

# int64 hash values
kd.hash_itemid(itemid)

# ItemIds can be allocated directly
kd.new_itemid()
kd.new_listid()
kd.new_dictid()

# UUIDs are unique determined

# A new UUID is allocated when a new uu
# object/entity/schema is created
o3 = kd.uuobj(x=1, y=2)
o4 = kd.uuobj(x=1, y=2)
assert o3.get_itemid() == o4.get_itemid()

kd.uu(x=1, y=2) # UU Entity
kd.uuobj(x=1, y=2) # UU Object
kd.uu_schema(x=kd.INT32, y=kd.INT64) # UU Schema

# UUID has a # prefix compared to $ for ItemId
str(o3.get_itemid()) # With Entity:# prefix

# Compute UUID from attr values
i1 = kd.uuid(x=1, y=2)
i2 = kd.uuid(x=1, y=2)
assert i1 == i2

# Traverse attrs instead of using their UUIDs
kd.deep_uuid(obj)

# Compute UUIDs by aggregating items
# over the last dimension
kd.agg_uuid(kd.slice([[1, 2, 3], [4, 5, 6]]))
```

</section>

## DataSlice Operators

Koda provides a comprehensive list of operators under `kd` module. Most
pointwise operatorƒ automatically broadcast all input DataSlices to the same
shape.

<section>

### Item Creation Operators

`kd.***_like(x)` creates a DataSlice of *** with the **same shape and sparsity**
as DataSlice `x`.

`kd.***_shaped_as(x)` creates a DataSlice of *** with the **same shape** as
DataSlice `x`.

`kd.***_shaped(shape)` creates a DataSlice of *** with the **shape** `shape`.

```py
x = kd.slice([[1, None], [3, 4]])
s = x.get_shape()

kd.val_like(x, 1) # [[1, None], [1, 1]]
kd.val_shaped_as(x, 1) # [[1, 1], [1, 1]]
kd.val_shaped(s, 1) # [[1, 1], [1, 1]]

# Note there is kd.present_like
kd.present_shaped_as(x)
kd.present_shaped(s)

# Note there is kd.empty_like
kd.empty_shaped_as(x)
kd.empty_shaped(s)

# Empty Object creation
# Note obj_xxx does not take schema= argument
kd.obj_like(x)
kd.obj_shaped_as(x)
kd.obj_shaped(s)

# Non-empty Object creation
kd.obj_like(x, **attr_dss)
kd.obj_shaped_as(x, **attr_dss)
kd.obj_shaped(s, **attr_dss)

# With provided itemids
kd.obj_like(x, itemid=itemid)
kd.obj_shaped_as(x, itemid=itemid)
kd.obj_shaped(s, itemid=itemid)

# Empty Entity creation
kd.new_like(x)
kd.new_shaped_as(x)
kd.new_shaped(s)

# Non-empty Entity creation
kd.new_like(x, **attr_dss)
kd.new_shaped_as(x, **attr_dss)
kd.new_shaped(s, **attr_dss)

# With provided itemids
kd.new_like(x, itemid=itemid)
kd.new_shaped_as(x, itemid=itemid)
kd.new_shaped(s, itemid=itemid)

schema = kd.new_schema(...)
kd.new_like(x, schema=schema)

# Empty List creation
kd.list_like(x)
kd.list_shaped_as(x)
kd.list_shaped(s)

# Non-empty List creation
kd.list_like(x, list_item_ds)
kd.list_shaped_as(x, list_item_ds)
kd.list_shaped(s, list_item_ds)

# With provided itemids
kd.list_like(x, itemid=itemid)
kd.list_shaped_as(x, itemid=itemid)
kd.list_shaped(s, itemid=itemid)

schema = kd.list_schema(...)
kd.list_like(x, schema=schema)

# Empty Dict creation
kd.dict_like(x)
kd.dict_shaped_as(x)
kd.dict_shaped(s)

# Non-empty Dict creation
kd.dict_like(x, key_ds, value_ds)
kd.dict_shaped_as(x, key_ds, value_ds)
kd.dict_shaped(s, key_ds, value_ds)

# With provided itemids
kd.dict_like(x, itemid=itemid)
kd.dict_shaped_as(x, itemid=itemid)
kd.dict_shaped(s, itemid=itemid)

schema = kd.dict_schema(...)
kd.dict_like(x, schema=schema)
```

</section>

<section>

### Pointwise Comparison Operators

```py
a = kd.slice([1, 2, 3])
b = kd.slice([3, 2, 1])

# Use infix operators
a > b
a >= b
a < b
a <= b
a == b
a != b

# Use kd operators
kd.greater(a, b)
kd.greater_equal(a, b)
kd.less(a, b)
kd.less_equal(a, b)
kd.equal(a, b)
kd.not_equal(a, b)
```

</section>

<section>

### DataSlice Comparison Operator

```py
a = kd.slice([1, None, 3])
b = kd.slice([1, None, 3])

kd.full_equal(a, b) # present
# Note it is different from
kd.all(a == b) # missing

# Auto-alignment rule applies
kd.full_equal(kd.item(1), kd.slice([1, 1])) # present

# Type promotion rule applies
kd.full_equal(kd.item(1), kd.slice([1, 1.0])) # present
```

</section>

<section>

### Presence Checking Operators

```py
a = kd.slice([1, None, 3])

kd.has(a) # [present, missing, present]
kd.has_not(a) # [missing, present, missing]

b = kd.slice([kd.obj(), kd.obj(kd.list()),
              kd.obj(kd.dict()), None, 1])

kd.has_entity(b)
# -> [present, missing, missing, missing, missing]
kd.has_list(b)
# -> [missing, present, missing, missing, missing]
kd.has_dict(b)
# -> [missing, missing, present, missing, missing]
kd.has_primitive(b)
# -> [missing, missing, missing, missing, present]
```

</section>

<section>

### Masking and Coalesce Operators

```py
# Masking
a = kd.slice([1, None, 3])
b = kd.slice([4, 5, 6])

# Use infix operator
a & (a > 1) # [None, None, 3]
kd.apply_mask(a, a > 1) # Same as above

# Use infix operator
a | b # [1, 5, 3]
kd.coalesce(a, b) # Same as above

# Works with a DataSlice with a different schema
c = kd.slice(['4', '5', '6'])
a | c # [1, '5', 3]

# Make sure inputs are disjoint
kd.disjoint_coalesce(a, c) # failed

d = kd.slice([None, '5', None])
kd.disjoint_coalesce(a, d) # [1, '5', 3]
```

</section>

<section>

### Conditional Selection Operators

```py
a = kd.slice([1, 2, 3])
kd.select(a, a > 1)
a.select(a > 1)
kd.select(a, lambda x: x > 1) # same as above

# Use expand_filter=False to avoid expanding
# the filter to the shape of the DataSlice
# so that empty dimensions are removed too
b = kd.slice([[1, None], [None, 4]])
fltr = kd.slice([kd.present, kd.missing])
kd.select(b, fltr) # [[1, None], []]
kd.select(b, fltr, expand_filter=False) # [[1, None]]

# Put items in present positions in filter.
kd.inverse_select(
    kd.slice([1, 2]),
    kd.slice([kd.missing, kd.present, kd.present])
)

kd.select_present(kd.slice([1, None, 3]))

b = kd.slice([3, 2, 1])
c = kd.slice([-1, 0, 1])
kd.cond(c > 1, a, b)
```

</section>

<section>

### Range Operator

```py
kd.range(1, 5) # [1, 2, 3, 4]
kd.range(1, kd.slice([3, 5]))
# -> [[1, 2], [1, 2, 3, 4]]
kd.range(kd.slice([0, 3]), kd.slice([3, 5]))
# -> [[0, 1, 2], [3, 4]]
```

</section>

<section>

### Math Operators

```py
x + y  # Short for kd.math.add(x, y)
x - y  # Short for kd.math.subtract(x, y)
x * y  # Short for kd.math.multiple(x, y)
x / y  # Short for kd.math..divide(x, y)
x % y  # Short for kd.math.mod(x, y)
x ** y  # Short for kd.math.pow(x, y)
x // y  # Short for kd.math.floor_divide(x, y)

kd.math.log(x)
kd.math.round(x)
kd.math.float(x)
kd.math.ceil(x)
kd.math.pos(x)
kd.math.abs(x)
kd.math.neg(x)
kd.math.maximum(x, y)
kd.math.mimimum(x, y)

kd.agg_min(x)
kd.agg_max(x)
kd.agg_sum(x)
kd.min(x)
kd.max(x)
kd.sum(x)

kd.math.cum_min(x)
kd.math.cum_max(x)
kd.math.cum_sum(x)
kd.math.agg_mean(x)
kd.math.agg_median(x)
kd.math.agg_std(x)
kd.math.agg_var(x)
kd.math.cdf(x)
kd.math.agg_inverse_cdf(x)
```

</section>

<section>

### String Operators

```py
kd.strings.length(x)
kd.strings.upper(x)
kd.strings.lower(x)
kd.strings.split(x, sep)
kd.strings.contains()
kd.strings.contains_regex(x, regex)
kd.strings.find(x, str)
kd.strings.replace(x, old, new)
kd.strings.substr(x, start, end)
kd.strings.count(x, sub)
kd.strings.agg_join(x, sep)
kd.strings.join(*args)

kd.strings.encode('abc') # b'abc'
kd.strings.decode(b'abc') # 'abc'

# Encode any bytes into base64 format string
# Useful for putting bytes into JSON
kd.strings.encode_base64(b'abc') # 'YWJj'
kd.strings.decode_base64('YWJj') # b'abc'
```

</section>

<section>

### String Format Operators

```py
kd.strings.printf('Hello %s', 'World') # 'Hello World'

kd.strings.printf('Hello %s',
  kd.slice(['World', 'Universe']))
# -> ['Hello World', 'Hello Universe']

o = kd.obj(greeting='Hello', name='World')
kd.strings.printf('%s %s', o.greeting, o.name) # 'Hello World'

kd.strings.printf('%s %d', "Hello", 1) # 'Hello 1'

# Python like f-string Format API
# Note that ":s" after the DataSlice var name
name = kd.str('World')
kd.strings.fstr(f'Hello {name:s}') # 'Hello World'

name = kd.slice(['World', 'Universe'])
kd.strings.fstr(f'Hello {name:s}')
# -> ['Hello World', 'Hello Universe']

o=kd.obj(greeting='Hello', name='World')
kd.strings.fstr(f'{o.greeting:s} {o.name:s}') # 'Hello World'

greeting, count = kd.str('Hello'), kd.int32(1)
kd.strings.fstr(f'{greeting:s} {count:s}') # 'Hello 1'

# Use format to specify kwargs
kd.strings.format('Hello {name}', name='World') # 'Hello World'

kd.strings.format('Hello {name}',
  name=kd.slice(['World', 'Universe']))
# -> ['Hello World', 'Hello Universe']

kd.strings.format('{greeting} {count}',
  greeting='Hello', count=1) # 'Hello 1'
```

</section>

<section>

### Aggregational Operators

```py
# collapse over the last dimension and
# returns the value if all item has the
# same value or None otherwise
x = kd.slice([[1], [], [2, None], [3, 4]])
kd.collapse(x) # [1, None, 2, None]

# 'count' returns counts of present items
kd.agg_count(x) # [1, 0, 1, 2]
# aggregates over the last 2 dimensions
kd.agg_count(x, ndim=2) # 4
# aggregates across all dimensions
kd.count(x) # the same as above
# aggregates over the last dimension and
# returns the accumulated count of present items
kd.cum_count(x) # [[1], [], [1, None], [1, 2]]
# aggregates over the last 2 dimensions
kd.cum_count(x, ndim=2) # [[1], [], [2, None], [3, 4]]

# 'index' returns indices starting from 0
# aggregates over the last dimension
kd.index(x) # [[[0, None, 2], [], [0]], [[], [0]]]
# indices at the second dimension
# note it is `dim` rather than `ndim` and
# the result is aligned with x
kd.index(x, dim=1) # [[[0, 0, 0], [], [2]], [[], [1]]]
# indices at the first dimension
kd.index(x, dim=0) # [[[0, 0, 0], [], [0]], [[], [1]]]

x = kd.slice([[kd.present, kd.present], [],
              [kd.present, kd.missing], [kd.missing]])

# 'all' returns present if all items are present
# aggregates over the last dimension
kd.agg_all(x) # [present, present, missing, missing]
# aggregates across all dimensions
kd.all(x) # missing

# 'any' returns present if any item is present
kd.agg_any(x) # [present, missing, present, missing]
# aggregates across all dimensions
kd.any(x) # present
```

</section>

<section>

### Type casting operators

```py
kd.to_int32(kd.slice([1., 2., 3.]))
kd.to_int64(kd.slice([1, 2, 3]))
kd.to_float32(kd.slice([1, 2, 3]))
kd.to_float64(kd.slice([1., 2., 3.]))
kd.to_bool(kd.slice([0, 1, 2]))
kd.to_str(kd.slice([1, 2, 3]))
kd.to_bytes(kd.slice([b'1', b'2', b'3']))

# Dispatches to the relevant kd.to_* operator
# or casts into specific schema.
kd.cast_to(kd.slice([1, 2, 3]), kd.INT64)
kd.cast_to(kd.new(x=1), kd.uu_schema(x=kd.INT32))
```

</section>

<section>

### Expand_to Operators

```py
a = kd.slice([[1, 2], [3]])
b = kd.slice([[[1, 2], [3]], [[4, 5]]])

kd.expand_to(a, b)
# returns [[[1, 1], [2]], [[3, 3]]]
# which is equivalent to
a.expand_to(b)

c = kd.slice([[1], [2, 3]])

kd.expand_to(a, c)
# raises an exception
# which is equivalent to
kd.expand_to(a, c, last_ndim_to_implode=0)

kd.expand_to(a, c, last_ndim_to_implode=1)
# returns [[[1, 2]], [[3], [3]]]

kd.expand_to(a, c, last_ndim_to_implode=2)
# returns [[[[1, 2], [3]]], [[[1, 2], [3]],
#          [[1, 2], [3]]]]

kd.expand_to(a, c, last_ndim_to_implode=3)
# raises an exception
```

</section>

<section>

### Explosion and Implosion Operators

```py
# kd.explode explodes the Lists ndim times
a = kd.list([[[1, 2], [3]], [[4], [5]]])

kd.explode(a, ndim=1) # [List:$64, List:$65]
a.explode(ndim=1) # the same as above

kd.explode(a, ndim=2)
# -> [[List:$68, List:$69], [List:$6A, List:$6B]]

kd.explode(a, ndim=3)
# ->[[[1, 2], [3]], [[4], [5]]]

# explode until the DataSlice does not contain
# Lists any more.
kd.explode(a, ndim=-1)
# ->[[[1, 2], [3]], [[4], [5]]]

# kd.implode implodes the DataSlice ndim times
a = kd.slice([[[1, 2], [3]], [[4], [5]]])

kd.implode(a, ndim=1)
# -> [List:$68, List:$69], [List:$6A, List:$6B]]

# which is equivalent to
a.implode(ndim=1)

kd.implode(a, ndim=2) # [List:$64, List:$65]

kd.implode(a, ndim=3) # List:$66

# implode until the result is a List DataItem
kd.implode(a, ndim=-1) # List:$66
```

</section>

<section>

### Joining DataSlice Operators

```py
# Stacks DataSlices and creates a new
# dimension at `ndim`
a = kd.slice([[1, None], [4]])
b = kd.slice([[7, 7], [7]])

kd.stack(a, b, ndim=0) # [[[1, 7], [None, 7]], [[4, 7]]]
kd.stack(a, b) # the same as above
kd.stack(a, b, ndim=1) # [[[1, None], [7, 7]], [[4], [7]]]
kd.stack(a, b, ndim=2) # [[[1, None], [4]], [[7, 7], [7]]]
# raise an exception
# kd.stack(a, b, ndim=4)

# Zip DataSlices over the last dimension
kd.zip(a, b) # [[[1, 7], [None, 7]], [[4, 7]]]
# Zip supports auto-alignment
kd.zip(a, 1) # [[[1, 1], [None, 1]], [[4, 1]]]

# Zip is equivalent to kd.stack(..., ndim=0)
# but supports auto-alignment
kd.stack(a, b) # [[[1, 7], [None, 7]], [[4, 7]]]
# raise an exception
# kd.stack(a, 1)

# Concatenates DataSlices on dimension `ndim`
a = kd.slice([[[1, 2], [3]], [[5], [7, 8]]])
b = kd.slice([[[1], [2]], [[3], [4]]])

kd.concat(a, b, ndim=1)
# -> [[[1, 2, 1], [3, 2]], [[5, 3], [7, 8, 4]]]
kd.concat(a, b) # the same as above
kd.concat(a, b, ndim=2)
# -> [[[1, 2], [3], [1], [2]], [[5], [7, 8], [3], [4]]]
kd.concat(a, b, ndim=3)
# -> [[[1, 2], [3]], [[5], [7, 8]], [[1], [2]], [[3], [4]]]
# raise an exception
# kd.concat(a, b, ndim=4)
```

</section>

<section>

### Group_by Operators

```py
cities = kd.obj(
  name=kd.slice(['sf', 'sj', 'la', 'nyc', 'albany']),
  population=kd.slice([100, 200, 300, 400, 500]),
  state=kd.slice(['ca', 'ca', 'ca', 'ny', 'ny'])
)

# Group cities by state name over the last dimension
# Note the result has one more dimension
cities_grouped = kd.group_by(cities, cities.state)

state_population = kd.agg_sum(cities_grouped.population)

# We can create the state lists
states = kd.obj(
  cities=kd.implode(cities_grouped),
  name=kd.collapse(cities_grouped.state),
  population=state_population
)

# Note that 'state' for 'la' is missing
cities = kd.obj(
  name=kd.slice(['sf', 'sj', 'la', 'nyc', 'albany']),
  population=kd.slice([100, 200, 300, 400, 500]),
  state=kd.slice(['ca', 'ca', None, 'ny', 'ny'])
)

cities_grouped = kd.group_by(cities, cities.state)

# Note that 'la' is missing
cities_grouped.name
# [['sf', 'sj'], ['nyc', 'albany']]

# Sort by both state name and population
# Note that we need to convert MASK to BOOLEAN
# if we want to use it as group_by value
cities_grouped = kd.group_by(cities, cities.state,
  kd.cond(cities.population > 100, True, False))

cities_grouped.name
# [['sf'], ['sj'], ['nyc', 'albany']]
```

</section>

<section>

### Unique Operator

```py
ds1 = kd.slice([[1, 1, 1.], [1, '1', None]])

# Get unique items over the last dimension
kd.unique(ds1) # [[1, 1.0], [1, '1']]

# Get unique items across all dimensions
kd.unique(ds1.flatten()) # [1, 1.0, '1']

o1 = kd.obj(a=1)
o2 = kd.obj(a=2)
l = kd.obj(kd.list())
d = kd.obj(kd.dict())
ds2 = kd.slice([[o1, o1, o2], [l, d, None]])

# For entities/objects/lists/dicts,
# compare their ItemIds
kd.unique(ds2) # [[o1, o2], [l, d]]
```

</section>

<section>

### Link Operators

```py
# One-to-many mapping
docs = kd.obj(
  did=kd.slice([[1, 2], [1, 2, 3]]),
  score=kd.slice([[4, 5], [6, 7, 8]]),
)
queries = kd.obj(did=kd.slice(
  [kd.list([1, 3, 1]), kd.list([3, 1])]))
kd.translate(queries.did[:], docs.did, docs.score)
# [[4, None, 4], [8, 6]]

# Many-to-one mapping
docs = kd.obj(
    qid=kd.slice([[1, 2, 2], [2, 2, 3]]),
    score=kd.slice([[1, 2, 3], [4, 5, 6]]),
)
queries = kd.obj(
  qid=kd.slice([[1, 2], [3, 2]]))

new_docs = kd.translate_group(
  queries.qid, docs.qid, docs)

# Add docs list since the result of
# translate_group is aligned with queries
queries.docs = kd.implode(new_docs)

queries.docs[:].score
# [[[1], [2, 3]], [[6], [4, 5]]]

# Many-to-many mapping is not directly
# supported as operator but can be done
# as multiple steps. E.g.
docs = kd.obj(
    qid=kd.slice([1, 2, 2, 2, 2, 3]),
    score=kd.slice([1, 2, 3, 4, 5, 6]),
)

query_terms = kd.obj(
    qid=kd.slice([1, 1, 1, 2, 2]),
    term=kd.slice(['a', 'b', 'c', 'd', 'e']),
)

docs_grouped = kd.group_by(docs, docs.qid)
terms_grouped = kd.group_by(query_terms, docs.qid)
queries = kd.obj(
  docs=kd.implode(docs_grouped),
  terms=kd.implode(terms_grouped)
)
```

</section>

<section>

### Random Number and Sampling Operators

```py
# Generate random integers
kd.randint_like(x)
# random int between 0 and 10
kd.randint_like(x, 10)
# random int between -5 and 200
kd.randint_like(x, -5, 200)
# setting the seed is supported
kd.randint_like(x, 10, seed=123)

kd.randint_shaped_as(x, ...)

kd.randint_shaped(shape, ...)

ds = kd.slice([[1, 2, None, 4],
            [5, None, None, 8]])
key = kd.slice([['a', 'b', 'c', 'd'],
             ['a', 'b', 'c', 'd']])

# Sampling is performed on flatten 'ds'
# rather than on the last dimension.
# Sample each element with .5 probability.
kd.sample(ds, 0.5, 123)
# Use 'key' for stability
kd.sample(ds, 0.5, 123, key)

# Select 2 items from last dimension
kd.sample_n(ds, 2, 123)
# Select one item from the first and
# two items from the second
kd.sample_n(ds, kd.slice([1, 2], 123))
# Use 'key' for stability
kd.sample_n(ds, 2, 123, key)
```

</section>

<section>

### Sort and Rank Operators

```py
ds1 = kd.slice([[10, 5, 10, 5], [30, 10]])
ds2 = kd.slice([[1, 2, 3, 4], [2, 1]])

# Ranking returns ranked positions
# Rank over the last dimension
kd.ordinal_rank(ds1)
# Rank with tie breaker
kd.ordinal_rank(ds1, tie_breaker=ds2)
# Rank with descending order
kd.ordinal_rank(ds1, descending=True)

# Sorting returns sorted DataSlice
# Sort over the last dimension
kd.sort(ds1)
# Sort with descending order
kd.sort(ds1, descending=True)

# Find the n-th element in the last dimension
kd.sort(ds1).take(2)
kd.sort(ds1).take(kd.slice([0, 1]))
```

</section>

<section>

### Reverse Operator

```py
ds = kd.slice([[10, 5, 10, 5], [30, 10]])

# Reverse items in the last dimension
kd.reverse(ds) # [[5, 10, None, 10], [10, 30]]

# Reverse items across all dimensions
kd.reverse(ds.flatten()).reshape_as(ds)
# -> [[10, 30, 5, 10], [None, 10]]
```

</section>

<section>

### map_py Operator

```py
ds = kd.slice([1, 2, 3])

# Pointwise
kd.map_py(lambda x: x + 1, ds,
          schema=kd.INT64)

# Aggregational
kd.map_py(lambda x: len(x), ds, ndim=1)

# Aggregaional but no dimension change
kd.map_py(lambda x: sorted(x), ds, ndim=1)[:]

# Expansion
kd.map_py(lambda x: [x] * 10, ds,
          schema=kd.list_schema(kd.INT64))[:]

# Pass Objects
a = kd.obj(x=kd.slice([[1, 2], [3, None, 5]]),
           y=kd.slice([[6, 7], [8, 9, None]]))
kd.map_py(lambda a: a.x + a.y, a,
          schema=kd.INT32)
kd.map_py(lambda a: [obj.x - obj.y for obj in a],
          a, ndim=1)[:]

# Pass Objects and primitives
b = kd.slice([[1, 2], [3, None, 5]])
kd.map_py(lambda a, b: a.x + a.y + b, a, b)

# Return Objects
f = lambda x: kd.obj(x=1) if x.y < 3 else kd.obj(y=1)
res = kd.map_py(f, kd.obj(y=kd.slice([[1, 2], [3]])))

# With max_thread to speed up the I/O-bound
# executions (e.g. reading from disk or RPCs)
kd.map_py(fn, ds, max_thread=20)

# Pass multiple arguments
f = lambda x, y, z: x + y + z
kd.map_py(f, 1, 2, 3) # positional
kd.map_py(f, z=3, y=2, x=1) # keyword
kd.map_py(f, 1, z=3, y=2) # mix of positional and keyword
```

</section>

<section>

### Conditional Apply Operators

```py
# Note that 'a' is passed as a DataSlice
def f1(a):
  return a + 1

a = kd.slice([1, 2, 3])

# Apply Python python
c = kd.apply_py_on_selected(f1, a > 1, a) # [None, 3, 4]

# Fill the missing items with values from 'a'
c | a # [1, 3, 4]

def f2(a, b):
  return a + b

def f3(a, b):
  return a - b

b = kd.slice([4, 5, 6])

kd.apply_py_on_cond(f2, f3, a > 1, a, b) # [-3, 7, 9]

# use kwargs
kd.apply_py_on_cond(f2, f3, a > 1, b=b, a=a)

# similarly map_py can be applied conditionally
kd.map_py_on_selected(lambda a: a + 1, a > 1, a,
                      schema=kd.INT32)

kd.map_py_on_cond(lambda a, b: a + b,
                  lambda a, b: a - b, a > 1, b=b, a=a,
                  schema=kd.INT32)
```

</section>

## Schema

Koda schema is used to describe the type of contents. There are five types of
schemas:

-   **Primitive schema (a.k.a. dtype)** (e.g. kd.INT32, kd.FLOAT64, kd.STRING)
-   **Entity schema** used to describe what attributes the Entity has and what
    types these attribute are. Note that List schema and Dict schema are special
    entity schemas
-   **OBJECT** used to indicate the schema is stored as data
-   **ITEMID** used to indicate the content should be interpreted as opaque
    ItemIds rather than the data those ItemIds refer to
-   **ANY** used to indicate unknown/mixed schema

Schema is used to specify the behaviors for:

-   Attribute lookup (including Dict lookup)
-   Clone and extract
-   Attribute setting (including List/Dict content modification)

Schemas can be attached to **DataSlices** or **individual items**. Attaching a
schema to a DataSlice means declaring that each item has the **same** schema.
Individual items can store their own schema in a special attribute `__schema__`
and such item is called Object. It allows storing Objects with **different**
schemas in one DataSlice. Primitives can be considered as OBJECT schemas as
dtypes are embedded in their values.

**DataSlice Schema** refers to schemas attached to DataSlices. **Embedded
Schema** refers schemas embedded inside Koda Objects as `__schema__` attribute
or dtypes for primitives. DataSlice Schema can be primitive schema, Entity
schema, OBJECT, ITEMID, or ANY. Embedded Schema cannot be ITEMID or ANY.

Entity schemas can be either **explicit** or **implicit**. Explicit schemas are
created using `kd.new_schema/list_schema/dict_schema()` while implicit schemas
are created as a by-product of `kd.obj()`.

<section>

### Schema Creation

`kd.named_schema/list_schema/dict_schema/uu_schema(...)` creates an explicit
schema.

```py
# Create a named schema
Point = kd.named_schema('Point', x=kd.INT32, y=kd.FLOAT64)

# Attribute 'end' can be anything
Line = kd.named_schema('Line', start=Point, end=kd.ANY)

# Get the attribute start's schema
Line.start

# Check if it is an Entity schema
assert Point.is_entity_schema()
assert Line.is_entity_schema()

# List schema
ls1 = kd.list_schema(kd.INT64)

# List entity schema is 's1'
ls2 = kd.list_schema(Point)

# Get the List item schema
ls2.get_item_schema()

# Check if it is a List schema
assert ls2.is_list_schema()

# Dict schema
# Dict value schema is kd.OBJECT. That is,
# schemas are stored in entity __schema__
# attribute
ds1 = kd.dict_schema(kd.STRING, kd.OBJECT)

# Dict value schema is 's2'
ds2 = kd.dict_schema(kd.ITEMID, Line)

# Get the Dict key/value schema
ds2.get_key_schema()
ds2.get_value_schema()

# Check if it is a Dict schema
assert ds2.is_dict_schema()

# UU schema
uus1 = kd.uu_schema(x=kd.INT32, y=kd.FLOAT64)

# UU schemas with the same contents are the same
uus2 = kd.uu_schema(x=kd.INT32, y=kd.FLOAT64)
assert uus1 == uus2

# It is also an Entity schema
assert uus1.is_entity_schema()

# In fact, named, list and dict schemas are also
# UU schemas
Point1 = kd.named_schema('Point', x=kd.INT32, y=kd.FLOAT64)
assert Point1.as_itemid() == Point.as_itemid()

## Create non-uu schema whose ItemId is allocated
s1 = kd.schema.new_schema(x=kd.INT32, y=kd.FLOAT64)
s2 = kd.schema.new_schema(x=kd.INT32, y=kd.FLOAT64)

assert s1.as_itemid() != s2.as_itemid()
```

</section>

<section>

### Item Creation Using Schemas

Schemas can be used to create entities by calling these schema DataItems
directly as factory methods. Or we can use `kd.new`. That is, `schema(...)` is
equivalent to `kd.new(..., schema=schema)`.

```py
Point = kd.named_schema('Point', x=kd.INT32, y=kd.FLOAT64)
Line = kd.named_schema('Line', start=Point, end=kd.ANY)

i1 = Point(x=1, y=2.3)
# which is equivalent to
i1 = kd.new(x=1, y=2.3, schema=Point)
i2 = Line(z=i1, w='4')

s3 = kd.list_schema(kd.INT64)
s4 = kd.list_schema(Point)

i3 = s3([1, 2, 3, 4])
i4 = s4([i1, i1])

s5 = kd.dict_schema(kd.STRING, kd.OBJECT)
s6 = kd.dict_schema(kd.ITEMID, Line)

i5 = s5({'a': kd.obj()})
i6 = s6(kd.obj().as_itemid(), i2)
```

</section>

<section>

### Item Creation without Schema

When no schema is provided to `kd.new`, an schema is automatically derived based
on provided arguments or keyword arguments. When a string is provided as schema,
an uu schema is created based on the name.

```py
# kd.new() creates entities with derived schema
i1 = kd.new(x=1, y=2.0, z='3')

# The result DataItem has a auto-drived schema
assert i1.get_schema().x == kd.INT32
assert i1.get_schema().y == kd.FLOAT32
assert i1.get_schema().z == kd.STRING

i2 = kd.new(x=1, y=2.0, schema='Point')
i3 = kd.new(x=2, y=3.0, schema='Point')
assert i2.get_schema() == i3.get_schema()
```

</section>

<section>

### Object Creation With Implicit Schemas

`kd.obj(...)` creates an object with an implicit schema.

```py
# An implicit schema is created and
# attached to the Koda object
o1 = kd.obj(x=1, y=2.0, z='3')

# The schema of the DataItem is kd.OBJECT
assert o1.get_schema() == kd.OBJECT

# The implicit schema is stored
# as __schema__ attribute and can be accessed
# using get_obj_schema()
o1.get_obj_schema()
# -> IMPLICIT_SCHEMA(x=INT32, y=FLOAT32, z=STRING)

# A new and different implicit schema is created
# every time kd.obj() is called
o2 = kd.obj(x=1, y=2.0, z='3')
assert o1.get_obj_schema() != o2.get_obj_schema()

# A new implicit schema is created for each
# newly created objects
o_ds = kd.obj(x=kd.slice([1, 2]))

# The schema of the DataSlice is kd.OBJECT
assert o_ds.get_schema() == kd.OBJECT

obj_ss = o_ds.get_obj_schema()
obj_ss.get_size()# 2
obj_ss.take(0)
# -> IMPLICIT_SCHEMA(x=INT32, y=FLOAT32, z=STRING)
assert obj_ss.take(0) != obj_ss.take(1)
```

</section>

## Interoperability

<section>

### From_py

```py
# Primitives
kd.from_py(1)
kd.from_py(1, schema=kd.INT64)
kd.from_py(1.0)
kd.from_py(1.0, schema=kd.FLOAT64)
kd.from_py('a')
kd.from_py(b'a')
kd.from_py(True)
kd.from_py(None) # NONE schema rather than MASK

# Entity/Object
@dataclasses.dataclass
class PyObj:
  x: int
  y: float

py_obj = PyObj(x=1, y=2.0)

kd.from_py(py_obj) # Object
kd.from_py(py_obj, schema=kd.OBJECT) # Same as above
s1 = kd.named_schema('Point', x=kd.INT32,
                     y=kd.FLOAT64)
kd.from_py(py_obj, schema=s1) # Entity

# dict_as_obj=True
py_dict = {'x': 1, 'y': 2.0}
kd.from_py(py_dict, dict_as_obj=True) # Object
kd.from_py(py_dict, schema=kd.OBJECT) # Same as above
kd.from_py(py_dict, dict_as_obj=True,
           schema=s) # Entity

# List
py_list = [[1, 2], [3], [4, 5]]
kd.from_py(py_list)
s2 = kd.list_schema(kd.list_schema(kd.INT64))
kd.from_py(py_list, schema=s2)

# Dict
py_dict = {'x': 1, 'y': 2.0}
kd.from_py(py_dict)
s3 = kd.dict_schema(kd.STRING, kd.FLOAT64)
kd.from_py(py_dict, schema=s3)

# Use provided itemids
id1 = kd.new_itemid()
kd.from_py(py_obj, itemid=id1)
id2 = kd.new_listid()
kd.from_py(py_list, itemid=id2)
id3 = kd.new_dictid()
kd.from_py(py_dict, itemid=id3)

# Use from_dim to treat the first X Phthon
# lists as DataSlice dimensions
py_list = [[1, 2], [3], [4, 5]]
kd.from_py(py_list, from_dim=2)

py_dicts = [{'x': 1, 'y': 2.0}, {'x': 3, 'y': 4.0}]
# Dicts as Objects
kd.from_py(py_dicts, from_dim=1)
kd.from_py(py_dicts, from_dim=1, schema=s3)
```

</section>

<section>

### To_py

```py
# Primitive DataItem
kd.item(1.0).to_py() # 1.0
kd.int64(1).to_py() # 1
# There is no MASK type in Python
kd.present.to_py() # kd.present
kd.missing.to_py() # None

# DataSlice of primitives
ds = kd.slice([[1, 2], [3], [4, 5]])
py_list = kd.to_py(ds)
py_list = ds.to_py() # same as above
assert py_list == [[1, 2], [3], [4, 5]]

# Entity DataItem
e = kd.new(x=1, y=2.0)
e.to_py() # Obj(x=1, y=2.0)
e.to_py(obj_as_dict=True) # {'x': 1, 'y': 2.0}
s = kd.named_schema('Point', x=kd.INT32, y=kd.FLOAT32)
e1 = kd.new(x=1, schema=s)
e1.to_py(obj_as_dict=True) # {'x': 1, 'y': None}
e1.to_py(obj_as_dict=True,
         include_missing_attrs=False) # {'x': 1}

# Object DataItem
o = kd.obj(x=1, y=2.0)
o.to_py() # Obj(x=1, y=2.0)
o.to_py(obj_as_dict=True) # {'x': 1, 'y': 2.0}

# List DataItem
l = kd.list([1, 2, 3])
l.to_py() # [1, 2, 3]

# Dict DataItem
d = kd.dict({'a': 1, 'b': 2})
py_dict = d.to_py() # {'a': 1, 'b': 2}

# Nested Entity/Object/List/Dict item
# Each item is counted as one depth
# When max_depth (default to 2) is reached,
# the item is kept as a DataItem
i = kd.obj(a=kd.obj(b=kd.obj(c=1),
                    d=kd.list([2, 3]),
                    e=kd.dict({'f': 4})))
i.to_py()
# Obj(a=Obj(b=DataItem(Obj(c=1)),
#           d=DataItem(List[2, 3]),
#           e=DataItem(Dict{'f'=4})))
i.to_py(max_depth=3)
# Obj(a=Obj(b=Obj(c=1), d=[2, 3], e={'f': 4}))
# Use -1 to convert everything to Python
i.to_py(max_depth=-1)
# Obj(a=Obj(b=Obj(c=1), d=[2, 3], e={'f': 4}))

# Primitive DataSlice
ds = kd.slice([[1, 2], [None, 3]])
ds.to_py() # [[1, 2], [None, 3]]

# Entity DataSlice
ds = kd.new(a=kd.slice([1, None, 3]),
            y=kd.slice([4, 5, None]))
res = ds.to_py()
# [Obj(x=1, y=4),
#  Obj(x=None, y=5),
#  Obj(x=3, y=None)]
# Note that each object has its own Python class
assert res[0].__class__ != res[1].__class__

# Object DataSlice
ds = kd.obj(a=kd.slice([1, None, 3]),
            y=kd.slice([4, 5, None]))
res = ds.to_py()
# [Obj(x=1, y=4),
#  Obj(x=None, y=5),
#  Obj(x=3, y=None)]

# List DataSlice
lists = kd.implode(kd.slice([[1, 2], [None, 3]]))
lists.to_py() # [[1, 2], [None, 3]]

# Dict DataSlice
dicts = kd.dict(kd.slice([['a', 'b'], ['c']]),
                kd.slice([[1, None], [3]]))
dicts.to_py() # [{'a': 1}, {'c': 3}]
```

</section>

<section>

### From/To Pytree

`kd.from_pytree` is equivalent to `kd.from_py` and `kd.to_pytree` is equivalent
to `kd.to_py(obj_as_dict=True)`.

</section>

<section>

### From/To Numpy Array

```py
from koladata import kd_ext

npkd = kd_ext.npkd

arr1 = np.int32([1, 2, 0, 3])
npkd.from_array(arr1) # kd.int32([1, 2, 0, 3])

arr2 = np.int64([1, 2, 0, 3])
npkd.from_array(arr2) # kd.int64([1, 2, 0, 3])

arr3 = np.float64([1., 2., 0., 3.])
npkd.from_array(arr3) # kd.int64([1., 2., 0., 3.])

# Numpy does not support sparse array
# None is converted to nan
arr4 = np.float64([1., None, 0., 3.])
npkd.from_array(arr4) # kd.float64([1., nan, 0., 3.])

ds1 = kd.int32([1, 2, 3])
npkd.to_array(ds1) # np.int32([1, 2, 3])

ds2 = kd.int64([1, 2, 3])
npkd.to_array(ds2) # np.int64([1, 2, 3])

ds3 = kd.float32([1., 2., 3.])
npkd.to_array(ds3) # np.float32([1., 2., 3.])

# Numpy does not support sparse array
# Missing is converted to nan
ds4 = kd.float64([1., None, 3.])
npkd.to_array(ds4) # np.float64([1., nan, 3.])

# Numpy does not support multidimensional array
# Jagged array is flattened
ds5 = kd.slice([[1, 2], [3, 4]])
npkd.to_array(ds5) # np.array([1, 2, 3, 4])

# Numpy does not support mixed types
# All types are converted to strings
ds6 = kd.slice([1, 2.0, '3'])
npkd.to_array(ds6) # np.array(['1', '2.0', '3'])

# Entities/objects/lists/dicts are converted to
# DataItems and stored as objects in Numpy array
ds7 = kd.obj(x=kd.slice([1, 2, 3]))
arr = npkd.to_array(ds7)
# -> np.array([Obj(x=1), Obj(x=2), Obj(x=3)],
#             dtype=object)

# It does support round-trip conversion
ds8 = npkd.from_array(arr)
assert kd.full_equal(ds7, ds8)
```

</section>

<section>

### From/To Panda DataFrame

```py
from koladata import kd_ext

pdkd = kd_ext.pdkd

# Primitive conversion is almost the same as numpy
df1 = pd.DataFrame({'x': [1, 2, 3]})
pdkd.from_dataframe(df1)
# -> kd.new(x=kd.int64([1, 2, 3]))

# Convert to objects instead of entities
pdkd.from_dataframe(df1, as_obj=True)
# -> kd.obj(x=kd.int64([1, 2, 3]))

# Multi-dimension array is supported
index = pd.MultiIndex.from_arrays(
  [[0, 0, 1, 3, 3], [0, 1, 0, 0, 1]])
df2 = pd.DataFrame({'x': [1, 2, 3, 4, 5]}, index=index)
pdkd.from_dataframe(df2)
# -> kd.new(x=kd.int64([[1, 2], [3], [], [4, 5]]))

# Primitive DataSlice is converted to a column
# named 'self_'
ds1 = kd.slice([1, 2, 3])
pdkd.to_dataframe(ds1)
# -> pd.DataFrame({'self_': [1, 2, 3]})

ds2 = kd.slice([[1, 2], [3, 4]])
pdkd.to_dataframe(ds2) # Multi-dimensional DataFrame

# Entity attributes become columns
ds3 = kd.new(x=kd.slice([1, 2, 3]))
pdkd.to_dataframe(ds3)
# -> pd.DataFrame({'x': [1, 2, 3]})

# Union of object attributes become columns
ds4 = kd.slice([kd.obj(x=1), kd.obj(y=2),
                kd.obj(x=3, y=4)])
pdkd.to_dataframe(ds4)
# -> pd.DataFrame({'x': ['1', None, '3'],
#                  'y': [None, '2', '4']})

# Use 'cols' to specify the columns
# Can use attribute names or Exprs
ds5 = kd.new(x=kd.slice([1, 2, 3]),
             y=kd.slice([4, 5, 6]))
pdkd.to_dataframe(ds5, col=['x', I.y, I.x + I.y])
# -> pd.DataFrame({'x': [1, 2, 3],
#                  'S.y': [4, 5, 6],
#                  'S.x + S.y': [5, 7, 9]})
```

</section>

<section>

### From/To Proto

```proto
message Query {
  string query_text = 1;
  float final_ir = 2;
  repeated Doc docs = 3;
  repeated int32 tags = 4;
  map<string, float> term_weight = 5;
  proto2.bridge.MessageSet ms_extensions = 6;

  extensions 1000 to max
}

message QueryExtension {
  extend Query {
    QueryExtension query_extension = 1000;
  }

  extend proto2.bridge.MessageSet {
    QueryExtension ms_extension = 1000;
  }

  int32 extra = 1;
}

message Doc {
  string url = 1;
  string title = 2;
  float score = 3;
  int32 word_count = 4;
  bool spam = 5;

  enum Type {
    UNDEFINED = 0;
    WEB = 1;
    IMAGE = 2;
  }

  Type type = 6;
}
```

```py
d1 = Doc(
    url='url 1',
    title='title 1',
    word_count=10,
    spam=False,
    type=test_pb2.Doc.Type.WEB,
),
d2 = Doc(
    url='url 2',
    title='title 2',
    score=1.0,
    spam=True,
    type=test_pb2.Doc.Type.IMAGE,
)

q = Query(
    query_text='query 1',
    final_ir=1.0,
    tags=[1, 2, 3],
    term_weight={'a': 1.0, 'b': 2.0},
    docs=[d1, d2],
)

q.Extensions[QueryExtension.query_extension].extra = 1
q.ms_extensions.Extensions[QueryExtension.ms_extension].extra = 2
```

```py
# Create an entity from a proto
# Note only present fields are set
# and enums are converted to ints
i1 = kd.from_proto(d1)
# -> Entity(spam=False, title='title 1', type=1,
#           url='url 1', word_count=10)

# By default, the resulting schema is an
# uu schema derived from proto descriptor
i2 = kd.from_proto(d2)
assert i1.get_schema() == i2.get_schema()

# Create object instead of entity
kd.from_proto(d1, schema=kd.OBJECT)

# Use a custom schema
kd.from_proto(d1, schema=my_own_schema)

# Use provided itemid
kd.from_proto(d1, itemid=my_itemid)

# Create an entity DataSlice from a list of protos
is1 = kd.from_proto([d1, None, d2])
# -> DataSlice([Doc1, None, Doc2])

# Repeated fields are converted to lists
# Maps are converted to dicts
# Extensions are ignored by default
kd.from_proto(q)
# Entity(docs=List[Doc1, Doc2], final_ir=1.0,
#        query_text='query 1', tags=List[1, 2, 3],
#        term_weight=Dict{'a': 1.0, 'b': 2.0})

# Explicitly specify extensions to include
qi = kd.from_proto(
  q,
  extensions=[
    '(koladata.testing.QueryExtension.message_a_extension)',
    'ms_extensions.(koladata.testing.QueryExtension.ms_extension)',
  ],
)

# Convert to proto
kd.to_proto(i1, test_pb2.Doc) # d1
kd.to_proto(is1, test_pb2.Doc) # [d1, None, d2]
kd.to_proto(q, test_pb2.Query) # q
```

</section>

<section>

### From/To Json

WIP and will be available soon.

</section>

<section>

### From/To Bytes (a.k.a. Serialization)

```py
# Serialize DataSlice into bytes
s = kd.dumps(ds)
ds = kd.loads(s)

# Serialize DataBag into bytes
s = kd.dumps(db)
db = kd.loads(s)
```

</section>

## Bags of Attributes

<section>

### DataBag

```py
# Empty bag creation
db1 = kd.bag()

# Bag created directly is mutable
assert db1.is_mutable()

# Get the bag from the DataSlice
obj = kd.uuobj(x=1, y=2)
db2 = obj.get_bag()

# Bag created from item creation APIs
# under kd is immutable
assert not db2.is_mutable()

# Attach a bag to the DataSlice
obj1 = obj.with_bag(db1)

# Remove the bag from the DataSlice
obj2 = obj1.no_bag()

# Approximate number of triples in the bag
db2.get_approx_size()

# Print quick stats about the bag
repr(db2)

# Print out data and schema triples
db2.contents_repr()
# Print out data triples only
db2.data_triples_repr()
# Print out schema triples only
db2.schema_triples_repr()

# Create a new mutable bag by forking the bag
db3 = db2.fork()
assert db3.is_mutable()

# Create a new immutable bag by freezing the bag
db4 = db3.freeze()
assert not db4.is_mutable()
```

</section>

<section>

### Merging DataBags

```py
# Merge two bags by creating a new bag
# with db1 and db2 as fallbacks
# db2 overrides db1 in terms of conflicts
db1 << db2
kd.updated_bag(db1, db2) # Same as above

# Can be chained together
db1 << db2 << db3
kd.updated_bag(db1, db2, db3) # Same as above

# Merge two bags by creating a new bag
# with db2 and db1 as fallbacks
# db1 overrides db2 in terms of conflicts
db1 >> db2
kd.enriched_bag(db1, db2) # Same as above

# Can be chained together
db1 >> db2 >> db3
kd.enriched_bag(db1, db2, db3) # Same as above

# Merge db2 into db1 in place
# db2 overrides db1 in terms of conflicts
# schema conflicts are not allowed
db1.merge_inplace(db2)
# Merge db2 and db3 into db1 in place
db1.merge_inplace([db2, db3])
# Do not overwrite
db1.merge_inplace(db2, overwrite=False)
# Allow schema conflicts
db1.merge_inplace(db2, allow_schema_conflicts=True)
# Disallow data conflicts
db1.merge_inplace(db2, allow_data_conflicts=False)

# Merge db and its fallbacks into a single bag in place
# It improves lookup performance (e.g. accessing attrs,
# list items) but can be expensive to merge all bags
db3 = db.merge_fallbacks()
db3 = kd.with_merged_bag(db) # Same as above
```

</section>

<section>

### Extract/Clone

```py
s1 = kd.uu_schema(x=kd.INT32, y=kd.INT32)
s2 = kd.uu_schema(z=s1, w=s1)

i1 = s2(z=s1(x=1, y=2), w=s1(x=3, y=4))

# extract creates a copy of i1 in a new
# bag with the same ItemIds
i2 = i1.extract()
assert i2.get_itemid() == i1.get_itemid()
assert i2.z.get_itemid() == i1.z.get_itemid()

# Extract a subset of attributes
s3 = kd.uu_schema(w=s1)
i3 = i1.extract(schema=s3)
i3 = i1.set_schema(s3).extract() # same as above

assert not i3.has_attr('z')

# clone creates a copy of i1 in with a new ItemId
# and keep same ItemIds for attributes
i4 = i1.clone()
assert i4.get_itemid() != i1.get_itemid()
assert i4.z.get_itemid() == i1.z.get_itemid()
i4.z.get_attr_names(intersection=True) # ['x', 'y']

# shallow_clone creates a copy of i1 in with a new
# ItemId and keep same ItemIds for top-level attributes
i5 = i1.shallow_clone()
assert i5.get_itemid() != i1.get_itemid()
assert i5.z.get_itemid() == i1.z.get_itemid()
i5.z.get_attr_names(intersection=True) # []

# deep_clone creates a copy of i1 and its attributes
# with new ItemIds
i6 = i1.deep_clone()
assert i6.get_itemid() != i1.get_itemid()
assert i6.z.get_itemid() != i1.z.get_itemid()
i6.z.get_attr_names(intersection=True) # ['x', 'y']
```

</section>

<section>

### Stub/Ref

A Entity/Object/Dict stub is an empty Entity/Object/Dict with necessary metadata
needed to describe its schema in a new bag. A List stub is a list with list item
stubs in a new bag. It can be used to add new data to the *same* item in the
newly created bag which can merged into the original data.

A Entity/Object/List/Dict ref is a reference to the same Entity/Object/List/Dict
without a bag attached.

```py
obj = kd.obj(x=1, y=kd.obj(z=1))

# Create a obj stub without attributes
obj2 = obj.stub()
assert obj2.get_itemid() == obj.get_itemid()
assert not obj2.get_attr_names(intersection=False) # []

# Create a list stub with list item stubs
l = kd.list([kd.obj(x=1), kd.obj(x=2)])
l2 = l.stub()
assert l2.get_itemid() == l.get_itemid()
# List items are stubbed as well
assert kd.agg_all(l2[:] == l[:])
assert not l2[:].get_attr_names(intersection=False) # []

# Create a dict stub without entries
d = kd.dict({'a': kd.obj(x=1), 'b': kd.obj(x=2)})
d2 = d.stub()
assert d2.get_itemid() == d.get_itemid()
assert assert kd.dict_size(d2) == 0

# Get a ref
obj_ref = obj.ref()
assert obj_ref.get_itemid() == obj.get_itemid()
assert obj_ref.get_bag() is None
```

</section>

## Tracing (a.k.a JIT Compilation)

<section>

### Tracing

**Tracing** is a process of transforming computation logic represented by a
Python function to a computation graph represented by a Koda Functor. Tracing
provides a smooth transition from eager workflow useful for interactive
iteration to lazy workflow which can have better performance and path to
serving.

</section>

<section>

### Trace a Python Function

To trace a Python function, we can use `kd.trace_py_fn(f)`. Variadic arguments
are not supported for tracing.

```py
a1 = kd.slice([1, 2])
a2 = kd.slice([2, 3])
b = kd.item('b')
c = kd.item('c')

def f1(a, b, c):
  return kd.cond(kd.all(a > 1), b, c)

f1(a1, b, c) # 'c'
f1(a2, b, c) # 'b'

f = kd.trace_py_fn(f1)

f(a=a1, b=b, c=c) # 'c'
f(a=a2, b=b, c=c) # 'b'

def f2(a, b, c):
  return b if kd.all(a > 1) else c

f2(a1, b, c) # 'c'
f2(a2, b, c) # 'b'

# Fail because f2 use "if" statement and
# the tracer I.a Expr cannot be used in "if"
kd.trace_py_fn(f2)

# Use positional only and keyword only arguments
def f3(pos_only, /, pos_or_kw, *, kwonly):
  return kd.cond(kd.all(pos_only > 1), pos_or_kw, kwonly)

f3(a1, b, kwonly=c) # 'c'
f3(a2, pos_or_kw=b, kwonly=c) # 'b'

f = kd.trace_py_fn(f3)
f(a1, b, kwonly=c) # 'c'
f(a2, pos_or_kw=b, kwonly=c) # 'b'

# Variadic arguments are not supported for tracing
def f4(*args):
  pass

# Fails to trace
kd.trace_py_fn(f4)

def f5(**kwargs):
  pass

# Fails to trace
kd.trace_py_fn(f5)

# Variadic argument can be added to allow pass
# unused arguments
def f6(a2, a2, *unused):
  return a1 + a2

# 'c' is unused
kd.trace_py_fn(f6)(a1, a2, c)
```

</section>

<section>

### How tracing works?

There are three execution modes in Koda:

-   **Eager** (i.e. `kdi`): always execute eagerly
-   **Auto** (i.e. `kd`): execute eagerly in normal context or lazily in tracing
    mode
-   **Lazy** (i.e. `kde`): return Expr as result which can be executed lazily

`kdi` and `kd` are similar to `np` and `jnp` in JAX, correspondingly. There is
no pure lazy model (i.e. `kde`) in JAX as JAX does not directly expose lazy
operators.

To simplify the mental model and avoid unexpected surprises, tracing in Koda
works the same way as the just-in-time
([**JIT**](https://jax.readthedocs.io/en/latest/jit-compilation.html))
compilation in JAX. It involves three steps:

-   Functor signature is derived from Python function signature
-   The Python function is called with tracers (just replace `arg` with `I.arg`)
    and the result is a Koda Expr
-   A Functor is created from the Expr and function signature

It is different from TensorFlow 2 which analyzes the Python AST and is able to
trace Python control logic (e.g. `if`/`for`). Therefore, users should primarily
use `kd` operators in the Python function and try to avoid Python control logic
(e.g. `if`/`for`) and `kde/kdf` operators unless understanding what they
actually do.

In most cases, users only need `kd` for eager and tracing mode and `kde` for if
they want to use Expr directly. `kdi` is only useful for eager execution in the
tracing mode.

```py
def f1(a):
  b = kd.item(1) + kd.item(2)
  return kd.add(a, b)

f1(1)  # 4
# Returns Functor(return=I.a + 1 + 2)
kd.trace_py_fn(f1)
kd.trace_py_fn(f1)(a=1)  # 4

def f2(a):
  b = kdi.item(1) + kdi.item(2)
  return kd.add(a, b)

f2(1)  # 4
# Returns Functor(return=I.a + 3)
kd.trace_py_fn(f2)
kd.trace_py_fn(f2)(a=1)  # 4

# TODO: Support kde.slice/item
def f3(a):
  b = kde.item(1) + kde.item(2)
  return kd.add(a, b)

# Fails as kd.add does not work
# for Expr in normal context
f3(1)
# Returns Functor(return=I.a + 1 + 2)
kd.trace_py_fn(f3)
kd.trace_py_fn(f3)(a=1)  # 4
```

</section>

</section>

<section>

### Invoke Another Function in a Traced Function

If we want to trace the invoked function, need to add `@trace_as_fn` decorator
to that function. Otherwise, it is inlined into the traced function.

```py
def fn1(a, b):
  x = a + 1
  y = b + 2
  z = 3
  return x + y + z

def fn2(c):
  return fn1(c, 1)

# The content of fn1 is inlined
kd.trace_py_fn(fn2)
# Functor(returns=I.c + 1 + 3 + 3)

@kd.trace_as_fn()
def fn3(a, b):
  x = a + 1
  y = b + 2
  z = 3
  return x + y + z

def fn4(c):
  return fn1(c, 1)

# fn3 is traced into a separate functor
kd.trace_py_fn(fn4)
# Functor(returns=V.fn3(I.c, 1),
#         n3=Functor(returns=I.a + 1 + (I.b + 2) + 3)))
```

</section>

<section>

### Avoid tracing Some Operations

If some operators should always be executed eagerly, use `kdi` instead of `kd`.

```py
def f1(a):
  b = kdi.item(1) + kdi.item(2)
  return kd.add(a, b)

kd.trace_py_fn(f1)
# Functor(return=I.a + 3)
```

</section>

<section>

### Avoid tracing Inside a Python Function

If you want to treat a Python function as a whole and execute statements in the
function line by line, you can `@kd.trace_as_fn(py_fn=True)` decorator to the
Python function. It allows you to use any Python codes including `if`, `for` or
sending RPC which do not work for tracing at the cost of losing all tracing
benefits.

```py
a1 = kd.slice([1, 2])
a2 = kd.slice([2, 3])
b = kd.item('b')
c = kd.item('c')

@kd.trace_as_fn(py_fn=True)
def f1(a, b, c):
  return b if kd.all(a > 1) else c

f1(a1, b, c) # 'c'
f1(a2, b, c) # 'b'

traced_f1 = kd.trace_py_fn(f1)
traced_f1(a1, b, c) # 'c'
traced_f1(a2, b, c) # 'b'

# You can call f1 in another traced function
def f2(a1, a2, b, c):
  return f1(a1, b, c) + f1(a2, b, b)

traced_f2 = kd.trace_py_fn(f2)
traced_f2(a1, a2, b, c) # 'cb'
```

</section>

### Improve Structure/Readability of Resulting Functor

By default, names of local variables are lost during tracing. To preserve these
names, we can use `kd.with_name`. In the eager mode, `kd.with_name` is a no-op.
In the lazy mode, it is just `kde.with_name` which adds a name annotation to the
resulting Expr.

```py
def fn1(a, b):
  x = a + 1
  y = b + 2
  z = 3
  return x + y + z

kd.trace_py_fn(fn1)
# Functor(returns=I.a + 1 + (I.b + 2) + 3)

def fn2(a, b):
  x = kd.with_name(a + 1, 'x')
  y = kd.with_name(b + 2, 'y')
  z = 3
  return x + y + z

kd.trace_py_fn(fn2)
# Functor(returns=V.x + V.y + 3, x=I.a + 1, y=I.b + 2)
```

</section>

## Advanced: Expr

Most Koda users should need Exprs **rarely** and they should use tracing to
convert eager logic into computation graph. Expr can still be useful for
advanced users who want to express/manipulate computation logic (e.g.
ranking/score logic) directly.

A **Koda Expr** is a tree-like structure representing **computation logic**.
More precisely, it is a **DAG** (Directed Acyclic Graph) consisting of **input**
nodes, **literal** nodes, and **operator** nodes.

**Input nodes** are terminal nodes representing data (e.g. DataSlices) to be
provided at Expr **evaluation** time. In Koda, inputs are denoted as
`I.input_name`. `I.self.input_name` or `S.input_name` for short is a special
input node to allow using positional argument in `kd.eval`. See the **Evaluating
Koda Expr** section for details.

**Literal nodes** are terminal nodes representing data (e.g. DataSlices,
primitives) provided at Expr **creation** time.

**Operator nodes** are internal nodes representing an operation to be performed
on the values of sub-Exprs. For example, `I.a + I.b` returns an Expr
representing a `+` operation, with two input nodes as its children. Koda Expr
provides a comprehensive list of operators for basic operations under `kde`
module.

Tip: Almost all operators from `kd` module have their corresponding Expr version
in `kde` module. A few exceptions (e.g. `kd.eval(expr)`,
`kd.expr.pack_expr(expr)`, etc.) should be self-explanatory based on the
context.

<section>

### Useful Aliases

```py
kde = kd.lazy
I = kd.I
V = kd.V
S = kd.S
```

</section>

<section>

### Creating Koda Expr

```py
# Create an Expr to represent a + b
expr1 = kde.add(I.a, I.b)
# which is equivalent to
expr2 = I.a + I.b

# We can also use literals
expr3 = I.a + 1

# Build Expr by composing other Exprs
add_ab = I.a + I.b
weighted_ab = I.w * add_ab
score = kde.agg_sum(weighted_ab)

# Print out an Expr
score

# Use [] for list explosion or dict lookup
a = kd.slice([1, 2, 3])
kd.eval(kde.list(I.a)[:], a=a)
b = kd.dict({1: 2, 3: 4})
kd.eval(I.b[I.a], a=a, b=b)

# Use . for accessing attributes on objects
d = kd.obj(x=1, y=2)
kd.eval(I.d.x + I.d.y, d=d)

# Use () for functor calls
kd.eval(I.f(a=1, b=2), f=kd.fn(I.a + I.b))

# Check if it is an Expr
assert kd.is_expr(expr1)
```

</section>

<section>

### Evaluating Koda Expr

```py
expr = kde.add(I.a, I.b)
kd.eval(expr, a=kd.slice([1, 2, 3]), b=kd.item(2))

# which is equivalent to
expr.eval(a=kd.slice([1, 2, 3]), b=kd.item(2))

# inputs can be bound to the same DataSlice
x = kd.slice([1, 2, 3])
kd.eval(expr, a=x, b=x)

# use positional argument
(I.self * 2).eval(3)
(I.self.a * I.self.b).eval(kd.obj(a=1, b=2))

# Use S as shortcut for I.self
(S.a * S.b).eval(kd.obj(a=1, b=2))

# Mix positional and keyword arguments
(S.a * S.b * I.c).eval(kd.obj(a=1, b=2), c=3)
```

</section>

<section>

### Packing/Unpacking Koda Expr to/from DataItem

```py
add_ab = I.a + I.b
weighted_ab = I.w * add_ab
score = kde.agg_sum(weighted_ab)

# Pack Expr into a DataItem
packed_expr = kd.expr.pack_expr(score)

# Packed Expr is a DataItem with schema EXPR
assert not kd.is_expr(packed_expr)
assert kd.is_item(packed_expr)
assert packed_expr.get_schema() == kd.EXPR

# Creates a task object containing
# both data and expr
task = kd.obj(expr=expr, a=kd.list([1, 2, 3]),
              b=kd.list([4, 5, 6]),
              w=kd.list([0.1, 0.2, 0.3]))

# Unpack Expr
kd.eval(kd.expr.unpack_expr(task.expr),
        a=task.a[:], b=task.b[:],
        w=task.w[:])
```

</section>

<section>

### Substituting Sub-Exprs

```py
expr1 = I.a + I.b
expr2 = I.b + I.c
expr3 = expr1 * expr2

# Substitute by providing one sub pair
kd.expr.sub(expr3, I.a, I.d)
# which is equivalent to
expr3.sub(I.a, I.d)

# Substitute by providing multiple sub pairs
kd.expr.sub(expr3, (I.a, I.d), (I.b, I.c))

# Note the substitution is done by traversing
# expr post-order and comparing fingerprints
# of sub-Exprs in the original expression and
# these in sub pairs
kd.expr.sub(I.x + I.y,
           (I.x, I.z), (I.x + I.y, I.k))
# returns I.k

kd.expr.sub(I.x + I.y,
           (I.x, I.y), (I.y + I.y, I.z))
# returns I.y + I.y
```

</section>

<section>

### Substituting Input Nodes

```py
expr = (I.a + I.b) * I.c

# Substitute by other inputs
new_expr = kd.expr.sub_inputs(expr, b=I.c, c=I.a)
# which is equivalent to
new_expr = expr.sub_inputs(b=I.c, c=I.a)

# Substitute by Exprs
new_expr = kd.expr.sub_inputs(expr, c=I.a + I.b)

# Substitute by literals
new_expr = kd.expr.sub_inputs(expr, b=kd.slice([2, 4]))
```

</section>

### Defining Custom Operators

```py
# Create a lambda operator 'score'
# under the existing namespace 'kde.core'
@kd.optools.add_to_registry()
@kd.optools.as_lambda_operator('kde.core.score')
def score(a, b, w):
  return kde.agg_sum(w * (a + b))

kd.eval(kde.core.score(I.a, I.b, I.w),
        a=a, b=b, w=w)

# It can be used as eager op too
kd.core.score(a, b, w)

# We can also add the operator to a
# different namespace 'E'
@kd.optools.add_to_registry()
@kd.optools.as_lambda_operator('E.my_func')
def my_func(a):
  return kde.add(a, a)

# Create the operator container 'E'
E = x = kd.optools.make_operators_container('E').E
kd.eval(E.my_func(I.a), a=1)

expect_data_slice =
    kd.optools.constraints.expect_data_slice

# Create an operator based on Py function
@kd.optools.add_to_registry()
@kd.optools.as_py_function_operator(
    'kde.core.fn',
    qtype_constraints=[
        expect_data_slice(arolla.P.a),
        expect_data_slice(arolla.P.pos)
    ],
    qtype_inference_expr=kd.qtypes.DATA_SLICE,
)
def fn(a, pos):
  return kd.slice(a.S[pos].to_py() + 1)

kd.eval(kde.core.fn(kd.slice([1, 2, 3]), 1))

# Register operator alias so that
# kde.add is same as E.add
kd.optools.add_alias('kde.add', 'E.Add')
```

</section>

## Advanced: Functor

Most Koda users should need to create Functors **rarely** and they should use
tracing to convert eager logic into Functors.

A Koda Functor is a special **DataItem** containing a single item representing a
**callable function**. It can be **called/run** using `kd.call(fn, **inputs)` or
simply `fn(**inputs)`.

<section>

### Creating Koda Functor

Koda Functors can be **created** from the following objects:

-   **Koda Expr** using `kd.functor.expr_fn(expr, **vars)`
-   **Format string** similar to Python f-string `f'text {input:s} text'` using
    `kd.functor.fstr_fn(str_fmt, **vars)`
-   **Python function** using `kd.functor.py_fn(pfn, **vars)`

`kd.fn(fn_obj, **vars)` is the **universal adaptor** for `kd.functor.expr_fn`,
and `kd.functor.py_fn` where `fn_obj` can be `expr` or `py_fn`.

**Conceptually**, creating a functor is equivalent to declaring a Python
function where the function signature consists of a function name and parameters
and the function body consists of local variable assignments and a return
statement.

-   Koda functor does not have a functor name, which is similar to a Python
    lambda function
-   Koda functor parameters are not explicitly declared but automatically
    derived from `fn_obj` and `**vars`
-   Local variables are notated as `V.var_name`, variable assignments are done
    through `**vars` in `kd.fn` and a variable can refer to other variables
-   Return statement is represented as `fn_obj` in `kd.fn` and can refer to
    local variables

```py
# Create a Functor from Expr
fn1 = kd.functor.expr_fn(I.a + I.b)

# Check if it is a Functor
assert kd.is_fn(fn1)

# Create a Functor with local variables
fn2 = kd.functor.expr_fn(V.a + I.b, c=I.d, a=V.c + I.d)

# Create a Functor from format string
fn3 = kd.functor.fstr_fn(
  f'a:{I.a:s} + b:{I.b:s} = {(I.a + I.b):s}')

# Create a Functor with local variables
fn4 = kd.functor.fstr_fn(
  f'{I.s.name:s} ({V.names:s})\n',
  names=kde.strings.agg_join(I.s.cities[:].name, ', '))
fn5 = kd.functor.fstr_fn(f'A: {V.fn(s=I.s1):s}'
                         f'B: {V.fn(s=I.s2):s}',
                         fn=fn4)

# Create a Functor from Py function
fn6 = kd.functor.py_fn(
  lambda a, b: a.get_size() + b.get_size())

# With default value for 'y'
fn7 = kd.functor.py_fn(lambda x, y: x + y, y=1)

# Use the universal adapter kd.fn
fn8 = kd.fn(I.a + I.b)
fn9 = kd.fn(lambda x, y: x + y)
fn10 = kd.fn(fn8)

# Create a functor calling another functor
fn11 = kd.fn(kde.call(fn1, a=I.c, b=I.d))
fn12 = kd.functor.fstr_fn(
  f'result {V.f(a=I.c, b=I.d):s}', f=fn1)
```

</section>

<section>

### Calling Koda Functor

```py
f = kd.fn(V.a + I.b, c=I.d, a=V.c + I.d)

# Pass inputs as **kwargs
# Preferred when input names are static
f(b=2, d=3)
kd.call(f, b=2, d=3) # Same as above

# Call a functor from another functor
f = kd.fn(I.x + I.y)
g = kd.fn(V.nf(x=I.x, y=2), nf=f)
g(x=1)  # 3

# Alternative
g = kd.fn(kde.call(V.nf, x=I.x, y=2), nf=f)
g(x=1)  # 3

# Functors can also be called using `fstr_fn`
f = kd.fn(I.x + I.y)
g = kd.functor.fstr_fn(f'result: {I.f(x=I.x, y=V.y):s}',
                y=1)
g(f=f, x=1)
```

</section>

<section>

### Partially Binding Koda Functor Parameters

```py
fn1 = kd.fn(I.a + I.b)

# Bind inputs with default values
fn2 = kd.functor.bind(fn1, a=1)

fn2(a=3, b=2)  # 5
fn2(b=2)  # 3

# Exprs and format strings can also be bound to
# arguments.
fn3 = kd.functor.bind(fn1, a=I.b + 1)
fn3(b=2)  # 5

# Binding DataItem params also works.
fn4 = kd.functor.bind(fn1, a=kd.item(1))

# Raise because binding a DataSlice is not supported
fn5 = kd.functor.bind(fn1, a=kd.slice([1, 2]))
# We have to use kd.list instead
fn5 = kd.functor.bind(fn1, a=kd.list([1, 2]))
```

</section>

<section>

### map_py_fn Functor

Also see `kd.map_py()` above.

```py
# Pointwise
kd.functor.map_py_fn(lambda x, y: x + y)

# Aggregation
kd.functor.map_py_fn(lambda x: len(x), ndim=1)

# Aggregaional but no dimension change
kd.functor.map_py_fn(lambda x: sorted(x), ndim=1)

# Expansion
kd.functor.map_py_fn(lambda x: [x] * 10)
```

</section>

## Advanced: Mutable Workflow

While it is enough for most Koda users to just use immutable workflow, it is
possible to use mutable workflow by managing DataBags manually in order to
achieve better performance.

Note code using mutable APIs cannot be traced and has more limited options for
productionalization.

<section>

### Creating Mutable Entities/Objects/Lists/Dicts from a DataBag

```py
db = kd.bag()

# Create a mutable entity and
# modify its attributes
e = db.new(x=1, y=2)
e.x = 3
e.z = 'a'
del e.y

# Create a mutable object and
# modify its attributes
o = db.new(x=1, y=2)
o.x = 'a'
o.z = 3
del o.y

# Create a mutable list and
# modify its items
l = db.list([1, 2, 3])
l[1] = 20
l.append(4)
l.append(kd.slice([5, 6]))
del l[:4]

# Create a mutable dict and
# modify its entries
d = db.dict({'a': 1, 'b': 2})
d['a'] = 20
d['c'] = 30
del d['b']

# Other APIs works similarly
db.new_like(...)
db.new_shaped(...)
db.new_shaped_as(...)
db.implode(ds)
```

</section>

<section>

### Forking and Freezing DataBags

An immutable DataBag can be forked to create a mutable DataBag at a cost of
`O(1)`. Similarly, a mutable DataBag can be frozen to create an immutable
DataBag at a cost of `O(1)`.

```py
e = kd.new(x=1, y=2)
e1 = e.fork_bag()
e1.x = 3
e2 = e1.fork_bag()
e2.x = 4
e.x # 1
e1.x # 3
e2.x # 4

o = db.new(x=1, y=2)
o1 = o.fork_bag()
o1.x = 'a'
o2 = o1.freeze_bag() # o2 is immutable

l = db.list([1, 2, 3])
l1 = l.fork_bag()
l1.append(4)

d = db.dict({'a': 1, 'b': 2})
d1 = d.fork_bag()
d['a'] = 20
```

</section>

<section>

### Moving Data from a DataSlice/DataBag to Another DataBag

```py
db1 = kd.bag()
db2 = kd.bag()

o1 = db1.new(x=1)

o23 = db2.new(x=kd.slice([2, 3]))
o4 = db2.new(x=4)

# Move entire db2 to db1
db1.merge_inplace(db2)

# Move o1 into db1
o1_in_db1 = db1.adopt(o1)
# Equivalent to the below
db1.merge_inplace(o1.extract())
o1_in_db1 = o1.with_db(db1)

# Adopt avoids multiple extractions
# The code below has two extractions
o5 = db1.obj(y=o1)
o6 = db1.obj(y=o1)
# The code below has one extraction
o1_in_db1 = db1.adopt(o1)
o5 = db1.obj(y=o1_in_db1)
o6 = db1.obj(y=o1_in_db1)
```

</section>

## Unit Testing

<section>

### kd.testing.assert_equal

```py
# Primitive DataSlices
ds1 = kd.slice([1, 2, 3])
ds2 = kd.slice([1, 2, 3])
kd.testing.assert_equal(ds1, ds2)

# Mixed primitive DataSlices
ds3 = kd.slice([1, 2., '3'])
ds4 = kd.slice([1, 2., '3'])
kd.testing.assert_equal(ds3, ds4)

# It compares schemas too for DataSlices
ds5 = kd.slice([1, 2, 3])
ds6 = kd.slice([1, 2, 3], kd.INT64)
kd.testing.assert_equal(ds5, ds6) # Fail
kd.testing.assert_equal(ds5, kd.to_int32(ds6))

# It compares DataBags too for DataSlices
ds7 = kd.uuobj(x=1)
ds8 = kd.uuobj(x=1)
kd.testing.assert_equal(ds7, ds8) # Fail
kd.testing.assert_equal(ds7.no_bag(), ds8.no_bag())

# It works for JaggedShapes
kd.testing.assert_equal(ds1.get_shape(),
                        ds2.get_shape())
kd.testing.assert_equal(ds1.get_shape(),
                        ds8.get_shape()) # Fail

# It works for DataBags and it checks DataBags
# are the same instance
db1 = kd.bag()
db2 = kd.bag()
kd.testing.assert_equal(db1, db2) # Fail
kd.testing.assert_equal(db1, db1)
```

</section>

<section>

### kd.testing.assert_equivalent

```py
# Different from assert_equal, it check DataBags
# are equivalent instead of the same instance
ds1 = kd.uuobj(x=1)
ds2 = kd.uuobj(x=1)
kd.testing.assert_equivalent(ds1, ds2)

ds3 = kd.uuobj(x=kd.slice([1, 2]))
# Fail as ds3.db has more contents than ds1.db
kd.testing.assert_equivalent(ds1, ds3.S[0])

# It works for DataBags too
kd.testing.assert_equivalent(ds1.get_bag(),
                             ds2.get_bag())

# DataBag comparison is tricky so it is not
# recommended to compare theirs contents directly
```

</section>

<section>

### kd.testing.assert_allclose

It works similar to `numpy.testing.assert_allclose`.

```py
ds1 = kd.slice([2.71, 2.71])
ds2 = kd.slice([2.7100, 2.710])

kd.testing.assert_allclose(ds1, ds2)

ds3 = kd.float32(3.145678)
ds4 = kd.float32(3.144)

kd.testing.assert_allclose(ds3, ds4) # Fail
kd.testing.assert_allclose(ds3, ds4, atol=0.01)
kd.testing.assert_allclose(ds3, ds4, rtol=0.01)

# Note it compares schemas too
ds5 = kd.float3264(3.145678)
kd.testing.assert_allclose(ds3, ds5) # Fail
```

</section>

<section>

### kd.testing.assert_unordered_equal

```py
ds1 = kd.slice([1, 2., '3'])
ds2 = kd.slice(['3', 2., 1])

kd.testing.assert_unordered_equal(ds1, ds2)

# Ignore the order of items in the last dimension
ds3 = kd.slice([[1, 2], [None, 3]])
ds4 = kd.slice([[2, 1], [3, None]])
kd.testing.assert_unordered_equal(ds3, ds4)

ds5 = kd.slice([[1, 3], [None, 2]])
ds6 = kd.slice([[2, 1], [3, None]])
kd.testing.assert_unordered_equal(ds5, ds6) # Fail
```

</section>

<section>

### Comparing Lists

Note: the following APIs only compares the contents of nested lists by
primitiveis or ItemIds. That is, they do not require these lists to have the
same ItemIds or check the nested contents of lists' contents.

```py
l1 = kd.list([1, 2, 3])
l2 = kd.list([1, 2, 3])
kd.testing.assert_nested_lists_equal(l1, l2)

l3 = kd.list([kd.uuobj(x=1), kd.uuobj(x=2)])
l4 = kd.list([kd.uuobj(x=1), kd.uuobj(x=2)])
kd.testing.assert_nested_lists_equal(l3, l4)

l5 = kd.list([kd.obj(x=1), kd.obj(x=2)])
l6 = kd.list([kd.obj(x=1), kd.obj(x=3)])
kd.testing.assert_nested_lists_equal(l5, l6) # Fail

# Nested lists
l7 = kd.list([[1, 2, 3],
              [kd.uuobj(x=1), kd.uuobj(x=2)]])
l8 = kd.list([[1, 2, 3],
              [kd.uuobj(x=1), kd.uuobj(x=2)]])
kd.testing.assert_nested_lists_equal(l7, l8)
```

</section>

<section>

### Comparing Dicts

Note: the following APIs only compares the contents of dicts by primitiveis or
ItemIds. That is, they do not require these dicts to have the same ItemIds or
check the nested contents of dicts' contents.

```py
d1 = kd.dict({'a': 1, 'b': 2})
d2 = kd.dict({'a': 1, 'b': 2})
kd.testing.assert_dicts_equal(d1, d2)

d3 = kd.dict({'a': kd.uuobj(x=1)})
d4 = kd.dict({'a': kd.uuobj(x=1)})
kd.testing.assert_dicts_equal(d3, d4)

d5 = kd.dict({'a': kd.obj(x=1)})
d6 = kd.dict({'a': kd.obj(x=2)})
kd.testing.assert_dicts_equal(d5, d6) # Fail

# Only check keys/values
kd.testing.assert_dicts_keys_equal(
  d1, kd.slice(['b', 'a']))
kd.testing.assert_dicts_values_equal(
  d1, kd.slice([2, 1]))
```

</section>

<section>

### Comparing Complex Objects

Right now, Koda does not provide a way to compare complex objects by
**contents** ignoring ItemIds, DataBags and schemas. The current recommendation
is to convert to pytrees and use Python comparison assertions.

```py
i1 = kd.obj(a=kd.obj(b=kd.obj(c=1),
                    d=kd.list([2, 3]),
                    e=kd.dict({'f': 4})))
i2 = kd.obj(a=kd.obj(b=kd.obj(c=1),
                    d=kd.list([2, 3]),
                    e=kd.dict({'f': 4})))

assert i1.to_pytree(max_depth=-1)
  == i2.to_pytree(max_depth=-1)
```

</section>
