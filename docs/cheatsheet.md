<!--* css: "//koladata/g3doc/cheatsheet.css" *-->
<!-- For the best formatting, please limit the line_max_char to 50. -->

# Koda Cheatsheet
## Basic API

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
ds.L[1].L[2]
# Python list of one-dim DataSlice
ds.L[:]
# Items at idx in the first dims
ds.L[idx]

# Subslice
ds.S[1, 2]
ds.S[2]  # Take 2nd item in the last dimension
ds.S[1:, :2]

# Expand the outermost dimension into a list
ds.to_pylist()

ds.to_py()
ds.to_str()

# DataItem is 0-dim DataSlice
i = kd.slice(1) # same as kd.item(1)
i.get_ndim() # 0
```

</section>

<section>

### Entities

```py
# Entity creation with named schema
e = kd.new(x=1, y=2, schema='Point')
es = kd.new(x=kd.slice([1, 2, None]),
            y=kd.slice([4, None, 6]),
            schema='Point')

e.get_schema()
assert e.get_schema() == es.get_schema()

# Use an existing schema
s = kd.new_schema(x=kd.INT32, y=kd.INT32)
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

# Access attribute
e.x # 1
e.get_attr('y') # 2
e.maybe('z') # None
e.get_attr('z', default=0) # 0
es.get_attr('x', default=0) # [1, 2, 0]

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
```

</section>

<section>

### Objects

```py
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

# Access attribute
o.x # 1
o.get_attr('y') # 2
o.maybe('z') # None
o.get_attr('z', default=0) # 0
os.get_attr('x', default=0) # [1, 0, 'a']

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
s1 = kd.new_schema(x=kd.INT32, y=kd.FLOAT64)
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
# Dicts as Objects
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
s = kd.new_schema(x=kd.INT32, y=kd.FLOAT32)
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
lists = kd.list(kd.slice([[1, 2], [None, 3]]))
lists.to_py() # [[1, 2], [None, 3]]

# Dict DataSlice
dicts = kd.dict(kd.slice([['a', 'b'], ['c']]),
                kd.slice([[1, None], [3]]))
dicts.to_py() # [{'a': 1}, {'c': 3}]
```

</section>

<section>

### Editable containers

```py
x = kd.container()
x.a = 1
x.d = kd.container()
x.d.e = 4
x.d.f = kd.list([1, 2, 3])
```

</section>

### Schemas

```py
kd.new_schema(x=kd.INT32)

kd.slice([1, 2, 3]).get_schema()
kd.slice([1, 2, 3]).get_dtype()

# Schema can be set and created explicitly.
kd.slice([None, None]).with_schema(kd.INT32)
kd.named_schema('Point', x=kd.INT32, y=kd.INT32)

# Universally-unique schema.
kd.uu_schema(x=kd.INT32)

# For lists.
ds.get_item_schema()
# For dicts.
ds.get_key_schema()
ds.get_value_schema()

# Returns DataSlice of schemas for
# Objects and primitives in argument.
kd.obj(x=1, y=2).get_obj_schema()
```

</section>

<section>

### Changing shape of a DataSlice

```py
# Flatten to one dimension.
ds.flatten()
# Remove the last dimension.
ds.flatten(-2)
# Remove the first dimension.
ds.flatten(0, 2)

# Reshape to the shape of another DataSlice.
ds.reshape_as(ds1)
ds.reshape(ds1.get_shape())

# Repeats values of ds.
ds.repeat(3)
# Repeats present values of ds.
ds.repeat_present(3)

# Stacks the DataSlices,
# creating a new dimension at index rank-ndim.
kd.stack(ds, ds+1)
kd.stack(ds, ds, ndim=1)
# Zip DataSlices over the last ndim dimension.
kd.zip(ds, ds*10)

# Concats the DataSlices on dimension rank-ndim.
kd.concat(ds1, ds2)
```

</section>

<section>

### Broadcasting and aligning

```py
# Expands x based on the shape of target.
kd.slice([100, 200]).expand_to(
    kd.slice([[1,2],[3,4,5]]))
# Implodes last ndim dimensions into lists,
# expands the slice of lists, explodes the result.
kd.slice([100, 200]).expand_to(
    kd.slice([[1,2],[3,4,5]]), ndim=1)

kd.is_expandable_to(x, target, ndim)
# Whether any of args is expandable to the other.
kd.is_shape_compatible(x, y)

# Expands DataSlices to the same common shape.
kd.align(kd.slice([[1, 2, 3], [4, 5]]),
         kd.slice('a'), kd.slice([1, 2]))
```

</section>

<section>

### Aggregational operators

```py
# Collapse over the last dimension and
# return the value if all items has the
# same value or None otherwise.
kd.collapse(kd.slice([[1],[2,2],[2,3,4]]))

 # Aggregates over the last dimension.
kd.agg_all(x)
kd.agg_any(x)
kd.agg_has(x)

# Counts both present and missing items.
kd.agg_size(x)
# Counts present items only.
kd.agg_count(x)
kd.agg_count(x, ndim=2)

# Aggregate across all dimensions.
kd.all(x)
kd.any(x)
kd.size(x)
kd.count(x)

# Adds dimension after grouping items.
kd.group_by(kd.slice([4,3,4,2,2,1,4,1,2]))

# Returns indices starting from 0.
# Aggregates over the last dimension.
kd.index(x)
# Aggregates over the last 3 dimensions
kd.index(x, dim=3)
# Create a new slice with items at indices.
x.take(indices)

# Returns the rank of x.
kd.get_ndim(x)
```

</section>

<section>

### Other DataSlice manipulations

```py
# Slice of INT64s with range [start, end).
kd.range(1, 5)
kd.range(0, kd.slice([3,2,1]))

# Unique values within each dimension.
kd.unique(ds, sort=False)

# Reverse on the last dimension.
kd.reverse(ds)

# Map keys to values using k=>v mapping.
kd.translate(keys_to, keys_from, values_from)
kd.translate_group(
  keys_to, keys_from, values_from)
```

</section>

<section>

### Explosion and implosion

```py
a = kd.list([[[1, 2], [3]], [[4], [5]]])
# Explodes the list ndim times.
kd.explode(a, ndim=2)
# Explode until there are no more lists.
kd.explode(a, ndim=-1)

a = kd.slice([[[1, 2], [3]], [[4], [5]]])
# Implodes the list ndim times.
kd.implode(a, ndim=2)
# Implode until the result is a List DataItem.
kd.implode(a, ndim=-1)
```

</section>

<section>

### Comparison operators
```py
# Returns masks.
kd.greater(ds1, ds2)
kd.less_equal(ds1, ds2)
kd.equal(ds1, ds2)

# Infix operators
ds1 > ds2
ds1 <= ds2
ds1 == ds2

# Compare with Python objects.
a > 1
a < [3, 2, 1]
```

</section>

<section>

### Sparsity and logical operators

```py
kd.has(a)
kd.agg_has(a)
kd.has_not(a)

kd.apply_mask(a, a > 1)
a & (a > 1)  # Same

~a  # Invert mask.

kd.coalesce(a, b)
a | b # Same

kd.cond(c > 1, a, b)

kd.masking.mask_and(a, b)
kd.masking.mask_or(a, b)
kd.masking.mask_equal(a, b)
kd.masking.mask_not_equal(a, b)
```

</section>

<section>

### Filtering operators

```py
# Filter out missing items in filter.
x.select(x >= 3)
# Remove missing items.
x.select_present()

# Put items in present positions in filter.
kd.inverse_select(
    kd.slice([1, 2]),
    kd.slice([kd.missing, kd.present, kd.present])
)
```

</section>

<section>

### Math operators

```py
x + y  # Short for kd.add(x, y)
x - y  # Short for kd.subtract(x, y)
x * y  # Short for kd.multiple(x, y)
x / y  # Short for kd.divide(x, y)
x % y  # Short for kd.mod(x, y)

kd.math.abs(x)
kd.math.pow(x, y)
kd.math.exp(x)
kd.math.log(x)
kd.math.log10(x)
kd.math.round(x)
kd.math.ceil(x)
kd.math.floor(x)
kd.math.floordiv(x, y)
kd.math.maximum(x, y)
kd.math.mimimum(x, y)

kd.math.agg_max(x)
kd.math.agg_min(x)
kd.math.agg_median(x)
kd.math.agg_mean(x)
kd.math.agg_sum(x)
kd.math.agg_count(x)
kd.math.agg_std(x)
kd.math.agg_var(x)
kd.math.min(x)
kd.math.max(x)
kd.math.sum(x)
kd.math.count(x)

kd.math.cum_min(x)
kd.math.cum_max(x)
kd.math.cum_sum(x)
kd.math.cum_count(x)

kd.math.cdf(x)
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
kd.to_text(kd.slice([1, 2, 3])
kd.to_bytes(kd.slice([b'1', b'2', b'3'])

# Dispatches to the relevant kd.to_* operator
# or casts into specific schema.
kd.cast_to(kd.slice([1, 2, 3]), kd.INT64)
kd.cast_to(kd.new(x=1), kd.new_schema(x=kd.INT32))
```

</section>

<section>

### Random number and sampling operators

```py
# Random integers.
kd.randint_like(x)
kd.randint_like(x, 10)
kd.randint_like(x, -5, 200)

kd.randint_shaped_as(ds)

# Sample from the last dimension.
kd.sample(ds, ratio=0.7, seed=42)
# Use key to make item selection robust.
kd.sample(x, ratio=0.7, seed=42, key=kd.index(x))
# Select n items from last dimension.
kd.sample_n(ds, n, seed)
```

</section>

<section>

### Sort and rank operators

```py
# Sort over the last dimension.
kd.sort(ds)

# Equal items get the same rank,
# no gap between rank numbers.
kd.dense_rank(ds)
# Equal items get distinct ranks based on
# tie breaker.
kd.ordinal_rank(ds), tie_breaker=-kd.index(x))
```

</section>

<section>

### String operators

```py
kd.strings.length(x)
kd.strings.fstr(f'${ds:s}')
kd.strings.printf("%d-%s", kd.index(ds), ds)
kd.strings.format("{index}-{val}",
                  index=kd.index(ds), val=ds)

kd.strings.split(kd.slice(["a,b", "c,d"]), ',')
kd.strings.agg_join(
    kd.slice([['a', 'b'], ['c', 'd']]), ',')

kd.strings.regex_match(ds, '^(.*):')
kd.strings.regex_extract(ds, '^a')

kd.strings.contains(ds, 'a')
kd.strings.count(ds, 'a')
kd.strings.find(ds, 'a')
kd.strings.rfind(ds, 'a')
kd.strings.join(ds, ds)
ks.strings.length(ds)
kd.strings.lower(ds)
kd.strings.upper(ds)
kd.strings.strip(ds, 'a')
kd.strings.lstrip(ds, 'a')
kd.strings.rstrip(ds, 'a')
kd.strings.replace(ds, 'a', 'A')
kd.strings.substr(ds, 1, 3)

kd.encode(ds)  # Encode as bytes.
kd.bytes.length(ds)

kd.strings.decode(x)  # Decode as text.
kd.strings.encode(x)  # Encode as bytes.
```

</section>

<section>

### Item creation operators

```py
# *_like operators follow sparsity.
# *_shaped_as follow just shape.

# Const value.
kd.val_like(x, 1)
kd.val_shaped_as(x, 1)
kd.val_shaped(s, 1)

# Mask.
kd.present_like(x)
kd.present_shaped_as(x)
kd.present_shaped(s)

kd.empty_shaped_as(x)
kd.empty_shaped(s)

# Entity.
kd.new_like(x)
kd.new_shaped_as(x)
kd.new_shaped(s)

# Object.
kd.obj_like(x)
kd.obj_shaped_as(x)
kd.obj_shaped(s)

kd.list_like(x)
kd.list_shaped_as(x)
kd.list_shaped(s)

kd.dict_like(x)
kd.dict_shaped_as(x)
kd.dict_shaped(s)
```

</section>

<section>

### IDs and UUIDs

```py
# Dtype for ItemId.
kd.ITEMID

# Get unique id.
kd.obj(x=1, y=2).get_itemid()

# Encode to/from base-62 number (as string).
kd.encode_itemid(kd.obj(x=1, y=2))
kd.decode_itemid(str_id)
# int64 hash values.
kd.hash_itemid(kd.uuid(a=1, b=2))

# Compute deterministic id from attr values.
kd.uuid(x=1, y=2)
# Traverse attrs instead of using their UUIDs.
kd.deep_uuid(obj)

# Compute uuid for multiple items.
kd.agg_uuid(kd.slice([[1,2,3], [4,5,6]]))

# Create entity with UUID.
kd.uu(x=1, y=2)
# Create object with UUID.
kd.uuobj(x=1, y=2)
```

</section>

<section>

### Serialization and protos

```py
# Serialize slices into bytes.
s = kd.dumps(ds)
ds = kd.loads(s)

# Serialize DataBag into bytes.
s = kd.dumps(db)
db = kd.loads(s)

# Serialize to proto.
kd.to_proto(obj, proto.db.MyMessage)
kd.from_proto(protos)
```

</section>

<section>

### Using Python functions

```py
# Apply a Python function to slices.
kd.apply_py(lambda x,y: x+y, a, b)
kd.apply_py_on_cond(lambda x,y: x+y, a > 2, a, b)
kd.apply_py_on_selected(lambda x,y: x+y,
                        a > 2, a, b)

# Apply to individual items.
kd.map_py(lambda x, y: x*y, ds, 2, max_threads=4)
# Can change number of dimensions.
kd.map_py(
    lambda x: print(x) if x else None, ds, ndim=2)
# Schema passed to handle empty inputs.
kd.map_py_on_present(
    lambda x: x.upper(), s, schema=kd.STRING)
```

</section>

<section>

### Interoperability

<!-- TODO: Update names in this section. -->

```py
# Pandas
from koladata.ext import pdkd
pdkd.to_dataframe(kd.obj(x=ds1, y=ds2),
                  cols=['x', 'y'])
pdkd.from_dataframe(
    pd.DataFrame(dict(x=[1,2,3], y=[10,20,30])))

# Numpy
from koladata.ext import npkd
npkd.to_array(ds)
npkd.from_array(np.array([1, 2, 0, 3]))
```

</section>

<section>

### Bags of attributes

```py
# DataBag creation.
db = kd.bag()  # Empty.
db = x.attrs(a=1, b=2)
# Get DataBag corresponding to entity or object.
db = x.get_bag()

# Explore DataBag contents.
db.get_size()
db.contents_repr()
db.data_triples_repr()
db.schema_triples_repr()

# Merge two bags.
kd.updated_bag(kd.attrs(x, a=1), kd.attrs(x, b=3))
kd.attrs(x, a=1) << kd.attrs(x, b=3)  # Same.
# Keeps attributes of the first bag.
kd.enriched_bag(
    kd.attrs(x, a=1), kd.attrs(x, a=2, b=3))
```

</section>

<section>

### Extract and clone

```py
# Copy with a bag containing
# only attrs accessible from x.
x.extract()
# Empty copy containing only schema.
x.stub()

# Non-recursive copy in a new bag.
x.clone()
# Recursive copy in a new bag.
x.deep_clone()
# Top-level attributes are copied by reference.
x.shallow_clone()
```

</section>

<section>

### Tracing and functors

```py
# Create functor.
kd.fn(lambda x: x + 1)
# Functor with no tracing.
kd.py_fn(lambda x: x + 1)

# Check if obj represents a functor.
kd.is_fn(obj)

# Avoid inlining Python function during tracing.
@kd.trace_as_fn()
```

</section>
