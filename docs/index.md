# Koda

* TOC
{:toc}

## Koda

Koda is a **Python + C++ library** that provides a **data representation** and
**domain-specific language** (a.k.a DSL) for **vectorized** data transformations
and computations on the transformed data with **high performance**. The data can
be in the form of protos, tables, interconnected objects, graphs, tensors and
more.

Such transformations and computations can be designed in an **interactive** REPL
(e.g. Colab), as well compiled into **computation graphs** and **served** in
production (i.e. C++ environments).

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

## Code Examples

Here are code examples demonstrating some of Koda capabilities.

### 1. Handling Irregular Data Shapes

Koda supports irregular multi-dimensional arrays (jagged arrays) without
padding.

```py
>>> from koladata import kd
>>> scores = kd.slice([[85, 90], [70], [95, 100, 88]])
>>> kd.agg_max(scores)
DataSlice([90, 70, 100], schema: INT32, ...)
```

### 2. Vectorized Operations on Complex, Nested Data

Koda provides C++ grade performance for vectorized operations on nested data
structures and jagged arrays.

```py
>>> users = kd.slice([
...     kd.obj(name='Alice', age=20, interests=kd.list(['tennis', 'alcohol', 'chess'])),
...     kd.obj(name='Bob', age=30, interests=kd.list(['music', 'chess'])),
...     kd.obj(name='Charlie', age=25, interests=kd.list(['tennis', 'alcohol'])),
... ])

# Simple filtering: find users older than 21. The lambda is traced and
# executed as a vectorized Koda functor (not pointwise Python code).
>>> users.select(lambda x: x.age > 21).name
DataSlice(['Bob', 'Charlie'], schema: STRING, ...)
```

You can perform more advanced operations on jagged arrays, such as conditional
filtering across dimensions and grouping:

```py
# Remove 'alcohol' interest for users under 21.
>>> interests = users.interests[:]
>>> interests
DataSlice([['tennis', 'alcohol', 'chess'], ['music', 'chess'], ['tennis', 'alcohol']], schema: STRING, ...)
>>> is_underage_alcohol = (interests == 'alcohol') & (users.age < 21)
>>> filtered_interests = interests.select(~is_underage_alcohol)
>>> filtered_interests
DataSlice([['tennis', 'chess'], ['music', 'chess'], ['tennis', 'alcohol']], schema: STRING, ...)

# Group users by their filtered interests.
>>> pairs = kd.new(user=users, interest=filtered_interests).flatten()
>>> grouped = kd.group_by(pairs, pairs.interest)
>>> interest_details = kd.new(interest=kd.collapse(grouped.interest),
...                           names=grouped.user.name.implode())
>>> interest_details
DataSlice([
    Entity(interest='tennis', names=List['Alice', 'Charlie']),
    Entity(interest='chess', names=List['Alice', 'Bob']),
    Entity(interest='music', names=List['Bob']),
    Entity(interest='alcohol', names=List['Charlie']),
], schema: ENTITY(interest=STRING, names=LIST[STRING]), ...)
```

### 3. Memory-Efficient Data Versioning

Koda structures are immutable, allowing modified versions with O(1) cost by
sharing underlying data.

```py
>>> users = kd.new(
...     name=kd.slice(['Alice', 'Bob', 'Charlie']),
...     age=kd.slice([30, 25, 35]),
... )

# Create a new version with updated ages for all users.
>>> aged_users = users.with_attrs(age=users.age + 1)
>>> users.age
DataSlice([30, 25, 35], schema: INT32, ...)
>>> aged_users.age
DataSlice([31, 26, 36], schema: INT32, ...)

# Alternative: `updates` can hold one or multiple updates across different
# levels and be passed around before applying with <<.
>>> updates = kd.attrs(users, age=users.age + 1)
>>> aged_users = users << updates
>>> aged_users.age
DataSlice([31, 26, 36], schema: INT32, ...)
```

### 4. Functors

Koda allows tracing Python functions to create reusable computational graphs
(Functors). Functors are normal DataSlices and can be stored in object
attributes, serialized, and served in production.

```py
# Define a Python function for Z-score normalization.
>>> def z_score(x):
...   return (x - kd.math.agg_mean(x)) / kd.math.agg_std(x)

# Trace it into a Koda functor.
>>> normalize_fn = kd.fn(z_score)

# Store the functor in a Koda object attribute.
>>> model = kd.obj(preprocess=normalize_fn)

# Evaluate the functor stored in the object.
>>> scores = kd.slice([10.0, 20.0, 30.0])
>>> model.preprocess(scores)
DataSlice([-1.0, 0.0, 1.0], schema: FLOAT32, ...)
```
