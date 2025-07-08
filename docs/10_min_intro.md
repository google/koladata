<!-- go/markdown -->

# 10 Minute Introduction

This is a 10-minute introduction into Koda intended for absolute beginners.
Please see the [Getting Started](getting_started.md) for more in-depth guides.

* TOC
{:toc}

## DataSlice

[`DataSlice`](overview.md#dataslices-and-items) is the most basic data structure
in Koda. A `DataSlice` is a multi-dimensional jagged array holding data of any
type (e.g. integers, strings, structured data), including mixed data. This
section showcases this fundamental data structure through a motivating example.

### The Scorebook Example

Imagine a school with two classes, each having a different number of students.
The scorebook, containing the scores of individual students, can be represented
using a nested Python list of integers:

```py
scores_of_students_in_a_school = [
  [10, 20, 30],       # class A: three students.
  [40, 50, None, 70]  # class B: four students, the score for one is unknown.
]
```

We can use this scorebook directly in Koda:

```py
scores_slice = kd.slice(
  scores_of_students_in_a_school)  # [[10, 20, 30], [40, 50, None, 70]]
scores_slice.get_shape()  # JaggedShape(2, [3, 4])
scores_slice.get_schema()  # INT32
```

<!-- disableFinding("first class") -->

The `scores_slice` variable holds a Koda `DataSlice`. The `DataSlice` keeps
track of the scores, and also of the [shape](overview.md#dataslices-and-items)
and [schema](fundamentals.md#schemas) (type) of the data. The associated
`JaggedShape(2, [3, 4])` shape indicates that there are two classes, with the
first class having three students and the second having four. The associated
`INT32` schema indicates that the score values are 32-bit integers. Missing data
is natively supported and is represented as `None`.

<!-- enableFinding("first class") -->

The `JaggedShape` allows each row to have a different number of associated
columns. This versatility distinguishes it from a traditional multi-dimensional
matrix, which is restricted to uniform dimensions and a regular shape, and is
better viewed as a *hierarchical partitioning* of the data. The dimensionality
of the shape corresponds to the depth of the nested input (which is required to
be the same for all elements) and is the same as the number of arguments in the
`JaggedShape(2, [3, 4])` representation: two.

### Basic Operations

Most of Koda's operations are pointwise *vectorized*, meaning that they operate
on each element in isolation and preserve the shape:

```py
scores_slice + 10  # [[20, 30, 40], [50, 60, None, 80]]
```

Other operations make use of the hierarchical information stored in the jagged
shape. For example, we can compute summary statistics for the scores:

```py
# Compute the average score of every class.
class_avg = kd.math.agg_mean(scores_slice)  # [20.0, 53.33]
# Compute the highest average score over all classes.
class_avg_max = kd.math.agg_max(class_avg)  # 53.33
# Compute the average of all students in the school. This considers the last
# `ndim=2` dimensions.
school_avg = kd.math.agg_mean(scores_slice, ndim=2)  # 36.67
```

We can naturally extend the representation to cover a collection of schools:

```py
scores_from_school_1 = [[10, 20, 30], [40, 50, None, 70]]
scores_from_school_2 = [[80, 90], [100], [110, 120]]
kd.slice(scores_from_school_1).get_shape()  # JaggedShape(2, [3, 4])
kd.slice(scores_from_school_2).get_shape()  # JaggedShape(3, [2, 1, 2])
many_schools_slice = kd.slice([scores_from_school_1, scores_from_school_2])
many_schools_slice.get_shape()  # JaggedShape(2, [2, 3], [3, 4, 2, 1, 2])
```

As we can see, the slice for the collection of schools has an additional outer
dimension, but the inner two dimensions have the same meaning as before (they
represent classes and students, respectively), and hence the exact same code
used to compute summary statistics for a single school can be reused for the
collection of schools:

```py
# Compute the average score from each class for each of the schools.
class_avg = kd.math.agg_mean(
    many_schools_slice
)  # [[20.0, 53.33], [85.0, 100.0, 115.0]]
# Compute the maximum of the average class scores for each school.
class_avg_max = kd.math.agg_max(class_avg)  # [53.33, 115.0].
```

### Broadcasting

Consider the case where we again wish to compute statistics for a school. We are
interested in the ratio of students per class that achieved a score higher than
some minimum value. To account for various differences between classes, the
threshold differs per class. In Koda, this is natural to express due to its
broadcasting logic:

```py
# The shape of `scores_slice` is `JaggedShape(2, [3, 4])`.
scores_slice.get_shape()
# The shape of `thresholds` is `JaggedShape(2)`.
thresholds = kd.slice([30, 50])
# `thresholds` is broadcasted to the shape of `scores_slice`, which is possible
# since the shape of `thresholds` is a "prefix" of the shape of `scores_slice`.
# The broadcasted `thresholds` are equivalent to:
#    thresholds = kd.slice([[30, 30, 30], [50, 50, 50, 50]])
# The pointwise computation is then performed.
acceptable_scores = scores_slice >= thresholds
number_of_acceptable_scores = kd.agg_count(acceptable_scores)  # [1, 2]
# Counting the number of total scores ignores missing values in `scores_slice`.
number_of_total_scores = kd.agg_count(scores_slice)  # [3, 3]
number_of_acceptable_scores / number_of_total_scores  # [0.33, 0.67]
```

As seen above, Koda broadcasts data from the outer dimensions to the inner (i.e.
from "left-to-right") which requires that one shape is the *prefix* of the
other. This plays well with Koda's hierarchical data model, and facilitates
broadcasting of jagged data which otherwise becomes tricky with alternative
broadcasting rules.

## Structured Data

`DataSlices` are able to represent structured data, beyond the tabular form of
pandas or the multidimensional array form of Numpy, while preserving the
parallelism in computation that ensures good performance. Elements are *not*
limited to primitives such as `INT32` or `STRING`. Just like Python classes, one
can define tailored *schemas* for objects representing
[structured data](overview.md/#dataslices-of-structured-data-explosionimplosion-and-attribute-access).

### DataItems with Structured Schemas

Continuing the previous example, a scorebook for a school's students typically
contains more data than just the bare scores. A student has a name and a score:

```py
Student = kd.named_schema('Student', student_name=kd.STRING, score=kd.INT32)
```

Next we define the schema for a class and a school. A class has a `class_name`,
and a list of `Students`, represented through a Koda `List`.

```py
Class = kd.named_schema(
    'Class', class_name=kd.STRING, students=kd.list_schema(Student)
)
School = kd.named_schema(
    'School', school_name=kd.STRING, classes=kd.list_schema(Class)
)
```

We can represent the scorebook of a school as follows (the syntax here is
intentionally verbose for educational purposes and can be done more concisely):

```py
# The 7 students:
s1 = Student.new(student_name="Alice", score=10)
s2 = Student.new(student_name="Bob", score=20)
s3 = Student.new(student_name="Carol", score=30)
s4 = Student.new(student_name="Dan", score=40)
s5 = Student.new(student_name="Erin", score=50)
s6 = Student.new(student_name="Frank", score=None)
s7 = Student.new(student_name="Grace", score=70)
# The 2 classes:
class_a = Class.new(class_name="A", students=kd.list([s1, s2, s3]))
class_b = Class.new(class_name="B", students=kd.list([s4, s5, s6, s7]))
# The school:
school_s1 = School.new(school_name="S1", classes=kd.list([class_a, class_b]))
```

`school_s1` is a 0-dimensional `DataSlice`, i.e. a scalar. In Koda, this is
known as a `DataItem`. This approach naturally associates attributes in the same
`DataItem` with each other, simplifying subsequent inspection and manipulation
of the data.

### Accessing Attributes

The name of `school_s1` is accessed as `school_s1.school_name`, which returns a
`DataItem` with schema `STRING`. As Koda operations are vectorized, we can
access an attribute of multiple items in a single call, where the output has the
same shape as the input:

```py
school_s1.school_name  # 'S1'
kd.slice([s1, s2]).student_name  # ['Alice', 'Bob']
```

The classes of the school are accessed as `school_s1.classes`. Since attribute
access preserves the shape of the input, and `school_s1` is a scalar, this
returns a *scalar* `List` `DataItem`. The `List` is a container holding several
`Class` items and does not itself have a `classes` attribute. We can *explode*
the `List` with the operator `[:]` to get a `DataSlice` with the `Class` items
from the `List`. Attributes can then be accessed as usual with the dot operator,
for instance `school_s1.classes[:].class_name`, which returns a `DataSlice` of
attributes with the same shape:

```py
school_s1.classes   # Scalar DataItem: List[class_a, class_b]
school_s1.classes[:]  # 1-dimensional DataSlice: [class_a, class_b]
school_s1.classes[:].class_name  # 1-dimensional DataSlice: ['A', 'B']
# Similarly, one can access all students' scores by
school_s1.classes[:].students[:].score  # [[10, 20, 30], [40, 50, None, 70]]
```

### DataSlice vs List

Introducing `Lists` may initially seem redundant. Why are the classes in
`school_s1` packed as a `List`, rather than a `DataSlice`, especially since a
`DataSlice` would conveniently allow direct access to class attributes without
an extra unpacking ("explosion") step?

The primary purpose of `List` is organizational - it turns many individual items
(e.g. classes) into a single manageable item (a `List` of classes) that is
atomic for the purposes of vectorized computations. This allows `Lists` to be
counted, broadcasted, and in general to behave like other kinds of primitive
data (e.g. a scalar school item can have an attribute that stores multiple
classes in exactly the same way as it can have an attribute that stores a scalar
school_name). Additionally, it allows a single school, which is a scalar
`DataItem`, to have multiple classes thus modelling a one-to-many relationship.
The consequences of this are more easily shown through an example:

```py
# `students` is a 1-dimensional DataSlice:
#   [List[s1, s2, s3], List[s4, s5, s6, s7]].
students = school_s1.classes[:].students
# Two classes have lists of students. We use kd.count to count the total
# number of items in the DataSlice, which is the number of student lists -> 2.
kd.count(school_s1.classes[:].students)  # 2
# The 2 lists contain 3 and 4 student items respectively, which we count by
# first exploding the lists:
kd.count(school_s1.classes[:].students[:])  # 7
# Using `kd.agg_count` we can compute aggregate statistics over the last
# dimension, thereby counting the number of students per class:
kd.agg_count(school_s1.classes[:].students[:])  # [3, 4]
```

Koda `Lists` and `DataSlices` are important concepts in Koda. Users will
frequently convert between these two structures: a `List` explodes into a
`DataSlice`, and a `DataSlice` can conversely *implode* its innermost level of
items into `Lists`. They are perfectly isomorphic and the conversion preserves
all information. As such, a `List` is a "packed" version of a `DataSlice`, and a
`DataSlice` is an "unpacked" version of a `List`. The following example
illustrates this relationship:

```py
# List[class_a, class_b], shape: (), i.e. a scalar
school_s1.classes

# [class_a, class_b], shape: (2)
school_s1.classes[:]

# [List[s1, s2, s3], List[s4, s5, s6, s7]], shape: (2)
school_s1.classes[:].students

# [[s1, s2, s3], [s4, s5, s6, s7]], shape: (2, [3, 4])
school_s1.classes[:].students[:]

# [List[s1, s2, s3], List[s4, s5, s6, s7]], shape: (2)
#
# Equivalent to `school_s1.classes[:].students`
school_s1.classes[:].students[:].implode()

# List[List[s1, s2, s3], List[s4, s5, s6, s7]], shape: (), i.e. a scalar
school_s1.classes[:].students[:].implode().implode()
```

Note that Koda supports `Dicts` in addition to `Lists` and the structured items
such as `school_s1` (known as Entities).

## Tracing and Serving

The logic expressed using Koda operators can be packaged into servable
[*Functors*](overview.md#functors-and-lazy-evaluation), which is Koda's way to
natively represent computations. Say, for example, that the school district
regularly wants to get an overview of all the schools in the district. At the
end of the year, they wish to find the student with the highest score in each
class to award them with a prize.

In Koda, we can represent this through:

```py
def get_student_with_highest_score(schools):
  students = schools.classes[:].students[:]
  # `.S[...]` is syntactic sugar for indexing into a DataSlice.
  return students.S[kd.argmax(students.score)].student_name
```

Because Koda is vectorized, this can be evaluated with one school, a flat
`DataSlice` of schools, or even with a multidimensional `DataSlice` of schools:

```py
get_student_with_highest_score(school_s1)  # ['Carol', 'Grace'].
get_student_with_highest_score(
  kd.slice([school_s1, school_s1]))  # [['Carol', 'Grace'], ['Carol', 'Grace']].
```

This function can be converted into a Functor through *tracing*, and saved to
disk. This Functor can then be loaded again and evaluated with data:

```py
functor = kd.fn(get_student_with_highest_score)
functor(school_s1)  # Can be evaluated directly, returns ['Carol', 'Grace']

serialized_functor = kd.dumps(functor)  # serializes the functor to bytes.
deserialized_functor = kd.loads(serialized_functor)  # loads the functor.
deserialized_functor(school_s1)  # ['Carol', 'Grace']
```

Internally, the Functor specifies the Koda operations to apply. The operations
are implemented in C++ and made available in Python, so the Functor can be
loaded and executed in both C++ and in Python. This way, Koda supports workflows
where an interactive Python environment can be used for experimenting with data
modeling and data manipulation, and thereafter exactly the same computation can
be executed in a production environment with C++.

## What's Next

The concepts shown above offer just a brief introduction to the world of Koda.
Koda has many more features and operators for data modeling and manipulation.
Moreover, it supports converting between other numerical libraries such as
Pandas, has native support for conversion to and from Protos, and can be traced
into a C++ compatible computational graph representation allowing code written
in Python to be served in a production environment.

Next-up: See the [Overview](overview.md) guide for a more comprehensive and
detailed introduction.
