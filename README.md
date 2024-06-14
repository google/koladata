# Koda

Koda (aka Kola Data) is a Python + C++ library that provides a data
representation and domain-specific language (a.k.a DSL) for orchestrating data
transformations and computations on the transformed data, which involve
structural changes to the data. The data can be in the form of protos, tables,
interconnected objects, graphs, tensors and more. Structural changes include
constructing new objects, protos, tables, graphs, tensors as an output or during
intermediate steps. Such transformations and computations can be designed in a
REPL (e.g. Google Colab), as well as compiled and served in C++.

In particular, Koda defines a family of data structures (DataBag, DataSlice and
its subclasses) that represent and manipulate pieces of data, sets of objects,
their attributes and their relations, as well as a DSL (Koda Expressions) to
work with them.

This is not an officially supported Google product.
