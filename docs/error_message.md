# Add user friendly error messages

* TOC
{:toc}

When Koda developers find a case where the error message is not readable,
contains a lot of internal implementation details or is not user oriented, this
document provides a way to improve it.

## Example error message

Here is an example of an improved error message.

```python
>>> obj = kd.obj(a=1)
>>> obj.with_bag(kd.bag()).a
...

ValueError: object schema is missing for the DataItem whose item is: $0000ShTxCVvMzRgwUPt5cb

  DataItem with the kd.OBJECT schema usually store its schema as an attribute or implicitly hold the type information when it's a primitive type. Perhaps, the OBJECT schema is set by mistake with
  foo.with_schema(kd.OBJECT) when 'foo' does not have stored schema attribute.

The above exception was the direct cause of the following exception:

ValueError: failed to get attribute 'a'
```

Because the `DataBag` is newly created and doesn't contain schema info,
accessing the attribute `a` will cause a missing schema error.

## How does it work

In Koda, most of the logic exists in C++. Python is a thin wrapper around the
C++.

![drawing](images/koda_error.svg)

When error happens,

1.  Koda C++ collects the necessary information, creates a specific error
    payload and attaches it to the `Status`. So the error can be propagated
    across all C++ call stacks. During the progatation, error will be clarified
    by `KodaErrorCausedByMissingObjectSchemaError` or similar functions when the
    additional context is available and the original error will be appended in
    the `cause`.

2.  In the topmost CPython layer, the
    [`SetPyErrFromStatus`](https://github.com/google/koladata/blob/main//py/koladata/types/data_slice.cc)
    C++ function recursively converts the error chain to python exceptions and
    raises exceptions.

## Implementation Steps

### Check existing error categories

There's already a list of error categories in
[errors.h](https://github.com/google/koladata/blob/main//koladata/internal/errors.h).

Check if the case to improve is already covered. If so, create the error payload
instance when error is happening and attaches to the `Status`. See
[example](https://github.com/google/koladata/blob/main//koladata/internal/data_bag.cc).

### Define the specific error struct type

If the error case is not listed, then create a new struct container, add the
necessary fields, create an payload instance and attach it to
`Status`.

### Clarify the error message

In the `Status` propagation path, a more readable error message can be
assembled with additional context. Here is an
[example](https://github.com/google/koladata/blob/main//koladata/data_slice.cc) of printing the schema from DataBag.

## Current limitations and future improvement

*   This error handling mechanism can only improve the error message on a case
    by case basis.
