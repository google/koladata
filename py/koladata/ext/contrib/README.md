# External Koda Contributions

## Overview

This directory includes external contributions to the Koda library in the form
of operators, functions, and other miscellaneous utilities. These tools are not
required to be a general solution to some larger problem and are not necessarily
endorsed by the Koda team.

## How-to

### Adding an operator

To add an *operator* that becomes available in both eager and tracing modes,
you should:

1.  Add the operator definition to
    py/koladata/ext/contrib/operators.py under the
    `"kd_ext.contrib"` namespace.
    *   See go/koda-apis#kd.optools for operator definition alternatives.
1.  Add an operator test in the
    py/koladata/ext/contrib/tests directory.

The new operator automatically becomes available for use in eager and tracing
modes under `kd_ext.contrib`.

### Adding other functionality

To add eager-only functionality such as a function or a class, you should:

1.  Create (or re-use) a relevant Python module in the
    py/koladata/ext/contrib directory.
1.  Implement the desired functionality.
1.  Add a test in a corresponding test module.
1.  Expose the functionality in
    py/koladata/ext/contrib/functions.py.

The new functionality automatically becomes available for using under
`kd_ext.contrib`.
