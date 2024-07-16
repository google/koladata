"""Supports writing Python modules in C++."""

load("@rules_python//python:defs.bzl", "py_library")

def py_extension(
        name = None,
        srcs = None,
        hdrs = None,
        visibility = None,
        testonly = False,
        tags = None,
        deps = None):
    """Creates a Python module implemented in C++.

    Python modules can depend on a py_extension.

    Args:
      name: Name for this target.
      srcs: C++ source files.
      hdrs: C++ header files, for other py_extensions which depend on this.
      visibility: Controls which rules can depend on this.
      testonly: Whether this is only used in tests.
      tags: Tags to apply to the generated rules.
      deps: Other C++ libraries that this library depends upon.
    """

    cc_library_name = name + "_cc"
    cc_binary_name = name + ".so"

    native.cc_library(
        name = cc_library_name,
        srcs = srcs,
        hdrs = hdrs,
        testonly = testonly,
        tags = tags,
        visibility = ["//visibility:private"],
        deps = deps,
        alwayslink = True,
    )

    native.cc_binary(
        name = cc_binary_name,
        linkshared = True,
        linkstatic = True,
        visibility = ["//visibility:private"],
        testonly = testonly,
        tags = tags,
        deps = [cc_library_name],
    )

    py_library(
        name = name,
        data = [cc_binary_name],
        visibility = visibility,
        testonly = testonly,
        tags = tags,
    )
