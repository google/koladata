""""Definitions of Koladata-specific py_extensions rules"""

load("@pybind11_bazel//:build_defs.bzl", "pybind_extension")
load("@rules_python//python:defs.bzl", "py_library")

load("@rules_cc//cc:cc_binary.bzl", "cc_binary")
load("@rules_cc//cc:cc_library.bzl", "cc_library")

def _pytype_extension(  # buildifier: disable=unused-variable
        name = None,
        srcs = None,
        hdrs = None,
        dynamic_deps = None,
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
      dynamic_deps: Dynamic dependencies for the C++ binary.
      visibility: Controls which rules can depend on this.
      testonly: Whether this is only used in tests.
      tags: Tags to apply to the generated rules.
      deps: Other C++ libraries that this library depends upon.
    """

    cc_library_name = name + "_cc"
    cc_binary_name = name + ".so"
    cc_library(
        name = cc_library_name,
        srcs = srcs,
        hdrs = hdrs,
        testonly = testonly,
        tags = tags,
        visibility = ["//visibility:private"],
        deps = deps,
        alwayslink = True,
    )
    cc_binary(
        name = cc_binary_name,
        linkshared = True,
        linkstatic = True,
        visibility = ["//visibility:private"],
        testonly = testonly,
        tags = tags,
        deps = [cc_library_name],
        dynamic_deps = dynamic_deps,
    )

    py_library(
        name = name,
        data = [cc_binary_name],
        visibility = visibility,
        testonly = testonly,
        tags = tags,
    )

def koladata_py_extension(
        name = None,
        srcs = None,
        hdrs = None,
        dynamic_deps = None,
        visibility = None,
        testonly = False,
        tags = [],
        deps = None,
        pytype_deps = (),
        pytype_srcs = ()):
    """Builds a Koladata py extension module.

    All extensions created using this rule share the same instance of
    the following libraries:

      * py/arolla:py_abc
      * Arolla (particularly, the operator registry)
      * Abseil
      * Protobuf
      * py/koladata/base:initializers

    These libraries are designed to function as singletons and may operate
    incorrectly if each Python extension statically links to a "private" copy
    of the library (even when the symbols are hidden).
    """
    _pytype_extension(
        name = name,
        srcs = srcs,
        hdrs = hdrs,
        dynamic_deps = [
             "//py/koladata/dynamic_deps:koladata_so",
        ],
        visibility = visibility,
        testonly = testonly,
        tags = tags,
        deps = deps,
    )

def koladata_pybind_extension(
        name = None,
        srcs = None,
        dynamic_deps = None,
        visibility = None,
        testonly = False,
        tags = [],
        deps = None,
        pytype_deps = (),
        pytype_srcs = ()):
    """Builds a Koladata pybind extension module.

    All extensions created using this rule share the same instance of
    the following libraries:

      * py/arolla:py_abc
      * Arolla (particularly, the operator registry)
      * Abseil
      * Protobuf
      * py/koladata/base:initializers

    These libraries are designed to function as singletons and may operate
    incorrectly if each Python extension statically links to a "private" copy
    of the library (even when the symbols are hidden).
    """

    pybind_extension(
        name = name,
        srcs = srcs,
        dynamic_deps = [
            "//py/koladata/dynamic_deps:koladata_so",
        ],
        visibility = visibility,
        testonly = testonly,
        tags = tags,
        deps = deps,
    )
