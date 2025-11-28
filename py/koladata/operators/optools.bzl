"""Utility for operator package generation."""

load("@com_google_arolla//py/arolla/optools:optools.bzl", "arolla_cc_operator_package", "arolla_operator_package_snapshot")

# TODO: b/464002636 - Update the documentation.
def koladata_cc_operator_package(
        name,
        preimports = (),
        imports = (),
        tool_deps = (),
        arolla_initializer = None,
        deps = (),
        tags = (),
        testonly = False,
        visibility = None):
    """Creates an operator package and a cc_library that loads it.

    This rule dumps operator definitions from .py to an operator package protobuf.

    Args:
      name: the name of the resulting library.
      imports: a list of python modules providing the new operator declarations.
      preimports: a list of python modules that should be imported before the operator declarations.
        The main purpose is to pre-load other operator libraries and serialization codecs.
      tool_deps: dependencies for the operator package snapshot genrule, these must contain the
        Python modules specified in `imports` and `preimports`.
      arolla_initializer: the initializer settings specified in the format
        `dict(name='<name>', deps=['<dep>', ...], reverse_deps=['<dep>', ...])`
        (see `arolla_initializer_spec` or `arolla/util/init_arolla.h` for more information).
      deps: a list of C++ dependencies needed by the generated C++ code, e.g. serialization codecs
        and QExpr operator implementations.
      tags: tags.
      testonly: if True, only testonly targets (such as tests) can depend on this target.
      visibility: target's visibility.
    """
    snapshot_name = "_{}_snapshot".format(name)

    arolla_operator_package_snapshot(
        name = snapshot_name,
        preimports = preimports,
        imports = imports,
        deps = tool_deps,
        tags = tags,
        testonly = testonly,
        visibility = ["//visibility:private"],
    )

    arolla_cc_operator_package(
        name = name,
        snapshot = ":" + snapshot_name,
        arolla_initializer = arolla_initializer,
        deps = deps,
        tags = tags,
        testonly = testonly,
        visibility = visibility,
    )
