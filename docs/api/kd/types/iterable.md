<!-- Note: This file is auto-generated, do not edit manually. -->

# kd.types.Iterable API

<pre class="no-copy"><code class="lang-text no-auto-prettify">QValue specialization for sequence qtype.

It intentionally provides only __iter__ method, and not __len__ for example,
since the users are expected to use iterables in such a way that they do not
need to know the length in advance, since they can represent streams in
streaming workflows.
</code></pre>





