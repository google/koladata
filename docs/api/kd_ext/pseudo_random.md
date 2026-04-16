<!-- Note: This file is auto-generated, do not edit manually. -->

# kd_ext.pseudo_random API

Pseudo-random utilities.





### `kd_ext.pseudo_random.epoch_id(...)` {#kd_ext.pseudo_random.epoch_id}

<pre class="no-copy"><code class="lang-text no-auto-prettify">epoch_id() -&gt; int</code></pre>

### `kd_ext.pseudo_random.next_fingerprint(...)` {#kd_ext.pseudo_random.next_fingerprint}

<pre class="no-copy"><code class="lang-text no-auto-prettify">next_fingerprint() -&gt; Fingerprint</code></pre>

### `kd_ext.pseudo_random.next_uint64(...)` {#kd_ext.pseudo_random.next_uint64}

<pre class="no-copy"><code class="lang-text no-auto-prettify">next_uint64() -&gt; int</code></pre>

### `kd_ext.pseudo_random.reseed(entropy: int | bytes | None = None) -> None` {#kd_ext.pseudo_random.reseed}

<pre class="no-copy"><code class="lang-text no-auto-prettify">Reseeds the pseudo-random sequence.

A successful call affects all subsequent calls to `next_uint64()`,
`next_fingerprint()`, and `epoch_id()`. During the execution of this function,
concurrent calls to these functions may return inconsistent or non-random
values.

The reseeding mechanism is primarily intended for cases where a process is
snapshotted and respawned multiple times. In such scenarios, multiple
instances sharing the same origin will also share the same pseudo-random
sequence, which can lead to collisions.

NOTE: This function does not guarantee that the resulting sequence will be
reproducible, even if the same `entropy` is used. It only guarantees that
distinct `entropy` values will produce distinct sequences.

Args:
  entropy: Entropy to use for reseeding; either an integer or a byte string.
    If not provided, uses `os.urandom(16)`.</code></pre>

