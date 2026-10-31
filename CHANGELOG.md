# CHANGELOG

<!-- version list -->

## v0.6.1 (2026-10-30)

### Bug Fixes

- **process**: Kill entire process tree on timeout and preserve verbatim shell commands
  ([`62ed29e`](https://github.com/Callosemic/akasa/commit/62ed29ef5408fadb66e23024776b7c0c67249c3e))


## v0.6.0 (2026-10-30)

### Features

- **vfs**: Add host-protected targets, import-shadowing guard, write drainage, and template sys
  namespace
  ([`be6906c`](https://github.com/Callosemic/akasa/commit/be6906caaff3ca9b04f3bd411c1d445db5d185e6))


## v0.5.0 (2026-10-29)

### Features

- **auth**: Add durable stdlib credential store, lazy boot token gate, and guarded worker execution
  settlement
  ([`baf5c0d`](https://github.com/Callosemic/akasa/commit/baf5c0d22b0411fdae78ff2c9adccd2cd9481989))

- **auth**: Durable stdlib credential store, lazy boot token, and guarded worker settlement
  ([`202db33`](https://github.com/Callosemic/akasa/commit/202db33053d3cbb07351bc2ce65128306a1bed80))


## v0.4.0 (2026-10-29)

### Features

- Fail-closed peer classification, dns-rebinding host gate, dag wavefronts, and worker quiesce
  ([`729ce08`](https://github.com/Callosemic/akasa/commit/729ce08f0ca72e3acc3426a13173954706e56eac))

- **auth**: Add peer classification, host validation gate, forwarded header membrane, and test
  sandbox quiescence
  ([`d7ba088`](https://github.com/Callosemic/akasa/commit/d7ba0884ec88b4a6dd4ef5f195650a96825132f6))

- **kernel**: Implement 5-ring acyclic dag, execution-frame contextvars, inter-process file locking,
  and causal parsed-object caching
  ([`7331e32`](https://github.com/Callosemic/akasa/commit/7331e3252ab2043b295d104e54a73b3a043211d1))

- **tests**: Codify 5-ring acyclic dag ast fitness rules and expand kernel verification suite
  ([`b808e94`](https://github.com/Callosemic/akasa/commit/b808e94e5d39e11b569dbc0fba1286a87f5f073d))


## v0.3.0 (2026-10-26)

### Features

- Introduce kernel memory governance, bounded parallel I/O, and secret rotation lifecycle
  ([`2adaf6c`](https://github.com/Callosemic/akasa/commit/2adaf6c9d02e19028fa219eb511e918e720ccfad))

- Introduce kernel memory governance, bounded parallel I/O, and secret rotation lifecycle
  ([`7c82410`](https://github.com/Callosemic/akasa/commit/7c824100d46361d19a92a589dc7b787815f3b991))

- Kernel bootloader lifecycle, vfs 3-way merge, pending write overlay, and substrate purity
  decoupling
  ([`600dc80`](https://github.com/Callosemic/akasa/commit/600dc80eafe7003513cd0d496c2d806cf97cf473))

- **kernel**: Implement vfs micro-batching, dual worker pools, worker signature validation, and
  in-process ipc gateway
  ([`d5cb8db`](https://github.com/Callosemic/akasa/commit/d5cb8db38ac7ca798e88474947c324ee48ccc609))

- **kernel**: Parallel dag orchestration, vfs ram caching, and inter-workspace routing
  ([`64a44bd`](https://github.com/Callosemic/akasa/commit/64a44bd4a0678b92fa46efe47792b1098dfaf1d4))

- **vfs**: Add asynchronous bulk mutation batching and transaction consolidation
  ([`db1cdb8`](https://github.com/Callosemic/akasa/commit/db1cdb87efcb2e7d01a6f1d4209561991c3055d5))


## v0.2.0 (2026-10-15)

### Features

- **workers**: Add pause, resume, and cancellation lifecycle controls to metronome and sdk
  ([`7c6995e`](https://github.com/Callosemic/akasa/commit/7c6995e90448d65862f111179c0f606e6f30e34a))


## v0.1.3 (2026-10-08)

### Performance Improvements

- **db**: Streamline connection pooling and add corrupt DB dump garbage collection
  ([`7b08280`](https://github.com/Callosemic/akasa/commit/7b08280b83303d8bfe6f135a219d62147984d549))


## v0.1.2 (2026-10-06)

### Bug Fixes

- **kernel**: Enforce VFS volume mounting guardrails, auto-purge missing worker callbacks, and
  support gather-only selection expansion
  ([`d6988cf`](https://github.com/Callosemic/akasa/commit/d6988cf957b0f976cc7e4937c8cb81208d228803))


## v0.1.1 (2026-09-24)

### Bug Fixes

- Toml fix
  ([`090d766`](https://github.com/Callosemic/akasa/commit/090d766726ea310641ce494d08b4f0d078b03b8b))


## v0.1.0 (2026-09-24)

- Initial Release
