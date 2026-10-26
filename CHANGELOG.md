# CHANGELOG

<!-- version list -->

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
