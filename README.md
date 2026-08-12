# Cora-Cache / Redis

> Enterprise-grade, high-performance Redis caching engine engineered for modern TypeScript backends.

## Cora Philosophy

**Cora-Cache** eliminates the overhead of traditional string-based caching setups. Instead of manually predicting, copying, and concatenating complex string keys across your application, the engine automatically resolves and maps a deterministic tag hierarchy using your **fetcher's native function name** combined with its runtime **argument dependencies**.

The package exposes a lightweight factory interface that distributes **isolated, decoupled runtime utilities** (`cache`, `invalidateCache`) [1.1]. Under the hood, it is meticulously optimized for high-throughput production workloads, providing bulletproof mitigations against **Cache Stampede** (via atomic `SET NX` locks and memory-level in-flight request deduplication), database degradation via automated expiration variance (**Jitter**), and lightning-fast tag evictions powered by optimized pipelined Redis batches (**Pipeline + SSCAN**).

---

### Key Highlights

* **Automatic Key-Mapping** — Seamlessly structures your Redis lookup footprints using native function definitions and flat primitive dependency arrays. No manual string concatenation. No typos.
* **Cache Stampede Protection** — Defends your backing database from traffic spikes when hot keys expire. Uses atomic distributed locking (`SET NX`) and synchronous in-flight Promise tracking inside the Node.js process memory.
* **Deterministic Invalidation** — Purges cache spaces dynamically at the entity level [1.1]. The engine automatically tracks and sweeps granular method collections via Redis Sets under a single root tag call.
* **Zero Runtime Bloat** — Shipped as a fully tree-shakeable, pure ESM module with declared `sideEffects: false` to guarantee an ultra-lean bundle size.
* **Smart Expiry (Jitter)** — Mitigates synchronized multi-key drop-offs on long-lived caches (`"days"`, `"weeks"`) by algorithmically altering TTLs, spreading database replenishment evenly across time windows.

---

## Installation

Install the core package along with its required `ioredis` peer dependency:

```bash
npm install @cora-cache/redis ioredis
```

*Requirements: Node.js >= 22.0.0, TypeScript >= 5.5.0 (Fully optimized for native TypeScript 6 structures).*

---

## Quick Start

### 1. Initialization (Factory Setup)

Initialize your cache client inside a single dedicated entry point (e.g., `@/cache/index.ts`). Pass your configured `ioredis` instance and export your decoupled primitives directly:

```typescript
// ../cache.ts

import Redis from "ioredis"
import { createCache } from "@cora-cache/redis"

const redisClient = new Redis(process.env.REDIS_URL as string)

// Initialize once and export your decoupled core functions
export const { cache, getCache, setCache, invalidateCache } = createCache({ redisClient })
```

### 2. Implementation in Services (Zero Hardcoded Keys)

To leverage automated key resolution, declare your fetchers explicitly using the standard `function` keyword. This preserves the `fetcher.name` metadata for the engine. Pass all query variables into the flat `deps` array:

```typescript
import { cache } from "../cache"

// Fetches a single user record matching the provided unique identifier from the database
const getUser = (userId: string) => {
  const fecther = () => {
    return drizzle
      .select()
      .from(users)
      .where(eq(users.id, userId))
  }

  return cache(fecther, "minutes", "users", `users:${userId}`)
}

// Fetches the complete, unfiltered collection of all user records from the database
const getUsers = () => {
  const fecther = () => {
    return drizzle
      .select()
      .from(users)
  }

  return cache(fecther, "hours", "users")
}
```

### 3. Pipelined Invalidation

When mutating records, you do not need to track and list down individual cache entries. Purging the root collection tag automatically clears all downstream methods safely [1.1].

```typescript
import { invalidateCache } from "../cache"

const invalidateUsers = () => {
  // Cascades through the root tag to clear all nested query footprints at once
  invalidateCache("users")
}

const invalidateUser = (userId: string) => {
  // Selectively evicts granular method tokens linked to this precise user record
  invalidateCache(`users:${userId}`)
}
```

### 4. Advanced Manual Control (Low-Level API)

If you need to bypass the automated compilation lifecycle of the `cache()` wrapper and manually handle conditional logic, serialization checks, or step-by-step pipelines, use the decoupled `getCache` and `setCache` methods [1.1]:

```typescript
import { getCache, setCache } from "../cache"

// Manually retrieves or hydrates custom session states using explicit tag routing
const getUser = async (userId: string) => {
  // Looks up the specific compiled hash matching this tag combination inside Redis
  const cachedUser = await getCache<User>("users", `users:${userId}`)

  if (cachedUser) {
    return cachedUser
  }

  // Fallback to primary database or identity provider if cache hits empty
  const freshUser = await drizzle
     .select()
     .from(users)
     .where(eq(users.id, userId))

  if (freshUser) {
    // Manually commits the structure with a deterministic lifetime and explicit tag tracking
    await setCache(freshUser, "minutes", "users", `users:${userId}`)
  }

  return freshUser
}
```

## Best Practices

Follow these simple integration patterns to maximize stability and caching accuracy:

* **Always Name Your Fetchers** — Avoid passing anonymous arrow functions `() => Promise` directly to `cache()`. Anonymous declarations fallback to `"anonymous"`, which increases key collision risk across identical entities.
* **Keep Dependencies Flat** — The `deps` argument only accepts flat array values (`string | number | boolean`). If you are working with compound query inputs, destructure them directly into primitive tokens: `[options.limit, options.status]`.
* **Rely on Jitter for Bulk Loads** — Expiration variance is applied automatically to high-duration lifespans (`"days"`, `"weeks"`). Do not hesitate to use long cache lifetimes for dense database lookups — the engine handles stampede risk natively.

---

## License

Licensed under the [MIT License](LICENSE). Cora-Cache is completely free to use for both personal and enterprise commercial environments.

---
Elevate your backend performance. Built by developers, for creators.