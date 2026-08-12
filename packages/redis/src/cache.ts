import Redis from "ioredis"

import { createHash, randomUUID } from "crypto"
import { serializer } from "./serializer"

declare module "ioredis" {
	interface Redis {
		releaseLock(key: string, value: string): Promise<number>
	}
}

type CacheLife = "seconds" | "minutes" | "hours" | "days" | "weeks" | "max"

const globalForRedis = global as unknown as { redis: Redis | undefined }

const redis = globalForRedis.redis ?? new Redis(process.env.REDIS_URL as string, {
	retryStrategy: (times) => Math.min(times * 50, 2000),
	maxRetriesPerRequest: null,
	enableOfflineQueue: true,
})

const RELEASE_LOCK_LUA = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  else
    return 0
  end
`

redis.defineCommand("releaseLock", {
	numberOfKeys: 1,
	lua: RELEASE_LOCK_LUA,
})

if (process.env.NODE_ENV !== "production") {
	globalForRedis.redis = redis
}

const inflightRequests = new Map<string, Promise<unknown>>()

const UNDEFINED_CACHE_MARKER = "__VAL_IS_UNDEFINED__"

const getSecondsFromLife = (life: CacheLife): number => {
	switch (life) {
		case "seconds":
			return 1
		case "minutes":
			return 60
		case "hours":
			return 3600
		case "days":
			return 86400
		case "weeks":
			return 604800
		case "max":
			return 2592000
		default:
			return 3600
	}
}

const applyJitter = (seconds: number): number => {
	if (seconds <= 60) return seconds

	const minFactor = 0.95
	const maxFactor = 1.05
	const jitter = Math.random() * (maxFactor - minFactor) + minFactor

	return Math.floor(seconds * jitter)
}

const generateDataKey = (tags: string[]): string => {
	const sortedTags = [...tags]
		.sort()
		.join(",")

	const hash = createHash("sha256")
		.update(sortedTags)
		.digest("hex")

	return `cache:data:${hash}`
}

const sleep = (ms: number) => {
	return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Low-level API to manually retrieve records from the Redis cache.
 * Looks up the compiled SHA-256 hash matching the provided tag combination.
 *
 * @template T - The expected return type of the deserialized cache payload.
 * @param {...string[]} tags - A flat collection of tags used to locate the cache entry.
 * @returns {Promise<T | undefined>} The deserialized typed data, or `undefined` if a cache miss occurs.
 *
 * @example
 * // Retrieve custom user with strict type boundaries
 * const user = await getCache<User>(`user:${id}`)
 */
export const getCache = async <T>(...tags: string[]): Promise<T | undefined> => {
	if (tags.length === 0) return

	try {
		const dataKey = generateDataKey(tags)
		const data = await redis.getBuffer(dataKey)

		if (data && data.toString() === UNDEFINED_CACHE_MARKER) {
			return
		}

		return serializer.deserialize<T>(data)
	} catch {
		return
	}
}

/**
 * Low-level API to manually commit records into the Redis cache.
 * Serializes the payload and registers explicit tag tracking boundaries.
 *
 * @template T - The type of the data payload being serialized and stored.
 * @param {T} value - The fresh database or application payload to cache.
 * @param {CacheLife} life - The expiration tier lifespan (e.g., `"minutes"`, `"hours"`, `"days"`).
 * @param {...string[]} tags - A flat collection of tags associated with this record for cascading invalidations.
 * @returns {Promise<void>} Resolves once the pipeline commit sequence finishes.
 *
 * @example
 * // Manually hydrate a user profile with a 15-minute expiration lifespan
 * await setCache(user, "minutes", "users", `users:${user.id}`);
 */
export const setCache = async (value: unknown, life: CacheLife, ...tags: string[]): Promise<"OK" | undefined> => {
	if (tags.length === 0) return

	try {
		const dataKey = generateDataKey(tags)
		const baseSeconds = getSecondsFromLife(life)
		const secondsWithJitter = applyJitter(baseSeconds)
		const pipeline = redis.pipeline()

		const payload = value === undefined
			? Buffer.from(UNDEFINED_CACHE_MARKER)
			: serializer.serialize(value)

		pipeline.set(dataKey, payload, "EX", secondsWithJitter)

		tags.forEach((tag) => {
			const tagKey = `cache:tag:${tag}`
			const maxTagLife = Math.floor(baseSeconds * 1.1) + 60

			pipeline.sadd(tagKey, dataKey)
			pipeline.expire(tagKey, maxTagLife)
		})

		await pipeline.exec()

		return "OK"
	} catch {
		return
	}
}

/**
 * Deterministic Invalidation API.
 * Purges cache spaces and sweeps all associated method tokens or granular variations
 * via optimized Redis batch pipelines under the provided root tag scopes.
 *
 * @param {...string[]} tags - A collection of root tags or specific keys to evict instantly.
 * @returns {Promise<void>} Resolves when the cleanup pipeline completes execution.
 *
 * @example
 * // Global eviction for all queries tied to the "users" collection space
 * await invalidateCache("users");
 */
export const invalidateCache = async (...tags: string[]): Promise<boolean> => {
	if (tags.length === 0) return false

	try {
		const TAGS_CONCURRENCY_LIMIT = 10

		for (let i = 0; i < tags.length; i += TAGS_CONCURRENCY_LIMIT) {
			const chunkTags = tags.slice(i, i + TAGS_CONCURRENCY_LIMIT)

			await Promise.all(
				chunkTags.map(async (tag) => {
					const tagKey = `cache:tag:${tag}`
					const CHUNK_SIZE = 500

					let cursor = "0"
					let deletePipeline = redis.pipeline()

					do {
						const result = await redis.sscan(
							tagKey,
							cursor,
							"COUNT",
							1000
						)

						const [nextCursor, dataKeys] = result as [string, string[]]

						cursor = nextCursor

						if (dataKeys && dataKeys.length > 0) {
							for (const key of dataKeys) {
								deletePipeline.del(key)

								if (deletePipeline.length >= CHUNK_SIZE) {
									await deletePipeline.exec()
									deletePipeline = redis.pipeline()
								}
							}
						}
					} while (cursor !== "0")

					deletePipeline.del(tagKey)
					await deletePipeline.exec()
				})
			)
		}
		return true
	} catch {
		return false
	}
}

/**
 * High-performance smart caching wrapper with built-in Cache Stampede mitigation.
 * Automatically resolves a deterministic key hierarchy using the fetcher's native function name
 * combined with flat runtime dependency inputs.
 *
 * @template T - The inferred return data type from the asynchronous fetcher function.
 * @param {() => Promise<T>} fetcher - An explicit function (preferably named via `function` keyword) that queries the database.
 * @param {CacheLife} life - The target caching lifespan tier (`"seconds"`, `"minutes"`, `"hours"`, etc.).
 * @param {string} tags - The overarching structural tag used to group and bind this query for safe invalidations.
 * @returns {Promise<T>} The cached data payload or a freshly hydrated response.
 *
 * @example
 * async function fetcherUser() { return db.select().from(users).where(...); }
 * const user = await cache(fetcherUser, "minutes", "users", [userId]);
 */
export const cache = async <T>(
	fetcher: () => Promise<T>,
	life: CacheLife,
	...tags: string[]
): Promise<T> => {
	if (tags.length === 0) {
		throw new Error("Caching requires passing at least one tag in options.tags")
	}

	const dataKey = generateDataKey(tags)

	if (inflightRequests.has(dataKey)) {
		return inflightRequests.get(dataKey) as Promise<T>
	}

	const operationPromise = (async () => {
		let cached = await getCache<T>(...tags)

		if (cached !== undefined) return cached

		const lockKey = `lock:${dataKey}`
		const lockValue = randomUUID()
		const lockTimeout = 10000
		const maxRetries = 10
		const baseDelay = 40

		let hasLock = false
		let retries = 0

		while (!hasLock && retries < maxRetries) {
			const status = await redis.set(
				lockKey,
				lockValue,
				"PX" as const,
				lockTimeout,
				"NX" as const
			)

			if (status === "OK") {
				hasLock = true
			} else {
				retries++

				const backoffDelay = Math.floor(baseDelay * Math.pow(1.5, retries) + Math.random() * 20)

				await sleep(backoffDelay)
				cached = await getCache<T>(...tags)

				if (cached !== undefined) return cached
			}
		}

		if (!hasLock) {
			cached = await getCache<T>(...tags)

			if (cached !== undefined) return cached

			return await fetcher()
		}

		try {
			const freshData = await fetcher()

			await setCache(freshData, life, ...tags)

			return freshData
		} finally {
			await redis.releaseLock(lockKey, lockValue)
		}
	})()

	inflightRequests.set(dataKey, operationPromise)

	try {
		return await operationPromise
	} finally {
		inflightRequests.delete(dataKey)
	}
}