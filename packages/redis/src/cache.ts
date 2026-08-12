import type Redis from "ioredis"

import { createHash, randomUUID } from "crypto"
import { serializer } from "./serializer"

declare module "ioredis" {
	interface Redis {
		releaseLock(key: string, value: string): Promise<number>
	}
}

type CreateCacheOptions = { redis: Redis }
type CacheLife = "seconds" | "minutes" | "hours" | "days" | "weeks" | "max"

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

const INFLIGHT_REQUESTS = new Map<string, Promise<unknown>>()
const UNDEFINED_CACHE_MARKER = "__VAL_IS_UNDEFINED__"
const RELEASE_LOCK_LUA = `
  if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
  else
    return 0
  end
`
/**
 * Creates an isolated high-performance Redis caching engine instance.
 * Distributes decoupled core methods: `cache`, `getCache`, `setCache`, `invalidateCache`.
 *
 * @param {CreateCacheOptions} options - Configuration object containing the `ioredis` instance.
 * @returns An object with high-throughput cache primitives.
 */
export const createCache = (options: CreateCacheOptions) => {
	const { redis } = options

	if (
		typeof redis.defineCommand === "function" &&
		!redis.releaseLock
	) {
		redis.defineCommand("releaseLock", {
			numberOfKeys: 1,
			lua: RELEASE_LOCK_LUA,
		})
	}

	/**
	 * Retrieves data from the cache using the specified tags.
	 * Returns `undefined` if a cache miss occurs.
	 */
	const getCache = async <T>(...tags: string[]): Promise<T | undefined> => {
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
	 * Saves data into the cache with a defined lifespan and associated tags.
	 */
	const setCache = async (value: unknown, life: CacheLife, ...tags: string[]): Promise<"OK" | undefined> => {
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
	 * Purges all cache entries and data linked to the specified tags.
	 */
	const invalidateCache = async (...tags: string[]): Promise<boolean> => {
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
	 * Fetches data from the cache or executes the fallback fetcher (with built-in stampede protection).
	 */
	const cache = async <T>(
		fetcher: () => Promise<T>,
		life: CacheLife,
		...tags: string[]
	): Promise<T> => {
		if (tags.length === 0) {
			throw new Error("Caching requires passing at least one tag in options.tags")
		}

		const dataKey = generateDataKey(tags)

		if (INFLIGHT_REQUESTS.has(dataKey)) {
			return INFLIGHT_REQUESTS.get(dataKey) as Promise<T>
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

		INFLIGHT_REQUESTS.set(dataKey, operationPromise)

		try {
			return await operationPromise
		} finally {
			INFLIGHT_REQUESTS.delete(dataKey)
		}
	}

	return {
		getCache,
		setCache,
		cache,
		invalidateCache,
	}
}