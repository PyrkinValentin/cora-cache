import { pack, unpack } from "msgpackr"

export const serializer = {
	serialize(value: unknown): Buffer {
		if (Buffer.isBuffer(value)) {
			return value
		}

		try {
			return pack(value)
		} catch (error) {
			throw error
		}
	},
	deserialize<T>(data: Buffer | Uint8Array | null | undefined): T | undefined {
		if (
			data === null ||
			data === undefined ||
			data.length === 0
		) {
			return undefined
		}

		try {
			return unpack(data) as T
		} catch {
			return undefined
		}
	}
} as const