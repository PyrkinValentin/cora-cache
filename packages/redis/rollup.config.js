import resolve from "@rollup/plugin-node-resolve"
import typescript from "@rollup/plugin-typescript"
import terser from "@rollup/plugin-terser"
import pkg from "./package.json" with { type: "json" }

const externalDependencies = [
	"crypto",
	...Object.keys(pkg.peerDependencies || {}),
	...Object.keys(pkg.dependencies || {}),
]

export default {
	input: "src/index.ts",
	external: (id) => {
		if (
			id === "src/index.ts" ||
			id.startsWith(".") ||
			id.startsWith("/") ||
			id.includes("src/") ||
			id.includes("build/")
		) {
			return false
		}

		return externalDependencies.some((dep) => id === dep || id.startsWith(`${dep}/`))
	},
	output: [
		{
			preserveModules: true,
			preserveModulesRoot: "src",
			dir: "build",
			format: "esm",
			entryFileNames: "[name].js",
		}
	],
	plugins: [
		resolve(),
		typescript({
			tsconfig: "./tsconfig.json",
			declaration: true,
			rootDir: "src",
			declarationDir: "build",
			compilerOptions: {
				outDir: "build",
				declarationMap: false,
			},
		}),
		terser(),
	],
}