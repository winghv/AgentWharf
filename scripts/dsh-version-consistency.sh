#!/bin/sh
set -eu

runtime_dir=${1:?usage: dsh-version-consistency.sh RUNTIME_DIR [EXPECTED_VERSION]}
expected_version=${2:-${AGENTWHARF_DSH_VERSION:-0.1.2-rc.1}}

[ -d "$runtime_dir" ] || { printf 'missing DSH runtime directory: %s\n' "$runtime_dir" >&2; exit 1; }
command -v node >/dev/null 2>&1 || { printf 'missing required command: node\n' >&2; exit 1; }

node - "$runtime_dir" "$expected_version" <<'NODE'
const fs = require('node:fs')
const path = require('node:path')

const root = fs.realpathSync(path.resolve(process.argv[2]))
const expected = process.argv[3]
const required = [
  '@deepseek-ai/dsh',
  '@deepseek-ai/dsh-agent',
  '@deepseek-ai/dsh-agent-loop',
  '@deepseek-ai/dsh-tools',
  '@deepseek-ai/dsh-code-runtime',
  '@deepseek-ai/dsh-session',
  '@deepseek-ai/dsh-llm',
]
const packages = new Map()

function packageInfo(directory) {
  const manifestPath = path.join(directory, 'package.json')
  try {
    const manifest = JSON.parse(fs.readFileSync(manifestPath, 'utf8'))
    if (typeof manifest.name === 'string' && typeof manifest.version === 'string') {
      return { ...manifest, directory }
    }
  } catch (_) {
    // Ignore directories without a package manifest.
  }
  return undefined
}

function visitNodeModules(directory) {
  if (!fs.existsSync(directory)) return
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    if (!entry.isDirectory() && !entry.isSymbolicLink()) continue
    const entryPath = path.join(directory, entry.name)
    if (entry.name.startsWith('@')) {
      if (!fs.existsSync(entryPath)) continue
      for (const scoped of fs.readdirSync(entryPath, { withFileTypes: true })) {
        if (!scoped.isDirectory() && !scoped.isSymbolicLink()) continue
        const packageDirectory = path.join(entryPath, scoped.name)
        const info = packageInfo(packageDirectory)
        if (info?.name.startsWith('@deepseek-ai/dsh')) packages.set(packageDirectory, info)
        visitNodeModules(path.join(packageDirectory, 'node_modules'))
      }
      continue
    }
    const info = packageInfo(entryPath)
    if (info?.name.startsWith('@deepseek-ai/dsh')) packages.set(entryPath, info)
    visitNodeModules(path.join(entryPath, 'node_modules'))
  }
}

function resolvedPackage(name) {
  const manifestPath = require.resolve(`${name}/package.json`, { paths: [root] })
  const info = packageInfo(path.dirname(manifestPath))
  if (info?.name === name) return info
  throw new Error(`resolved package manifest has an unexpected name for ${name}: ${manifestPath}`)
}

for (const name of required) {
  const info = resolvedPackage(name)
  if (info.version !== expected) {
    throw new Error(`${name} resolved to ${info.version}; expected ${expected}`)
  }
}

visitNodeModules(path.join(root, 'node_modules'))
for (const info of packages.values()) {
  if (info.version !== expected) {
    throw new Error(`${info.name} at ${info.directory} resolved to ${info.version}; expected ${expected}`)
  }
}

console.log(`dsh_version_consistent expected=${expected} packages=${packages.size}`)
NODE
