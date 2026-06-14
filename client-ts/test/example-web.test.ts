import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { test } from 'node:test'

const here = dirname(fileURLToPath(import.meta.url))
const exampleDir = resolve(here, '../../../examples/web')

test('example web UI is wired to the built client SDK', async () => {
  const [html, app] = await Promise.all([
    readFile(resolve(exampleDir, 'index.html'), 'utf8'),
    readFile(resolve(exampleDir, 'app.js'), 'utf8'),
  ])

  for (const id of ['hub-url', 'token', 'session-id', 'connect', 'events', 'message', 'send']) {
    assert.match(html, new RegExp(`id="${id}"`))
  }
  assert.match(app, /from ['"]\.\.\/\.\.\/client-ts\/dist\/src\/index\.js['"]/)
  assert.match(app, /new AgentWharfClient/)
  assert.match(app, /sendMessage/)
})
