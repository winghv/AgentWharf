import assert from 'node:assert/strict'
import test from 'node:test'
import { decodeStrictJson } from '../src/experimental/strictJson.js'

test('strict JSON preserves values without prototype assignment', () => {
  const wire = '{"__proto__":{"polluted":true},"a":[true,false,null,-1.2e3,"escaped\\\"\\\\"],"unicode":"你好"}'
  const result = decodeStrictJson(wire)
  assert.equal(JSON.stringify(result), JSON.stringify(JSON.parse(wire)))
  assert.equal(Object.getPrototypeOf(result), Object.prototype)
  assert.ok(Object.hasOwn(result as object, '__proto__'))
  assert.equal(({} as Record<string, unknown>).polluted, undefined)
})
test('strict JSON rejects duplicate, malformed and unbounded input', () => {
  for (const wire of [
    '{"x":1,"x":2}', '{"x":1,"\\u0078":2}', '{"a":{"x":1,"x":2}}',
    '{"a":1} {}', '[1,]', '{"a":1,}', '01', '+1', 'NaN', '1e999',
    '"bad\\x"', '"unclosed', '\u000bnull', '[[', '',
    '['.repeat(66) + '0' + ']'.repeat(66), '[ ' + '0,'.repeat(8192) + '0]',
  ]) assert.throws(() => decodeStrictJson(wire), /invalid encrypted JSON/)
  assert.throws(() => decodeStrictJson('"你好"', 7))
})
