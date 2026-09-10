import assert from 'node:assert/strict'
import test from 'node:test'
import { decodePacketUTF8 } from '../src/experimental/packet.js'

test('native and fallback UTF-8 decoding agree and reject malformed input', () => {
  const original = Object.getOwnPropertyDescriptor(globalThis, 'TextDecoder')!
  const valid = ['plain', '\u4f60\u597d', '\ud83d\ude00', '\ufeffliteral', '\u0000']
  const invalid = [[0xff], [0xc0, 0xaf], [0xed, 0xa0, 0x80], [0xf4, 0x90, 0x80, 0x80], [0xe2, 0x82]]
  try {
    for (const fallback of [false, true]) {
      if (fallback) Object.defineProperty(globalThis, 'TextDecoder', { ...original, value: undefined })
      for (const value of valid) assert.equal(decodePacketUTF8(new TextEncoder().encode(value)), value)
      for (const bytes of invalid) assert.throws(() => decodePacketUTF8(Uint8Array.from(bytes)))
    }
  } finally { Object.defineProperty(globalThis, 'TextDecoder', original) }
})
