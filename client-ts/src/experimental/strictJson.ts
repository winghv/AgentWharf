// Bounded JSON decoding for untrusted encrypted containers. JSON.parse alone
// silently accepts duplicate object members, which is unsafe across runtimes.
export function decodeStrictJson(text: string, maxBytes = 48 * 1024): unknown {
  function invalid(): never { throw new Error('invalid encrypted JSON') }
  if (typeof text !== 'string' || text.length > maxBytes || new TextEncoder().encode(text).length > maxBytes) invalid()
  let offset = 0
  let nodes = 0
  function whitespace(): void {
    while (offset < text.length && (text[offset] === ' ' || text[offset] === '\n' || text[offset] === '\r' || text[offset] === '\t')) offset++
  }
  function string(): string {
    const start = offset++
    while (offset < text.length) {
      const char = text[offset++]
      if (char === '"') {
        try { return JSON.parse(text.slice(start, offset)) as string } catch { return invalid() }
      }
      if (char === '\\') offset++
    }
    return invalid()
  }
  function value(depth: number): unknown {
    if (depth > 64 || ++nodes > 8192) invalid()
    whitespace()
    const char = text[offset]
    if (char === '"') return string()
    if (char === '{') {
      offset++
      const result: Record<string, unknown> = {}
      whitespace()
      if (text[offset] === '}') { offset++; return result }
      for (;;) {
        whitespace()
        if (text[offset] !== '"') invalid()
        const key = string()
        if (Object.hasOwn(result, key)) invalid()
        whitespace()
        if (text[offset++] !== ':') invalid()
        Object.defineProperty(result, key, { value: value(depth + 1), enumerable: true, writable: true, configurable: true })
        whitespace()
        const separator = text[offset++]
        if (separator === '}') return result
        if (separator !== ',') invalid()
      }
    }
    if (char === '[') {
      offset++
      const result: unknown[] = []
      whitespace()
      if (text[offset] === ']') { offset++; return result }
      for (;;) {
        result.push(value(depth + 1))
        whitespace()
        const separator = text[offset++]
        if (separator === ']') return result
        if (separator !== ',') invalid()
      }
    }
    const start = offset
    while (offset < text.length && !/[\s,\]}]/.test(text[offset])) offset++
    const token = text.slice(start, offset)
    if (!/^(?:true|false|null|-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?)$/.test(token)) invalid()
    const parsed: unknown = JSON.parse(token)
    if (typeof parsed === 'number' && !Number.isFinite(parsed)) invalid()
    return parsed
  }
  try {
    const result = value(0)
    whitespace()
    if (offset !== text.length) invalid()
    return result
  } catch { return invalid() }
}
