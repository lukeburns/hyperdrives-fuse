const z32 = require('z32')

const Z32_LEN = 52
const LABEL_MAX = 200

/**
 * @param {string} s
 */
function isValidLabelSegment (s) {
  if (typeof s !== 'string' || s.length === 0 || s.length > LABEL_MAX) {
    return false
  }
  if (s.indexOf('/') >= 0 || s.indexOf('\0') >= 0) return false
  if (/[\n\r]/.test(s)) return false
  return true
}

/**
 * A valid z32 segment decodes to 32 bytes. Charset is the holepunch "z32" (not b32-only).
 * @param {string} s
 */
function isValidZ32 (s) {
  if (typeof s !== 'string' || s.length !== Z32_LEN) return false
  try {
    const k = z32.decode(s)
    return k && k.length === 32
  } catch {
    return false
  }
}

/**
 * `label.` + 52 z32 chars. The public key is always exactly 52 z32 after the final delimiter.
 * (z32 allows digits/letters; do not over-restrict the charset in a regex.)
 * @param {string} name
 * @returns {{ label: string, key: Buffer, keyZ32: string } | null}
 */
function tryParseDashed (name) {
  if (name.length < Z32_LEN + 2) return null
  if (name[name.length - Z32_LEN - 1] !== '.') return null
  const keyZ32 = name.slice(-Z32_LEN)
  const label = name.slice(0, name.length - Z32_LEN - 1)
  if (!label || !isValidLabelSegment(label)) return null
  let key
  try {
    key = z32.decode(keyZ32)
  } catch {
    return null
  }
  if (!key || key.length !== 32) return null
  return { label, key, keyZ32 }
}

/**
 * Legacy: "label" + "#" + z32
 * @param {string} name
 * @returns {{ label: string, key: Buffer, keyZ32: string } | null}
 */
function tryParseHash (name) {
  const i = name.lastIndexOf('#')
  if (i <= 0) return null
  const label = name.slice(0, i)
  const keyZ32 = name.slice(i + 1)
  if (!label || label.indexOf('#') >= 0) return null
  if (!isValidLabelSegment(label)) return null
  if (!isValidZ32(keyZ32)) return null
  let key
  try {
    key = z32.decode(keyZ32)
  } catch {
    return null
  }
  if (!key || key.length !== 32) return null
  return { label, key, keyZ32 }
}

/**
 * @param {string} name
 * @returns {{ label: string, key: Buffer, keyZ32: string } | null}
 */
function tryParseFolderName (name) {
  return tryParseDashed(name) || tryParseHash(name) || null
}

/**
 * Same structural rule as `tryParseDashed` (label, '.', 52 z32 chars) before decode.
 * @param {string} name
 */
function isDashedKeyShaped (name) {
  if (name.length < Z32_LEN + 2) return false
  if (name[name.length - Z32_LEN - 1] !== '.') return false
  const label = name.slice(0, name.length - Z32_LEN - 1)
  if (!label || !isValidLabelSegment(label)) return false
  return true
}

/**
 * Same structural rule as `tryParseHash` (label#52chars) before decode validity.
 * @param {string} name
 */
function isHashKeyShaped (name) {
  const i = name.lastIndexOf('#')
  if (i <= 0) return false
  const label = name.slice(0, i)
  const keyZ32 = name.slice(i + 1)
  if (label.indexOf('#') >= 0) return false
  if (!isValidLabelSegment(label)) return false
  if (keyZ32.length !== Z32_LEN) return false
  return true
}

/**
 * String looks like a full canonical `label.z32` / `label#z32` but does not parse
 * (e.g. invalid z32 in the key segment). Reject for plain renames so we do not
 * store a name that is visually indistinguishable from a keyed folder.
 * @param {string} name
 */
function isAmbiguousKeyForm (name) {
  if (tryParseFolderName(name)) return false
  return isDashedKeyShaped(name) || isHashKeyShaped(name)
}

function formatFolderName (label, key) {
  return `${label}.${z32.encode(key)}`
}

/**
 * A single path segment the user can mkdir (before it becomes "label.z32" in the listing).
 * Must not be parseable as a full folder name (dashed or legacy hash).
 */
function isPlainLabel (name) {
  if (!name || tryParseFolderName(name)) {
    return false
  }
  return isValidLabelSegment(name)
}

module.exports = {
  tryParseFolderName,
  tryParseDashed,
  tryParseHash,
  formatFolderName,
  isValidZ32,
  isPlainLabel,
  isValidLabelSegment,
  isDashedKeyShaped,
  isHashKeyShaped,
  isAmbiguousKeyForm,
  LABEL_MAX
}
