const fs = require('fs')
const p = require('path')
const b4a = require('b4a')
const z32 = require('z32')
const { formatFolderName, tryParseFolderName } = require('./folder')

const REGISTRY_FILE = 'hyperdrives-fuse-registry.json'

class Registry {
  constructor (dir) {
    this.dir = dir
    this.path = p.join(dir, REGISTRY_FILE)
    this.data = { v: 1, drives: [] }
  }

  get filePath () {
    return this.path
  }

  load () {
    try {
      const raw = fs.readFileSync(this.path, 'utf8')
      const j = JSON.parse(raw)
      if (j && Array.isArray(j.drives)) {
        const v = (j.v | 0) || 1
        this.data = { v, drives: j.drives.filter(isWellFormed) }
      }
    } catch (e) {
      if (e.code === 'ENOENT') {
        this.data = { v: 1, drives: [] }
        return
      }
      throw e
    }
  }

  listEntries () {
    return this.data.drives.slice()
  }

  listFolderNames () {
    return this.data.drives.map((d) => formatFolderName(d.n, b4a.from(d.k, 'hex'))).sort()
  }

  getByFolderName (folderName) {
    const parsed = tryParseFolderName(folderName)
    if (!parsed) return null
    return this.getByKeyAndLabel(parsed.key, parsed.label)
  }

  getByKeyAndLabel (key, label) {
    const hex = b4a.toString(key, 'hex')
    for (const d of this.data.drives) {
      if (d.k === hex && d.n === label) {
        return { label, key, keyZ32: z32.encode(key), hex }
      }
    }
    return null
  }

  getByLabel (label) {
    for (const d of this.data.drives) {
      if (d.n === label) {
        const key = b4a.from(d.k, 'hex')
        return { label: d.n, key, keyZ32: z32.encode(key), hex: d.k }
      }
    }
    return null
  }

  canAdd (label) {
    if (!/^[a-zA-Z0-9._-]{1,200}$/.test(label)) {
      return { ok: false, reason: 'label must be 1–200 of [A-Za-z0-9._-]' }
    }
    if (this.data.drives.some((d) => d.n === label)) {
      return { ok: false, reason: 'a drive with that label already exists' }
    }
    return { ok: true }
  }

  addNew (label, publicKey) {
    const c = this.canAdd(label)
    if (!c.ok) {
      return { err: c.reason }
    }
    const hex = b4a.toString(publicKey, 'hex')
    if (this.data.drives.some((d) => d.k === hex)) {
      return { err: 'that public key is already registered' }
    }
    this.data.drives.push({ n: label, k: hex })
    this._save()
    return { label, key: publicKey, folder: formatFolderName(label, publicKey) }
  }

  addImport (label, publicKey) {
    return this.addNew(label, publicKey)
  }

  removeByFolderName (folderName) {
    let g = this.getByFolderName(folderName)
    if (!g) g = this.getByLabel(folderName)
    if (!g) return { err: 'unknown drive folder' }
    this.data.drives = this.data.drives.filter((d) => d.k !== g.hex)
    this._save()
    return { ok: true, hex: g.hex }
  }

  _save () {
    const tmp = this.path + '.tmp'
    fs.writeFileSync(tmp, JSON.stringify(this.data, null, 0), 'utf8')
    fs.renameSync(tmp, this.path)
  }
}

function isWellFormed (d) {
  return d && typeof d.n === 'string' && typeof d.k === 'string' && isValidZ32ishKey(d)
}

function isValidZ32ishKey (d) {
  if (!/^[0-9a-f]{64}$/i.test(d.k)) return false
  return true
}

/**
 * @param {Registry} reg
 * @param {string} label
 * @param {string} z32str
 */
function importByZ32 (reg, label, z32str) {
  if (!z32str || typeof z32str !== 'string') {
    return { err: 'missing z32 key' }
  }
  let key
  try {
    key = z32.decode(z32str)
  } catch (e) {
    return { err: 'invalid z32: ' + (e && e.message) }
  }
  if (!key || key.length !== 32) {
    return { err: 'invalid z32' }
  }
  return reg.addImport(label, key)
}

module.exports = {
  Registry,
  REGISTRY_FILE,
  importByZ32
}
