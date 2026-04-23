'use strict'

const os = require('os')
const p = require('path')
const z32 = require('z32')

const INTERVAL_MS = 900

/**
 * @param {object} o
 * @param {import('./registry').Registry} o.registry
 * @param {import('hyperswarm') | null} o.swarm
 * @param {boolean} o.noSwarm
 * @param {string} o.mountPath
 * @param {string} o.storage
 * @param {string} o.name
 * @param {string} o.version
 */
function createMountTui (o) {
  if (!process.stderr.isTTY) {
    return { start () {}, stop () {}, bump () {} }
  }

  const { registry, noSwarm } = o
  let timer = null
  let onUpdate = null
  let onWinch = null
  let stopped = false

  const countTopics = (swarm) => {
    if (!swarm) return 0
    let n = 0
    try {
      for (const _ of swarm.topics()) n++
    } catch {
      // ignore
    }
    return n
  }

  const shortPath = (abs) => {
    const h = os.homedir()
    if (abs.startsWith(h + p.sep) || abs === h) {
      return '~' + (abs.length > h.length ? abs.slice(h.length) : '')
    }
    return abs
  }

  const draw = () => {
    if (stopped) return
    try {
      registry.load()
    } catch {
      // ignore; keep last good view
    }
    const mountShort = shortPath(p.resolve(o.mountPath))
    const storeShort = shortPath(p.resolve(o.storage))
    const folders = registry.listFolderNames()

    const lines = []
    lines.push(`\x1b[1m${o.name}\x1b[0m v${o.version}  (Ctrl+C unmount)\x1b[K`)
    lines.push(`\x1b[90mMount\x1b[0m   ${mountShort}\x1b[K`)
    lines.push(`\x1b[90mStore\x1b[0m   ${storeShort}\x1b[K`)
    lines.push('')

    lines.push(
      `\x1b[1mDrives\x1b[0m  ${folders.length}\x1b[K`
    )
    for (const folder of folders) {
      const ent = registry.getByFolderName(folder)
      if (!ent) continue
      const label = ent.label
      const fullZ = ent.keyZ32
      lines.push(`  \x1b[36m${label}\x1b[0m - ${fullZ}\x1b[K`)
    }
    if (folders.length === 0) {
      lines.push('  \x1b[90m(none)\x1b[0m\x1b[K')
    }

    lines.push('')
    if (noSwarm) {
      lines.push('\x1b[1mSwarm\x1b[0m   \x1b[33moff\x1b[0m (--no-swarm)\x1b[K')
      lines.push(
        `\x1b[90mKey\x1b[0m   \x1b[90m—\x1b[0m  (--no-swarm)\x1b[K`
      )
    } else {
      const swarm = o.swarm
      if (swarm && !swarm.destroyed) {
        const conn = swarm.connections ? swarm.connections.size : 0
        const pending = typeof swarm.connecting === 'number' ? swarm.connecting : 0
        const topicN = countTopics(swarm)
        const st = swarm.stats && swarm.stats.connects
        const att = st && st.client && typeof st.client.attempted === 'number' ? st.client.attempted : 0
        lines.push(
          `\x1b[1mSwarm\x1b[0m   ${conn} connected  ·  ${topicN} topics  ·  ${pending} connecting  ·  ${att} connect attempts\x1b[K`
        )
        const pk = swarm.keyPair && swarm.keyPair.publicKey
        const swarmZ32 = pk && pk.length ? z32.encode(pk) : '—'
        lines.push(`\x1b[90mKey\x1b[0m   ${swarmZ32}\x1b[K`)
      } else {
        lines.push('\x1b[1mSwarm\x1b[0m   starting…\x1b[K')
        lines.push(
          `\x1b[90mKey\x1b[0m   \x1b[90m—\x1b[0m\x1b[K`
        )
      }
    }

    const block = lines.join('\n') + '\n'
    process.stderr.write('\x1b[3J\x1b[H\x1b[2J' + block)
  }

  return {
    start () {
      draw()
      timer = setInterval(draw, INTERVAL_MS)
      const sw = o.swarm
      if (sw) {
        onUpdate = () => {
          draw()
        }
        sw.on('update', onUpdate)
        sw.on('connection', onUpdate)
      }
      onWinch = () => {
        draw()
      }
      process.on('SIGWINCH', onWinch)
    },
    stop () {
      if (stopped) return
      stopped = true
      if (timer) {
        clearInterval(timer)
        timer = null
      }
      if (onWinch) {
        process.removeListener('SIGWINCH', onWinch)
        onWinch = null
      }
      const sw = o.swarm
      if (sw && onUpdate) {
        try {
          sw.removeListener('update', onUpdate)
          sw.removeListener('connection', onUpdate)
        } catch {
          // ignore
        }
      }
      onUpdate = null
    },
    bump () {
      if (!stopped) draw()
    }
  }
}

module.exports = { createMountTui, INTERVAL_MS }
