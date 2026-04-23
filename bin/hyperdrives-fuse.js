#!/usr/bin/env node
'use strict'

;(() => {
  const a = process.argv
  for (let i = 0; i < a.length; i++) {
    if (a[i] === '--debug' || a[i] === '-d') {
      const m = 'hyperdrives-fuse'
      process.env.DEBUG = process.env.DEBUG ? process.env.DEBUG + ',' + m : m
      break
    }
  }
})()

const p = require('path')
const fs = require('fs')
const os = require('os')

const Corestore = require('corestore')
const Hyperdrive = require('hyperdrive')
const { discoveryKey } = require('hypercore-crypto')
const z32 = require('z32')
const b4a = require('b4a')

const { HyperdrivesFuse, isConfigured, unmount: fuseUnmount } = require('..')
const { Registry } = require('../lib/registry')
const { DRIVE_ROOT_NS } = require('../lib/constants')

const name = 'hyperdrives-fuse'
const version = readPkgVersion()
const isDarwin = process.platform === 'darwin'

function readPkgVersion () {
  try {
    const j = JSON.parse(
      fs.readFileSync(p.join(__dirname, '..', 'package.json'), 'utf8')
    )
    return j.version || '0.0.0'
  } catch {
    return '0.0.0'
  }
}

function die (code, msg) {
  if (msg) {
    process.stderr.write(msg + (msg.endsWith('\n') ? '' : '\n'))
  }
  process.exit(typeof code === 'number' ? code : 1)
}

function help () {
  process.stdout.write(`\
${name} v${version}

One FUSE mount: each drive lives in its own Corestore namespace
(corestore.namespace('hyperdrives-fuse-drives').namespace('<label>')), and appears as
<label>-<z32-52> in the root listing. You can mkdir <label> at the volume root to create
a drive, or use the add command; ls shows the final name with the public key.

Commands:
  ${name} mount <mountpoint> [options]   Start FUSE (multi-drive)
  ${name} add <label> [options]          Create a new drive and register it (or import with -k)
  ${name} list [options]               Print registered drives (label + folder name)
  ${name} remove <label#z32> [options]  Unregister a drive (storage must be consistent with -s)
  ${name} unmount <mountpoint>         Unmount
  ${name} help
  ${name} version

Options (where applicable):
  -d, --debug            FUSE op logging
  -s, --storage <path>   Corestore directory (default: ~/.hyperdrives-fuse)
  --no-swarm             Do not use Hyperswarm (local only)
  -k, --key <z32>        (add) register an existing public key instead of a new drive

add/remove/list read and write the registry file next to the corestore
(<storage>/hyperdrives-fuse-registry.json). Use the same -s for mount, add, list, and remove.
While the filesystem is mounted, prefer not to run add/remove on the same storage from another
process; unmount first to avoid a stale directory listing until remount.
`)
}

function defaultStorage () {
  return p.join(os.homedir(), '.hyperdrives-fuse')
}

function openRegistry (storage) {
  fs.mkdirSync(storage, { recursive: true })
  const r = new Registry(storage)
  r.load()
  return r
}

function parseWithStorage (raw) {
  let storage = defaultStorage()
  const pos = []
  for (let i = 0; i < raw.length; i++) {
    const a = raw[i]
    if (a === '-s' || a === '--storage') {
      const v = raw[++i]
      if (v == null) die(1, 'Missing value for ' + a)
      storage = p.resolve(v)
    } else if (a === '-d' || a === '--debug') {
    } else if (a === '-h' || a === '--help') {
      help()
      process.exit(0)
    } else if (a.startsWith('-')) {
      die(1, `Unknown option: ${a}\nRun "${name} help" for usage.`)
    } else {
      pos.push(a)
    }
  }
  return { storage, pos }
}

function parseAdd (raw) {
  let storage = defaultStorage()
  let keyStr = null
  const pos = []
  for (let i = 0; i < raw.length; i++) {
    const a = raw[i]
    if (a === '-s' || a === '--storage') {
      const v = raw[++i]
      if (v == null) die(1, 'Missing value for ' + a)
      storage = p.resolve(v)
    } else if (a === '-k' || a === '--key') {
      const v = raw[++i]
      if (v == null) die(1, 'Missing value for ' + a)
      keyStr = v
    } else if (a === '-d' || a === '--debug') {
    } else if (a === '-h' || a === '--help') {
      help()
      process.exit(0)
    } else if (a.startsWith('-')) {
      die(1, `Unknown option: ${a}\nRun "${name} help" for usage.`)
    } else {
      pos.push(a)
    }
  }
  return { storage, keyStr, pos }
}

async function cmdAdd (rest) {
  const o = parseAdd(rest)
  if (!o.pos[0]) {
    die(1, 'add: missing <label>\nRun "' + name + ' help" for usage.')
  }
  if (o.pos.length > 1) {
    die(1, 'add: only one <label> is allowed (use a label without spaces, or quote it).')
  }
  const label = o.pos[0]
  const r = openRegistry(o.storage)
  const store = new Corestore(o.storage)
  const driveStore = store.namespace(DRIVE_ROOT_NS).namespace(label)

  if (o.keyStr) {
    let key
    try {
      key = z32.decode(o.keyStr)
    } catch (e) {
      await store.close()
      die(1, 'Invalid z32: ' + (e && e.message))
    }
    if (!key || key.length !== 32) {
      await store.close()
      die(1, 'Invalid z32 key length after decode')
    }
    const drive = new Hyperdrive(driveStore, key)
    await drive.ready()
    const out = r.addNew(label, drive.key)
    if (out.err) {
      await drive.close()
      await store.close()
      die(1, out.err)
    }
    await drive.close()
    process.stdout.write('Registered (import): ' + out.folder + '\n')
    await store.close()
    return
  }

  const drive = new Hyperdrive(driveStore)
  await drive.ready()
  const out = r.addNew(label, drive.key)
  if (out.err) {
    await drive.close()
    await store.close()
    die(1, out.err)
  }
  await drive.close()
  process.stdout.write('Registered: ' + out.folder + '\n  Public key (z32): ' + z32.encode(drive.key) + '\n')
  await store.close()
}

function cmdList (rest) {
  const o = parseWithStorage(rest)
  const r = openRegistry(o.storage)
  const list = r.listFolderNames()
  for (const f of list) {
    const ent = r.getByFolderName(f)
    if (ent) {
      process.stdout.write(`${ent.label}\t${f}\n`)
    } else {
      process.stdout.write(`${f}\n`)
    }
  }
}

function cmdRemove (rest) {
  const o = parseWithStorage(rest)
  const folder = o.pos[0]
  if (!folder) {
    die(1, 'remove: missing <label#z32>\nRun "' + name + ' help" for usage.')
  }
  if (o.pos.length > 1) {
    die(1, 'remove: pass a single argument <label#z32>')
  }
  const r = openRegistry(o.storage)
  const out = r.removeByFolderName(folder)
  if (out.err) {
    die(1, out.err)
  }
  process.stdout.write('Unregistered: ' + folder + '\n')
}

function parseMountArgs (raw) {
  let storage = defaultStorage()
  let noSwarm = false
  const pos = []
  for (let i = 0; i < raw.length; i++) {
    const a = raw[i]
    if (a === '-s' || a === '--storage') {
      const v = raw[++i]
      if (v == null) die(1, 'Missing value for ' + a)
      storage = p.resolve(v)
    } else if (a === '--no-swarm') {
      noSwarm = true
    } else if (a === '-d' || a === '--debug') {
    } else if (a === '-h' || a === '--help') {
      help()
      process.exit(0)
    } else if (a.startsWith('-')) {
      die(1, `Unknown option: ${a}\nRun "${name} help" for usage.`)
    } else {
      pos.push(a)
    }
  }
  if (pos.length < 1) {
    die(1, 'mount: missing <mountpoint>\nRun "' + name + ' help" for usage.')
  }
  if (pos.length > 1) {
    die(1, 'mount: only one <mountpoint> is allowed.')
  }
  return { storage, noSwarm, mountPath: p.resolve(pos[0]) }
}

async function cmdMount (rest) {
  const { storage, noSwarm, mountPath } = parseMountArgs(rest)

  fs.mkdirSync(storage, { recursive: true })
  const registry = new Registry(storage)
  registry.load()

  if (isDarwin) {
    await new Promise((resolve) => {
      isConfigured((err, ok) => {
        if (!err && !ok) {
          process.stderr.write(
            'Warning: FUSE may not be configured. On macOS see @zkochan/fuse-native (e.g. fuse-native configure)\n'
          )
        }
        resolve()
      })
    })
  }

  const store = new Corestore(storage)

  let swarm = null
  const joinSwarmForDriveKey = (key) => {
    if (!swarm) return
    const topic = discoveryKey(key)
    swarm.join(topic, { server: true, client: true })
  }

  if (!noSwarm) {
    const Hyperswarm = require('hyperswarm')
    swarm = new Hyperswarm()
    swarm.on('connection', (conn) => {
      store.replicate(conn)
    })
    for (const ent of registry.listEntries()) {
      const key = b4a.from(ent.k, 'hex')
      try {
        joinSwarmForDriveKey(key)
      } catch (e) {
        process.stderr.write(
          'Warning: Hyperswarm join failed for a drive: ' + (e && e.message ? e.message : e) + '\n'
        )
      }
    }
    void swarm.flush()
  }

  const swarmTopicHooks = !noSwarm && swarm
    ? {
        onDriveAdded (key) {
          try {
            joinSwarmForDriveKey(key)
            void swarm.flush()
          } catch (e) {
            process.stderr.write(
              'Warning: Hyperswarm join (new drive at runtime): ' + (e && e.message ? e.message : e) + '\n'
            )
          }
        },
        onDriveRemoved (key) {
          const topic = discoveryKey(key)
          void swarm.leave(topic).catch(() => {})
        }
      }
    : {}

  const fuse = new HyperdrivesFuse(store, mountPath, {
    registry,
    ...swarmTopicHooks
  })

  let result
  try {
    result = await fuse.mount()
  } catch (e) {
    if (swarm) {
      try {
        await swarm.destroy()
      } catch {
        // ignore
      }
    }
    try {
      await store.close()
    } catch {
      // ignore
    }
    die(1, 'Mount failed: ' + (e && e.message ? e.message : e))
  }

  const folders = registry.listFolderNames()
  const swarmLine = noSwarm
    ? '  Hyperswarm: disabled (--no-swarm)\n'
    : '  Hyperswarm: DHT P2P enabled for each registered drive\n'

  process.stderr.write(
    `${name} mounted\n` +
    `  Mount:     ${result.mnt}\n` +
    `  Storage:   ${storage}\n` +
    `  Registry:  ${registry.filePath}\n` +
    `  Drives:    ${folders.length} (${folders.join(', ') || 'none — use "${name} add"'})\n` +
    swarmLine +
    `  Node PID:  ${process.pid} (keep running; Ctrl+C to unmount.)\n`
  )

  const shutdown = async (signal) => {
    if (signal) {
      process.stderr.write(`\n${signal} received, unmounting…\n`)
    }
    if (swarm) {
      try {
        await swarm.destroy()
      } catch {
        // ignore
      }
    }
    try {
      await fuse.unmount()
    } catch (e) {
      process.stderr.write('Unmount: ' + (e && e.message) + '\n')
    }
    try {
      await store.close()
    } catch {
      // ignore
    }
    process.exit(0)
  }
  process.once('SIGINT', () => {
    shutdown('SIGINT')
  })
  process.once('SIGTERM', () => {
    shutdown('SIGTERM')
  })
}

function cmdUnmount (args) {
  if (args.length < 1) {
    die(1, 'unmount: missing <mountpoint>\nRun "' + name + ' help" for usage.')
  }
  if (args.length > 1) {
    die(1, 'unmount: only one <mountpoint> is allowed.')
  }
  const mnt = p.resolve(args[0])
  fuseUnmount(mnt, (err) => {
    if (err) {
      die(1, 'Unmount failed: ' + (err.message || err))
    }
    process.stderr.write('Unmounted ' + mnt + '\n')
  })
}

function main () {
  const args = process.argv.slice(2)
  if (args.length === 0) {
    help()
    process.exit(0)
  }
  const cmd = args[0]
  if (cmd === 'help' || cmd === '-h' || cmd === '--help') {
    help()
    process.exit(0)
  }
  if (cmd === 'version' || cmd === '-V' || cmd === '--version') {
    process.stdout.write(name + ' v' + version + '\n')
    process.exit(0)
  }
  if (cmd === 'mount' || cmd === 'm') {
    cmdMount(args.slice(1)).catch((e) => {
      process.stderr.write((e && e.stack) || String(e) + '\n')
      process.exit(1)
    })
    return
  }
  if (cmd === 'add' || cmd === 'a') {
    cmdAdd(args.slice(1)).catch((e) => {
      process.stderr.write((e && e.stack) || String(e) + '\n')
      process.exit(1)
    })
    return
  }
  if (cmd === 'list' || cmd === 'ls' || cmd === 'l') {
    try {
      cmdList(args.slice(1))
    } catch (e) {
      process.stderr.write((e && e.stack) || String(e) + '\n')
      process.exit(1)
    }
    return
  }
  if (cmd === 'remove' || cmd === 'rm' || cmd === 'r') {
    try {
      cmdRemove(args.slice(1))
    } catch (e) {
      process.stderr.write((e && e.stack) || String(e) + '\n')
      process.exit(1)
    }
    return
  }
  if (cmd === 'unmount' || cmd === 'u' || cmd === 'umount') {
    cmdUnmount(args.slice(1))
    return
  }
  if (cmd.startsWith('-')) {
    die(1, `Unknown option: ${cmd}\nRun "${name} help" for usage.`)
  }
  process.stderr.write(
    `Unknown command: ${cmd}\nRun "${name} help" for usage.\n`
  )
  process.exit(1)
}

main()
