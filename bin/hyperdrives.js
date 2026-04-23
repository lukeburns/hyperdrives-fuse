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
const os = require('os')
const { spawn } = require('child_process')

const fuse = p.join(__dirname, 'hyperdrives-fuse.js')
const defaultMount = p.join(os.homedir(), 'Hyperdrives')

/**
 * @returns {{ out: string[], incomplete: boolean, unknown: boolean }}
 */
function mountInfo (args) {
  const out = []
  let incomplete = false
  let unknown = false
  for (let i = 0; i < args.length; i++) {
    const a = args[i]
    if (a === '-s' || a === '--storage') {
      const v = args[i + 1]
      if (v == null || v.startsWith('-')) {
        incomplete = true
        break
      }
      i++
      continue
    }
    if (a === '-d' || a === '--debug' || a === '--no-tui' || a === '--no-swarm' || a === '-h' || a === '--help') {
      continue
    }
    if (a.startsWith('-')) {
      unknown = true
      break
    }
    out.push(a)
  }
  return { out, incomplete, unknown }
}

function help () {
  process.stdout.write(`\
Usage: hyperdrives [options] [mountpoint]

  Runs: hyperdrives-fuse mount [options] [mountpoint]
  Default mountpoint if omitted: ${defaultMount}

  Options: same as hyperdrives-fuse mount (-s, --no-swarm, --no-tui, -d, --help)
  New drives at the volume root use the same label rules as hyperdrives-fuse (see its help).

  For add, list, remove, unmount, and other commands use: hyperdrives-fuse
`)
}

const raw = process.argv.slice(2)

if (raw[0] === 'help' || raw[0] === '-h' || raw[0] === '--help') {
  help()
  process.exit(0)
}

const other = new Set(['add', 'a', 'list', 'ls', 'l', 'remove', 'rm', 'r', 'unmount', 'u', 'umount', 'version', '-V', '--version'])
if (raw[0] && other.has(raw[0])) {
  process.stderr.write(
    'hyperdrives only runs mount. Use: hyperdrives-fuse ' + raw[0] + ' …\n'
  )
  process.exit(1)
}

let args = raw
if (args[0] === 'mount') {
  args = args.slice(1)
}

const { out: pos, incomplete, unknown } = mountInfo(args)
if (pos.length > 1) {
  process.stderr.write(
    'hyperdrives: only one <mountpoint> is allowed. See: hyperdrives --help\n'
  )
  process.exit(1)
}

if (pos.length === 0 && !incomplete && !unknown) {
  args = args.concat(defaultMount)
}

const child = spawn(process.execPath, [fuse, 'mount', ...args], {
  stdio: 'inherit'
})
child.on('exit', (code, signal) => {
  if (signal) {
    process.exit(1)
  }
  process.exit(typeof code === 'number' ? code : 0)
})
child.on('error', (err) => {
  process.stderr.write(String(err && err.message ? err.message : err) + '\n')
  process.exit(1)
})
