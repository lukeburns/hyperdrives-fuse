const p = require('path')
const fs = require('fs')
const os = require('os')

const b4a = require('b4a')
const W_OK = fs.constants.W_OK != null ? fs.constants.W_OK : 2
const EOPNOTSUPP = os.constants.errno.EOPNOTSUPP

/* fs open access mode: O_RDONLY=0, O_WRONLY=1, O_RDWR=2 */
const ACCMODE = 3
const O_WRONLY = (fs.constants && fs.constants.O_WRONLY) || 0o1
const O_RDWR = (fs.constants && fs.constants.O_RDWR) || 0o2
const O_CREAT = (fs.constants && fs.constants.O_CREAT) || 0o100
const O_EXCL = (fs.constants && fs.constants.O_EXCL) || 0o200
const O_TRUNC = (fs.constants && fs.constants.O_TRUNC) || 0o1000
const O_APPEND = (fs.constants && fs.constants.O_APPEND) || 0o2000

const unixPathResolve = require('unix-path-resolve')
const fsConstants = require('filesystem-constants')
const { translate, linux } = fsConstants
const hypercoreCrypto = require('hypercore-crypto')
const Fuse = require('fuse-native')
const createPosixAdapter = require('hyperdrive-fuse').createPosixAdapter
const Hyperdrive = require('hyperdrive')
const { tryParseFolderName, formatFolderName, isPlainLabel, isValidLabelSegment, isAmbiguousKeyForm } = require('./lib/folder')
const { DRIVE_ROOT_NS } = require('./lib/constants')
const debug = require('debug')('hyperdrives-fuse')

const platform = os.platform()
const norm = (s) => unixPathResolve('/', s)
const XATTR_DRIVE_KEY = 'user.hyperdrive.key'
const XATTR_DRIVE_KEY_Z32 = 'user.hyperdrive.key.z32'
const XATTR_DRIVE_DISCOVERY_KEY = 'user.hyperdrive.discovery_key'
const DRIVE_ROOT_XATTRS = [XATTR_DRIVE_KEY, XATTR_DRIVE_KEY_Z32, XATTR_DRIVE_DISCOVERY_KEY]

const ROOT_FD_BASE = 0x2f000000
let _rootFdSeq = 0

class HyperdrivesFuse {
  constructor (corestore, mnt, opts = {}) {
    this.corestore = corestore
    this.mnt = p.resolve(mnt)
    this.registry = opts.registry
    if (!this.registry) {
      throw new Error('HyperdrivesFuse: opts.registry (Registry) is required')
    }
    this.opts = opts
    /** @type {((key: Buffer) => void) | undefined} after a new drive is registered (e.g. root mkdir) */
    this._onDriveAdded = typeof opts.onDriveAdded === 'function' ? opts.onDriveAdded : null
    /** @type {((key: Buffer) => void) | undefined} after a drive is unregistered (empty rmdir) */
    this._onDriveRemoved = typeof opts.onDriveRemoved === 'function' ? opts.onDriveRemoved : null
    this._readOnly = opts.readOnly === true
    /** @type {string} */
    this.driveRootNs = typeof opts.driveRootNs === 'string' && opts.driveRootNs
      ? opts.driveRootNs
      : DRIVE_ROOT_NS
    this.fuse = null
    this._driveState = new Map() // key hex -> { raw, posix }
    this._rootFds = new Set()
  }

  _allocRootFd () {
    const fd = ROOT_FD_BASE + (++_rootFdSeq)
    this._rootFds.add(fd)
    return fd
  }

  _isRootFd (fd) {
    return this._rootFds.has(fd)
  }

  _releaseRootFd (fd) {
    this._rootFds.delete(fd)
  }

  /**
   * Corestore session for one drive; hypercores for that drive are namespaced from other labels.
   * @param {string} label
   */
  /** @param {string} storageNs - stable Corestore sub-key (see registry `s`); not always current display `label` */
  _driveStore (storageNs) {
    return this.corestore.namespace(this.driveRootNs).namespace(storageNs)
  }

  /**
   * @returns {{ kind: 'fsRoot' } | { kind: 'enoent' } | { kind: 'drive', folder: string, key: Buffer, keyZ32: string, label: string, storageNs: string, inner: string }}
   */
  splitPath (fspath) {
    const n = norm(fspath)
    if (n === '/') return { kind: 'fsRoot' }
    const segs = n.split('/').filter(Boolean)
    const first = segs[0]
    const inner = segs.length === 1 ? '/' : norm('/' + segs.slice(1).join('/'))
    const parsed = tryParseFolderName(first)
    if (parsed) {
      const ent = this.registry.getByKeyAndLabel(parsed.key, parsed.label)
      if (!ent) {
        return { kind: 'enoent' }
      }
      return {
        kind: 'drive',
        folder: formatFolderName(ent.label, ent.key),
        key: ent.key,
        keyZ32: ent.keyZ32,
        label: ent.label,
        storageNs: ent.storageNs,
        inner
      }
    }
    if (isPlainLabel(first)) {
      const ent = this.registry.getByLabel(first)
      if (!ent) {
        return { kind: 'enoent' }
      }
      return {
        kind: 'drive',
        folder: formatFolderName(ent.label, ent.key),
        key: ent.key,
        keyZ32: ent.keyZ32,
        label: ent.label,
        storageNs: ent.storageNs,
        inner
      }
    }
    return { kind: 'enoent' }
  }

  _ensure (split, cb) {
    if (split.kind !== 'drive') {
      return process.nextTick(() => cb(new Error('not drive segment')))
    }
    const hex = b4a.toString(split.key, 'hex')
    let s = this._driveState.get(hex)
    if (!s) {
      const store = this._driveStore(split.storageNs)
      const raw = new Hyperdrive(store, split.key)
      s = { raw, posix: createPosixAdapter(raw) }
      this._driveState.set(hex, s)
    }
    s.posix.ready((err) => {
      if (err) {
        this._driveState.delete(hex)
        return cb(err)
      }
      cb(null, s)
    })
  }

  _rootNlink () {
    return 2 + this.registry.listFolderNames().length
  }

  _fillStat (st) {
    st.uid = process.getuid()
    st.gid = process.getgid()
    return st
  }

  /**
   * Fresh Hyperdrive root often reports mtime/atime/ctime as epoch 0, which shows as Dec 31 1969 / Jan 1 1970 in GUIs.
   * @param {any} d
   * @returns {Date}
   */
  _coerceMtime (d) {
    if (d == null) return new Date()
    const t = d instanceof Date ? d.getTime() : new Date(d).getTime()
    if (!Number.isFinite(t) || t === 0) return new Date()
    return d instanceof Date ? d : new Date(d)
  }

  /**
   * @param {object} stat
   * @returns {object}
   */
  _fixEpochDirTimes (stat) {
    if (!stat) return stat
    stat.mtime = this._coerceMtime(stat.mtime)
    stat.atime = this._coerceMtime(stat.atime)
    if (stat.ctime != null) stat.ctime = this._coerceMtime(stat.ctime)
    if (stat.birthtime != null) stat.birthtime = this._coerceMtime(stat.birthtime)
    return stat
  }

  getBaseHandlers () {
    const self = this
    const handlers = {}
    const log = this.opts.log || debug

    const rootStat = (cb) => {
      const now = new Date()
      return cb(0, self._fillStat({
        mtime: now,
        atime: now,
        ctime: now,
        size: 4096,
        mode: 16877,
        nlink: self._rootNlink()
      }))
    }

    const errCb = (cb, n) => cb(-(n != null ? n : 1) || Fuse.EPERM)
    const isDriveRootPath = (fspath, sp) => {
      if (sp.kind !== 'drive') return false
      if (sp.inner !== '/') return false
      return norm(fspath).split('/').filter(Boolean).length === 1
    }
    const driveRootXattrValue = (name, sp) => {
      if (name === XATTR_DRIVE_KEY) {
        return Buffer.from(b4a.toString(sp.key, 'hex'))
      }
      if (name === XATTR_DRIVE_KEY_Z32) {
        return Buffer.from(sp.keyZ32)
      }
      if (name === XATTR_DRIVE_DISCOVERY_KEY) {
        return Buffer.from(b4a.toString(hypercoreCrypto.discoveryKey(sp.key), 'hex'))
      }
      return null
    }

    handlers.getattr = function (fspath, cb) {
      log('getattr', fspath)
      const sp = self.splitPath(fspath)
      if (sp.kind === 'fsRoot') {
        return rootStat(cb)
      }
      if (sp.kind === 'enoent') {
        return errCb(cb, 2) // ENOENT
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 2)
        st.posix.lstat(sp.inner, (e2, stat) => {
          if (e2) {
            if (e2.code === 'ENOENT' || (e2.errno != null && e2.errno === 2)) {
              return errCb(cb, 2)
            }
            return errCb(cb, e2.errno)
          }
          if (
            sp.inner === '/' &&
            norm(fspath).split('/').filter(Boolean).length === 1 &&
            stat
          ) {
            self._fixEpochDirTimes(stat)
          }
          return cb(0, self._fillStat(stat))
        })
      })
    }

    handlers.readdir = function (fspath, cb) {
      log('readdir', fspath)
      const sp = self.splitPath(fspath)
      if (sp.kind === 'fsRoot') {
        return cb(0, self.registry.listFolderNames())
      }
      if (sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 2)
        st.posix.readdir(sp.inner, (e2, files) => {
          if (e2) {
            if (e2.code === 'ENOENT' || (e2.errno != null && e2.errno === 2)) {
              return errCb(cb, 2)
            }
            return errCb(cb, e2.errno)
          }
          return cb(0, files)
        })
      })
    }

    handlers.open = function (fspath, flags, cb) {
      log('open', fspath, flags)
      let f = flags
      if (platform !== 'linux') {
        f = translate(fsConstants[platform], linux, f)
      }
      const n = norm(fspath)
      if (n === '/') {
        if (f & (O_CREAT | O_EXCL | O_TRUNC | O_APPEND)) {
          return cb(Fuse.EISDIR)
        }
        const acc = f & ACCMODE
        if (acc === O_WRONLY || acc === O_RDWR) {
          return cb(Fuse.EISDIR)
        }
        return cb(0, self._allocRootFd())
      }
      const sp = self.splitPath(fspath)
      if (sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      if (sp.kind !== 'drive') {
        return errCb(cb, 2)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 2)
        st.posix.open(sp.inner, f, (e2, fd) => {
          if (e2) return errCb(cb, e2.errno)
          return cb(0, fd)
        })
      })
    }

    handlers.opendir = function (fspath, flags, cb) {
      return handlers.open(fspath, flags, cb)
    }

    handlers.release = function (fspath, handle, cb) {
      log('release', fspath, handle)
      if (self._isRootFd(handle)) {
        self._releaseRootFd(handle)
        return cb(0)
      }
      const sp = self.splitPath(fspath)
      if (sp.kind === 'enoent' || sp.kind === 'fsRoot') {
        return errCb(cb, 9)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 9)
        st.posix.close(handle, (e2) => {
          if (e2) {
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 9)
          }
          return cb(0)
        })
      })
    }

    handlers.releasedir = handlers.release

    handlers.read = function (fspath, handle, buf, len, offset, cb) {
      log('read', fspath, handle, len, offset)
      if (self._isRootFd(handle)) {
        return cb(Fuse.EISDIR)
      }
      const sp = self.splitPath(fspath)
      if (sp.kind !== 'drive') {
        return errCb(cb, 2)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 2)
        const proxy = Buffer.from(buf)
        st.posix.read(handle, proxy, 0, len, offset, (e2, bytesRead) => {
          if (e2) {
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 9)
          }
          proxy.copy(buf, 0, 0, bytesRead)
          return cb(0, bytesRead)
        })
      })
    }

    handlers.write = function (fspath, handle, buf, len, offset, cb) {
      log('write', fspath, handle, len, offset)
      const sp = self.splitPath(fspath)
      if (sp.kind !== 'drive') {
        return errCb(cb, 1)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 1)
        buf = Buffer.from(buf)
        st.posix.write(handle, buf, 0, len, offset, (e2, bytesWritten) => {
          if (e2) {
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 9)
          }
          return cb(0, bytesWritten)
        })
      })
    }

    // flush: (path, fd, cb) — fsync: (path, datasync, fd, cb) per fuse-native _op_fsync
    const doFsync = (fspath, handle, cb) => {
      if (self._isRootFd(handle)) {
        return cb(0)
      }
      const sp = self.splitPath(fspath)
      if (sp.kind !== 'drive') {
        return errCb(cb, 5)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 5)
        st.posix.fsync(handle, (e2) => {
          if (e2) {
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 5)
          }
          return cb(0)
        })
      })
    }

    handlers.flush = function (fspath, handle, cb) {
      return doFsync(fspath, handle, cb)
    }
    handlers.fsync = function (fspath, _datasync, handle, cb) {
      return doFsync(fspath, handle, cb)
    }

    handlers.fsyncdir = function (fspath, datasync, handle, cb) {
      if (self._isRootFd(handle)) {
        return cb(0)
      }
      return handlers.flush(fspath, handle, cb)
    }

    const truncateInner = (sp, size, cb) => {
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 1)
        st.posix.truncate(sp.inner, size, (e2) => {
          if (e2) {
            if (e2.code === 'EOPNOTSUPP' || (e2.errno != null && e2.errno === EOPNOTSUPP)) {
              return cb(Fuse.EOPNOTSUPP)
            }
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 1)
          }
          return cb(0)
        })
      })
    }

    handlers.truncate = function (fspath, size, cb) {
      const sp = self.splitPath(fspath)
      if (sp.kind === 'fsRoot') {
        return errCb(cb, 1)
      }
      if (sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      if (sp.inner === '/' && norm(fspath).split('/').filter(Boolean).length === 1) {
        return errCb(cb, 1)
      }
      truncateInner(sp, size, cb)
    }

    handlers.ftruncate = function (fspath, fd, size, cb) {
      if (self._isRootFd(fd)) {
        return cb(Fuse.EISDIR)
      }
      const sp = self.splitPath(fspath)
      if (sp.kind !== 'drive') {
        return errCb(cb, 1)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 1)
        st.posix.ftruncate(fd, size, (e2) => {
          if (e2) {
            if (e2.code === 'EOPNOTSUPP' || (e2.errno != null && e2.errno === EOPNOTSUPP)) {
              return cb(Fuse.EOPNOTSUPP)
            }
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 1)
          }
          return cb(0)
        })
      })
    }

    handlers.rename = function (from, to, cb) {
      log('rename', from, to)
      const a = self.splitPath(from)
      if (a.kind === 'drive' && a.inner === '/') {
        const toN = norm(to)
        const toSegs = toN.split('/').filter(Boolean)
        if (toSegs.length !== 1) {
          return errCb(cb, 1)
        }
        const dest = toSegs[0]
        const pto = tryParseFolderName(dest)
        if (pto) {
          if (!b4a.equals(pto.key, a.key)) {
            return cb(Fuse.EINVAL)
          }
          if (pto.label === a.label) {
            return cb(0)
          }
          const out = self.registry.relabel(a.key, pto.label)
          if (out.err) {
            if (out.err === 'label taken') return cb(Fuse.EEXIST)
            if (out.err === 'invalid label') return cb(Fuse.EINVAL)
            return errCb(cb, 1)
          }
          return cb(0)
        }
        if (isAmbiguousKeyForm(dest)) {
          return cb(Fuse.EINVAL)
        }
        if (!isValidLabelSegment(dest)) {
          return cb(Fuse.EINVAL)
        }
        const taken = self.registry.getByLabel(dest)
        if (taken && !b4a.equals(taken.key, a.key)) {
          return cb(Fuse.EEXIST)
        }
        if (taken && b4a.equals(taken.key, a.key)) {
          return cb(0)
        }
        const out = self.registry.relabel(a.key, dest)
        if (out.err) {
          if (out.err === 'label taken') return cb(Fuse.EEXIST)
          if (out.err === 'invalid label') return cb(Fuse.EINVAL)
          return errCb(cb, 1)
        }
        return cb(0)
      }
      const b = self.splitPath(to)
      if (a.kind === 'fsRoot' || b.kind === 'fsRoot') {
        return errCb(cb, 1)
      }
      if (a.kind === 'enoent' || b.kind === 'enoent') {
        return errCb(cb, 2)
      }
      if (a.kind !== 'drive' || b.kind !== 'drive') {
        return errCb(cb, 1)
      }
      if (!b4a.equals(a.key, b.key)) {
        return errCb(cb, 18) // EXDEV
      }
      if (a.inner === '/' || b.inner === '/') {
        return errCb(cb, 1)
      }
      self._ensure(a, (err, st) => {
        if (err) return errCb(cb, 1)
        st.posix.rename(a.inner, b.inner, (e2) => {
          if (e2) {
            const n = e2.errno
            if (n != null) {
              if (n === EOPNOTSUPP) {
                return cb(Fuse.EOPNOTSUPP)
              }
              return errCb(cb, n)
            }
            return errCb(cb, 1)
          }
          return cb(0)
        })
      })
    }

    handlers.link = function (from, to, cb) {
      log('link', from, to)
      const a = self.splitPath(from)
      const b = self.splitPath(to)
      if (a.kind === 'fsRoot' || b.kind === 'fsRoot') {
        return errCb(cb, 1)
      }
      if (a.kind === 'enoent' || b.kind === 'enoent') {
        return errCb(cb, 2)
      }
      if (a.kind !== 'drive' || b.kind !== 'drive') {
        return errCb(cb, 18) // EXDEV
      }
      if (!b4a.equals(a.key, b.key)) {
        return errCb(cb, 18) // EXDEV
      }
      if (a.inner === '/' || b.inner === '/') {
        return errCb(cb, 1)
      }
      self._ensure(a, (err, st) => {
        if (err) return errCb(cb, 1)
        st.posix.link(a.inner, b.inner, (e2) => {
          if (e2) {
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 1)
          }
          return cb(0)
        })
      })
    }

    handlers.unlink = function (fspath, cb) {
      log('unlink', fspath)
      const sp = self.splitPath(fspath)
      if (sp.kind === 'fsRoot') {
        return errCb(cb, 1)
      }
      if (sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      if (sp.inner === '/' && norm(fspath).split('/').filter(Boolean).length === 1) {
        return cb(Fuse.EISDIR)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 2)
        st.posix.unlink(sp.inner, (e2) => {
          if (e2) {
            if (e2.code === 'ENOENT' || (e2.errno != null && e2.errno === 2)) {
              return errCb(cb, 2)
            }
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 1)
          }
          return cb(0)
        })
      })
    }

    handlers.mkdir = function (fspath, mode, cb) {
      log('mkdir', fspath, mode)
      if (self._readOnly) {
        return cb(Fuse.EROFS)
      }
      const n = norm(fspath)
      const nSeg = n.split('/').filter(Boolean).length
      if (n === '/' || nSeg === 0) {
        return errCb(cb, 1)
      }
      const one = n.split('/').filter(Boolean)[0]
      if (nSeg === 1) {
        // Canonical "label.z32" (or legacy "label#z32")
        if (tryParseFolderName(one)) {
          if (self.registry.getByFolderName(one)) {
            return cb(Fuse.EEXIST)
          }
          // do not fabricate a full folder name; mkdir a short label only
          return cb(Fuse.EINVAL)
        }
        if (isPlainLabel(one)) {
          const c = self.registry.canAdd(one)
          if (!c.ok) {
            if (c.reason && String(c.reason).includes('already')) {
              return cb(Fuse.EEXIST)
            }
            return cb(Fuse.EINVAL)
          }
          let done = false
          const finish = (errn) => {
            if (done) return
            done = true
            if (errn === 0) return cb(0)
            if (errn === 17) return cb(Fuse.EEXIST)
            return errCb(cb, errn)
          }
          void (async () => {
            let raw
            try {
              raw = new Hyperdrive(self._driveStore(one))
              await raw.ready()
            } catch (e) {
              return finish(5)
            }
            const out = self.registry.addNew(one, raw.key)
            if (out.err) {
              try {
                await raw.close()
              } catch {
                // ignore
              }
              if (out.err && String(out.err).includes('already')) {
                return finish(17)
              }
              return finish(1)
            }
            const hx = b4a.toString(raw.key, 'hex')
            const posix = createPosixAdapter(raw)
            self._driveState.set(hx, { raw, posix })
            posix.ready((e) => {
              if (e) {
                self._driveState.delete(hx)
                void raw.close()
                return finish(5)
              }
              if (self._onDriveAdded) {
                try {
                  self._onDriveAdded(raw.key)
                } catch (err) {
                  log('onDriveAdded', err)
                }
              }
              // Listing shows <one>-<z32>, not the transient mkdir name. getattr/readdir for both spellings
              // resolve via getByKeyAndLabel + getByLabel in splitPath.
              return finish(0)
            })
          })()
          return
        }
        // not a valid root label (see lib/folder isValidLabelSegment)
        return cb(Fuse.EINVAL)
      }
      const sp = self.splitPath(fspath)
      if (sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      if (sp.kind === 'fsRoot') {
        return errCb(cb, 1)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 1)
        st.posix.mkdir(
          sp.inner,
          { mode, uid: process.getuid(), gid: process.getgid() },
          (e2) => {
            if (e2) {
              if (e2.errno != null) {
                if (e2.errno === 17) return cb(Fuse.EEXIST)
                return errCb(cb, e2.errno)
              }
              return errCb(cb, 1)
            }
            return cb(0)
          }
        )
      })
    }

    handlers.rmdir = function (fspath, cb) {
      log('rmdir', fspath)
      const n = norm(fspath)
      if (n === '/') {
        return errCb(cb, 1)
      }
      const segs = n.split('/').filter(Boolean)
      if (segs.length === 1) {
        const sp = self.splitPath(fspath)
        if (sp.kind === 'enoent') {
          return errCb(cb, 2)
        }
        if (sp.kind === 'fsRoot') {
          return errCb(cb, 1)
        }
        if (sp.inner !== '/' || segs.length > 1) {
          // unreachable
        }
        self._ensure(sp, (err, st) => {
          if (err) return errCb(cb, 2)
          st.posix.readdir('/', (e2, files) => {
            if (e2) {
              if (e2.code === 'ENOENT' || (e2.errno != null && e2.errno === 2)) {
                return errCb(cb, 2)
              }
              return errCb(cb, e2.errno)
            }
            if (files && files.length > 0) {
              return errCb(cb, 39) // ENOTEMPTY
            }
            const hx = b4a.toString(sp.key, 'hex')
            st.raw.close()
              .then(() => {
                self._driveState.delete(hx)
                const r = self.registry.removeByFolderName(segs[0])
                if (r.err) {
                  return errCb(cb, 1)
                }
                if (self._onDriveRemoved) {
                  try {
                    self._onDriveRemoved(sp.key)
                  } catch (err) {
                    log('onDriveRemoved', err)
                  }
                }
                return cb(0)
              })
              .catch(() => {
                return errCb(cb, 1)
              })
          })
        })
        return
      }
      const sp = self.splitPath(fspath)
      if (sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      if (sp.kind === 'fsRoot') {
        return errCb(cb, 1)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 2)
        st.posix.rmdir(sp.inner, (e2) => {
          if (e2) {
            if (e2.code === 'ENOENT' || (e2.errno != null && e2.errno === 2)) {
              return errCb(cb, 2)
            }
            if (e2.errno != null) {
              if (e2.errno === 39) return errCb(cb, 39)
              return errCb(cb, e2.errno)
            }
            return errCb(cb, 1)
          }
          return cb(0)
        })
      })
    }

    handlers.create = function (fspath, mode, cb) {
      log('create', fspath, mode)
      const segs = norm(fspath).split('/').filter(Boolean)
      if (segs.length === 1) {
        return errCb(cb, 1)
      }
      const sp = self.splitPath(fspath)
      if (sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      if (sp.kind === 'fsRoot') {
        return errCb(cb, 1)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 1)
        const opts = { mode, uid: process.getuid(), gid: process.getgid() }
        st.posix.create(sp.inner, opts, (e2) => {
          if (e2) {
            if (e2.code === 'ENOENT' || (e2.errno != null && e2.errno === 2)) {
              return errCb(cb, 2)
            }
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 1)
          }
          st.posix.open(sp.inner, 'w', (e3, fd) => {
            if (e3) {
              if (e3.code === 'ENOENT' || (e3.errno != null && e3.errno === 2)) {
                return errCb(cb, 2)
              }
              if (e3.errno != null) return errCb(cb, e3.errno)
              return errCb(cb, 1)
            }
            return cb(0, fd)
          })
        })
      })
    }

    const metaUpdate = (fspath, fields, cb) => {
      const sp = self.splitPath(fspath)
      if (sp.kind === 'fsRoot') {
        return cb(0)
      }
      if (sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      if (sp.kind !== 'drive') {
        return errCb(cb, 2)
      }
      if (sp.inner === '/' && norm(fspath).split('/').filter(Boolean).length === 1) {
        return cb(0)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 1)
        st.posix._update(sp.inner, fields, (e2) => {
          if (e2) {
            if (e2.code === 'ENOENT' || (e2.errno != null && e2.errno === 2)) {
              return errCb(cb, 2)
            }
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 1)
          }
          return cb(0)
        })
      })
    }

    handlers.chown = function (fspath, uid, gid, cb) {
      return metaUpdate(fspath, { uid, gid }, cb)
    }

    handlers.chmod = function (fspath, mode, cb) {
      return metaUpdate(fspath, { mode }, cb)
    }

    handlers.utimens = function (fspath, atime, mtime, cb) {
      return metaUpdate(fspath, { atime, mtime }, cb)
    }

    handlers.symlink = function (target, linkname, cb) {
      log('symlink', target, linkname)
      const sp = self.splitPath(linkname)
      if (sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      if (sp.kind === 'fsRoot' || sp.kind !== 'drive') {
        return errCb(cb, 1)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 1)
        st.posix.symlink(target, sp.inner, (e2) => {
          if (e2) {
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 1)
          }
          return cb(0)
        })
      })
    }

    handlers.readlink = function (fspath, cb) {
      log('readlink', fspath)
      const sp = self.splitPath(fspath)
      if (sp.kind === 'fsRoot' || sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 2)
        st.posix.lstat(sp.inner, (e2, stn) => {
          if (e2) {
            if (e2.code === 'ENOENT' || (e2.errno != null && e2.errno === 2)) {
              return errCb(cb, 2)
            }
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 1)
          }
          const linkname =
            !p.isAbsolute(stn.linkname) && !stn.linkname.startsWith('.') ? '/' + stn.linkname : stn.linkname
          const resolved = p.isAbsolute(stn.linkname)
            ? p.join(self.mnt, linkname)
            : p.join(self.mnt, p.resolve(fspath, linkname))
          return cb(0, resolved)
        })
      })
    }

    handlers.statfs = function (fspath, cb) {
      cb(0, {
        bsize: 4096,
        frsize: 4096,
        blocks: 1000000,
        bfree: 1000000,
        bavail: 1000000,
        files: 1000000,
        ffree: 1000000,
        favail: 1000000,
        fsid: 0,
        flag: 0,
        namemax: 255
      })
    }

    handlers.access = function (fspath, mode, cb) {
      const sp = self.splitPath(fspath)
      if (sp.kind === 'fsRoot') {
        if (self._readOnly && (mode & W_OK)) {
          return cb(Fuse.EROFS)
        }
        return cb(0)
      }
      if (sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 2)
        st.posix.lstat(sp.inner, (e2) => {
          if (e2) {
            if (e2.code === 'ENOENT' || (e2.errno != null && e2.errno === 2)) {
              return errCb(cb, 2)
            }
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 1)
          }
          if (!st.raw.writable && (mode & W_OK)) {
            return cb(Fuse.EROFS)
          }
          return cb(0)
        })
      })
    }

    handlers.setxattr = function (fspath, name, buffer, position, flags, cb) {
      if (platform === 'darwin' && name && name.startsWith('com.apple')) {
        return cb(0)
      }
      const sp = self.splitPath(fspath)
      if (sp.kind === 'fsRoot') {
        return errCb(cb, 1)
      }
      if (sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 1)
        st.posix.setMetadata(sp.inner, name, Buffer.from(buffer), (e2) => {
          if (e2) {
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 1)
          }
          return cb(0)
        })
      })
    }

    handlers.getxattr = function (fspath, name, position, cb) {
      const sp = self.splitPath(fspath)
      if (sp.kind === 'fsRoot' || sp.kind === 'enoent') {
        if (sp.kind === 'fsRoot' && platform === 'darwin' && name && name.startsWith('com.apple')) {
          return cb(0, null)
        }
        return sp.kind === 'fsRoot' ? errCb(cb, 1) : errCb(cb, 2)
      }
      if (isDriveRootPath(fspath, sp)) {
        const value = driveRootXattrValue(name, sp)
        if (value) return cb(0, value)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 2)
        st.posix.getxattr(sp.inner, name, position, (e2, value) => {
          if (e2) {
            if (e2.code === 'ENODATA') {
              return cb(platform === 'darwin' ? -93 : Fuse.ENODATA, null)
            }
            if (e2.errno != null) {
              return cb(-e2.errno, null)
            }
            return cb(-1, null)
          }
          return cb(0, value)
        })
      })
    }

    handlers.listxattr = function (fspath, cb) {
      const sp = self.splitPath(fspath)
      if (sp.kind === 'fsRoot') {
        return cb(0, [])
      }
      if (sp.kind === 'enoent') {
        return errCb(cb, 2)
      }
      if (isDriveRootPath(fspath, sp)) {
        return cb(0, DRIVE_ROOT_XATTRS.slice())
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 2)
        st.posix.listxattr(sp.inner, (e2, list) => {
          if (e2) {
            if (e2.code === 'ENOENT' || (e2.errno != null && e2.errno === 2)) {
              return errCb(cb, 2)
            }
            if (e2.errno != null) {
              return cb(-e2.errno, null)
            }
            return cb(-1, null)
          }
          return cb(0, list)
        })
      })
    }

    handlers.removexattr = function (fspath, name, cb) {
      const sp = self.splitPath(fspath)
      if (sp.kind === 'fsRoot' || sp.kind === 'enoent') {
        return sp.kind === 'fsRoot' ? errCb(cb, 1) : errCb(cb, 2)
      }
      self._ensure(sp, (err, st) => {
        if (err) return errCb(cb, 1)
        st.posix.removeMetadata(sp.inner, name, (e2) => {
          if (e2) {
            if (e2.code === 'ENODATA') {
              return cb(platform === 'darwin' ? -93 : Fuse.ENODATA)
            }
            if (e2.errno != null) return errCb(cb, e2.errno)
            return errCb(cb, 1)
          }
          return cb(0)
        })
      })
    }

    return handlers
  }

  async mount (extraHandlers) {
    if (this.fuse) {
      throw new Error('Cannot remount the same HyperdrivesFuse instance.')
    }
    const self = this
    const handlers = extraHandlers
      ? { ...this.getBaseHandlers(), ...extraHandlers }
      : this.getBaseHandlers()
    const mountOpts = {
      uid: process.getuid(),
      gid: process.getgid(),
      displayFolder: true,
      autoCache: true,
      force: true,
      mkdir: true,
      debug: debug.enabled,
      timeout: {
        write: false,
        read: false,
        release: false,
        releasedir: false,
        readdir: false,
        open: false,
        create: false,
        default: 60 * 1000
      }
    }
    const fuse = new Fuse(this.mnt, handlers, mountOpts)
    return new Promise((resolve, reject) => {
      return fuse.mount((e) => {
        if (e) return reject(e)
        self.fuse = fuse
        return resolve({
          handlers,
          mnt: self.mnt,
          registry: self.registry
        })
      })
    })
  }

  unmount () {
    if (!this.fuse) {
      return this.closeAllDrives()
    }
    return new Promise((resolve, reject) => {
      return this.fuse.unmount((err) => {
        if (err) return reject(err)
        this.fuse = null
        return this.closeAllDrives()
          .then(() => resolve())
          .catch(reject)
      })
    })
  }

  closeAllDrives () {
    const p = []
    for (const [, s] of this._driveState) {
      p.push(
        s.raw
          .close()
          .catch(() => {})
      )
    }
    this._driveState.clear()
    return Promise.all(p)
  }
}

module.exports = {
  HyperdrivesFuse,
  configure: Fuse.configure,
  unconfigure: Fuse.unconfigure,
  isConfigured: Fuse.isConfigured,
  beforeMount: Fuse.beforeMount,
  beforeUnmount: Fuse.beforeUnmount,
  unmount: Fuse.unmount
}
