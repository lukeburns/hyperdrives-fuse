/**
 * All multi-drive FUSE volume drives live under this Corestore root namespace, then
 * one segment per user label: corestore.namespace(DRIVE_ROOT_NS).namespace('friendship') → Hyperdrive
 */
const DRIVE_ROOT_NS = 'hyperdrives-fuse-drives'

module.exports = { DRIVE_ROOT_NS }
