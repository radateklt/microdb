/**
 * MicroDB
 * @version 1.0.6
 * @package @radatek/microdb
 * @copyright Darius Kisonas 2022
 * @license MIT
 */

import { promises as fs, createReadStream, createWriteStream, WriteStream } from 'fs'
import { randomBytes } from 'crypto'
import { createInterface as readline } from 'readline'
import path from 'path'

interface FieldFilter {[key: string]: any}
interface QueryOperators {
  $eq?: any,
  $ne?: any,
  $in?: any[],
  $nin?: any[],
  $gt?: number,
  $gte?: number,
  $lt?: number,
  $lte?: number,
  $regex?: RegExp,
  $like?: string,
  $nlike?: string
}
interface Query {
  [key: string]: any | QueryOperators,
  $not?: Query,
  $and?: Query[],
  $or?: Query[]
}
interface UpdateOperator {
  $set?: {[key: string]: any},
  $setOnInsert?: {[key: string]: any},
  $unset?: {[key: string]: any},
  $inc?: {[key: string]: number},
  $push?: {[key: string]: any},
  $addToSet?: {[key: string]: any},
  $pop?: {[key: string]: number},
  $pullAll?: {[key: string]: Query},
  $pull?: Query
}
interface Update {
  $set?: {[key: string]: any},
  $setOnInsert?: {[key: string]: any},
  $unset?: {[key: string]: any},
  $inc?: {[key: string]: number},
  $push?: {[key: string]: any},
  $addToSet?: {[key: string]: any},
  $pop?: {[key: string]: number},
  $pullAll?: {[key: string]: Query},
  $pull?: Query
  [key: string]: any | UpdateOperator
}

let globalObjectId: Buffer = randomBytes(8)
function newObjectId(): string {
  for (let i = 7; i >= 0; i--)
    if (++globalObjectId[i] < 256)
      break
  return (new Date().getTime() / 1000 | 0).toString(16) + globalObjectId.toString('hex')
}

export function clone<T extends Document> (obj: T, fields?: FieldFilter): Partial<T> {
  function copyValue (v: any, fields?: any): any {
    if (typeof v === 'object') {
      if (Array.isArray(v))
        return v.map(item => copyValue(item, fields))
      if (!Object.getPrototypeOf(v) || Object.getPrototypeOf(v) === Object.prototype)
        return copy(v, {} as T, fields)
      if (v.constructor.name === 'ObjectId')
        return v.valueOf()
      if (v instanceof Date)
        return new Date(v)
      console.warn('Unknonw ducument property type: ' + v.constructor.name)
    }
    return v
  }
  function copy (obj: T, res: T, fields?: FieldFilter): T {
    for (const n in obj) {
      res[n] = fields ? copyValue(obj[n], fields[n as string]) : copyValue(obj[n])
    }
    return res
  }
  return copyValue(obj, fields)
}

export function serialize<T extends Document> (doc: T): string {
  function copy (doc: any, cdoc: any): any {
    for (const n in doc) {
      const v = doc[n]
      if (typeof v === 'object') {
        if (Array.isArray(v)) {
          copy(v, cdoc[n] = [])
        } else if (!Object.getPrototypeOf(v) || Object.getPrototypeOf(v) === Object.prototype) {
          copy(v, cdoc[n] = {})
        } else if (v.constructor.name === 'ObjectId') {
          cdoc[n] = v.valueOf()
        } else if (v instanceof Date) {
          cdoc[n] = { '@type': 'date', v: v.toJSON() }
        } else {
          console.warn('Unknonw ducument property type: ' + v.constructor.name)
        }
      } else
        cdoc[n] = doc[n]
    }
    return cdoc
  }
  return JSON.stringify(copy(doc, {}))
}

export function deserialize<T extends Document> (doc: string): T {
  function fix (doc: any): any {
    for (const n in doc) {
      const v = doc[n]
      if (typeof v === 'object') {
        if (Array.isArray(v)) {
          fix(v)
        } else {
          const t = v['@type']
          if (t) {
            switch (t) {
              case 'date': doc[n] = new Date(v.v); break
              default:
                throw new Error('Unknown object type')
            }
          } else
            fix(v)
        }
      }
    }
    return doc
  }
  return fix(JSON.parse(doc))
}

interface QueueItem<T> {
  o: T,
  n?: QueueItem<T>,
  p?: QueueItem<T>
}

export class Queue<T = any> {
  private _head: QueueItem<T> | undefined
  private _tail: QueueItem<T> | undefined
  public length: number = 0

  push (o: T): T {
    this.length++
    if (!this._tail) {
      this._head = this._tail = { o: o }
    } else {
      const node = { o: o, p: this._tail }
      this._tail.n = node
      this._tail = node
    }
    return o
  }

  pop (): T | undefined {
    if (this._tail) {
      const node = this._tail
      this._tail = this._tail.p
      delete node.p
      if (this._tail)
        delete this._tail.n
      else
        this._head = this._tail
      this.length--
      return node.o
    }
  }

  add (o: T): T {
    return this.push(o)
  }

  shift (): T | undefined {
    if (this._head) {
      const node = this._head
      this._head = this._head.n
      delete node.n
      if (this._head)
        delete this._head.p
      else
        this._tail = this._head
      this.length--
      return node.o
    }
  }

  unshift (o: T): T {
    this.length++
    if (!this._head) {
      this._head = this._tail = { o: o }
    } else {
      const node = { o: o, n: this._head }
      this._head.p = node
      this._head = node
    }
    return o
  }

  clear (cb?: (data: T, index: number, queue: Queue<T>) => boolean | void, self?: any): void {
    if (cb) {
      for (let item: QueueItem<T> | undefined = this._head, i: number = 0; item; i++, item = item.n) {
        if (cb.call(self, item.o, i, this) === true) {
          if (item === this._head) {
            this._head = item.n
            if (item.n)
              item.n.p = this._head
          }
          else {
            if (item.p)
              item.p.n = item.n
            if (item.n)
              item.n.p = item.p
          }
        }
      }
      return
    }

    this._head = this._tail = undefined
    this.length = 0
  }

  first (): T | undefined {
    return this._head && this._head.o
  }

  last (): T | undefined {
    return this._tail && this._tail.o
  }

  isEmpty (): boolean {
    return !this._head
  }

  *[Symbol.iterator] (): Generator<T> {
    let item = this._head
    while (item) {
      const o = item.o
      item = item.n
      yield o
    }
  }

  toArray (): T[] {
    const arr: T[] = []
    for (let item: QueueItem<T> | undefined = this._head; item; item = item.n)
      arr.push(item.o)
    return arr
  }

  forEach (cb: (value: T, index: number, queue: Queue<T>) => boolean | void, self?: any): void {
    for (let item = this._head, i = 0; item; i++, item = item.n)
      if (cb.call(self, item.o, i, this) === false)
        break
  }
}

interface Document {
  [key: string]: any
}

interface ModifyOptions extends CursorOptions {
  remove?: boolean
  replace?: boolean
  upsert?: boolean
}

interface FindAndModifyOptions extends ModifyOptions {
  query?: Query
  update?: any
  new?: boolean
}

interface CursorOptions {
  limit?: number
  skip?: number
  sort?: {[key: string]: 1|-1}
  fields?: FieldFilter
}

export class Cursor {
  private _collection?: Collection
  private _iter?: (options?: CursorOptions) => Generator<Document>
  private _options: CursorOptions

  constructor(collection: Collection, options: CursorOptions, iter: (options?: CursorOptions) => Generator<Document>) {
    this._collection = collection
    this._iter = iter
    this._options = options || {}
  }

  limit(n: number): this {
    this._options.limit = n
    return this
  }

  sort(obj: {[key: string]: any}): this {
    this._options.sort = obj
    return this
  }

  skip(n: number): this {
    this._options.skip = n
    return this
  }

  async toArray(): Promise<Document[]> {
    if (!this._collection || !this._iter)
      return []
    if (!this._collection.isReady)
      await this._collection.ready()

    const res: Document[] = []
    for (const doc of this._iter(this._options))
      res.push(clone(doc, this._options.fields))
    return res
  }

  async next(): Promise<Document | undefined> {
    if (!this._collection || !this._iter)
      return
    if (!this._collection.isReady)
      await this._collection.ready()

    return clone(this._iter(this._options).next().value, this._options.fields)
  }

  async forEach(cb: (doc: Document) => boolean | void): Promise<number> {
    if (!this._collection || !this._iter)
      return 0
    if (!this._collection.isReady)
      await this._collection.ready()
    const docs = this._iter(this._options)

    return new Promise((resolve, reject) => {
      process.nextTick(() => {
        let count = 0
        for (const doc of docs) {
          count++
          try {
            if (cb(clone(doc, this._options.fields) as Document) === false)
              break
          } catch (e) {
            reject(e)
          }
        }
        resolve(count)
      })
    })
  }

  close(): void {
    delete this._iter
    delete this._collection
  }
}

export class Collection {
  public name: string
  public db?: MicroDB

  private _items: Map<string, Document> = new Map()
  private _readyResolve: Function | undefined
  private _ready: Promise<void> | undefined
  private _queryCache: {[key: string]: Function} = {}
  private _openev?: Function[]

  constructor(db: MicroDB, name: string) {
    this.name = name
    this.db = db
    this._ready = new Promise((resolve: Function) => {
      this._readyResolve = resolve
    }).then(() => this._readyResolve = undefined)
  }

  get isReady (): boolean { return !this._readyResolve || !this.db }

  async ready(): Promise<void> {
    if (!this._readyResolve || !this.db)
      return
    if (!this._openev) {
      this._openev = []
      await this.db.adapter.action(this, 'open')
      this._readyResolve()
      const arr: Function[] = this._openev
      delete this._openev
      arr.forEach(cb => cb())
    }
    return this._ready
  }

  private _query(query: Query, options?: CursorOptions): Generator<Document> {
    function generateFunction (query: Query) {
      const args: {[key: string]: string} = {}, argsList: string[] = []
      let argId = 0
      function getVar (prefix: string, name: string, ext?: string) {
        name = (prefix ? prefix + '.' : '') + name + (ext ? '.' + ext : '')
        let n = args[name]
        if (!n) {
          n = 'arg' + (++argId)
          args[name] = n
          argsList.push(n + '=obj.' + name)
        }
        return n
      }
      function _filter (prefix: string, query: any, oper: string = ' && '): string {
        const list: string[] = []
        if (Array.isArray(query)) {
          query.forEach((v, i) => {
            list.push(_filter(prefix + '[' + i + ']', v, ' && '))
          })
          return list.length ? '(' + list.join(oper) + ')' : 'true'
        }
        for (const name in query) {
          const v = query[name]
          switch (name) {
            case '$not':
              list.push('!(' + _filter((prefix ? prefix + '.' : '') + name, v, ' && ') + ')')
              break
            case '$and':
              list.push('(' + _filter((prefix ? prefix + '.' : '') + name, v, ' && ') + ')')
              break
            case '$or':
              list.push('(' + _filter((prefix ? prefix + '.' : '') + name, v, ' || ') + ')')
              break
            default:
              if (name[0] === '$')
                console.warn('operator ' + name + ' not supported')
              else {
                if (v === null) {
                  list.push('doc.' + name + '===null')
                  continue
                } else
                if (typeof (v) === 'object') {
                  const sublist: string[] = []
                  for (const q in v) {
                    switch (q) {
                      case '$exists':
                        sublist.push('!!doc.' + name + '===' + getVar(prefix, name, q))
                        break
                      case '$eq':
                        sublist.push('doc.' + name + '===' + getVar(prefix, name, q))
                        break
                      case '$ne':
                        sublist.push('doc.' + name + '!==' + getVar(prefix, name, q))
                        break
                      case '$in':
                        if (!Array.isArray(v[q]))
                          throw new Error('invalid operator ' + q + ' usage')
                        sublist.push(getVar(prefix, name, q) + '.includes(doc.' + name + ')')
                        break
                      case '$nin':
                        if (!Array.isArray(v[q]))
                          throw new Error('invalid operator ' + q + ' usage')
                        sublist.push(getVar(prefix, name, q) + '.includes(doc.' + name + ')')
                        break
                      case '$gt':
                        sublist.push('doc.' + name + '>' + getVar(prefix, name, q))
                        break
                      case '$gte':
                        sublist.push('doc.' + name + '>=' + getVar(prefix, name, q))
                        break
                      case '$lt':
                        sublist.push('doc.' + name + '<' + getVar(prefix, name, q))
                        break
                      case '$lte':
                        sublist.push('doc.' + name + '<=' + getVar(prefix, name, q))
                        break
                      case '$regex':
                        sublist.push('doc.' + name + '.match(' + getVar(prefix, name, q) + ')')
                        break
                      case '$like':
                        sublist.push('doc.' + name + '.includes(' + getVar(prefix, name, q) + ')')
                        break
                      case '$nlike':
                        sublist.push('!doc.' + name + '.includes(' + getVar(prefix, name, q) + ')')
                        break
                      default:
                        console.warn('operator ' + q + ' not supported')
                    }
                  }
                  list.push(sublist.sort().join(' && '))
                } else {
                  if (name !== '_id')
                    list.push('doc.' + name + '===' + getVar(prefix, name))
                }
              }
          }
        }
        return list.sort().join(oper || ' && ')
      }
      const filter = _filter('', query), vars = argsList.length ? 'const ' + argsList.join(',') + ';' : ''
      return vars + 'return function*query(iter){let item,doc;while(item=iter.next().value){doc=item[1];' + (filter ? 'if(' + filter + ')' : '') + '{if(skip)skip--;else{yield doc;if(--limit<=0)return}}}}'
    }

    const skip = Math.max(options?.skip || 0, 0),
      limit = Math.max(options?.limit || 99999999, 1) + skip
    let filter: Function | undefined, items: any

    if (query) {
      const queryFunc = generateFunction(query)
      if (query._id) {
        const doc = this._items.get(query._id.valueOf())
        if (!queryFunc.startsWith('const'))
          return (function* (doc) { if (doc) yield doc })(doc)
        items = (function* (v) { yield v })(doc && [doc._id.valueOf(), doc])
      }
      if (!filter) {
        if (!(filter = this._queryCache[queryFunc])) {
          filter = this._queryCache[queryFunc] = new Function('obj', 'skip', 'limit', 'iter', queryFunc)
        }
      }
    } else {
      if (!(filter = this._queryCache['*']))
        filter = this._queryCache['*'] = new Function('obj', 'skip', 'limit', 'iter', generateFunction({}))
    }
    return filter(query, skip, limit)(items || this._items.entries())
  }

  compactCollection (): Promise<void> {
    if (!this.db) return Promise.resolve()
    return this.db.adapter.action(this, 'compact')
  }

  drop () {
    this._items.clear()
    if (!this.db) return Promise.resolve()
    return this.db.adapter.action(this, 'drop')
  }

  private _queryObject(query: Query, data: Document) {
    for (const item in query) {
      const v = query[item]
      if (typeof v === 'object') {
        for (const oper in v) {
          switch (oper) {
            case '$eq':
              if (data[item] !== v[oper])
                return false
              break
            case '$ne':
              if (data[item] === v[oper])
                return false
              break
           case '$in':
              if (!Array.isArray(v[oper]))
                throw new Error('invalid operator ' + oper + ' usage')
              if (!v[oper].includes(data[item]))
                return false
              break
            case '$nin':
              if (!Array.isArray(v[oper]))
                throw new Error('invalid operator ' + oper + ' usage')
              if (v[oper].includes(data[item]))
                return false
              break
            case '$gt':
              if (data[item] <= v[oper])
                return false
              break
            case '$gte':
              if (data[item] < v[oper])
                return false
              break
            case '$lt':
              if (data[item] >= v[oper])
                return false
              break
            case '$lte':
              if (data[item] > v[oper])
                return false
              break
            case '$like':
              if (!data[item].includes(v[oper]))
                return false
              break
            case '$nlike':
              if (data[item].includes(v[oper]))
                return false
              break
            case '$regex':
              if (!data[item].match(v[oper]))
                return false
              break
            default:
              if (typeof data[item] !== 'object' || data[item][oper] !== v[oper])
                return false
              break
          }
        }
      } else
        if (v !== data[item])
          return false
    }
    return true
  }

  private _equal (doc: Document, upd: Update) {
    let count: number = 0
    for (const n in upd) {
      count++
      if (doc[n] !== upd[n]) {
        if (typeof upd[n] === 'object' && typeof doc[n] === 'object' && this._equal(doc[n], upd[n]))
          continue
        return false
      }
    }
    return count === Object.keys(doc).length
  }

  private _update (doc: Document, upd: Update, insert?: boolean) {
    let res = false
    if (!doc || !upd)
      return res
    for (const n in upd) {
      const v = upd[n]
      if (n[0] === '$') {
        switch (n) {
          case '$set':
            for (const nn in v) {
              if (doc[nn] !== v[nn]) {
                doc[nn] = v[nn]
                res = true
              }
            }
            break
          case '$setOnInsert':
            if (insert)
              for (const nn in v) {
              if (doc[nn] !== v[nn]) {
                  doc[nn] = v[nn]
                  res = true
                }
              }
            break
          case '$unset':
            for (const nn in v) {
              if (nn in doc) {
                delete doc[nn]
                res = true
              }
            }
            break
          case '$inc':
            for (const nn in v) {
              doc[nn] = (doc[nn] || 0) + 1
              res = true
            }
            break;
          case '$push':
            for (const nn in v) {
              const arr = doc[nn] = doc[nn] || []
              if (!Array.isArray(arr))
                throw new Error('field ' + nn + ' is not an array')
              const vv = Array.isArray(v[nn]) ? v[nn] : [v[nn]]
              arr.push.apply(arr, vv)
              res = true
            }
            break
          case '$addToSet':
            for (const nn in v) {
              const arr = doc[nn] = doc[nn] || []
              if (!Array.isArray(arr))
                throw new Error('field ' + nn + ' is not an array')
              const vv = Array.isArray(v[nn]) ? v[nn] : [v[nn]]
              for (const subitem of vv) {
                if (!arr.includes(subitem)) {
                  arr.push(subitem)
                  res = true
                }
              }
            }
            break
          case '$pop':
            for (const nn in v) {
              const arr = doc[nn] = doc[nn] || []
              if (!Array.isArray(arr))
                throw new Error('field ' + nn + ' is not an array')
              if (arr.length !== 0) {
                const vv = v[nn]
                if (vv === -1)
                  arr.shift()
                else
                  arr.pop()
                res = true
              }
            }
            break
          case '$pullAll':
            for (const nn in v) {
              const arr = doc[nn] = doc[nn] || []
              if (!Array.isArray(arr))
                throw new Error('field ' + nn + ' is not an array')
              if (arr.length !== 0) {
                doc[nn] = []
                res = true
              }
            }
            break
          case '$pull':
            for (const nn in v) {
              const arr = doc[nn] = doc[nn] || []
              if (!Array.isArray(arr))
                throw new Error('field ' + nn + ' is not an array')
              // filter items
              const filter = v[nn]
              const arrOut = arr.filter(item => this._queryObject(filter, item))
              if (arrOut.length !== arr.length) {
                doc[nn] = arrOut
                res = true
              }
            }
            break;
          default:
            throw new Error('operator ' + n + ' not supported')
        }
      } else {
        if (doc[n] !== upd[n]) {
          doc[n] = upd[n]
          res = true
        }
      }
    }
    return res
  }

  async countDocuments (query?: Query, options?: CursorOptions) {
    if (!this.isReady)
      await this.ready()

    let count = this._items.size
    if (query && Object.keys.length) {
      count = 0
      for (const iter = this._query(query, options); iter.next().value; count++) { }
    }
    return count
  }

  find (query: Query, fields?: FieldFilter, options?: CursorOptions) {
    return new Cursor(this, { fields, ...options }, (options?: CursorOptions) => this._query(query, options))
  }

  async findOne (query: Query, fields?: FieldFilter, options: CursorOptions = {}) {
    if (!this.isReady)
      await this.ready()
    return clone(this._query(query, options).next().value, fields)
  }

  private async _modify (query: Query, update: Update, options: ModifyOptions | undefined, cb: Function) {
    if (!this.isReady)
      await this.ready()

    if (!update && !options?.remove && !query)
      throw new TypeError()
    if (!query && update._id)
      query = { _id: update._id }

    let count: number = 0, doc: Document | undefined //, resdoc
    for (const iter = this._query(query, options); doc = iter.next().value;) {
      count++
      cb(count, doc, false)
      if (options?.remove) {
        this._items.delete(doc._id)
        this.db?.adapter.action(this, 'remove', doc._id)
        doc = undefined
      } else {
        if (options?.replace) {
          if (!update._id)
            update._id = doc._id
          if (doc._id.valueOf() !== update._id.valueOf())
            throw new Error('replace requires _id to be the same')
          if (!this._equal(doc, update)) {
            doc = update
            this._items.set(doc._id.valueOf(), doc)
            this.db?.adapter.action(this, 'update', doc)
          }
        } else if (this._update(doc, update)) {
          this._items.set(doc._id.valueOf(), doc)
          this.db?.adapter.action(this, 'update', doc)
        }
      }
      cb(count, doc, true)
    }
    if (!count && options?.upsert) {
      doc = { _id: newObjectId() }
      if (query)
        for (const n in query)
          if (n[0] !== '$')
            doc[n] = query[n]
      this._update(doc, update, true)
      this._items.set(doc._id.valueOf(), doc)
      this.db?.adapter.action(this, 'insert', doc)
      cb(0, doc, true)
    }
  }

  async findAndModify (options: FindAndModifyOptions = {}): Promise<Document | undefined> {
    options.limit = 1
    let resdoc: Document | undefined
    await this._modify(options.query || {}, options.update, options, (pos: number, doc: Document, after: boolean) => {
      if (!pos || (options.new && after) || (!options.new && !after))
        resdoc = clone(doc, options.fields)
    })
    return resdoc
  }

  async insertOne (doc: Document): Promise<any> {
    return (await this.insertMany([doc])).insertedIds[0]
  }

  async insertMany (docs: Document[]): Promise<{insertedIds: any[]}> {
    if (!this.isReady)
      await this.ready()

    const insertedIds: string[] = []

    docs.forEach((doc: Document) => {
      if (doc._id && this._items.get(doc._id.valueOf()))
        throw new Error('Document dupplicate')
      if (!doc._id)
        doc._id = newObjectId()
      this._items.set(doc._id.valueOf(), doc)
      this.db?.adapter.action(this, 'insert', doc)
      insertedIds.push(doc._id)
    })

    return { insertedIds: insertedIds }
  }

  updateOne (query: Query, doc: Document, options?: ModifyOptions) {
    return this.updateMany(query, doc, { ...options, limit: 1 })
  }

  async updateMany (query: Query, update: any, options?: ModifyOptions) {
    let upsertedId, modifiedCount = 0
    await this._modify(query, update, options, (pos: number, doc: Document, after: boolean) => {
      if (!pos)
        upsertedId = doc._id
      else
        modifiedCount = pos
    })
    return { upsertedId, modifiedCount }
  }

  replaceOne (query: Query, doc: Document, options?: ModifyOptions) {
    return this.updateMany(query, doc, { ...options, replace: true, limit: 1 })
  }

  deleteOne (query: Query, options?: ModifyOptions) {
    return this.deleteMany(query, { ...options, limit: 1 })
  }

  async deleteMany (query: Query, options?: ModifyOptions) {
    if (!this.isReady)
      await this.ready()

    let count = 0, doc
    for (const iter = this._query(query, options); doc = iter.next().value; count++) {
      this._items.delete(doc._id)
      this.db?.adapter.action(this, 'remove', doc._id)
    }
    return { deletedCount: count }
  }

  wait (): Promise<void> {
    return this.db?.adapter.action(this) ?? Promise.resolve()
  }
}

export class StorageAdapter {
  constructor (db: MicroDB, options: any) {
  }

  openDb () {
    return Promise.resolve()
  }

  closeDb () {
    return Promise.resolve()
  }

  dropDb () {
    return Promise.resolve()
  }

  list (): Promise<string[]> {
    return Promise.resolve([])
  }

  action (col: Collection, action?: string, data?: any): Promise<any> {
    return Promise.resolve()
  }
}

class CollectionFile {
  public fileName: string
  private _stream: WriteStream | undefined
  private _lines: number = 0
  constructor (fileName: string) {
    this.fileName = path.resolve(fileName)
  }

  get lines () { return this._lines }

  async open (): Promise<Map<string, Document>> {
    const items = new Map<string, Document>()
    try {
      await fs.stat(this.fileName)
      await new Promise((resolve: Function) => {
        const frs = createReadStream(this.fileName), reader = readline(frs)
        let fileLines = 0, maxId = 0

        const readLine = (line: string) => {
          try {
            fileLines++
            if (line.startsWith('{"$$')) {
              const obj = JSON.parse(line)
              if (obj.$$id)
                maxId = parseInt(obj.$$id) || maxId
              if (obj.$$delete) {
                if (typeof obj.$$delete === 'number' && obj.$$delete > maxId)
                  maxId = obj.$$delete
                items.delete(obj.$$delete)
              }
            } else
            if (line.startsWith('{')) {
              const obj = deserialize(line)
              if (obj._id) { // document must contain _id
                items.set(obj._id, obj)
                if (typeof obj._id === 'number' && obj._id > maxId)
                  maxId = obj._id
              }
            }
          } catch {
          }
        }

        const readDone = () => {
          frs.close()
          this._lines = fileLines
          resolve()
        }

        reader.on('line', readLine)
        reader.on('close', readDone)
        frs.on('error', readDone)
      })
    } catch { }
    this._stream = createWriteStream(this.fileName, { flags: 'a' })
    return items
  }

  async close (): Promise<void> {
    const stream = this._stream
    if (stream) {
      this._stream = undefined
      await new Promise((resolve: Function) => stream.end(resolve))
    }
  }

  async write (line: string): Promise<void> {
    return new Promise((resolve: Function) => {
      this._lines++
      this._stream?.write(line + '\n', () => resolve())
    }) 
  }

  async compact (items: Map<string, Document>): Promise<void> {
    if (this._lines <= items.size * 4)
      return
    await this.close()
    this._stream = createWriteStream(this.fileName + '.tmp', { flags: 'w' })
    let lines = 0
    const keys: string[] = []
    for (const key of items.keys())
      keys.push(key)

    await new Promise((resolve: Function, reject: Function) => {
      const iter = (function* (arr) { yield* arr })(keys),
      nextWrite = (error?: any) => {
        if (error) {
          this._stream?.close(() => {
            this._stream = undefined
            reject(error)
          })
          return
        }
        while (true) {
          const id = iter.next().value
          if (!id) {
            fs.rename(this.fileName + '.tmp', this.fileName)
              .then(() => resolve())
              .catch((e: Error) => nextWrite(e))
            return
          }
          const doc = items.get(id)
          if (doc) {
            lines++
            return this._stream?.write(serialize(doc) + '\n', nextWrite)
          }
        }
      }
      nextWrite()
    })
  }
}

interface FileAdapterItem {
  collection?: Collection
  action?: string
  data?: any
  resolve: Function
  reject: Function
}

interface FileAdapterOptions {
  path?: string
}

export class FileAdapter extends StorageAdapter {
  private _db: MicroDB
  private _dir: string
  private _execqueue: Queue<FileAdapterItem> = new Queue()
  private _processing: boolean = false

  constructor (db: MicroDB, options?: FileAdapterOptions) {
    super(db, options)
    this._db = db
    this._dir = path.resolve(options?.path || 'db')
  }

  private _getFile(col: Collection): CollectionFile | undefined {
    return (col as any)._file
  }

  private _setFile(col: Collection, file: CollectionFile | undefined): void {
    (col as any)._file = file
  }

  async openDb () {
    try {
      await fs.mkdir(this._dir, { recursive: true })
    } catch (e) {
      console.error('Create directory error: ' + this._dir, e)
    }
  }

  closeDb (): Promise<void> {
    return Promise.resolve()
  }

  dropDb (): Promise<void> {
    return fs.rm(this._dir, { recursive: true, force: true })
  }

  async list (): Promise<string[]> {
    const names: string[] = []
    try {
      const dirs = await fs.readdir(this._dir)
      dirs.forEach(name => {
        name = (name.match(/^(\w[\w_$-]+)\.db$/) || [])[1]
        if (name)
          names.push(name)
      })
    } catch { }
    return names
  }

  action (col: Collection, action?: string, data?: any): Promise<any> {
    return new Promise((resolve, reject) => {
      this._execqueue.push({ collection: col, action, data: clone(data), resolve, reject })
      this._next()
    })
  }

  async drop (col: Collection) {
    this.close(col)
    try {
      await fs.unlink(path.join(this._dir, col.name + '.db'))
    } catch { }
  }

  async open (col: Collection) {
    if (col.isReady)
      return

    const file = new CollectionFile(path.join(this._dir, col.name + '.db'))
    this._setFile(col, file);
    (col as any)._items = await file.open()
    this._processing = false
    this._next()
  }

  async close (col: Collection): Promise<void> {
    const file = this._getFile(col)
    this._setFile(col, undefined)
    return file?.close()
  }

  async compact (col: Collection, options?: any): Promise<void> {
    return this._getFile(col)?.compact((col as any)._items)
  }

  private async _write (col: Collection, doc: Document | string): Promise<void> {
    const file = this._getFile(col)
    if (!file)
      return
    await file.write(typeof doc === 'string' ? doc : serialize(doc))
    if (file.lines > (col as any)._items.size * 4) {
      this._execqueue.clear((item: FileAdapterItem) => item.action === 'compact' && item.collection === col)
      this._execqueue.add({
        collection: col,
        action: 'compact',
        data: 'auto',
        resolve: () => { },
        reject: () => { }
      })
    }
  }

  insert (col: Collection, data: Document): Promise<void> {
    return this._write(col, data)
  }

  update (col: Collection, data: Document): Promise<void> {
    return this._write(col, data)
  }

  remove (col: Collection, data: string ): Promise<void> {
    return this._write(col, {$$delete: data.valueOf()})
  }

  private _next (): void {
    if (!this._processing && this._execqueue.length)
      process.nextTick(this._process.bind(this))
  }

  private _process (): void {
    if (this._processing || !this._execqueue.length) return

    this._processing = true

    const item: FileAdapterItem | undefined = this._execqueue.shift()
    if (!item) {
      this._processing = false
      return
    }
    if (!item.action) {
      item.resolve()
      this._processing = false
      return this._process()
    }
    const action: Function = (this as any)[item.action]
    if (!action)
      throw new Error('Unknown action: ' + item.action)
    action.call(this, item.collection, item.data).then((res?: any) => {
      item.resolve(res)
    }).catch((err: Error) => {
      item.reject(err)
    }).then(() => {
      this._processing = false
      this._next()
    })
  }
}

export class MicroDB {
  private _isConnected: boolean = false
  private _collections: { [name: string]: Collection } = {}
  private _adapter: StorageAdapter
  
  constructor (url: string | URL, options: any = {}) {
    url = typeof url === 'string' ? new URL(url) : url
    const adapter: typeof StorageAdapter = options.adapter || (url.host && FileAdapter) || StorageAdapter
    options.path = options.path || decodeURI(url.host + url.pathname)
    this._adapter = new adapter(this, options)
  }

  get adapter (): StorageAdapter { return this._adapter }

  get isConnected (): boolean {
    return this._isConnected
  }

  async open (): Promise<MicroDB> {
    await this._adapter.openDb()
    this._isConnected = true
    return this
  }

  async close (): Promise<void> {
    // close all collections
    const promises: Promise<any>[] = []
    for (const n in this._collections)
      promises.push(this.adapter.action(this._collections[n], 'close'))
    this._collections = {}
    await Promise.all(promises)
    return this._adapter.closeDb()
  }

  collection (name: string): Promise<Collection> {
    return Promise.resolve(this._collections[name] = new Collection(this, name))
  }

  collections (): Promise<Collection[]> {
    const res: Collection[] = []
    for (const col in this._collections)
      res.push(this._collections[col])
    return Promise.resolve(res)
  }

  async collectionNames (): Promise<string[]> {
    const names: string[] = await this._adapter.list()
    for (const col in this._collections)
      if (!names.includes(col))
        names.push(col)
    return names
  }

  async dropDatabase (): Promise<void> {
    // drop all collections
    const promises: Promise<any>[] = []
    for (const n in this._collections)
      promises.push(this._adapter.action(this._collections[n], 'drop'))
    this._collections = {}
    await Promise.all(promises)
    return this._adapter.dropDb()
  }

  async dropCollection (name: string): Promise<void> {
    return this.adapter.action(this._collections[name], 'drop')
  }
}

export class Client {
  private _db: MicroDB
  constructor (url: string | URL, options?: any) {
    this._db = new MicroDB(url, options)
  }

  async connect () {
    await this._db.close()
    return this._db.open()
  }

  db (name?: string) {
    return this._db
  }

  isConnected () {
    return this._db.isConnected
  }

  close () {
    return this._db.close()
  }
}
