import assert from 'assert'
import fs from 'fs/promises'
import { Collection, MicroDB } from './microdb.ts'

const test: {
  (name: string, fn?: Function): void
  skip: (name: string, ...args: any) => void
  mode: (mode: 'stop' | 'continue') => void
  run: () => Promise<void>
} = ((global: any) => {
  let tests: {name: string, fn?: Function}[] = [], _mode: 'stop' | 'skip' | 'continue' = 'continue', _skip: boolean, _run: boolean
  const test = (name: string, fn?: Function) => {
    tests.push({ name, fn })
    !_run && tests.length === 1 && process.nextTick(test.run)
  }
  test.skip = (name: string, ...args: any) => name ? test(name) : _skip = true
  test.mode = (mode: 'stop' | 'skip' | 'continue') => _mode = mode
  test.run = async () => {
    if (_run) return
    const {log, error, warn} = console
    _run = true
    let count = 0, fail = 0, lastError: Error | undefined 
    const run = async (prefix: string) => {
      for (const {name, fn} of tests) {
        const stime = performance.now(), out = (pre: string, msg?: string, post?: string) => log(`${prefix}\x1b[${pre} ${name}${msg?': '+msg:''}\x1b[90m (${post||(performance.now() - stime).toFixed(1)+'ms'})\x1b[0m`)
        try {
          count++
          tests = []
          if (!(_skip = !fn))
            await fn()
          if (_skip) {
            out('90m—', '', 'skipped')
            count--
          } else if (tests.length) {
            out('32m—')
            await run(prefix + '  ')
            if (_mode === 'stop' && lastError)
              return
            lastError = undefined
          } else
            out('32m✔')
        } catch (e: any) {
          fail++
          out('31m✘', e.message)
          e.name !== 'AssertionError' && error(e.stack);
          lastError = e
          if (_mode !== 'continue')
            return
        }
      }
    }
    tests.length && await run('');
    _run = false
    if (count) {
      const ignRes = ['CloseReq', 'PipeWrap', 'TTYWrap', 'FSReqCallback']
      const l = '—'.repeat(16)+'\n', res = process.getActiveResourcesInfo().filter(n => !ignRes.includes(n))
      log(`\x1b[${fail?'33m'+l+'✘':'32m'+l+'✔'} ${count-fail}/${count} ${fail?'FAILED':'SUCCESS'}\x1b[0m`)
      res.length && warn('Active resources:', ...res)
      res.length && setTimeout(() => process.exit(1), 1000)
    }
  }
  return global.test = test
})(global)

let db: MicroDB, col: Collection

test('Prepare', async () => {
  await fs.mkdir('tmp').catch(() => {})
  await fs.rm('tmp/col.db', { force: true }).catch(() => {})
  db = new MicroDB('microdb://tmp')
  col = await db.collection('col')
})

test('Content', async () => {
  await col.insertOne({_id:'000000000000000000000001', test: 'test'})
  await col.updateOne({_id:'000000000000000000000001'}, {test: 'test2'})
  await col.deleteOne({_id:'000000000000000000000001'})
  await db.adapter.action(col)
  assert.equal(await fs.readFile('./tmp/col.db', 'utf8'),
    JSON.stringify({_id:'000000000000000000000001', test: 'test'}) + '\n' +
    JSON.stringify({_id:'000000000000000000000001', test: 'test2'}) + '\n' +
    JSON.stringify({$$delete:'000000000000000000000001'}) + '\n')
})

test('Many', async () => {
  await col.insertMany([{_id:'000000000000000000000001', test: 'test'}, {_id:'000000000000000000000002', test: 'test2'}, {_id:'000000000000000000000003', test: 'test3'}])
  assert.deepEqual(await col.findOne({_id:'000000000000000000000001'}), {_id:'000000000000000000000001', test: 'test'})
  assert.deepEqual(await col.findOne({_id:'000000000000000000000003'}), {_id:'000000000000000000000003', test: 'test3'})
})

test('Update', async () => {
  await col.insertOne({_id:'000000000000000000000004', test: 'test4'})
  await col.updateMany({test:{$gt:'test2'}}, {test: 'test5'})
  assert.deepEqual(await col.findOne({_id:'000000000000000000000003'}), {_id:'000000000000000000000003', test: 'test5'})
  assert.deepEqual(await col.findOne({_id:'000000000000000000000004'}), {_id:'000000000000000000000004', test: 'test5'})
})

test('Count', async () => {
  assert.deepEqual(await col.countDocuments(), 4)
  assert.deepEqual(await col.countDocuments({test:{$eq:'test2'}}), 1)
  assert.deepEqual(await col.countDocuments({$or:[{test:{$eq:'test'}},{test:{$eq:'test5'}}]}), 3)
})

test('Reopen', async () => {
  await col.insertOne({_id:'100000000000000000000001', test: 'test'})
  await col.updateOne({_id:'100000000000000000000001'}, {test: 'test2'})
  await db.close()

  db = new MicroDB('microdb://tmp')
  col = await db.collection('col')
  assert.deepEqual(await col.findOne({_id:'100000000000000000000001'}), {_id:'100000000000000000000001', test: 'test2'})
})

test('Close', async () => {
  await db.close()
})

test('Cleanup', async () => {
  await fs.rm('./tmp', { recursive: true })
})
