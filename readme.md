## Micro Database engine

Plain file storage embedded database engine with MongoDB API

Features:
- MongoDB interface
- non blocking plain file store
- no dependencies

### Usage example:

```js
const db = new MicroDB('microdb://./data')
const col = await db.collection('col')

await col.insertOne({name: 'test', score: 100})
await col.updateOne({score: {$gt: 50}}, {comment: 'Best score'})
const documents = await col.find({}).toArray()
await col.deleteOne({name: 'test'})
```
