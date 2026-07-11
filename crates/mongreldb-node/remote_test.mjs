import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { Worker, isMainThread, parentPort, workerData } = require('node:worker_threads');
const { RemoteDatabase } = require('./index.js');
import { createServer } from 'node:http';
import assert from 'node:assert';

if (!isMainThread) {
  // Worker thread: run the mock HTTP server.
  const mode = workerData ?? 'success';
  const server = createServer((req, res) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk;
    });
    req.on('end', () => {
      if (req.url === '/history/retention' && req.method === 'GET') {
        if (mode === 'error') {
          res.writeHead(503, { 'content-type': 'text/plain' });
          res.end('unavailable');
          return;
        }
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ history_retention_epochs: 7, earliest_retained_epoch: 3 }));
      } else if (req.url === '/history/retention' && req.method === 'PUT') {
        assert.strictEqual(req.headers['content-type'], 'application/json');
        assert.deepStrictEqual(JSON.parse(body), { history_retention_epochs: 42 });
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ history_retention_epochs: 42, earliest_retained_epoch: 1 }));
      } else {
        res.writeHead(404);
        res.end('not found');
      }
    });
  });
  server.listen(0, '127.0.0.1', () => {
    const { port } = server.address();
    parentPort.postMessage({ port });
  });
  // Keep the worker alive until the main thread explicitly terminates it.
}

async function startServer(mode = 'success') {
  return new Promise((resolve, reject) => {
    const worker = new Worker(import.meta.filename, { workerData: mode });
    worker.once('message', ({ port }) => {
      resolve({ url: `http://127.0.0.1:${port}`, worker });
    });
    worker.once('error', reject);
  });
}

async function stopServer(worker) {
  await worker.terminate();
}

if (isMainThread) {
  // GET /history/retention parses the response fields.
  {
    const { url, worker } = await startServer('success');
    const db = new RemoteDatabase(url);
    assert.strictEqual(db.historyRetentionEpochs(), 7n);
    assert.strictEqual(db.earliestRetainedEpoch(), 3n);
    await stopServer(worker);
    console.log('remote: GET /history/retention ✓');
  }

  // PUT /history/retention sends the correct JSON body.
  {
    const { url, worker } = await startServer('success');
    const db = new RemoteDatabase(url);
    db.setHistoryRetentionEpochs(42n);
    await stopServer(worker);
    console.log('remote: PUT /history/retention ✓');
  }

  // Non-2xx responses propagate as thrown errors.
  {
    const { url, worker } = await startServer('error');
    const db = new RemoteDatabase(url);
    let threw = false;
    try {
      db.historyRetentionEpochs();
    } catch (e) {
      threw = true;
      assert(e.message.includes('503'), `error should mention 503: ${e.message}`);
    }
    assert(threw, 'expected historyRetentionEpochs to throw on 503');
    await stopServer(worker);
    console.log('remote: error propagation ✓');
  }

  console.log('All remote tests passed.');
}
