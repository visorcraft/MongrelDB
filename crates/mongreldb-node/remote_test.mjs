import { createRequire } from 'node:module';
const require = createRequire(import.meta.url);
const { Worker, isMainThread, parentPort, workerData } = require('node:worker_threads');
const { RemoteDatabase } = require('./index.js');
import { createServer } from 'node:http';
import assert from 'node:assert';

if (!isMainThread) {
  // Worker thread: run the mock HTTP server and record incoming requests.
  const mode = workerData ?? 'success';
  const requests = [];
  const server = createServer((req, res) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk;
    });
    req.on('end', () => {
      requests.push({ method: req.method, path: req.url, headers: req.headers, body });
      if (req.url === '/history/retention' && req.method === 'GET') {
        if (mode === 'error') {
          res.writeHead(503, { 'content-type': 'text/plain' });
          res.end('unavailable');
          return;
        }
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({ history_retention_epochs: 7, earliest_retained_epoch: 3 }));
      } else if (req.url === '/history/retention' && req.method === 'PUT') {
        if (mode === 'error') {
          res.writeHead(503, { 'content-type': 'text/plain' });
          res.end('unavailable');
          return;
        }
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
    parentPort.postMessage({ type: 'ready', port });
  });
  parentPort.on('message', (msg) => {
    if (msg.type === 'getRequests') {
      parentPort.postMessage({ type: 'requests', requests });
    }
  });
  // Keep the worker alive until the main thread explicitly terminates it.
}

async function startServer(mode = 'success') {
  return new Promise((resolve, reject) => {
    const worker = new Worker(import.meta.filename, { workerData: mode });
    worker.once('message', (msg) => {
      if (msg.type === 'ready') {
        resolve({ url: `http://127.0.0.1:${msg.port}`, worker });
      }
    });
    worker.once('error', reject);
  });
}

async function getRequests(worker) {
  return new Promise((resolve) => {
    worker.once('message', (msg) => {
      if (msg.type === 'requests') {
        resolve(msg.requests);
      }
    });
    worker.postMessage({ type: 'getRequests' });
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

    const requests = await getRequests(worker);
    assert.strictEqual(requests.length, 2);
    assert.strictEqual(requests[0].method, 'GET');
    assert.strictEqual(requests[0].path, '/history/retention');
    assert.strictEqual(requests[1].method, 'GET');
    assert.strictEqual(requests[1].path, '/history/retention');

    await stopServer(worker);
    console.log('remote: GET /history/retention ✓');
  }

  // PUT /history/retention sends the correct JSON body.
  {
    const { url, worker } = await startServer('success');
    const db = new RemoteDatabase(url);
    const result = db.setHistoryRetentionEpochs(42n);
    assert.strictEqual(result, undefined);

    const requests = await getRequests(worker);
    assert.strictEqual(requests.length, 1);
    assert.strictEqual(requests[0].method, 'PUT');
    assert.strictEqual(requests[0].path, '/history/retention');
    assert.strictEqual(requests[0].headers['content-type'], 'application/json');
    assert.deepStrictEqual(JSON.parse(requests[0].body), { history_retention_epochs: 42 });

    await stopServer(worker);
    console.log('remote: PUT /history/retention ✓');
  }

  // Non-2xx responses propagate as thrown errors for all three methods.
  {
    const { url, worker } = await startServer('error');
    const db = new RemoteDatabase(url);

    let threw = false;
    try {
      db.historyRetentionEpochs();
    } catch (e) {
      threw = true;
      assert(/\b503\b/.test(e.message), `error should mention 503: ${e.message}`);
    }
    assert(threw, 'expected historyRetentionEpochs to throw on 503');

    threw = false;
    try {
      db.earliestRetainedEpoch();
    } catch (e) {
      threw = true;
      assert(/\b503\b/.test(e.message), `error should mention 503: ${e.message}`);
    }
    assert(threw, 'expected earliestRetainedEpoch to throw on 503');

    threw = false;
    try {
      db.setHistoryRetentionEpochs(7n);
    } catch (e) {
      threw = true;
      assert(/\b503\b/.test(e.message), `error should mention 503: ${e.message}`);
    }
    assert(threw, 'expected setHistoryRetentionEpochs to throw on 503');

    await stopServer(worker);
    console.log('remote: error propagation ✓');
  }

  console.log('All remote tests passed.');
}
