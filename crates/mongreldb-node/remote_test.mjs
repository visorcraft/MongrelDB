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
  let recoveryStatusReads = 0;
  let pendingSqlResponse;
  const server = createServer((req, res) => {
    let body = '';
    req.on('data', (chunk) => {
      body += chunk;
    });
    req.on('end', () => {
      requests.push({ method: req.method, path: req.url, headers: req.headers, body });
      if (mode === 'malformed-scalars') {
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end('{}');
        return;
      }
      if (mode.startsWith('cancel-malformed') && req.method === 'POST') {
        if (mode === 'cancel-malformed-json') {
          res.writeHead(202, { 'content-type': 'application/json' });
          res.end('{');
          return;
        }
        const queryId = req.url.split('/')[2];
        if (mode === 'cancel-malformed-duplicate') {
          res.writeHead(202, { 'content-type': 'application/json' });
          res.end(`{"query_id":"${queryId}","query_id":"${queryId}","state":"cancellation_requested","cancel_outcome":"accepted"}`);
          return;
        }
        const payload = mode === 'cancel-malformed-id'
          ? { query_id: 'ffffffffffffffffffffffffffffffff', state: 'cancellation_requested', cancel_outcome: 'accepted' }
          : mode === 'cancel-malformed-conflict'
            ? { query_id: queryId, state: 'finished', cancel_outcome: 'accepted' }
            : mode === 'cancel-malformed-unknown-field'
              ? { query_id: queryId, state: 'cancellation_requested', cancel_outcome: 'accepted', surprise: true }
              : { query_id: queryId, state: 'mystery', cancel_outcome: 'mystery' };
        res.writeHead(202, { 'content-type': 'application/json' });
        res.end(JSON.stringify(payload));
        return;
      }
      if (mode.startsWith('status-malformed') && req.method === 'GET') {
        const requestedId = req.url.slice('/queries/'.length);
        const queryId = mode === 'status-malformed-id'
          ? 'ffffffffffffffffffffffffffffffff'
          : requestedId;
        const payload = {
          query_id: queryId,
          status: 'committed',
          terminal_state: 'committed',
          state: 'completed',
          server_state: 'completed',
          operation: 'UPDATE',
          committed: true,
          committed_statements: 1,
          last_commit_epoch: 7,
          last_commit_epoch_text: mode === 'status-malformed-epoch' ? '8' : '7',
          first_commit_statement_index: 0,
          last_commit_statement_index: 0,
          completed_statements: 1,
          statement_index: 0,
          cancel_outcome: 'already_finished',
          cancellation_reason: 'none',
          retryable: false,
          terminal_error: null,
          outcome: {
            committed: true,
            committed_statements: mode === 'status-malformed-count' ? 2 : 1,
            last_commit_epoch: 7,
            last_commit_epoch_text: '7',
            first_commit_statement_index: 0,
            last_commit_statement_index: 0,
            completed_statements: 1,
            statement_index: 0,
            serialization: 'succeeded',
          },
        };
        if (mode === 'status-malformed-unknown-field') payload.surprise = true;
        res.writeHead(200, { 'content-type': 'application/json' });
        if (mode === 'status-malformed-duplicate') {
          const json = JSON.stringify(payload);
          res.end(json.replace('{', `{"query_id":"${queryId}",`));
          return;
        }
        res.end(JSON.stringify(payload));
        return;
      }
      if (mode.startsWith('receipt-malformed')) {
        if (req.url === '/sql' && req.method === 'POST') {
          const request = JSON.parse(body);
          const payload = {
            query_id: mode === 'receipt-malformed-id'
              ? 'ffffffffffffffffffffffffffffffff'
              : request.query_id,
            original_query_id: request.query_id,
            status: 'committed',
            terminal_state: 'committed',
            server_state: 'completed',
            committed: true,
            committed_statements: 1,
            last_commit_epoch: 7,
            last_commit_epoch_text: mode === 'receipt-malformed-epoch' ? '8' : '7',
            first_commit_statement_index: 0,
            last_commit_statement_index: 0,
            completed_statements: mode === 'receipt-malformed-index' ? 0 : 1,
            statement_index: mode === 'receipt-malformed-index' ? 1 : 0,
            cancel_outcome: 'already_finished',
            cancellation_reason: 'none',
            retryable: false,
            idempotency_replayed: false,
            idempotency_persisted: true,
            idempotency_expires_at_ms: 999999,
            terminal_error: null,
            outcome: {
              committed: mode === 'receipt-malformed-committed' ? false : true,
              committed_statements: 1,
              last_commit_epoch: 7,
              last_commit_epoch_text: '7',
              first_commit_statement_index: 0,
              last_commit_statement_index: 0,
              completed_statements: mode === 'receipt-malformed-index' ? 0 : 1,
              statement_index: mode === 'receipt-malformed-index' ? 1 : 0,
              serialization: 'succeeded',
            },
          };
          if (mode === 'receipt-malformed-unknown-field') payload.surprise = true;
          res.writeHead(200, { 'content-type': 'application/json' });
          if (mode === 'receipt-malformed-duplicate') {
            const json = JSON.stringify(payload);
            res.end(json.replace('{', `{"query_id":"${request.query_id}",`));
            return;
          }
          res.end(JSON.stringify(payload));
          return;
        }
        const queryId = req.url.split('/')[2];
        if (req.method === 'POST') {
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(JSON.stringify({
            query_id: queryId,
            state: 'finished',
            cancel_outcome: 'already_finished',
          }));
          return;
        }
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({
          query_id: queryId,
          status: 'completed',
          terminal_state: 'completed',
          state: 'completed',
          server_state: 'completed',
          operation: 'INSERT',
          committed: false,
          committed_statements: 0,
          last_commit_epoch: null,
          last_commit_epoch_text: null,
          first_commit_statement_index: null,
          last_commit_statement_index: null,
          completed_statements: 1,
          statement_index: 0,
          cancel_outcome: 'already_finished',
          cancellation_reason: 'none',
          retryable: false,
          terminal_error: null,
          outcome: {
            committed: false,
            committed_statements: 0,
            last_commit_epoch: null,
            last_commit_epoch_text: null,
            first_commit_statement_index: null,
            last_commit_statement_index: null,
            completed_statements: 1,
            statement_index: 0,
            serialization: 'succeeded',
          },
        }));
        return;
      }
      if (mode === 'non-sql-malformed') {
        let payload;
        if (req.url === '/tables/conflict/commit') {
          payload = {
            status: 'committed',
            committed: true,
            epoch: 9007199254740992,
            epoch_text: '9007199254740993',
            error: { code: 'COMMIT_OUTCOME', message: 'conflicting epoch fields' },
          };
        } else if (req.url === '/kit/procedures/noncanonical/call') {
          payload = {
            status: 'committed',
            committed: true,
            epoch_text: '09007199254740993',
            error: { code: 'COMMIT_OUTCOME', message: 'noncanonical epoch text' },
          };
        } else if (req.url === '/kit/procedures/missing_epoch/call') {
          payload = {
            status: 'committed',
            committed: true,
            error: { code: 'COMMIT_OUTCOME', message: 'missing commit epoch' },
          };
        } else if (req.url === '/triggers' && req.method === 'POST') {
          payload = {
            status: 'outcome_unknown',
            committed: true,
            epoch_text: '9007199254740993',
            error: { code: 'QUERY_OUTCOME_UNKNOWN', message: 'unknown claimed commit' },
          };
        } else {
          payload = {
            status: 'committed',
            committed: true,
            last_commit_epoch: 9007199254740992,
            last_commit_epoch_text: '9007199254740993',
            error: { code: 'COMMIT_OUTCOME', message: 'conflicting last commit epoch fields' },
          };
        }
        res.writeHead(409, { 'content-type': 'application/json' });
        res.end(JSON.stringify(payload));
        return;
      }
      if (mode === 'non-sql-errors') {
        let status;
        let payload;
        if (
          (req.url?.startsWith('/tables/') && req.url?.endsWith('/commit'))
          || (req.url?.startsWith('/kit/procedures/') && req.url?.endsWith('/call'))
        ) {
          status = 409;
          payload = {
            status: 'committed',
            committed: true,
            epoch: 42,
            epoch_text: '42',
            retryable: false,
            error: { code: 'COMMIT_OUTCOME', message: 'write committed; response failed' },
          };
        } else if (req.url === '/triggers' && req.method === 'POST') {
          status = 409;
          payload = {
            status: 'outcome_unknown',
            committed: null,
            epoch: 43,
            epoch_text: '43',
            retryable: false,
            error: { code: 'QUERY_OUTCOME_UNKNOWN', message: 'write outcome unknown' },
          };
        } else {
          const code = req.url === '/procedures'
            ? 'PROCEDURE_VALIDATION'
            : req.url?.startsWith('/procedures/')
              ? 'PROCEDURE_NOT_FOUND'
              : req.method === 'PUT'
                ? 'TRIGGER_VALIDATION'
                : 'TRIGGER_NOT_FOUND';
          status = code.endsWith('NOT_FOUND') ? 404 : 400;
          payload = {
            status: 'aborted',
            committed: false,
            retryable: true,
            error: { code, message: 'normal remote write error' },
          };
        }
        res.writeHead(status, { 'content-type': 'application/json' });
        res.end(JSON.stringify(payload));
        return;
      }
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
      } else if (req.url === '/sql' && req.method === 'POST' && mode === 'sql-error') {
        res.writeHead(413, { 'content-type': 'application/json' });
        res.end(JSON.stringify({
          query_id: '00112233445566778899aabbccddeeff',
          status: 'partially_committed',
          terminal_state: 'partially_committed',
          server_state: 'failed',
          committed: true,
          committed_statements: 1,
          last_commit_epoch: null,
          last_commit_epoch_text: '18446744073709551615',
          first_commit_statement_index: 0,
          last_commit_statement_index: 0,
          completed_statements: 1,
          statement_index: 1,
          cancel_outcome: 'already_finished',
          cancellation_reason: 'none',
          retryable: false,
          outcome: {
            committed: true,
            committed_statements: 1,
            last_commit_epoch: null,
            last_commit_epoch_text: '18446744073709551615',
            first_commit_statement_index: 0,
            last_commit_statement_index: 0,
            completed_statements: 1,
            statement_index: 1,
            serialization: 'failed',
          },
          error: {
            code: 'RESULT_LIMIT_EXCEEDED',
            message: 'SQL result limit exceeded',
            query_id: '00112233445566778899aabbccddeeff',
            committed: true,
            retryable: false,
          },
        }));
      } else if (req.url === '/sql' && req.method === 'POST' && mode === 'sql-idempotent') {
        const request = JSON.parse(body);
        setTimeout(() => {
          res.writeHead(200, { 'content-type': 'application/json' });
          res.end(JSON.stringify({
            query_id: request.query_id,
            original_query_id: request.query_id,
            status: 'committed',
            terminal_state: 'committed',
            server_state: 'completed',
            committed: true,
            committed_statements: 1,
            last_commit_epoch: 7,
            last_commit_epoch_text: '7',
            first_commit_statement_index: 0,
            last_commit_statement_index: 0,
            completed_statements: 1,
            statement_index: 0,
            cancel_outcome: 'already_finished',
            cancellation_reason: 'none',
            retryable: false,
            idempotency_replayed: false,
            idempotency_persisted: true,
            idempotency_expires_at_ms: 999999,
            outcome: {
              committed: true,
              committed_statements: 1,
              last_commit_epoch: 7,
              last_commit_epoch_text: '7',
              first_commit_statement_index: 0,
              last_commit_statement_index: 0,
              completed_statements: 1,
              statement_index: 0,
              serialization: 'succeeded',
            },
          }));
        }, 75);
      } else if (req.url === '/sql' && req.method === 'POST' && mode === 'async-cancel') {
        const request = JSON.parse(body);
        pendingSqlResponse = { response: res, queryId: request.query_id };
      } else if (
        req.url?.startsWith('/queries/')
        && req.method === 'POST'
        && mode === 'async-cancel'
      ) {
        const queryId = req.url.split('/')[2];
        res.writeHead(202, { 'content-type': 'application/json' });
        res.end(JSON.stringify({
          query_id: queryId,
          state: 'cancellation_requested',
          cancel_outcome: 'accepted',
          committed: false,
        }));
        if (pendingSqlResponse) {
          const pending = pendingSqlResponse;
          pendingSqlResponse = undefined;
          pending.response.writeHead(499, { 'content-type': 'application/json' });
          pending.response.end(JSON.stringify({
            query_id: pending.queryId,
            status: 'cancelled_before_commit',
            terminal_state: 'cancelled_before_commit',
            server_state: 'cancelled',
            committed: false,
            committed_statements: 0,
            last_commit_epoch: null,
            last_commit_epoch_text: null,
            first_commit_statement_index: null,
            last_commit_statement_index: null,
            completed_statements: 0,
            statement_index: 0,
            cancel_outcome: 'accepted',
            cancellation_reason: 'client_request',
            retryable: false,
            outcome: {
              committed: false,
              committed_statements: 0,
              last_commit_epoch: null,
              last_commit_epoch_text: null,
              first_commit_statement_index: null,
              last_commit_statement_index: null,
              completed_statements: 0,
              statement_index: 0,
              serialization: 'failed',
            },
            error: {
              code: 'QUERY_CANCELLED',
              message: 'SQL query cancelled',
              query_id: pending.queryId,
              committed: false,
              retryable: false,
            },
          }));
        }
      } else if (
        req.url === '/sql'
        && req.method === 'POST'
        && ['transport-recovery', 'transport-recovery-malformed'].includes(mode)
      ) {
        req.socket.destroy();
      } else if (
        req.url?.startsWith('/queries/')
        && req.method === 'POST'
        && ['transport-recovery', 'transport-recovery-malformed'].includes(mode)
      ) {
        const queryId = req.url.split('/')[2];
        res.writeHead(202, { 'content-type': 'application/json' });
        res.end(JSON.stringify({
          query_id: queryId,
          state: 'pre_cancelled',
          cancel_outcome: 'pre_cancelled',
          committed: false,
        }));
      } else if (
        req.url?.startsWith('/queries/')
        && req.method === 'GET'
        && ['transport-recovery', 'transport-recovery-malformed'].includes(mode)
      ) {
        recoveryStatusReads += 1;
        const queryId = req.url.slice('/queries/'.length);
        res.writeHead(200, { 'content-type': 'application/json' });
        if (recoveryStatusReads === 1) {
          if (mode === 'transport-recovery-malformed') {
            res.end(JSON.stringify({
              query_id: 'ffffffffffffffffffffffffffffffff',
              status: 'committed',
              terminal_state: 'committed',
              state: 'completed',
              server_state: 'completed',
              operation: 'UPDATE',
              committed: true,
              committed_statements: 1,
              last_commit_epoch: 7,
              last_commit_epoch_text: '7',
              first_commit_statement_index: 0,
              last_commit_statement_index: 0,
              completed_statements: 1,
              statement_index: 0,
              cancel_outcome: 'already_finished',
              cancellation_reason: 'none',
              retryable: false,
              terminal_error: null,
              outcome: {
                committed: true,
                committed_statements: 1,
                last_commit_epoch: 7,
                last_commit_epoch_text: '7',
                first_commit_statement_index: 0,
                last_commit_statement_index: 0,
                completed_statements: 1,
                statement_index: 0,
                serialization: 'succeeded',
              },
            }));
            return;
          }
          res.end(JSON.stringify({
            query_id: queryId,
            status: 'running',
            terminal_state: null,
            state: 'executing',
            server_state: 'executing',
            committed: false,
            committed_statements: 0,
            last_commit_epoch: null,
            last_commit_epoch_text: null,
            first_commit_statement_index: null,
            last_commit_statement_index: null,
            completed_statements: 0,
            statement_index: 0,
            cancel_outcome: null,
            cancellation_reason: 'none',
            retryable: false,
            terminal_error: null,
            outcome: {
              committed: false,
              committed_statements: 0,
              last_commit_epoch: null,
              last_commit_epoch_text: null,
              first_commit_statement_index: null,
              last_commit_statement_index: null,
              completed_statements: 0,
              statement_index: 0,
              serialization: 'in_progress',
            },
          }));
        } else {
          res.end(JSON.stringify({
            query_id: queryId,
            status: 'cancelled_before_start',
            terminal_state: 'cancelled_before_start',
            state: 'pre_cancelled',
            server_state: 'pre_cancelled',
            committed: false,
            committed_statements: 0,
            last_commit_epoch: null,
            last_commit_epoch_text: null,
            first_commit_statement_index: null,
            last_commit_statement_index: null,
            completed_statements: 0,
            statement_index: 0,
            cancel_outcome: 'pre_cancelled',
            cancellation_reason: 'client_request',
            retryable: false,
            terminal_error: {
              code: 'QUERY_CANCELLED',
              category: 'cancellation',
            },
            outcome: {
              committed: false,
              committed_statements: 0,
              last_commit_epoch: null,
              last_commit_epoch_text: null,
              first_commit_statement_index: null,
              last_commit_statement_index: null,
              completed_statements: 0,
              statement_index: 0,
              serialization: 'not_started',
            },
          }));
        }
      } else if (req.url?.startsWith('/queries/') && req.method === 'GET') {
        if (mode === 'query-not-found-malformed') {
          res.writeHead(404, { 'content-type': 'application/json' });
          res.end(JSON.stringify({
            error: { code: 'QUERY_NOT_FOUND', message: 'query not found' },
          }));
          return;
        }
        if (mode === 'query-not-found') {
          const queryId = req.url.slice('/queries/'.length);
          res.writeHead(404, { 'content-type': 'application/json' });
          res.end(JSON.stringify({
            query_id: queryId,
            status: 'unknown',
            terminal_state: null,
            committed: null,
            committed_statements: null,
            last_commit_epoch: null,
            last_commit_epoch_text: null,
            first_commit_statement_index: null,
            last_commit_statement_index: null,
            completed_statements: null,
            statement_index: null,
            cancel_outcome: 'not_found',
            cancellation_reason: null,
            retryable: false,
            server_state: 'not_found',
            outcome: {
              committed: null,
              committed_statements: null,
              last_commit_epoch: null,
              last_commit_epoch_text: null,
              first_commit_statement_index: null,
              last_commit_statement_index: null,
              completed_statements: null,
              statement_index: null,
              serialization: 'unknown',
            },
            error: {
              code: 'QUERY_NOT_FOUND',
              message: 'query not found',
              query_id: queryId,
              committed: null,
              retryable: false,
            },
          }));
          return;
        }
        const queryId = req.url.slice('/queries/'.length);
        res.writeHead(200, { 'content-type': 'application/json' });
        res.end(JSON.stringify({
          query_id: queryId,
          status: 'committed',
          terminal_state: 'committed',
          state: 'completed',
          server_state: 'completed',
          operation: 'UPDATE',
          committed: true,
          completed_statements: 1,
          statement_index: 0,
          committed_statements: 1,
          last_commit_epoch: null,
          last_commit_epoch_text: '18446744073709551615',
          first_commit_statement_index: 0,
          last_commit_statement_index: 0,
          cancel_outcome: 'already_finished',
          cancellation_reason: 'none',
          retryable: false,
          terminal_error: null,
          outcome: {
            committed: true,
            committed_statements: 1,
            last_commit_epoch: null,
            last_commit_epoch_text: '18446744073709551615',
            first_commit_statement_index: 0,
            last_commit_statement_index: 0,
            completed_statements: 1,
            statement_index: 0,
            serialization: 'succeeded',
          },
        }));
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
  assert.throws(
    () => new RemoteDatabase('https://example.test?token=secret'),
    /must not include a query or fragment/,
  );
  assert.throws(
    () => new RemoteDatabase('https://example.test', { transportTimeoutMs: 0 }),
    /transportTimeoutMs must be positive/,
  );
  for (const options of [
    { bearerToken: 'secret\r\ninjected' },
    { username: '', password: 'secret' },
    { username: 'alice:admin', password: 'secret' },
    { username: 'alice', password: 'secret\ninjected' },
  ]) {
    assert.throws(
      () => new RemoteDatabase('http://127.0.0.1:1', options),
      /remote auth credentials are invalid or ambiguous/,
    );
  }

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

  // Non-SQL writes keep structured protocol outcomes and never attach raw rows.
  {
    const { url, worker } = await startServer('non-sql-errors');
    const db = new RemoteDatabase(url, { bearerToken: 'auth-secret' });
    const caught = (fn) => {
      try {
        fn();
      } catch (error) {
        return error;
      }
      assert.fail('expected remote write to throw');
    };
    const assertSafe = (error) => {
      assert(error.remoteQueryError);
      assert(!Object.hasOwn(error.remoteQueryError, 'result'));
      assert(!Object.hasOwn(error.remoteQueryError, 'token'));
      assert(!error.message.includes('auth-secret'));
      assert(!error.message.includes('raw-secret-row'));
    };

    const commitError = caught(() => db.commit('jobs'));
    assert.strictEqual(commitError.code, 'COMMIT_OUTCOME');
    assert.strictEqual(commitError.queryId, 'unknown');
    assert.strictEqual(commitError.status, 'committed');
    assert.strictEqual(commitError.httpStatus, 409);
    assert.strictEqual(commitError.outcomeKnown, true);
    assert.strictEqual(commitError.committed, true);
    assert.strictEqual(commitError.epoch, 42n);
    assert.strictEqual(commitError.epochText, '42');
    assert.strictEqual(commitError.lastCommitEpoch, 42n);
    assert.strictEqual(commitError.lastCommitEpochText, '42');
    assert.strictEqual(commitError.retryable, false);
    assertSafe(commitError);

    const callError = caught(() => db.callProcedure('write_job', {
      argsJson: JSON.stringify({ id: 1 }),
      idempotencyKey: 'procedure-call-key',
    }));
    assert.strictEqual(callError.code, 'COMMIT_OUTCOME');
    assert.strictEqual(callError.committed, true);
    assert.strictEqual(callError.epochText, '42');
    assertSafe(callError);

    const unknownError = caught(() => db.createTrigger({
      json: '{}',
      idempotencyKey: 'trigger-create-key',
    }));
    assert.strictEqual(unknownError.code, 'QUERY_OUTCOME_UNKNOWN');
    assert.strictEqual(unknownError.status, 'outcome_unknown');
    assert.strictEqual(unknownError.outcomeKnown, false);
    assert.strictEqual(unknownError.committed, null);
    assert.strictEqual(unknownError.epoch, 43n);
    assert.strictEqual(unknownError.epochText, '43');
    assert.strictEqual(unknownError.lastCommitEpoch, null);
    assert.strictEqual(unknownError.retryable, false);
    assertSafe(unknownError);

    const normalWrites = [
      [() => db.createProcedure({ json: '{}' }), 'PROCEDURE_VALIDATION'],
      [() => db.dropProcedure('missing'), 'PROCEDURE_NOT_FOUND'],
      [() => db.replaceTrigger('jobs_ai', { json: '{}', idempotencyKey: 'replace-key' }), 'TRIGGER_VALIDATION'],
      [() => db.dropTrigger('jobs_ai', 'drop-key'), 'TRIGGER_NOT_FOUND'],
    ];
    for (const [write, code] of normalWrites) {
      const error = caught(write);
      assert.strictEqual(error.code, code);
      assert.strictEqual(error.status, 'aborted');
      assert.strictEqual(error.outcomeKnown, true);
      assert.strictEqual(error.committed, false);
      assert.strictEqual(error.retryable, true);
      assertSafe(error);
    }

    const requests = await getRequests(worker);
    const procedureCall = requests.find((request) => request.path === '/kit/procedures/write_job/call');
    assert.strictEqual(JSON.parse(procedureCall.body).idempotency_key, 'procedure-call-key');
    const triggerCreate = requests.find((request) => request.path === '/triggers' && request.method === 'POST');
    assert.strictEqual(JSON.parse(triggerCreate.body).idempotency_key, 'trigger-create-key');
    const triggerReplace = requests.find((request) => request.path === '/triggers/jobs_ai' && request.method === 'PUT');
    assert.strictEqual(JSON.parse(triggerReplace.body).idempotency_key, 'replace-key');
    const triggerDrop = requests.find((request) => request.path === '/triggers/jobs_ai' && request.method === 'DELETE');
    assert.strictEqual(triggerDrop.headers['idempotency-key'], 'drop-key');
    await stopServer(worker);
    console.log('remote: structured non-SQL write errors ✓');
  }

  // Malformed commit envelopes fail closed without preserving claimed outcomes.
  {
    const { url, worker } = await startServer('non-sql-malformed');
    const db = new RemoteDatabase(url);
    const caught = (fn) => {
      try {
        fn();
      } catch (error) {
        return error;
      }
      assert.fail('expected malformed remote write to throw');
    };
    const assertUnknown = (error) => {
      assert.strictEqual(error.code, 'QUERY_OUTCOME_UNKNOWN');
      assert.strictEqual(error.status, 'outcome_unknown');
      assert.strictEqual(error.httpStatus, 409);
      assert.strictEqual(error.outcomeKnown, false);
      assert.strictEqual(error.committed, null);
      assert.strictEqual(error.epoch, null);
      assert.strictEqual(error.epochText, null);
      assert.strictEqual(error.lastCommitEpoch, null);
      assert.strictEqual(error.lastCommitEpochText, null);
      assert.strictEqual(error.retryable, false);
      assert.strictEqual(error.serverState, 'invalid_response');
      assert.strictEqual(error.remoteQueryError.code, 'QUERY_OUTCOME_UNKNOWN');
    };

    assertUnknown(caught(() => db.commit('conflict')));
    assertUnknown(caught(() => db.callProcedure('noncanonical')));
    assertUnknown(caught(() => db.callProcedure('missing_epoch')));
    assertUnknown(caught(() => db.createTrigger({ json: '{}' })));
    assertUnknown(caught(() => db.replaceTrigger('last_conflict', { json: '{}' })));

    await stopServer(worker);
    console.log('remote: malformed write outcomes fail closed ✓');
  }

  // Query status is typed, preserves u64 epochs, and uses shared auth.
  {
    const { url, worker } = await startServer('success');
    const queryId = '00112233445566778899aabbccddeeff';
    const db = new RemoteDatabase(url, { bearerToken: 'test-token' });
    const status = db.queryStatus(queryId);
    assert.strictEqual(status.queryId, queryId);
    assert.strictEqual(status.terminalState, 'committed');
    assert.strictEqual(status.outcomeKnown, true);
    assert.strictEqual(status.committed, true);
    assert.strictEqual(status.durableOutcome.lastCommitEpoch, 18446744073709551615n);
    assert.strictEqual(status.cancelOutcome, 'AlreadyFinished');

    const requests = await getRequests(worker);
    assert.strictEqual(requests.length, 1);
    assert.strictEqual(requests[0].path, `/queries/${queryId}`);
    assert.strictEqual(requests[0].headers.authorization, 'Bearer test-token');
    await stopServer(worker);
    console.log('remote: authenticated query status ✓');
  }

  // Query-status 404 keeps its stable protocol code.
  {
    const { url, worker } = await startServer('query-not-found');
    const db = new RemoteDatabase(url, { username: 'alice', password: 'secret' });
    let error;
    try {
      db.queryStatus('00112233445566778899aabbccddeeff');
    } catch (caught) {
      error = caught;
    }
    assert(error, 'expected queryStatus to throw');
    assert.strictEqual(error.code, 'QUERY_NOT_FOUND');
    assert(!error.message.includes('secret'));

    const requests = await getRequests(worker);
    assert.strictEqual(requests.length, 1);
    assert.strictEqual(requests[0].headers.authorization, 'Basic YWxpY2U6c2VjcmV0');
    await stopServer(worker);
    console.log('remote: query status error code ✓');
  }

  // An incomplete 404 envelope cannot establish a safe status result.
  {
    const { url, worker } = await startServer('query-not-found-malformed');
    const db = new RemoteDatabase(url);
    let error;
    try {
      db.queryStatus('00112233445566778899aabbccddeeff');
    } catch (caught) {
      error = caught;
    }
    assert(error, 'expected malformed queryStatus to throw');
    assert.strictEqual(error.code, 'QUERY_OUTCOME_UNKNOWN');
    await stopServer(worker);
    console.log('remote: malformed query status fails closed ✓');
  }

  // Query status rejects wrong IDs and conflicting duplicate durable fields.
  for (const mode of [
    'status-malformed-id',
    'status-malformed-epoch',
    'status-malformed-count',
    'status-malformed-unknown-field',
    'status-malformed-duplicate',
  ]) {
    const { url, worker } = await startServer(mode);
    const db = new RemoteDatabase(url);
    let error;
    try {
      db.queryStatus('00112233445566778899aabbccddeeff');
    } catch (caught) {
      error = caught;
    }
    assert(error, `expected ${mode} to throw`);
    assert.strictEqual(error.code, 'QUERY_OUTCOME_UNKNOWN');
    assert.strictEqual(error.outcomeKnown, false);
    assert.strictEqual(error.committed, null);
    await stopServer(worker);
  }
  console.log('remote: strict query status validation ✓');

  // Cancellation requires valid JSON, matching ID, and agreeing known states.
  for (const mode of [
    'cancel-malformed-json',
    'cancel-malformed-id',
    'cancel-malformed-conflict',
    'cancel-malformed-unknown',
    'cancel-malformed-unknown-field',
    'cancel-malformed-duplicate',
  ]) {
    const { url, worker } = await startServer(mode);
    const db = new RemoteDatabase(url);
    assert.throws(
      () => db.cancelSql('00112233445566778899aabbccddeeff'),
      (error) => error.code === 'INVALID_RESPONSE',
      mode,
    );
    await stopServer(worker);
  }
  console.log('remote: strict cancellation validation ✓');

  // Scalar read/write responses never turn malformed data into zero/success.
  {
    const { url, worker } = await startServer('malformed-scalars');
    const db = new RemoteDatabase(url);
    assert.throws(() => db.historyRetentionEpochs());
    assert.throws(() => db.earliestRetainedEpoch());
    assert.throws(() => db.count('items'));
    for (const operation of [
      () => db.setHistoryRetentionEpochs(7n),
      () => db.commit('items'),
    ]) {
      assert.throws(operation, (error) => {
        assert.strictEqual(error.code, 'QUERY_OUTCOME_UNKNOWN');
        assert.strictEqual(error.outcomeKnown, false);
        return true;
      });
    }
    await stopServer(worker);
    console.log('remote: malformed scalar responses fail closed ✓');
  }

  // SQL failures retain protocol code and exact durable receipt fields.
  {
    const { url, worker } = await startServer('sql-error');
    const db = new RemoteDatabase(url);
    let error;
    try {
      await db.sqlWithOptions('SELECT 1', {
        queryId: '00112233445566778899aabbccddeeff',
      });
    } catch (caught) {
      error = caught;
    }
    assert(error, 'expected sqlWithOptions to throw');
    assert.strictEqual(error.code, 'RESULT_LIMIT_EXCEEDED');
    assert.strictEqual(error.queryId, '00112233445566778899aabbccddeeff');
    assert.strictEqual(error.committed, true);
    assert.strictEqual(error.committedStatements, 1);
    assert.strictEqual(error.lastCommitEpoch, 18446744073709551615n);
    assert.strictEqual(error.statementIndex, 1);
    assert.strictEqual(error.message, 'SQL result limit exceeded');
    await stopServer(worker);
    console.log('remote: structured SQL error receipt ✓');
  }

  // Idempotent writes use JSON receipts through a typed, dedicated method.
  {
    const { url, worker } = await startServer('sql-idempotent');
    const db = new RemoteDatabase(url);
    const queryId = '00112233445566778899aabbccddeeff';
    const pendingReceipt = db.sqlWriteIdempotent('INSERT INTO jobs VALUES (1)', {
      queryId,
      idempotencyKey: 'create-job-1',
    });
    let timerFired = false;
    await new Promise((resolve) => setTimeout(() => {
      timerFired = true;
      resolve();
    }, 25));
    assert.strictEqual(timerFired, true);
    const receipt = await pendingReceipt;
    assert.strictEqual(receipt.queryId, queryId);
    assert.strictEqual(receipt.committed, true);
    assert.strictEqual(receipt.durableOutcome.lastCommitEpoch, 7n);
    assert.strictEqual(receipt.idempotencyPersisted, true);

    const requests = await getRequests(worker);
    const request = JSON.parse(requests[0].body);
    assert.strictEqual(request.idempotency_key, 'create-job-1');
    assert(!Object.hasOwn(request, 'format'));
    await stopServer(worker);
    console.log('remote: typed idempotent SQL receipt ✓');
  }

  // Idempotent receipts reject wrong IDs, duplicate conflicts, and bad indexes.
  for (const mode of [
    'receipt-malformed-id',
    'receipt-malformed-committed',
    'receipt-malformed-epoch',
    'receipt-malformed-index',
    'receipt-malformed-unknown-field',
    'receipt-malformed-duplicate',
  ]) {
    const { url, worker } = await startServer(mode);
    const db = new RemoteDatabase(url);
    const error = await db.sqlWriteIdempotent('INSERT INTO jobs VALUES (1)', {
      queryId: '00112233445566778899aabbccddeeff',
      idempotencyKey: 'strict-receipt',
    }).then(
      () => assert.fail(`expected ${mode} to fail`),
      (caught) => caught,
    );
    assert.strictEqual(error.code, 'QUERY_OUTCOME_UNKNOWN');
    assert.strictEqual(error.outcomeKnown, false);
    assert.strictEqual(error.committed, null);
    await stopServer(worker);
  }
  console.log('remote: strict idempotent receipt validation ✓');

  // Lost response transport polls status, cancels, then reports final state.
  {
    const { url, worker } = await startServer('transport-recovery');
    const db = new RemoteDatabase(url, { transportTimeoutMs: 1_000 });
    const queryId = '00112233445566778899aabbccddeeff';
    let error;
    try {
      await db.sqlWithOptions('SELECT 1', { queryId });
    } catch (caught) {
      error = caught;
    }
    assert(error, 'expected transport recovery to throw');
    assert.strictEqual(error.code, 'QUERY_CANCELLED');
    assert.strictEqual(error.queryId, queryId);
    assert.strictEqual(error.outcomeKnown, true);
    assert.strictEqual(error.committed, false);
    assert.strictEqual(error.cancelOutcome, 'pre_cancelled');

    const requests = await getRequests(worker);
    assert.deepStrictEqual(
      requests.map((request) => `${request.method} ${request.path}`),
      [
        'POST /sql',
        `GET /queries/${queryId}`,
        `POST /queries/${queryId}/cancel`,
        `GET /queries/${queryId}`,
      ],
    );
    await stopServer(worker);
    console.log('remote: transport-loss query recovery ✓');
  }

  // Recovery ignores a terminal response for the wrong query ID.
  {
    const { url, worker } = await startServer('transport-recovery-malformed');
    const db = new RemoteDatabase(url, { transportTimeoutMs: 1_000 });
    const queryId = '00112233445566778899aabbccddeeff';
    const error = await db.sqlWithOptions('SELECT 1', { queryId }).then(
      () => assert.fail('expected recovered cancellation'),
      (caught) => caught,
    );
    assert.strictEqual(error.code, 'QUERY_CANCELLED');
    assert.strictEqual(error.queryId, queryId);
    assert.strictEqual(error.committed, false);
    const requests = await getRequests(worker);
    assert.deepStrictEqual(
      requests.map((request) => `${request.method} ${request.path}`),
      [
        'POST /sql',
        `GET /queries/${queryId}`,
        `POST /queries/${queryId}/cancel`,
        `GET /queries/${queryId}`,
      ],
    );
    await stopServer(worker);
    console.log('remote: recovery binds query ID ✓');
  }

  // The exported SQL wrapper leaves the JS thread free to cancel an active request.
  {
    const { url, worker } = await startServer('async-cancel');
    const db = new RemoteDatabase(url);
    const queryId = '00112233445566778899aabbccddeeff';
    const result = db.sqlWithOptions('SELECT 1', { queryId });
    let timerFired = false;
    await new Promise((resolve) => setTimeout(() => {
      timerFired = true;
      resolve();
    }, 25));
    assert.strictEqual(timerFired, true);
    assert.strictEqual(db.cancelSql(queryId), 'Accepted');
    let error;
    try {
      await result;
    } catch (caught) {
      error = caught;
    }
    assert(error, 'expected cancelled remote result to reject');
    assert.strictEqual(error.code, 'QUERY_CANCELLED');
    assert.strictEqual(error.queryId, queryId);
    assert.strictEqual(error.committed, false);

    const requests = await getRequests(worker);
    assert.deepStrictEqual(
      requests.map((request) => `${request.method} ${request.path}`),
      ['POST /sql', `POST /queries/${queryId}/cancel`],
    );
    await stopServer(worker);
    console.log('remote: async wrapper cancellation ✓');
  }

  console.log('All remote tests passed.');
}
