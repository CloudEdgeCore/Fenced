import test from 'node:test';
import assert from 'node:assert/strict';

import { splitFrames, parseFrame, readEventStream } from '../build/lib.js';

test('a frame is split on the empty line', () => {
  const { frames, rest } = splitFrames('event: a\ndata: 1\n\nevent: b\ndata: 2\n\npartial');
  assert.equal(frames.length, 2);
  assert.equal(rest, 'partial');
});

test('the name and the data of a frame are read', () => {
  const frame = parseFrame('event: task.updated\nid: 7\ndata: {"id":"task-1"}');
  assert.equal(frame.name, 'task.updated');
  assert.equal(frame.data, '{"id":"task-1"}');
});

test('a keep-alive comment carries no event', () => {
  assert.equal(parseFrame(': keepalive'), undefined);
  assert.equal(parseFrame('   '), undefined);
});

test('a frame without data carries no event', () => {
  assert.equal(parseFrame('event: task.updated'), undefined);
});

function streamOf(chunks) {
  const encoder = new TextEncoder();
  let index = 0;
  return {
    getReader() {
      return {
        read: async () => {
          if (index >= chunks.length) {
            return { done: true, value: undefined };
          }
          const value = encoder.encode(chunks[index]);
          index += 1;
          return { done: false, value };
        },
      };
    },
  };
}

test('the stream is read frame by frame', async () => {
  const seen = [];
  await readEventStream(
    streamOf(['event: task.updated\ndata: {"n":1}\n\n', 'event: task.terminal\ndata: {"n":2}\n\n']),
    (frame) => seen.push(frame.name),
  );
  assert.deepEqual(seen, ['task.updated', 'task.terminal']);
});

test('a frame split across two reads is still one frame', async () => {
  const seen = [];
  await readEventStream(
    streamOf(['event: task.updated\nda', 'ta: {"n":1}\n\n']),
    (frame) => seen.push(frame.data),
  );
  assert.deepEqual(seen, ['{"n":1}']);
});
