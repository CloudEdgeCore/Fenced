// The task event stream.
//
// The stream is read with fetch, not with EventSource: an EventSource cannot
// carry an Authorization header, and the operator's token must not travel in
// the query string, where it would end up in access logs.

import type { ControlClient } from './api';

export interface Frame {
  name: string;
  data: string;
}

/** Split a buffer into complete frames; the rest belongs to the next read. */
export function splitFrames(buffer: string): { frames: string[]; rest: string } {
  const parts = buffer.split('\n\n');
  const rest = parts.pop() ?? '';
  return { frames: parts, rest };
}

/** Read one Server-Sent Events frame. A comment frame carries no event. */
export function parseFrame(frame: string): Frame | undefined {
  const trimmed = frame.trim();
  if (!trimmed || trimmed.startsWith(':')) {
    return undefined;
  }
  let name = '';
  let data = '';
  for (const line of trimmed.split('\n')) {
    if (line.startsWith('event:')) {
      name = line.slice('event:'.length).trim();
    } else if (line.startsWith('data:')) {
      data = line.slice('data:'.length).trim();
    }
  }
  if (!data) {
    return undefined;
  }
  return { name, data };
}

/** Read a stream of frames until it ends. */
export async function readEventStream(
  body: ReadableStream<Uint8Array>,
  onFrame: (frame: Frame) => void,
): Promise<void> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = '';
  for (;;) {
    const chunk = await reader.read();
    if (chunk.done) {
      return;
    }
    buffer += decoder.decode(chunk.value, { stream: true });
    const { frames, rest } = splitFrames(buffer);
    buffer = rest;
    for (const raw of frames) {
      const frame = parseFrame(raw);
      if (frame) {
        onFrame(frame);
      }
    }
  }
}

/** Follow the events of one task. The server closes with task.terminal. */
export async function streamTaskEvents(
  client: ControlClient,
  taskId: string,
  onFrame: (frame: Frame) => void,
  signal?: AbortSignal,
): Promise<void> {
  const response = await fetch(`/v1/tasks/${encodeURIComponent(taskId)}/events`, {
    headers: {
      Accept: 'text/event-stream',
      Authorization: `Bearer ${client.token}`,
    },
    ...(signal ? { signal } : {}),
  });
  if (!response.ok || !response.body) {
    throw new Error(`event stream failed: HTTP ${response.status}`);
  }
  await readEventStream(response.body, onFrame);
}
