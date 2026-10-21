// The build of the management console.
//
// The bundle is written into the Go package that serves it, so the control
// plane binary carries the console. The artifact is committed and CI
// verifies that a fresh build reproduces it, the same way the generated
// protobuf bindings are kept honest.

import { build } from 'esbuild';
import { mkdir, rm, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const dist = path.resolve(here, '../internal/control/console/dist');
const scratch = path.resolve(here, 'build');

await rm(dist, { recursive: true, force: true });
await mkdir(path.join(dist, 'assets'), { recursive: true });
await mkdir(scratch, { recursive: true });

// The application: one script, one stylesheet, no external asset.
await build({
  entryPoints: [path.join(here, 'src/main.tsx')],
  bundle: true,
  minify: true,
  sourcemap: false,
  format: 'iife',
  target: ['es2022'],
  jsx: 'automatic',
  define: { 'process.env.NODE_ENV': '"production"' },
  outdir: path.join(dist, 'assets'),
  entryNames: 'app',
  assetNames: 'app',
  loader: { '.css': 'css' },
  legalComments: 'none',
});

// The library the tests exercise: the pure modules, without the views.
await build({
  entryPoints: [path.join(here, 'src/lib.ts')],
  bundle: true,
  format: 'esm',
  target: ['es2022'],
  outfile: path.join(scratch, 'lib.js'),
  legalComments: 'none',
});

// The application without its stylesheet, for the render test.
await build({
  entryPoints: [path.join(here, 'src/app.tsx')],
  bundle: true,
  format: 'esm',
  target: ['es2022'],
  jsx: 'automatic',
  outfile: path.join(scratch, 'app.mjs'),
  loader: { '.css': 'empty' },
  // The render test uses the react-dom of node_modules; two copies of
  // React would break the hooks.
  external: ['react', 'react-dom', 'react/jsx-runtime'],
  legalComments: 'none',
});

const document = `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta name="referrer" content="no-referrer" />
    <title>Fenced Management Console</title>
    <link rel="stylesheet" href="/console/assets/app.css" />
  </head>
  <body>
    <div id="root"></div>
    <script src="/console/assets/app.js" defer></script>
  </body>
</html>
`;

await writeFile(path.join(dist, 'index.html'), document, 'utf8');

console.log('the console bundle is in internal/control/console/dist');
