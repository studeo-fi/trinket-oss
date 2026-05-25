/**
 * Pygame Shell Server
 *
 * Spawns Python processes with pygame in a graphical environment.
 * Runs inside a container with Xvnc providing the display.
 */

import { spawn, execSync } from 'child_process';
import { createHash } from 'crypto';
import { mkdir, writeFile, rm, chmod } from 'node:fs/promises';
import { createWriteStream } from 'node:fs';
import { join, basename } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { watch } from 'chokidar';
import config from 'config';

const PYTHON = '/usr/bin/python3';
const PORT = config.get('shell.port');
const TMP_DIR = config.get('shell.tmpDir');

// Socket.io server
import { Server } from 'socket.io';
const io = new Server(PORT, {
  cors: {
    origin: config.get('shell.cors.origin'),
    credentials: true,
    methods: ['GET', 'POST']
  }
});

console.log(`Pygame shell listening on port ${PORT}`);

// Reset display background
function resetDisplay() {
  try {
    execSync('xsetroot -display :1 -solid GhostWhite');
  } catch (e) {
    // Ignore errors if Xvnc not ready yet
  }
}

resetDisplay();

io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);
  resetDisplay();

  let child = null;
  let sessionDir = null;
  let watcher = null;
  let childReady = false;
  let childEnded = false;
  let processTimeout = null;
  let exitTimeout = null;
  let evalGeneration = 0;

  // Handle code execution
  socket.on('eval', async (data) => {
    try {
      // Increment generation so stale exit handlers from killed processes are ignored
      evalGeneration++;

      // Clean up any previous session before starting a new one
      if (exitTimeout) {
        clearTimeout(exitTimeout);
        exitTimeout = null;
      }
      if (child) {
        try {
          child.stdin.end();
          child.kill('SIGKILL');
        } catch (e) { /* ignore */ }
        child = null;
      }
      if (watcher) {
        await watcher.close();
        watcher = null;
      }
      if (sessionDir) {
        try { await rm(sessionDir, { recursive: true, force: true }); } catch (e) { /* ignore */ }
        sessionDir = null;
      }
      clearTimeout(processTimeout);
      childReady = false;
      childEnded = false;

      // Create session directory
      const hash = createHash('sha256');
      hash.update(Math.random().toString() + Date.now());
      const sessionId = hash.digest('hex').substring(0, 16);
      sessionDir = join(TMP_DIR, sessionId);

      await mkdir(sessionDir, { recursive: true });
      await chmod(sessionDir, 0o777);

      // Parse files from code payload
      let files = [];
      const ignoreFiles = [];

      if (data.code) {
        try {
          files = JSON.parse(data.code);
          if (!Array.isArray(files)) {
            throw new Error('Not an array');
          }
        } catch (e) {
          files = [{ name: 'main.py', content: data.code }];
        }

        // Write files to session directory
        for (const file of files) {
          if (file.name === 'assets' && Array.isArray(file.content)) {
            // Download assets
            for (const asset of file.content) {
              await downloadAsset(sessionDir, asset);
              ignoreFiles.push(join(sessionDir, asset.name));
            }
          } else {
            await writeFile(join(sessionDir, file.name), file.content, 'utf8');
          }
        }

        // Ignore the main file in watcher
        if (files[0] && files[0].name) {
          ignoreFiles.push(join(sessionDir, files[0].name));
        }
      }

      // Set up file watcher for generated files (matplotlib plots, etc.)
      const ignored = [...ignoreFiles, /[\/\\]\./];
      watcher = watch(sessionDir, {
        ignored,
        ignoreInitial: true,
        persistent: true,
        awaitWriteFinish: { stabilityThreshold: 500 }
      });

      watcher.on('add', (filepath) => {
        const filename = basename(filepath);
        import('node:fs').then(fs => {
          const buffer = fs.readFileSync(filepath);
          socket.emit('file added', { name: filename, buffer });
        });
      });

      watcher.on('change', (filepath) => {
        const filename = basename(filepath);
        import('node:fs').then(fs => {
          const buffer = fs.readFileSync(filepath);
          socket.emit('file added', { name: filename, buffer });
        });
      });

      watcher.on('error', (error) => {
        console.error('Watcher error:', error);
      });

      // Wait for watcher to be ready, then spawn Python
      watcher.on('ready', () => {
        const args = ['-u', '-B', join(sessionDir, 'main.py')];
        const options = {
          cwd: sessionDir,
          env: { ...process.env, DISPLAY: ':1' }
        };

        child = spawn(PYTHON, args, options);
        child.stdout.setEncoding('utf-8');
        child.stdin.setEncoding('utf-8');
        child.stderr.setEncoding('utf-8');

        const errors = [];

        // Capture current generation to detect stale exit handlers.
        // If a new eval arrives and kills this process, the exit handler
        // must not interfere with the new session.
        const myGeneration = evalGeneration;

        // Kill process after 60 seconds (handles turtle.mainloop() blocking forever)
        processTimeout = setTimeout(() => {
          if (child && !childEnded) {
            console.log(`Process timeout reached for ${socket.id}, killing child`);
            try {
              child.kill('SIGTERM');
              // Force kill after 2 seconds if SIGTERM doesn't work
              setTimeout(() => {
                if (child && !childEnded) {
                  child.kill('SIGKILL');
                }
              }, 2000);
            } catch (e) {
              console.error('Timeout kill error:', e);
            }
          }
        }, 60000);

        child.stdout.on('data', (data) => {
          if (myGeneration !== evalGeneration) return;
          // Check for clear screen escape sequence
          if (/\x1b\[H\x1b\[2J/.test(data)) {
            socket.emit('clear');
          } else {
            socket.emit('stdout', data);
          }
        });

        child.stderr.on('data', (data) => {
          if (myGeneration !== evalGeneration) return;
          errors.push(data);
        });

        child.on('exit', async (code, signal) => {
          // Ignore exit from a process that was superseded by a newer eval
          if (myGeneration !== evalGeneration) return;

          childEnded = true;

          if (errors.length) {
            const parsedError = parseError(errors);
            if (parsedError.trim()) {
              socket.emit('script error', { error: parsedError });
            }
          }

          // Small delay to allow file watcher to catch any final files
          setTimeout(async () => {
            socket.emit('exit');
            await cleanup();
          }, 500);
        });

        child.on('error', (err) => {
          if (myGeneration !== evalGeneration) return;
          console.error('Child process error:', err);
          socket.emit('script error', {
            error: 'Error: The Python process ended unexpectedly. Please try again.'
          });
        });

        childReady = true;
        socket.emit('child ready');
      });

    } catch (err) {
      console.error('Eval error:', err);
      socket.emit('script error', {
        error: 'Error: Failed to start Python process. Please try again.'
      });
    }
  });

  // Handle stdin input
  socket.on('write', (data) => {
    if (childReady && !childEnded && child && child.stdin.writable) {
      let input = data.input;

      // Ensure newline at end
      if (input && !input.endsWith('\n')) {
        input += '\n';
      }

      child.stdin.write(input);
    }
  });

  // Handle stop request
  socket.on('stop', () => {
    stopChild();
  });

  // Handle disconnect
  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
    stopChild();
  });

  socket.on('error', (err) => {
    console.error('Socket error:', err);
  });

  async function cleanup() {
    if (watcher) {
      await watcher.close();
      watcher = null;
    }
    if (sessionDir) {
      try {
        await rm(sessionDir, { recursive: true, force: true });
      } catch (e) {
        console.error('Cleanup error:', e);
      }
      sessionDir = null;
    }
  }

  function stopChild() {
    if (child) {
      try {
        child.stdin.end();
        child.kill('SIGKILL');
      } catch (e) {
        console.error('Kill error:', e);
      }
      child = null;
    }
    cleanup();

    // Restart Xvnc to clear display (optional, may cause flicker)
    // execSync('/usr/bin/supervisorctl restart xvnc');
  }
});

function parseError(errors) {
  let errorStr = errors.join('');

  // Filter out noise messages that aren't actual errors
  const noisePatterns = [
    /Xlib:\s+extension "RANDR" missing on display[^\n]*\n?/g,
    /pygame \d+\.\d+\.\d+ \(SDL[^)]+\)[^\n]*\n?/g,
    /Hello from the pygame community\.[^\n]*\n?/g,
    /ALSA lib[^\n]*\n?/g,
    /Failed to create secure directory[^\n]*\n?/g,
    /Cannot connect to server socket[^\n]*\n?/g,
    /Cannot connect to server request channel[^\n]*\n?/g,
    /jack server is not running[^\n]*\n?/g,
  ];

  for (const pattern of noisePatterns) {
    errorStr = errorStr.replace(pattern, '');
  }

  // Handle "Original exception was:" pattern
  const origStr = 'Original exception was:';
  if (errorStr.includes(origStr)) {
    errorStr = errorStr.substring(errorStr.indexOf(origStr) + origStr.length);
  }

  // Clean up
  errorStr = errorStr.replace(/^\n+/g, '');
  errorStr = errorStr.replace(/\n*>>> \n*/g, '');
  errorStr = errorStr.replace(/\n*\.\.\. \n*/g, '');

  return errorStr;
}

async function downloadAsset(dir, asset) {
  const filepath = join(dir, asset.name);
  const file = createWriteStream(filepath);

  try {
    const response = await fetch(asset.url);
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    await pipeline(response.body, file);
  } catch (err) {
    console.error('Asset download error:', err);
    file.close();
    try {
      await rm(filepath);
    } catch (e) {}
    throw err;
  }
}
