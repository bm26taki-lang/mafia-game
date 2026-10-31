Simulation tests (npm run test:sim)
These run server.js against a small FAKE of express/socket.io (tests/stubs), with fake timers.
They check room + role logic only. They are NOT a replacement for testing with the real socket.io,
a real browser and real devices.
