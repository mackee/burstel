# AGENTS.md

Guidance for coding agents working on **burstel** through itako.

## Workflow

- Specs live in the itako project `burstel`.
- Read the spec with `spec_get` before implementing anything.
- Implement against the current revision and its acceptance criteria. Do not assume an older draft.
- Commit your work inside the sandbox only.
- Push only through the itako `git_push` capability `git:push:burstel`.
- Never run a raw `git push` (no `git push`, `gh`, API calls, or credential setup).
- Do not modify specs unless explicitly asked.

## Toolchain on build-host-1 (observed)

| Tool | Version |
| --- | --- |
| bun | 1.4.2 |
| go | absent |
| node | absent |
| git | 2.55.0 |
