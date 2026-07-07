# Anvil Documentation Site

This folder contains the Fission static documentation site for Anvil.

The site is intentionally structured as a learning path, not a command dump. The home page explains why Anvil exists. Learn teaches the core concepts from first principles. Tutorials turn those concepts into complete workflows. Operators is the production runbook. CLIs is the exact command reference for the public and admin command lines.

## Build

```sh
fission site build --project-dir documentation
```

The generated static site is written to `documentation/target/fission/site`.

## Check routes and content

```sh
fission site check --project-dir documentation --release
fission site routes --project-dir documentation
```

## Serve locally

```sh
fission site serve --project-dir documentation
```

## Content layout

- `src/app.rs` - custom Fission marketing home page.
- `content/learn/` - progressive conceptual guide.
- `content/tutorials/` - operation tutorials using the Rust client shipped in this release.
- `content/operators/` - operator book for deployment, operations, repair, security, and release readiness.
- `content/reference/` - public and administrative CLI references.
- `site/*-sidebar.toml` - navigation order for documentation sections.
