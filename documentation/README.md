# Anvil Documentation Site

This folder contains the Fission static documentation site for Anvil.

The site is intentionally structured as a learning path, not a command dump. The home page explains why Anvil exists. The Learn section teaches object storage, key design, indexing, search, authorisation, watches, and PersonalDB from first principles. The Developer section then applies those concepts to application code. The Operator section explains deployment, identity, indexing operations, backup, and release work. The Reference section is for exact settings, command families, package outputs, and errors after the concepts are clear.

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
- `content/developers/` - application development guide.
- `content/operators/` - deployment, operations, and release guide.
- `content/reference/` - configuration, CLI, security errors, and packages.
- `site/*-sidebar.toml` - navigation order for documentation sections.
