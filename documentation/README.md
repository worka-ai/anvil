# Anvil Documentation Site

This folder contains the Fission static documentation site for Anvil.

## Build

```sh
fission site build --project-dir documentation
```

The generated static site is written to `documentation/target/fission/site`.

## Check routes

```sh
fission site routes --project-dir documentation
```

## Serve locally

```sh
fission site serve --project-dir documentation
```

## Content layout

- `content/index.md` - documentation landing page
- `content/developers/` - API and integration documentation
- `content/operators/` - deployment, security, runbook, and release documentation
- `content/reference/` - configuration and API references
