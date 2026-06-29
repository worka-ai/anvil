# Static site target

Static multi-page website target. The site shell renders Markdown content through real Fission widgets, lowers nodes to Core IR, and emits semantic static HTML.

- Add Markdown or MDX content under `content/`.
- Run `fission site routes --project-dir .` to list generated routes.
- Run `fission site build --project-dir .` to render HTML into `target/fission/site`.
- Run `fission site serve --project-dir .` to build and serve the generated site locally.
- Unsupported interactive widgets fail during the static render instead of silently falling back to JavaScript.
