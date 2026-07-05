# Fission App Guidelines

These instructions apply when building or reviewing a Fission-based app in this
tree.

## Source-Grounded Work

- Start from the real app entrypoint, then trace into screens, reusable widgets,
  and lower-level render behavior before changing UI code.
- For UI reviews, group findings by visible region. Do not stop at a screenshot
  description; trace the component and widget code that creates each issue.
- When a visual problem looks small, keep looking for related spacing,
  alignment, overflow, typography, state, and target-specific issues before
  reporting completion.
- Keep edits scoped to the component, widget, route, shell, or target behavior
  being changed. Avoid broad cleanup unless it is required for the task.

## Widget Structure

- Prefer one reusable widget per file when introducing app UI.
- Model widgets as concrete structs and implement `From<YourWidget> for Widget`.
- Use `#[fission_component]` for components that own retained local widget
  state. Do not model retained UI state as ordinary mutable struct fields.
- Keep screen modules focused on app state, routing, effects, and composition.
  Move reusable presentation pieces into widget modules.
- Avoid screen-level helper functions that build large `Widget` trees. If a UI
  fragment is meaningful or reused, make it a named widget struct instead.
- Small private helper functions are acceptable for narrow formatting,
  conversion, or leaf construction, but they should not hide reusable component
  boundaries. In fact, completely avoid functions that return Widget altogether if possible.
  Fission is a retained UI like Flutter, just like Flutter, functions break some of the optimisations Fission can do with Widget objects so building a UI with functions is an antipattern that should be avoided in almost all cases.

Local-state component shape:

```rust
use fission::prelude::*;

#[fission_component]
pub struct DisclosureSection {
    pub title: String,

    #[local_state(default = false)]
    open: bool,
}

#[fission_reducer(ToggleOpen)]
fn on_toggle_open(open: &mut bool) {
    *open = !*open;
}

impl From<DisclosureSection> for Widget {
    fn from(section: DisclosureSection) -> Widget {
        let (ctx, _) = fission::build::current::<()>();
        let open = section.open();
        let toggle = ctx.bind_local(ToggleOpen, open.clone(), reduce!(on_toggle_open));

        Column {
            gap: Some(8.0),
            children: widgets![
                Button {
                    on_press: Some(toggle),
                    child: Some(Text::new(section.title).into()),
                    ..Default::default()
                },
                if open.get() {
                    Text::new("The details are visible.").into()
                } else {
                    Text::new("The details are hidden.").into()
                },
            ],
            ..Default::default()
        }
        .into()
    }
}
```

Use `fission::build::current::<()>()` only when the component is intentionally
state-agnostic and does not read app state. Use the concrete app state type when
the component reads `GlobalState`, reads environment values tied to that app, or
binds reducers that update app state.

## State, Reducers, Routing and Runtime Data

- Choose the smallest state bucket that matches the lifetime and ownership of
  the data.
- Use `GlobalState` for durable app truth: product data, routing, sync,
  persistence, user preferences, shared filters, and values read by distant
  screens.
- Use `#[local_state]` for retained UI memory owned by one widget identity:
  open/closed flags, isolated draft text, local selected tabs, hoverless
  interaction state, or a counter-like local value.
- Use reducers on `GlobalState` when an action updates app data. Use
  `ctx.bind_local(...)` when an action updates one local-state field.
- Use reducers for explicit state transitions. Match the dispatched action to the
  reducer registration used by the widget.
- Do not store `BuildCtxHandle` or `ViewHandle` in structs, reducers, services,
  async tasks, statics, or other long-lived places. They are build-scope handles.
- Do not mutate `GlobalState` during component conversion. Dispatch actions and
  update state in reducers.
- Do not start network requests, file writes, or host operations during component
  conversion. Request work through effects, jobs, services, capabilities, or
  resources.
- For local-state components rendered from dynamic, reorderable, insertable, or
  filterable data, assign stable widget identities with
  `.id(WidgetId::explicit(...))`. Use a durable data id, not the list index.
- Do not pad production UI with fixture, mock, or demo data. Production paths
  should render persisted data, an explicit empty state, or a clear error state.
- Keep fixture parsing, fixture environment variables, and test-only data inside
  test infrastructure.
- When adding routed screens, verify the frontend route shape and any backend or
  deep-link route shape intentionally match.
- Use Fission's native Router and RouterParams (https://fission.rs/reference/widgets/router/). This ensures routing behaves consistently across ios, android, windows, linux, mac, terminal, static and server rendered sites

## Design System

Fission supports Adobe's Design System Package (DSP). Use DSP JSON as the design
source, generate the Rust design-system type at build time, and read generated
tokens from the active Fission environment at runtime.

Use these Fission defaults as the starting point for a custom design system:

- Copy `dsp.json` from
  <https://github.com/fission-ui/fission/blob/main/crates/core/fission-theme/design/default/dsp.json>
  into the app, for example at `design/dsp.json`.
- Copy `tokens.json` from
  <https://github.com/fission-ui/fission/blob/main/crates/core/fission-theme/design/default/tokens.json>
  into the app, for example at `design/tokens.json`.
- Customize the checked-in copies. Do not parse the upstream default files at
  runtime.
- Follow the official design-system example:
  <https://github.com/fission-ui/fission/tree/main/examples/todo-design-system>.

Add the code generator as a build dependency. Use the dependency source that
matches this repository, and keep it version-aligned with the Fission runtime.
Do not leave a path dependency in place unless that path exists in this checkout.

Outside the Fission source workspace, prefer the published crate pinned to the same Fission revision used by the app:

```toml
[build-dependencies]
fission-design-system-codegen = { version = "0.6.1" }
```

Generate a typed design system from the copied DSP file in `build.rs`:

```rust
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=design/dsp.json");
    println!("cargo:rerun-if-changed=design/tokens.json");

    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let dsp_path = manifest_dir.join("design/dsp.json");

    fission_design_system_codegen::generate(fission_design_system_codegen::Config {
        dsp_path,
        out_file: "app_design_system.rs".into(),
        type_name: "AppDesignSystem".into(),
        crate_path: "fission::theme".into(),
    })
    .expect("failed to generate AppDesignSystem from design/dsp.json");
}
```

Include the generated type and install it on app startup:

```rust
use fission::prelude::*;

include!(concat!(env!("OUT_DIR"), "/app_design_system.rs"));

#[derive(Debug, Clone, PartialEq, Eq)]
struct AppState {
    theme_mode: DesignMode,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            theme_mode: DesignMode::Light,
        }
    }
}

impl GlobalState for AppState {}

fn main() -> anyhow::Result<()> {
    DesktopApp::<AppState, _>::new(App)
        .with_design_system::<AppDesignSystem>(DesignMode::Light)
        .with_sync_env(|state: &AppState, env: &mut Env| {
            env.theme = AppDesignSystem::theme(state.theme_mode);
        })
        .run()
}
```

Inside widgets, read colors, spacing, typography, radii, and component styling
from `view.env().theme`:

```rust
impl From<App> for Widget {
    fn from(_app: App) -> Widget {
        let (_ctx, view) = fission::build::current::<AppState>();
        let tokens = &view.env().theme.tokens;

        Container::new(Text::new("Dashboard"))
            .bg(tokens.colors.background)
            .padding_all(tokens.spacing.xl)
            .into()
    }
}
```

Do not guess token names. Inspect the generated Rust output or the copied DSP
and tokens files before wiring new design values.

## Theme and Locale Synchronization

Theme and locale are app-wide presentation inputs. Keep user-controlled choices
in `GlobalState`, then mirror them into `Env` with `.with_sync_env(...)`.

```rust
DesktopApp::<AppState, _>::new(App)
    .with_env(create_env()?)
    .with_design_system::<AppDesignSystem>(DesignMode::Light)
    .with_sync_env(|state: &AppState, env: &mut Env| {
        env.locale = state.locale.clone();
        env.theme = AppDesignSystem::theme(state.theme_mode);
    })
    .run()
```

Use `.with_sync_env(...)` only for app-wide presentation inputs such as theme,
locale, and similar environment values. Do not use it for network loading, file
writes, job startup, or ordinary domain data.

## Internationalization

Follow the Fission theming and i18n guide:
<https://github.com/fission-ui/fission/blob/main/documentation/content/docs/guides/theming-and-i18n.mdx>.

- Do not hard-code user-facing text inside reusable widgets.
- Use stable translation keys based on meaning, not on the current layout.
- Keep translations in checked-in files and embed them with `include_str!` unless
  the app has a stronger reason to load translations dynamically.
- Parse translation files into `HashMap<String, String>`, wrap each map in a
  `TranslationBundle`, and add it to `env.i18n`.
- If the user can change language, store the current locale in `GlobalState` and
  mirror it to `env.locale` in `.with_sync_env(...)`.
- Test long translated strings, not only short English labels.

Example translation files:

```yaml
# i18n/en-US.yaml
settings.title: "Settings"
settings.theme.light: "Light"
settings.theme.dark: "Dark"
settings.language.english: "English"
settings.language.spanish: "Spanish"
```

```yaml
# i18n/es-ES.yaml
settings.title: "Configuracion"
settings.theme.light: "Claro"
settings.theme.dark: "Oscuro"
settings.language.english: "Ingles"
settings.language.spanish: "Espanol"
```

Add a YAML parser if the app uses YAML translation files:

```toml
[dependencies]
serde_yaml = "0.9"
```

Load bundles into `Env`:

```rust
use std::collections::HashMap;

use fission::i18n::{Locale, TranslationBundle};
use fission::prelude::*;

fn load_bundle(locale: &str, raw_yaml: &str) -> anyhow::Result<TranslationBundle> {
    let messages: HashMap<String, String> = serde_yaml::from_str(raw_yaml)?;

    Ok(TranslationBundle {
        locale: Locale::from(locale),
        messages,
    })
}

fn create_env() -> anyhow::Result<Env> {
    let mut env = Env::default();

    env.i18n
        .add_bundle(load_bundle("en-US", include_str!("../i18n/en-US.yaml"))?);
    env.i18n
        .add_bundle(load_bundle("es-ES", include_str!("../i18n/es-ES.yaml"))?);

    Ok(env)
}
```

Render translated text with keys instead of literals:

```rust
Text::new(TextContent::Key("settings.title".into()))
```

For SSR and Static site targets, seed `Env` with the same bundles. Resolve the
locale from request or build context in the shell instead of making individual
widgets guess.

## Source References

- Design-system example directory:
  <https://github.com/fission-ui/fission/tree/main/examples/todo-design-system>
- Design-system example `build.rs`:
  <https://github.com/fission-ui/fission/blob/main/examples/todo-design-system/build.rs>
- Design-system example `main.rs`:
  <https://github.com/fission-ui/fission/blob/main/examples/todo-design-system/src/main.rs>
- Theming and i18n guide:
  <https://github.com/fission-ui/fission/blob/main/documentation/content/docs/guides/theming-and-i18n.mdx>
- State, handles, and providers guide:
  <https://github.com/fission-ui/fission/blob/main/documentation/content/docs/guides/state-handles-and-providers.mdx>
- Default DSP package:
  <https://github.com/fission-ui/fission/blob/main/crates/core/fission-theme/design/default/dsp.json>
- Default token file:
  <https://github.com/fission-ui/fission/blob/main/crates/core/fission-theme/design/default/tokens.json>

## Validation

- Run formatting before handing off code changes.
- Run the narrowest compile or test command that exercises the changed app,
  widget crate, route, or target shell.
- For UI changes, verify a real rendered target when possible, and check mobile
  and desktop layouts when the screen is responsive.
- If docs or configuration target names are changed, keep terminology consistent
  with Fission's public target names: `macOS`, `Windows`, `Linux`, `Web`,
  `Android`, `iOS`, `Terminal`, `Static site`, and `SSR`.
