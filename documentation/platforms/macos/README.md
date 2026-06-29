# macOS target

Runnable target. Desktop platforms share the default `src/main.rs` entrypoint through `DesktopApp`.

- Run `fission run --project-dir .` from the project root to launch the desktop app and attach output.
- Run `fission build --project-dir . --release` for a release desktop build.
- Run `fission test --project-dir .` for the app crate's Rust tests.
- This target uses the default Vello desktop shell path.
