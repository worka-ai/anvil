#!/usr/bin/env bash
set -euo pipefail
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/blog"
cat > "$scratch/blog/release.md" <<'MD'
---
title: "Anvil v9.9.9: Test"
release: v9.9.9
release_date: 2026-07-06
artifacts:
  rust_crate: anvil-storage 9.9.9
---

# Anvil v9.9.9

Release body.
MD
python3 scripts/render-release-notes.py --tag v9.9.9 --blog-dir "$scratch/blog" --docker-image ghcr.io/example/anvil:v9.9.9 --image-digest sha256:test --crate-version 'anvil-storage 9.9.9' --commit-sha deadbeef --docs-url https://docs.example.invalid > "$scratch/notes.md"
grep -q 'Release body.' "$scratch/notes.md"
grep -q 'sha256:test' "$scratch/notes.md"
grep -q 'anvil-storage 9.9.9' "$scratch/notes.md"
if python3 scripts/render-release-notes.py --tag v0.0.0 --blog-dir "$scratch/blog" >/tmp/anvil-release-missing.out 2>&1; then
  echo 'missing blog should fail' >&2
  exit 1
fi
grep -q 'no release blog post found for v0.0.0' /tmp/anvil-release-missing.out
cat > "$scratch/blog/bad-artifacts.md" <<'MD'
---
title: "Anvil v9.9.8: Bad"
release: v9.9.8
release_date: 2026-07-06
artifacts:
  docker_image: ghcr.io/example/anvil:v9.9.7
  rust_crate: anvil-storage 9.9.8
---

# Bad release
MD
if python3 scripts/render-release-notes.py --tag v9.9.8 --blog-dir "$scratch/blog" >/tmp/anvil-release-bad-artifacts.out 2>&1; then
  echo 'wrong artifact version should fail' >&2
  exit 1
fi
grep -q 'docker image must end with :v9.9.8' /tmp/anvil-release-bad-artifacts.out
cat > "$scratch/blog/bad-crate.md" <<'MD'
---
title: "Anvil v9.9.7: Bad"
release: v9.9.7
release_date: 2026-07-06
artifacts:
  docker_image: ghcr.io/example/anvil:v9.9.7
  rust_crate: anvil-storage 9.9.6
---

# Bad release
MD
if python3 scripts/render-release-notes.py --tag v9.9.7 --blog-dir "$scratch/blog" >/tmp/anvil-release-bad-crate.out 2>&1; then
  echo 'wrong crate version should fail' >&2
  exit 1
fi
grep -q 'rust_crate artifact must end with version 9.9.7' /tmp/anvil-release-bad-crate.out
