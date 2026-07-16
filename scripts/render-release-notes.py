#!/usr/bin/env python3
import argparse
from pathlib import Path
import sys


def parse_front_matter(path: Path):
    text = path.read_text()
    if not text.startswith('---\n'):
        return {}, text
    end = text.find('\n---\n', 4)
    if end == -1:
        return {}, text
    raw = text[4:end].splitlines()
    body = text[end + 5:].lstrip()
    data = {}
    current = None
    for line in raw:
        if not line.strip():
            continue
        if line.startswith('  ') and current:
            key, _, value = line.strip().partition(':')
            if not isinstance(data.get(current), dict):
                data[current] = {}
            data[current][key.strip()] = value.strip().strip('\"')
            continue
        key, _, value = line.partition(':')
        current = key.strip()
        data[current] = value.strip().strip('"')
    return data, body


def find_blog(blog_dir: Path, tag: str):
    matches = []
    for path in blog_dir.glob('*.md'):
        data, body = parse_front_matter(path)
        if data.get('release') == tag:
            matches.append((path, data, body))
    if not matches:
        raise SystemExit(f'no release blog post found for {tag}')
    if len(matches) > 1:
        raise SystemExit(f'multiple release blog posts found for {tag}: ' + ', '.join(str(m[0]) for m in matches))
    return matches[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--tag', required=True)
    parser.add_argument('--blog-dir', default='documentation/content/blog')
    parser.add_argument('--image-digest', default='sha256:<pending>')
    parser.add_argument('--docker-image', default='')
    parser.add_argument('--crate-version', default='')
    parser.add_argument('--docs-url', default='')
    parser.add_argument('--commit-sha', default='')
    parser.add_argument('--output')
    args = parser.parse_args()

    path, data, body = find_blog(Path(args.blog_dir), args.tag)
    if data.get('release') != args.tag:
        raise SystemExit('release front matter does not match tag')
    version = args.tag[1:] if args.tag.startswith('v') else args.tag
    artifacts = data.get('artifacts', {})
    docker_image = args.docker_image or str(artifacts.get('docker_image', ''))
    if docker_image and not docker_image.endswith(f':{args.tag}'):
        raise SystemExit(
            f'docker image must end with :{args.tag}'
        )
    rust_crate = str(artifacts.get('rust_crate', ''))
    if rust_crate and not rust_crate.endswith(f' {version}'):
        raise SystemExit(
            f'release blog rust_crate artifact must end with version {version}'
        )
    crate_version = args.crate_version or rust_crate
    notes = body.rstrip() + '\n\n## Artifact metadata\n\n'
    notes += f'- Release tag: `{args.tag}`\n'
    notes += f'- Source commit: `{args.commit_sha or "<unknown>"}`\n'
    if docker_image:
        notes += f'- Docker image: `{docker_image}`\n'
    else:
        notes += '- Docker image: `<not provided>`\n'
    notes += f'- Docker digest: `{args.image_digest}`\n'
    notes += f'- Rust crate: `{crate_version}`\n'
    if args.docs_url:
        notes += f'- Documentation: {args.docs_url}\n'
    notes += f'\nGenerated from `{path}`.\n'
    if args.output:
        Path(args.output).write_text(notes)
    else:
        print(notes)

if __name__ == '__main__':
    main()
