#!/usr/bin/env python3
"""Conservative prose-only CI classification; unknown inputs run the full gates."""
from __future__ import annotations

import argparse
import difflib
import json
import os
from pathlib import Path
import re
import subprocess

ROOT_PROSE = frozenset({'README.md', 'CHANGELOG.md'})
# Public reader pages only. Additions require review of code/data consumers too.
PUBLIC_PROSE = frozenset({
    'docs/api/assertions.mdx',
    'docs/api/documents.mdx',
    'docs/api/entities.mdx',
    'docs/api/overview.mdx',
    'docs/api/predicates.mdx',
    'docs/api/relationships.mdx',
    'docs/api/sources.mdx',
    'docs/api/system.mdx',
    'docs/architecture/domain-model.mdx',
    'docs/architecture/overview.mdx',
    'docs/cli/overview.mdx',
    'docs/concepts/assertions.mdx',
    'docs/concepts/documents.mdx',
    'docs/concepts/entities.mdx',
    'docs/concepts/events.mdx',
    'docs/concepts/overview.mdx',
    'docs/concepts/predicates.mdx',
    'docs/concepts/relationships.mdx',
    'docs/concepts/security.mdx',
    'docs/concepts/spaces.mdx',
    'docs/concepts/truth-maintenance.mdx',
    'docs/guides/building-graphs.mdx',
    'docs/guides/event-streaming.mdx',
    'docs/guides/ingesting-data.mdx',
    'docs/guides/putting-it-together-part2.mdx',
    'docs/guides/putting-it-together.mdx',
    'docs/guides/searching.mdx',
    'docs/guides/truth-slots.mdx',
    'docs/installation.mdx',
    'docs/introduction.mdx',
    'docs/mcp/overview.mdx',
    'docs/mcp/tools.mdx',
    'docs/mcp/workflows.mdx',
    'docs/operations/deployment.mdx',
    'docs/operations/development.mdx',
    'docs/operations/observability.mdx',
    'docs/operations/scenarios.mdx',
    'docs/operations/service-setup.mdx',
    'docs/operations/surrealdb-patterns.mdx',
    'docs/pipeline/extraction.mdx',
    'docs/pipeline/overview.mdx',
    'docs/pipeline/truth-maintenance.mdx',
    'docs/pipeline/work-queue.mdx',
    'docs/quickstart.mdx',
})
# living-wiki.mdx is an include_str! input to the demo-wiki executable.
# Internal plans, other Markdown, configuration, examples and scripts never qualify.
SHA = re.compile(r'[0-9a-f]{40}|[0-9a-f]{64}')
FENCE = re.compile(r'^\s*(`{3,}|~{3,})')
SENSITIVE = re.compile(r'[`<>{}]|^\s*(?:import|export)\b|^(?: {4}|\t)\S')


def protected_content(text: str) -> tuple[str, ...]:
    """Preserve code/configuration exactly, including MDX attributes/expressions.

    This intentionally prefers false negatives: prose containing inline code or
    indented lines is not eligible when changed. Malformed fences/frontmatter
    also take the full path; the documentation check still reports their errors.
    """
    if '\x00' in text:
        raise ValueError('binary documentation')
    lines = text.splitlines(keepends=True)
    protected: list[str] = []
    frontmatter = bool(lines and lines[0].strip() == '---')
    fence: str | None = None
    for index, line in enumerate(lines):
        if frontmatter:
            protected.append(line)
            if index and line.strip() == '---':
                frontmatter = False
            continue
        match = FENCE.match(line)
        if fence:
            protected.append(line)
            if match and match[1][0] == fence[0] and len(match[1]) >= len(fence):
                fence = None
        elif match:
            fence = match[1]
            protected.append(line)
        elif SENSITIVE.search(line):
            protected.append(line)
    if frontmatter or fence:
        raise ValueError('unclosed configuration or code block')
    return tuple(protected)


def prose_only(path: str, before: bytes, after: bytes) -> bool:
    if path not in ROOT_PROSE | PUBLIC_PROSE:
        return False
    try:
        old, new = before.decode('utf-8'), after.decode('utf-8')
        if protected_content(old) != protected_content(new):
            return False
        # A change from ordinary prose to an indented blank line can alter a
        # neighboring Markdown code block. Reject changed whitespace-only lines
        # with indentation as well as sensitive lines on either side.
        for line in difflib.ndiff(old.splitlines(), new.splitlines()):
            if line[:2] in {'- ', '+ '}:
                value = line[2:]
                if SENSITIVE.search(value) or (value and not value.strip()):
                    return False
        return True
    except (UnicodeError, ValueError):
        return False


def git(root: Path, *args: str) -> bytes:
    return subprocess.check_output(['git', '-C', str(root), *args], stderr=subprocess.PIPE)


def classify(root: Path, event: str, base: str, head: str) -> dict:
    result = {'docs_only': False, 'reason': 'Full validation is the default', 'paths': []}
    if not SHA.fullmatch(head):
        raise ValueError('head must be an exact commit identifier')
    git(root, 'cat-file', '-e', f'{head}^{{commit}}')
    if event == 'workflow_dispatch':
        result['reason'] = 'Manual runs always execute the full gates'
        return result
    if event not in {'push', 'pull_request'}:
        result['reason'] = 'Unknown event always executes the full gates'
        return result
    if base and set(base) == {'0'}:
        result['reason'] = 'Initial push has no trusted comparison base'
        return result
    if not SHA.fullmatch(base):
        raise ValueError('base must be an exact available commit identifier')
    git(root, 'cat-file', '-e', f'{base}^{{commit}}')
    git(root, 'merge-base', '--is-ancestor', base, head)
    if event == 'pull_request':
        parents = git(root, 'rev-list', '--parents', '-n', '1', head).decode().split()[1:]
        if len(parents) != 2 or parents[0] != base:
            raise ValueError('PR checkout must be the merge result with its actual base parent')
    raw = git(root, 'diff', '--raw', '-z', '--no-renames', base, head, '--')
    fields = raw.split(b'\x00')
    if fields.pop() != b'' or len(fields) % 2:
        raise ValueError('invalid Git change inventory')
    if not fields:
        result['reason'] = 'Empty comparison does not establish a prose change'
        return result
    eligible = True
    for header, encoded_path in zip(fields[::2], fields[1::2]):
        path = encoded_path.decode('utf-8', errors='strict')
        result['paths'].append(path)
        parts = header.decode('ascii').split()
        if len(parts) != 5:
            raise ValueError('invalid Git raw change record')
        old_mode, new_mode, _, _, status = parts
        if old_mode != ':100644' or new_mode != '100644' or status != 'M':
            eligible = False
            continue
        if path not in ROOT_PROSE | PUBLIC_PROSE:
            eligible = False
            continue
        eligible &= prose_only(path, git(root, 'show', f'{base}:{path}'), git(root, 'show', f'{head}:{path}'))
    result.update(docs_only=eligible, reason=('Only reviewed reader prose changed; code/configuration and runtime inputs are unchanged' if eligible else 'At least one path, mode or changed content requires full validation'))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--event', required=True)
    parser.add_argument('--base', default='')
    parser.add_argument('--head', required=True)
    args = parser.parse_args()
    result = classify(args.root, args.event, args.base, args.head)
    print(json.dumps(result, indent=2))
    if output := os.environ.get('GITHUB_OUTPUT'):
        with open(output, 'a', encoding='utf-8') as stream:
            stream.write(f"docs_only={str(result['docs_only']).lower()}\n")
    if summary := os.environ.get('GITHUB_STEP_SUMMARY'):
        with open(summary, 'a', encoding='utf-8') as stream:
            stream.write('### Change classification\n\n' + result['reason'] + '.\n')


if __name__ == '__main__':
    main()
