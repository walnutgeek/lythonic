---
name: release_prep
description: Prepare release notes and clean up superpowers docs for the next version
disable-model-invocation: true
argument-hint: <next_version>
---

Prepare a release for version $ARGUMENTS.

## Step 1: Generate Release Notes

Find the last release tag (highest `v*` tag) and generate a summary of all
commits since that tag.

```bash
git log $(git tag --list 'v*' --sort=-v:refname | head -1)..HEAD --oneline
```

Write a release notes file to `docs/release_notes/v$ARGUMENTS.md` following
the style of the previous release notes (see `docs/release_notes/` for
examples). The release notes should:

- Group changes by category: **New**, **Changed**, **Fixes**, **Documentation**,
  **Dependencies** (omit empty categories)
- Be concise — one bullet per logical change, not per commit
- Collapse multiple commits for the same feature into one bullet
- Reference module paths (e.g., `lythonic.compose.namespace`) where relevant
- Do NOT list every commit — summarize the intent of related changes

## Step 2: Review superpowers docs

Per `documenting.md`, `docs/superpowers/` is wiped after release. List the
specs and plans that will be archived with this release:

```bash
find docs/superpowers/specs docs/superpowers/plans -name '*.md' | sort
```

Mention this in the release notes under a **Design Documents** section — just
list the spec/plan filenames so the release tag preserves context.

## Step 3: Run verification

```bash
make lint && make test
```

Report the result. Do NOT proceed if tests fail.

## Step 4: Present for review

Show the release notes to the user and ask for approval before committing.
Do NOT commit, tag, or push without explicit approval.

Once approved:
```bash
git add docs/release_notes/v$ARGUMENTS.md
git commit -m "docs: add v$ARGUMENTS release notes"
```

Remind the user of the remaining manual steps:
1. Push to main: `git push`
2. Create GitHub release with tag `v$ARGUMENTS`
3. GitHub Actions will run tests, build, and publish to PyPI
4. After release: clean `docs/superpowers/specs/` and `docs/superpowers/plans/`
