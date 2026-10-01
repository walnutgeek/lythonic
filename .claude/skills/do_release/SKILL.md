---
name: do_release
description: Cut a release end to end - release notes, GitHub release via gh (which publishes to PyPI), PyPI check, design-doc cleanup
disable-model-invocation: false
argument-hint: "[next_version]"
---

Cut a release. The version number is the only thing the user confirms; every
other step runs unattended. The user fixes release text afterwards through the
GitHub Edit button, so do not stop to preview notes or the release message.

Stop and report (do not ask, do not work around) whenever a step below says
**stop**.

## Step 1: Confirm the version

Find the last release tag and note it as {LAST_RELEASE_TAG}:
```bash
git fetch --tags -q && git tag --list 'v*' --sort=-v:refname | head -1
```

If `$ARGUMENTS` is a version, use it. Otherwise propose the next patch version
after {LAST_RELEASE_TAG} (`v0.0.25` -> `0.0.26`).

Ask the user once to confirm the version. This is the only question in the
whole skill. Below, {VERSION} is the confirmed version without the `v` prefix.

## Step 2: Pre-flight checks

**Stop** if any of these fail:

- On `main`: `git branch --show-current`
- Clean working tree: `git status --porcelain` is empty
- Up to date with origin: after `git pull --ff-only`, `git status -sb` shows
  no `ahead`/`behind`
- Tag is new: `git tag --list "v{VERSION}"` is empty and
  `gh release view v{VERSION}` fails
- There is something to release: `git log {LAST_RELEASE_TAG}..HEAD --oneline`
  is non-empty

Then check CI on `HEAD`, the code that is being released:
```bash
gh run list --workflow ci.yml --commit $(git rev-parse HEAD) \
  --json databaseId,status,conclusion --limit 1
```
- `completed` / `success`: continue.
- Still running: `gh run watch <databaseId> --exit-status`, then continue on
  success.
- Failed, cancelled, or no run for `HEAD`: **stop**.

## Step 3: Release notes

Generate a summary of all commits since the last tag:
```bash
git log {LAST_RELEASE_TAG}..HEAD --oneline
```

### Collect ADRs and closed tickets

Find the ADRs added or amended during this cycle:
```bash
git diff --name-status {LAST_RELEASE_TAG}..HEAD -- docs/adr 'src/*/docs/adr'
```

Find the tickets closed during this cycle. Commit messages carry the
authoritative link (`Closes #N`, `Fixes #N`); the tracker fills in the titles:
```bash
git log {LAST_RELEASE_TAG}..HEAD --grep='#[0-9]' -i --pretty=%s%n%b | grep -oiE '(closes|fixes|resolves) #[0-9]+'
gh issue view {N} --json title --jq .title  # per ticket
```

Keep both lists — they go in the release notes and in the release body.

Write a release notes file to `docs/release_notes/v{VERSION}.md` following
the style of the previous release notes (see `docs/release_notes/` for
examples). The release notes should:

- Group changes by category: **New**, **Changed**, **Fixes**, **Documentation**,
  **Dependencies** (omit empty categories)
- Be concise — one bullet per logical change, not per commit
- Collapse multiple commits for the same feature into one bullet
- Reference module paths (e.g., `lythonic.compose.namespace`) where relevant
- Call out behaviour changes to released APIs explicitly
- Do NOT list every commit — summarize the intent of related changes
- End with a **Decisions and tickets** section (omit if both lists are empty):
  the ADRs from this cycle, each as a link to its file with its title and one
  line on what it decided, and the closed tickets, each as `#N` plus its title.
  New and amended ADRs are distinguished; an amended one says what changed.

### Update release index

Add a row for `v{VERSION}` at the top of the list in
`docs/release_notes/index.md`, following the existing pattern (latest first):
```
* [v{VERSION}](v{VERSION}.md) [github](https://github.com/walnutgeek/lythonic/releases/tag/v{VERSION})
```

### Update mkdocs.yml nav

Update the `Releases:` section in `mkdocs.yml` nav to list the 3 most recent
releases (based on the full list in `release_notes/index.md`), followed by a
`"..."` link to the index page for older releases. Example with v0.0.16 being new:
```yaml
  - Releases:
      - v0.0.16: release_notes/v0.0.16.md
      - v0.0.15: release_notes/v0.0.15.md
      - v0.0.14: release_notes/v0.0.14.md
      - "...": release_notes/index.md
```

Ensure the `not_in_nav` glob in `mkdocs.yml` covers older release note files
(e.g. `/release_notes/v*.md`) so mkdocs doesn't warn about them.

Commit the release notes, index update and mkdocs.yml together as
`docs: release notes for v{VERSION}`, push, and record the commit as
{RELEASE_SHA} (`git rev-parse HEAD`). Do not wait for the CI run this push
starts: it only touches docs, and CI on the released code passed in Step 2.

## Step 4: Create the GitHub release

Find all design docs and sort by date if present in filename:
```bash
find docs/superpowers/ docs/ai/ -name \*.md 2>/dev/null
```

Come up with a title, 80 characters or less, that catches the common theme
among all changes; cut it short with "..." if there are too many things to
mention. {RELEASE_TITLE} below.

Write the release body to `$CLAUDE_JOB_DIR/tmp/release-notes.md` if that
variable is set, otherwise to a `mktemp` file. Omit each of the ADRs, Closed
and Design docs sections entirely (heading included) when its list is empty:

```markdown
**Full Changelog**: https://github.com/walnutgeek/lythonic/compare/{LAST_RELEASE_TAG}...v{VERSION}

[Release notes](https://github.com/walnutgeek/lythonic/blob/v{VERSION}/docs/release_notes/v{VERSION}.md)

**ADRs**:
- [{ADR_TITLE}](https://github.com/walnutgeek/lythonic/blob/v{VERSION}/{ADR_PATH}){AMENDED_NOTE}

**Closed**:
- #{N} {TICKET_TITLE}

**Design docs**:
https://github.com/walnutgeek/lythonic/tree/v{VERSION}/{DESIGN_DOC_PATH}
```

Create the release. Pin the tag to {RELEASE_SHA}, not `main`, so a concurrent
push cannot end up on PyPI under this version (the package version is derived
from the tag):
```bash
gh release create v{VERSION} --target {RELEASE_SHA} \
  --title "v{VERSION}: {RELEASE_TITLE}" --notes-file <body file>
```

Publishing the release triggers `.github/workflows/publish.yml`.

## Step 5: Watch the PyPI publish

Find the publish run for the release; it can take a few seconds to appear:
```bash
gh run list --workflow publish.yml --event release --commit {RELEASE_SHA} \
  --json databaseId,status --limit 1
```
Then `gh run watch <databaseId> --exit-status`.

If it fails, **stop**. Leave the release and tag in place: deleting a pushed
tag is destructive, and PyPI never accepts the same version twice, so there is
no clean rollback. Report the failed step (`gh run view <databaseId>
--log-failed | tail -50`) and how to retry once fixed:
`gh workflow run publish.yml --ref v{VERSION}`. Skip Step 7.

## Step 6: Verify on PyPI

PyPI can lag the upload briefly. Poll for up to about 5 minutes:
```bash
for i in $(seq 10); do
  curl -sf https://pypi.org/pypi/lythonic/{VERSION}/json >/dev/null && echo live && break
  sleep 30
done
```
If it never appears, report that (the workflow succeeded but PyPI does not list
the version yet) and continue.

## Step 7: Clean up design docs

The design docs stay reachable through the tag, so delete them from main:
```bash
for dir in docs/superpowers docs/ai
do
[ -d $dir ] && git rm -r $dir
done
```
If anything was removed, commit and push.

## Step 8: Report

- Release URL: `https://github.com/walnutgeek/lythonic/releases/tag/v{VERSION}`
- PyPI URL: `https://pypi.org/project/lythonic/{VERSION}/` and whether it is live
- Commits made (release notes, design-doc cleanup)
- Anything skipped or stopped, with the reason
