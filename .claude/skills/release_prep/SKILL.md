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

Note that tag and subtitue everywhere you see {LAST_RELEASE_TAG}

Write a release notes file to `docs/release_notes/v$0.md` following
the style of the previous release notes (see `docs/release_notes/` for
examples). The release notes should:

- Group changes by category: **New**, **Changed**, **Fixes**, **Documentation**,
  **Dependencies** (omit empty categories)
- Be concise — one bullet per logical change, not per commit
- Collapse multiple commits for the same feature into one bullet
- Reference module paths (e.g., `lythonic.compose.namespace`) where relevant
- Do NOT list every commit — summarize the intent of related changes


Commit and push release notes.

## Step 2: Review generate template for 

Come up with title for this release 80 words or less, try to catch common theme among all changes, yet you can cat it short with "..." if there are too many things to mention

Replace {RELEASE_TITLE} with that title in the message for humman in the loop 

Display the message for humman:
"""
Draft new release at: https://github.com/walnutgeek/lythonic/releases/new

Title: v$0: {RELEASE_TITLE}

**Full Changelog**: https://github.com/walnutgeek/lythonic/compare/{LAST_RELEASE_TAG}...v$0

**Design docs**: [v$0/docs/superpowers](https://github.com/walnutgeek/lythonic/tree/v$0/docs/superpowers)

**Release notes**: [v$0](https://github.com/walnutgeek/lythonic/blob/main/docs/release_notes/v$0.md)
"""

Wait for human to confirm that release is triggered.

When human confirmed, check if tag aleady exist ```git pull; git tag --list "v$1"|wc -l``` Output from git tag command should confirm that one tag matching. Don't proceed to the next step if it is not.

## Ster 3: Cleaning up old design docs

After a release is properly tagged, we can easily find old design docs by that tag. So, we want to delete them by `git rm -r docs/superpowers` then commit and push
