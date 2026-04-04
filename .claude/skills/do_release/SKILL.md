---
name: do_release
description: Prepare release notes and clean up superpowers docs for the next version
disable-model-invocation: true
argument-hint: <next_version>
---

Prepare a release for version $ARGUMENTS.

## Step 1: Generate Release Notes

Ensure that you are on main branch. Stop if you are not.

Find the last release tag (highest `v*` tag) and generate a summary of all
commits since that tag.

```bash
git log $(git tag --list 'v*' --sort=-v:refname | head -1)..HEAD --oneline
```

Note that tag and substitute everywhere you see {LAST_RELEASE_TAG}.

Write a release notes file to `docs/release_notes/v$ARGUMENTS.md` following
the style of the previous release notes (see `docs/release_notes/` for
examples). The release notes should:

- Group changes by category: **New**, **Changed**, **Fixes**, **Documentation**,
  **Dependencies** (omit empty categories)
- Be concise — one bullet per logical change, not per commit
- Collapse multiple commits for the same feature into one bullet
- Reference module paths (e.g., `lythonic.compose.namespace`) where relevant
- Do NOT list every commit — summarize the intent of related changes

Commit and push release notes.

## Step 2: Draft release message


Find all design docs and sort by date if present in filename:
```bash
find docs/superpowers/ docs/ai/ -name \*.md 2>/dev/null
```
Craft snippet, keep it empty if no design docs found, otherwise follow template: 
---
**Design docs**: 
<foreach design docs>
https://github.com/walnutgeek/lythonic/tree/v$ARGUMENTS/{DESIGN_DOC_PATH}
</foreach>

---
Replace {DESIGN_DOCS_SNIPPET} in the message below.


Come up with a title for this release, 80 characters or less. Try to catch the
common theme among all changes, yet you can cut it short with "..." if there
are too many things to mention.

Replace {RELEASE_TITLE} with that title in the message below.

Display the message for human review:
---

Draft new release at: https://github.com/walnutgeek/lythonic/releases/new

Title: v$ARGUMENTS: {RELEASE_TITLE}

**Full Changelog**: https://github.com/walnutgeek/lythonic/compare/{LAST_RELEASE_TAG}...v$ARGUMENTS

[Release notes](https://github.com/walnutgeek/lythonic/blob/main/docs/release_notes/v$ARGUMENTS.md)

{DESIGN_DOCS_SNIPPET}

---

Wait for human to confirm that the release is triggered.

When human confirms, check if the tag exists:
```bash
git pull && git tag --list "v$ARGUMENTS" | wc -l
```
Output should confirm exactly one matching tag. Do not proceed to the next step
if it does not.

## Step 3: Clean up design docs

After a release is properly tagged, the design docs are accessible via the tag.
Delete them from main:
```bash
for dir in docs/superpowers docs/ai
do
[ -d $dir ] && git rm -r $dir
done
```

Commit and push.
