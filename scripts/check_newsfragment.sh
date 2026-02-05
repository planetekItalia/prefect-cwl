#!/usr/bin/env bash
set -euo pipefail

# Files staged for commit
CHANGED=$(git diff --cached --name-only)

# If only docs / ci / non-code files changed, skip
if ! echo "$CHANGED" | grep -Eq '\.py$'; then
  exit 0
fi

# If a newsfragment is staged, we're good
if echo "$CHANGED" | grep -Eq '^newsfragments/.+'; then
  exit 0
fi

cat << 'EOF'
❌ Python code changes detected, but no towncrier newsfragment staged.

Please add a file under newsfragments/
  e.g. newsfragments/123.feature.rst

If this change does not require a changelog entry (docs/refactor/ci),
you can skip with:
  git commit --no-verify
EOF

exit 1
