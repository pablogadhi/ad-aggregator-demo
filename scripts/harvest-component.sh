#!/usr/bin/env bash
# Push a component built in this design back to the template, as branch component/<name>.
#
#   scripts/harvest-component.sh <component>
#
# Only infra/components/<component>/ (and PLAYBOOK.md, if this design updated it) are included —
# nothing design-specific. Review and merge in the template:
#   cd <template> && git diff main...component/<component> && git merge component/<component>
source "$(dirname "$0")/lib.sh"

comp=${1:-}; [ -n "$comp" ] || die "usage: harvest-component.sh <component>"
cd "$SDL_ROOT"
[ -d "infra/components/$comp/flux" ] || die "infra/components/$comp/flux/ missing (the Flux base, see AUTHORING.md)"
[ -x "infra/components/$comp/smoke.sh" ] || die "infra/components/$comp/smoke.sh missing — components need a smoke test before harvesting"
# the template is the 'template' remote (designs published to their own repo); older designs: 'origin'
remote=template
git remote get-url "$remote" >/dev/null 2>&1 || remote=origin
url=$(git remote get-url "$remote" 2>/dev/null) || die "no 'template' (or 'origin') remote pointing at the template — is this a design created with new-design.sh?"

git fetch -q "$remote" main
wt="$SDL_ROOT/.cache/harvest-$comp"
rm -rf "$wt"; git worktree prune
git worktree add -q --detach "$wt" "$remote/main"
trap 'git -C "$SDL_ROOT" worktree remove --force "$wt" >/dev/null 2>&1 || true' EXIT

rm -rf "$wt/infra/components/$comp"
cp -r "infra/components/$comp" "$wt/infra/components/$comp"
files=("infra/components/$comp")
if ! git diff --quiet "$remote/main" --infra/components/PLAYBOOK.md 2>/dev/null; then
  cp infra/components/PLAYBOOK.md "$wt/infra/components/PLAYBOOK.md"
  files+=("infra/components/PLAYBOOK.md")
fi

git -C "$wt" add -A "${files[@]}"
if git -C "$wt" diff --cached --quiet; then
  ok "template already has this version of '$comp' — nothing to harvest"; exit 0
fi
git -C "$wt" commit -qm "component: $comp (harvested from design $(basename "$SDL_ROOT"))"
git -C "$wt" push -qf "$remote" "HEAD:refs/heads/component/$comp"
ok "pushed component/$comp to $url"
echo "   review + merge in the template:  git diff main...component/$comp && git merge component/$comp"
