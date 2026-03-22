#!/usr/bin/env bash
set -euo pipefail
INTERVAL="${1:-60}"
BRANCH="$(git branch --show-current)"
log() {
  printf '[%s] %s\n' "$(date '+%F %T')" "$*"
}
while true; do
  log "=== nytt varv börjar ==="
  log "branch: $BRANCH"
  log "fetchar från origin"
  git fetch --prune origin
  if git show-ref --verify --quiet "refs/remotes/origin/$BRANCH"; then
    log "rebasing mot origin/$BRANCH"
    git rebase --autostash "origin/$BRANCH"
  else
    log "hittar inte origin/$BRANCH, skippar rebase"
  fi
  log "status före sync:"
  git status --short --branch || true
  log "kör sync_archive.py --limit-threads 5"
  tmp="$(mktemp)"
  if ! python3 sync_archive.py --limit-threads 5 2>&1 | tee "$tmp"; then
    rc=$?
    log "sync_archive.py misslyckades"
    rm -f "$tmp"
    exit "$rc"
  fi
  result="$(sed -n 's/^RESULT=//p' "$tmp" | tail -n1)"
  rm -f "$tmp"
  log "sync gav RESULT=${result:-<tomt>}"
  case "${result:-}" in
    none)
      log "inget ändrat, ingen commit"
      ;;
    sync_only)
      if ! git diff --quiet -- data archive.json archive_no_tag.json; then
        log "sync_only: committar lokalt"
        git add data archive.json archive_no_tag.json
        git commit -m "synkar trådar lokalt"
      else
        log "sync_only men inget diffar"
      fi
      ;;
    votes_changed)
      if ! git diff --quiet -- data archive.json archive_no_tag.json; then
        log "votes_changed: committar"
        git add data archive.json archive_no_tag.json
        git commit -m "uppdaterar röster"
      else
        log "votes_changed men inget diffar"
      fi
      log "pushar till origin/$BRANCH"
      git push origin "$BRANCH"
      ;;
    *)
      log "okänd status från sync_archive.py"
      exit 1
      ;;
  esac
  log "status efter varv:"
  git status --short --branch || true
  log "sover ${INTERVAL}s"
  sleep "$INTERVAL"
done
