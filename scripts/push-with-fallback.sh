#!/usr/bin/env bash
# push-with-fallback.sh
#
# Pushes to GitHub over HTTPS, working around an unreachable or intercepting
# address for the git host.
#
# Why this exists: the git host (github.com) is reached through a different
# route than the API host (api.github.com), and the two fail independently. A
# deployment whose route to the git host is down sees a connection timeout
# that looks like a network outage; an address that answers but is not the
# git endpoint returns "OK" and makes git fail with "not valid: is this a git
# repository?". Neither failure says which address is at fault, and both look
# like the remote refusing the push.
#
# The script therefore probes the real git endpoint - the smart-HTTP
# advertisement, not the web page - on the current answer and on samples from
# the published git ranges. It waits for one of them to answer, pins it for
# the duration of the push, and restores the original hosts file on every
# exit path.
#
# Usage:
#   ./scripts/push-with-fallback.sh [--check] [--dry-run] [--wait=SECONDS] [REMOTE] [BRANCH]
#
#   --check          Probe until an address answers, then report. No push.
#   --dry-run        Pin the working address and print the push command.
#   --wait=SECONDS   How long to keep looking for a working address
#                    (default 600). The route to the git host comes and goes;
#                    the script waits it out instead of failing at once.
#   REMOTE           Remote name or URL. Defaults to origin.
#   BRANCH           Branch to push. Defaults to the current branch.
#
# Exit codes: 0 = pushed (or the check succeeded), 1 = no address served the
# git endpoint within the deadline, 2 = the push itself failed.
#
# Notes:
#   - Pinning an address needs write access to /etc/hosts. Without it the
#     script still retries the push against the current resolution.
#   - The original /etc/hosts is restored on every exit path.
#   - The address that worked last is remembered and probed first.

set -euo pipefail

GIT_HOST="github.com"
GIT_REPOSITORY="CloudEdgeCore/Fenced"
GIT_PATH="/${GIT_REPOSITORY}.git/info/refs?service=git-upload-pack"
META_URL="https://api.github.com/meta"
HOSTS_FILE="/etc/hosts"
ADVERTISEMENT="001e# service=git-upload-pack"
PROBE_TIMEOUT=4
PASS_INTERVAL=15
DEADLINE=600
CACHE_FILE="${XDG_CACHE_HOME:-${HOME}/.cache}/fenced/push-host"

CHECK_ONLY=0
DRY_RUN=0
POSITIONAL=()

log() { printf '%s\n' "$*" >&2; }
for argument in "$@"; do
	case "$argument" in
		--check) CHECK_ONLY=1 ;;
		--dry-run) DRY_RUN=1 ;;
		--wait=*) DEADLINE="${argument#--wait=}" ;;
		-*) ;;
		*) POSITIONAL+=("$argument") ;;
	esac
done
REMOTE="${POSITIONAL[0]:-origin}"
BRANCH="${POSITIONAL[1]:-$(git rev-parse --abbrev-ref HEAD)}"

cleanup() {
	if [ -n "${HOSTS_BACKUP:-}" ] && [ -f "$HOSTS_BACKUP" ]; then
		cp "$HOSTS_BACKUP" "$HOSTS_FILE" 2>/dev/null || true
		rm -f "$HOSTS_BACKUP"
		log "hosts: restored the original ${HOSTS_FILE}"
	fi
}
trap cleanup EXIT

# answers_git_endpoint reports whether the address serves the git smart-HTTP
# endpoint. A middlebox that answers "OK" does not.
#
# The body is captured whole and matched afterwards: piping curl into head
# makes curl fail with a broken pipe once head has read enough, and with
# "pipefail" that turns a working address into a false negative.
answers_git_endpoint() {
	local address="$1" body
	body=$(curl -s --max-time "$PROBE_TIMEOUT" --resolve "${GIT_HOST}:443:${address}" \
		"https://${GIT_HOST}${GIT_PATH}" 2>/dev/null | tr -d '\0') || return 1
	case "$body" in
		"${ADVERTISEMENT}"*) return 0 ;;
		*) return 1 ;;
	esac
}

# candidate_addresses lists the addresses to probe, most likely first: the
# address that worked last, the address the resolver answers with, then
# samples from the published git ranges. The samples are the first host
# addresses of each range, because the network address itself never serves.
candidate_addresses() {
	local remembered current ranges network offset
	if [ -f "$CACHE_FILE" ]; then
		remembered=$(head -1 "$CACHE_FILE" 2>/dev/null || true)
		[ -n "$remembered" ] && printf '%s\n' "$remembered"
	fi
	current=$(getent hosts "$GIT_HOST" 2>/dev/null | awk '{print $1}' | head -1)
	[ -n "$current" ] && printf '%s\n' "$current"

	ranges=$(curl -s --max-time 20 "$META_URL" 2>/dev/null | tr ',' '\n' | tr -d '" ' | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/[0-9]+$') || true
	local count=0
	for network in $ranges; do
		count=$((count + 1))
		[ "$count" -gt 10 ] && break
		for offset in 1 2 3; do
			printf '%s\n' "${network%%/*}" | awk -F. -v o="$offset" '{printf "%s.%s.%s.%s\n", $1, $2, $3, $4 + o}'
		done
	done
}

# find_working_address probes the candidates until one serves the git
# endpoint, or the deadline passes.
find_working_address() {
	local started now remaining pass address
	started=$(date +%s)
	pass=0
	while true; do
		pass=$((pass + 1))
		while read -r address; do
			[ -n "$address" ] || continue
			if answers_git_endpoint "$address"; then
				printf '%s\n' "$address"
				return 0
			fi
		done < <(candidate_addresses)

		now=$(date +%s)
		remaining=$((DEADLINE - (now - started)))
		if [ "$remaining" -le 0 ]; then
			return 1
		fi
		log "pass ${pass}: no address served the git endpoint of ${GIT_HOST}; retrying in ${PASS_INTERVAL}s (${remaining}s left)"
		sleep "$PASS_INTERVAL"
	done
}

pin_address() {
	local address="$1" current
	current=$(getent hosts "$GIT_HOST" 2>/dev/null | awk '{print $1}' | head -1)
	if [ "$current" = "$address" ]; then
		log "the current resolution already works (${address}); nothing to pin"
		return 0
	fi
	if [ ! -w "$HOSTS_FILE" ]; then
		log "cannot pin ${GIT_HOST} (no write access to ${HOSTS_FILE}); using the current resolution"
		return 0
	fi
	HOSTS_BACKUP=$(mktemp)
	cp "$HOSTS_FILE" "$HOSTS_BACKUP"
	sed -i "/[[:space:]]${GIT_HOST}([[:space:]]|$)/d" "$HOSTS_FILE"
	printf '%s\t%s\n' "$address" "$GIT_HOST" >>"$HOSTS_FILE"
	log "pinned ${GIT_HOST} to ${address} for this run"
}

main() {
	log "probing the git endpoint of ${GIT_HOST} (deadline ${DEADLINE}s) ..."
	local working
	if ! working=$(find_working_address); then
		log "no address served the git endpoint of ${GIT_HOST} within ${DEADLINE}s"
		return 1
	fi
	log "  ${working} serves the git endpoint"
	mkdir -p "$(dirname "$CACHE_FILE")" 2>/dev/null || true
	printf '%s\n' "$working" >"$CACHE_FILE" 2>/dev/null || true
	pin_address "$working"

	if [ "$CHECK_ONLY" = "1" ]; then
		log "check succeeded: ${working}"
		return 0
	fi
	if [ "$DRY_RUN" = "1" ]; then
		log "dry run: git push ${REMOTE} ${BRANCH}"
		return 0
	fi

	log "pushing to ${REMOTE} ${BRANCH} ..."
	local attempt
	for attempt in 1 2 3; do
		if git push "$REMOTE" "$BRANCH"; then
			return 0
		fi
		log "  push attempt ${attempt} failed; retrying in ${PASS_INTERVAL}s"
		sleep "$PASS_INTERVAL"
	done
	log "the push failed after three attempts"
	return 2
}

main "$@"
