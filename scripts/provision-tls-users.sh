#!/bin/sh
# Provisions fixture-only users on the secured test server:
#   limited/limited  - read-write role (can CRUD, cannot create indexes)
#   go-micro-tls-client - PKI-only user (CN of the client cert, mTLS test)
set -eu
HOST="${AEROSPIKE_TLS_HOST:-aerospike-tls}"
asadm="timeout 15 asadm --no-config-file --no-color --single-node --host $HOST --port 3000 --user admin --password admin -e"

# acl runs an asadm ACL command, succeeding when the command succeeds or fails
# with the expected idempotency message (e.g. "already exists"). Any other
# failure — unreachable server, auth error, unexpected response — aborts the
# script instead of being masked.
acl() {
	if out=$($asadm "$1" 2>&1); then
		return 0
	fi
	case $out in
		*"$2"*) return 0 ;;
	esac
	printf '%s\n' "$out" >&2
	return 1
}

acl 'enable; manage acl create user limited password limited roles read-write' 'already exists'
# PKI client-cert user needs sys-admin: the store's namespace check issues
# info commands, which require sys-admin on EE. The password is a required
# placeholder — Aerospike demands one at user creation, but this fixture user
# authenticates by certificate CN only and the password is never used.
acl 'enable; manage acl create user go-micro-tls-client password nopassword roles sys-admin' 'already exists'
acl 'enable; manage acl grant user go-micro-tls-client roles sys-admin' 'already has'
# The eval admin defaults to user-admin; grant it sys-admin (index/info) and
# read-write (data) so fixture setup and tests can operate (idempotent).
acl 'enable; manage acl grant user admin roles sys-admin' 'already has'
acl 'enable; manage acl grant user admin roles read-write' 'already has'
acl 'enable; manage acl grant user go-micro-tls-client roles read-write' 'already has'
# Pre-create the secondary index the store expects (read-write users cannot
# create it themselves; the denied-creation path is a separate test).
if ! out=$(timeout 15 asinfo --user admin --password admin -h "$HOST" \
	-v 'sindex-create:ns=test;set=micro;indexname=go_micro_table;indextype=default;indexdata=t,string' 2>&1); then
	case $out in
		*already*exists*) ;;
		*) printf '%s\n' "$out" >&2; exit 1 ;;
	esac
fi
echo "fixture users provisioned"
