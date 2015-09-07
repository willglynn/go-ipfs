#!/bin/sh

test_description="Test resolve command"

. lib/test-lib.sh

test_init_ipfs

test_expect_success "resolve: prepare files" '
	mkdir -p a/b &&
	echo "a/b/c" >a/b/c &&
	a_hash=$(ipfs add -q -r a | tail -n1) &&
	b_hash=$(ipfs add -q -r a/b | tail -n1) &&
	c_hash=$(ipfs add -q -r a/b/c | tail -n1)
'

test_resolve_setup_name() {
	ref=$1

	test_expect_success "resolve: prepare name" "
		id_hash=$(ipfs id -f='<id>') &&
		ipfs name publish '$1' &&
		printf '$1' >expected_nameval &&
		ipfs name resolve >actual_nameval &&
		test_cmp expected_nameval actual_nameval
	"
}

test_resolve() {
	src=$1
	dst=$2

	test_expect_success "resolve succeeds: $1" "
		ipfs resolve '$1' >actual
	"

	test_expect_success "resolved correctly: $1 -> $2" "
		printf '$2' >expected &&
		test_cmp expected actual
	"
}


test_resolve_cmd() {

	test_resolve "/ipfs/$a_hash" "/ipfs/$a_hash"
	test_resolve "/ipfs/$a_hash/b" "/ipfs/$b_hash"
	test_resolve "/ipfs/$a_hash/b/c" "/ipfs/$c_hash"
	test_resolve "/ipfs/$b_hash/c" "/ipfs/$c_hash"

	test_resolve_setup_name "/ipfs/$a_hash"
	test_resolve "/ipns/$id_hash" "/ipfs/$a_hash"
	test_resolve "/ipns/$id_hash/b" "/ipfs/$b_hash"
	test_resolve "/ipns/$id_hash/b/c" "/ipfs/$c_hash"

	test_resolve_setup_name "/ipfs/$b_hash"
	test_resolve "/ipns/$id_hash" "/ipfs/$b_hash"
	test_resolve "/ipns/$id_hash/c" "/ipfs/$c_hash"

	test_resolve_setup_name "/ipfs/$c_hash"
	test_resolve "/ipns/$id_hash" "/ipfs/$c_hash"
}

# should work offline
test_resolve_cmd

# should work online
test_launch_ipfs_daemon
test_resolve_cmd
test_kill_ipfs_daemon

test_done
