all: compile dialyzer test

###===================================================================
### build
###===================================================================
.PHONY: co compile es escriptize run
export PROFILE=release
co:compile
compile:
	./rebar3 compile
redis_api:
    ## debug
	cargo build -p redis_api
	## release
	cargo build -p redis_api --release
es:escriptize
escriptize: clean
	./rebar3 escriptize

### clean
.PHONY: clean distclean
clean:
	./rebar3 clean

distclean:
	./rebar3 clean -a

###===================================================================
### test
###===================================================================
.PHONY: test eunit ct testclean

test: epmd dialyzer 
	sh crates/build_crates.sh test
	./rebar3 do eunit --dir test/eunit -v
	./rebar3 do ct --dir test/ct --config test/ct/ct.config --sys_config config/test.config -v

eunit: epmd
	./rebar3 do eunit --dir test/eunit -v
redis: epmd
	./rebar3 do ct --dir test/ct  --suite big_data_redis_SUITE -v --config test/ct/ct.config --sys_config config/test.config
ct: epmd
	sh crates/build_crates.sh clippy
	sh crates/build_crates.sh test
	./rebar3 do ct --dir test/ct  --suite big_data_local_SUITE -v --config test/ct/ct.config --sys_config config/test.config
	./rebar3 do ct --dir test/ct  --suite bd_log_wal_SUITE -v --config test/ct/ct.config --sys_config config/test.config

testclean:
	@rm -fr _build/test

shell: epmd config
	./rebar3 as test shell

config: epmd
	# ./tool.sh replace_config

dialyzer: epmd
	sh crates/build_crates.sh clippy
	./rebar3 do dialyzer

tar: epmd
	rm -rf _build/prod
	./rebar3 as prod release
	./rebar3 as prod tar
###===================================================================
### other
###===================================================================
.PHONY: epmd

epmd:
	@pgrep epmd 2> /dev/null > /dev/null || epmd -daemon || true
