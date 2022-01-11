#/!bin/bash
sudo dtrace -o out.stacks -n 'profile-997 /execname == "redis-server"/ { @[ustack(100)] = count(); }'
stackcollapse.pl out.stacks | flamegraph.pl > pretty-graph.svg