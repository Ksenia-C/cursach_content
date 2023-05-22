#!/bin/bash

### partition real dags into types
# for i in {0..47}
# do
#    python3 form_part.py $i || exit 1
#    cargo run -- --action from_csv || exit 1
#    cargo run -- --action form --k-part $i || exit 1
# done


### generate tasks graphs
# cargo run -- --action pure --graph-type tree_incr || exit 1
# cargo run -- --action task --graph-type tree_incr --min-cp 2 --max-cp 4  || exit 1
# cargo run -- --action task --graph-type tree_incr --min-cp 5 --max-cp 7 || exit 1
# cargo run -- --action task --graph-type tree_incr --min-cp 8 --max-cp 10 || exit 1

# cargo run -- --action pure --graph-type tree_decr || exit 1
# cargo run -- --action task --graph-type tree_decr --min-cp 2 --max-cp 4 || exit 1
# cargo run -- --action task --graph-type tree_decr --min-cp 5 --max-cp 7 || exit 1
# cargo run -- --action task --graph-type tree_decr --min-cp 8 --max-cp 10 || exit 1
# cargo run -- --action task --graph-type tree_decr --min-cp 11 --max-cp 14 || exit 1
# cargo run -- --action task --graph-type tree_decr --min-cp 19 --max-cp 24 || exit 1

# cargo run -- --action pure --graph-type other || exit 1
# cargo run -- --action task --graph-type other --min-cp 2 --max-cp 4 || exit 1
# cargo run -- --action task --graph-type other --min-cp 5 --max-cp 7 || exit 1
# cargo run -- --action task --graph-type other --min-cp 8 --max-cp 10 || exit 1
# cargo run -- --action task --graph-type other --min-cp 11 --max-cp 14 || exit 1
# cargo run -- --action task --graph-type other --min-cp 15 --max-cp 18 || exit 1
# cargo run -- --action task --graph-type other --min-cp 19 --max-cp 24 || exit 1


### create instance dags
# for ccr in  0.5 1.0 5.0
# do
#     cargo run -- --action ins --graph-type tree_incr --ccr-set $ccr || exit 1
#     cargo run -- --action ins --graph-type tree_decr --ccr-set $ccr || exit 1
#     cargo run -- --action ins --graph-type other --ccr-set $ccr || exit 1
# done