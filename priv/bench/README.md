bench
=====

A simple NATS K/V benachmark


Requirements
------------

- Erlang OTP 27+
- NATS 2.10.21+

Run
---

* start nats, e.g:
```
$ podman run --network=host nats -js -m 8222
```

* run the benchmark
```
$ rebar3 shell
shell V15.1 (press Ctrl+G to abort, type help(). for help)
1> bench:bench().
first:  0foobar.0foobar.0foobar.0foobar.0foobar.0foobar              2928.573000 ms, 1000 ops, 2.928573 ms/op
second: 0foobar.0foobar.0foobar.0foobar.0foobar.0foobar              3011.704000 ms, 1000 ops, 3.011704 ms/op

first:  *.0foobar.0foobar.0foobar.0foobar.0foobar                    114.481000 ms, 10 ops, 11.448100 ms/op
second: *.0foobar.0foobar.0foobar.0foobar.0foobar                    91.026000 ms, 10 ops, 9.102600 ms/op

first:  0foobar.*.0foobar.0foobar.0foobar.0foobar                    83.240000 ms, 10 ops, 8.324000 ms/op
second: 0foobar.*.0foobar.0foobar.0foobar.0foobar                    88.415000 ms, 10 ops, 8.841500 ms/op

first:  0foobar.0foobar.*.0foobar.0foobar.0foobar                    127.501000 ms, 10 ops, 12.750100 ms/op
second: 0foobar.0foobar.*.0foobar.0foobar.0foobar                    80.261000 ms, 10 ops, 8.026100 ms/op

first:  0foobar.0foobar.0foobar.*.0foobar.0foobar                    978.433000 ms, 10 ops, 97.843300 ms/op
second: 0foobar.0foobar.0foobar.*.0foobar.0foobar                    898.490000 ms, 10 ops, 89.849000 ms/op

first:  0foobar.0foobar.0foobar.0foobar.*.0foobar                    328.352000 ms, 10 ops, 32.835200 ms/op
second: 0foobar.0foobar.0foobar.0foobar.*.0foobar                    353.534000 ms, 10 ops, 35.353400 ms/op

first:  0foobar.0foobar.0foobar.0foobar.0foobar.*                    203.782000 ms, 10 ops, 20.378200 ms/op
second: 0foobar.0foobar.0foobar.0foobar.0foobar.*                    250.079000 ms, 10 ops, 25.007900 ms/op

ok
```
