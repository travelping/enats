overload
========

Overload a **single** NATS connection with a list operation on a K/V

This loads a K/V store with 1 million entries and then runs a concurent select
operation with a filter expression that matches only a single key.

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

* load the NATS K/V
  ```
  $ rebar3 shell
  shell V15.1 (press Ctrl+G to abort, type help(). for help)
  1> overload:load().
  ok
  ```

* run the overload tests, note how the maximum execution times increases

  ```
  $ rebar3 shell
  Eshell V15.1 (press Ctrl+G to abort, type help(). for help)
  1> overload:run(1, 1).
  min: 370.608 ms / 370.608000 ms/op, max: 370.608 ms / 370.608000 ms/op, avg: 370.608 ms / 370.608000 ms/op
  ok
  2> overload:run(1, 2).
  min: 357.858 ms / 357.858000 ms/op, max: 529.339 ms / 529.339000 ms/op, avg: 443.5985 ms / 443.598500 ms/op
  ok
  3> overload:run(1, 3).
  min: 347.954 ms / 347.954000 ms/op, max: 680.37 ms / 680.370000 ms/op, avg: 513.8816666666667 ms / 513.881667 ms/op
  ok
  4> overload:run(1, 4).
  min: 350.508 ms / 350.508000 ms/op, max: 913.4 ms / 913.400000 ms/op, avg: 620.5985 ms / 620.598500 ms/op
  ok
  5> overload:run(1, 5).
  min: 367.847 ms / 367.847000 ms/op, max: 1048.508 ms / 1048.508000 ms/op, avg: 706.1311999999999 ms / 706.131200 ms/op
  ```

* overload NATS

  ```
  $ rebar3 shell
  Eshell V15.1 (press Ctrl+G to abort, type help(). for help)
  1> overload:run(10, 1000).
  ```

  * watch NATS server spill out `[WRN] 127.0.0.1:38188 - cid:3506 - Readloop processing time: 9.235195513s` message.
  * check NATS server socket stats and watch receive queue grow:
    ```
	sudo ss -atnp | grep nats-server
	ESTAB     465821 0                             [::ffff:127.0.0.1]:4222                   [::ffff:127.0.0.1]:38188 users:(("nats-server",pid=16038,fd=13)) 
	```
