nats
=====

Erlang NATS.io client library.

Build
-----

    $ rebar3 compile

Usage
-----

```
application:ensure_all_started([enats]),

{ok, C} = nats:connect({127,0,0,1}, 4222, #{verbose => false}),
receive {C, ready} -> ok end,

{ok, Sid} = nats:sub(C, ~"foo.*"),

nats:pub(C, ~"foo.bar", ~"My payload"),

receive
    {C, Sid, {msg, ~"foo.bar", Body, _}} ->
        io:format("Got NATS Msg: ~0tp~n", [Body]),
        ok
after 1000 ->
        throw(did_not_receive_a_msg)
end.
```

Authentication
--------------

```
# with user and password
{ok, C} = nats:connect({127,0,0,1}, 4222, #{user => ~"myname", pass => ~"password", auth_required => true}).

# with token
{ok, C} = nats:connect({127,0,0,1}, 4222, #{token => ~"secret", auth_required => true}).

# with an nkey seed
{ok, C} = nats:connect({127,0,0,1}, 4222, #{nkey_seed => ~"SUAM...", auth_required => true}).

# with decentralized user credentials (JWT)
{ok, C} = nats:connect({127,0,0,1}, 4222, #{nkey_seed => ~"SUAM...", jwt => ~"eyJ0eX...", auth_required => true}).

# connect to NGS with JWT
{ok, C} = nats:connect("connect.ngs.global", 4222, #{tls_required => true, tls_opts => [{verify, verify_peer}, {cacerts, public_key:cacerts_get()}], nkey_seed => ~"SUAM...", jwt => ~"eyJ0eX...", auth_required => true}).
```
