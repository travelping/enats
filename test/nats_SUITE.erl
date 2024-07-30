%% Copyright 2016 Yuce Tekol <yucetekol@gmail.com>
%% Copyright 2024, Travelping GmbH <info@travelping.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

%% this test suite is based on the test suite of nats_teacup (https://github.com/yuce/teacup_nats)

-module(nats_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-compile([export_all, nowarn_export_all]).

%% NOTE: a gnatsd instance must be running at 127.0.0.1:4222

all() ->
    [connect_ok,
     connect_fail_no_host,
     connect_fail_no_port,
     connect_verbose_ok,
     connect_verbose_fail,
     disconnect_ok,
     pub_ok,
     pub_verbose_ok,
     pub_with_buffer_size,
     sub_ok,
     sub_verbose_ok,
     unsub_verbose_ok,
     request_no_responders,
     micro_ok,
     micro_verbose_ok].

init_per_suite(Config) ->
    application:ensure_started(enats),
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

connect_ok(_) ->
    %% connect returns a new connection process, once the connection succeeds,
    %% a {Connection, ready} message is sent to the owner

    {ok, C} = nats:connect(<<"127.0.0.1">>, 4222),
    receive
        {C, ready} -> ok
    after 1000 ->
            throw(ready_msg_not_sent)
    end.

connect_fail_no_host(_) ->
    %% connect returns a new connection process,
    %% If the connection fails (host not found),
    %% a {Connection, {error, nxdomain}} message is sent to the owner

    {ok, C} = nats:connect(<<"doesnt-exist.google.com">>, 4222),
    receive
        {C, {error, nxdomain}} -> ok
    after 1000 ->
            throw(error_on_fail_not_sent)
    end.

connect_fail_no_port(_) ->
    %% connect returns a new connection process,
    %% If the connection fails (port is not open),
    %% a {Connection, {error, econnrefused}} message is sent to the owner

    NonExistingPort = 4444,
    {ok, C} = nats:connect(<<"127.0.0.1">>, NonExistingPort),
    receive
        {C, {error, econnrefused}} -> ok
    after 1000 ->
            throw(error_on_fail_not_sent)
    end.

connect_verbose_ok(_) ->
    %% connect returns a new connection process, once the connection succeeds,
    %% a {Connection, ready} message is sent to the owner

    {ok, C} = nats:connect(<<"127.0.0.1">>, 4222, #{verbose => true}),
    receive
        {C, ready} -> ok
    after 1000 ->
            throw(ready_msg_not_sent)
    end,
    true = nats:is_ready(C).

connect_verbose_fail(_) ->
    %% connect returns a new connection process,
    %% If the connection fails (port is not open),
    %% a {Connection, {error, econnrefused}} message is sent to the owner

    NonExistingPort = 4444,
    {ok, C} = nats:connect(<<"127.0.0.1">>, NonExistingPort),
    receive
        {C, {error, econnrefused}} -> ok
    after 1000 ->
            throw(error_on_fail_not_sent)
    end.

disconnect_ok(_) ->
    Host = <<"127.0.0.1">>,
    Port = 4222,
    {ok, C} = nats:connect(Host, Port, #{verbose => true}),
    ok = nats:disconnect(C),
    {error, not_found} = nats:is_ready(C).


pub_ok(_) ->
    {ok, C} = nats:connect(<<"127.0.0.1">>, 4222),
    receive {C, ready} -> ok end,
    nats:pub(C, <<"foo.bar">>, <<"My payload">> ),
    timer:sleep(100).

pub_verbose_ok(_) ->
    {ok, C} = nats:connect(<<"127.0.0.1">>, 4222,
                           #{verbose => true}),
    ok = nats:pub(C, <<"foo.bar">>, <<"My payload">>).

pub_with_buffer_size(_) ->
    {ok, C} = nats:connect(<<"127.0.0.1">>, 4222, #{buffer_size => 1}),
    nats:pub(C, <<"foo.bar">>, <<"My payload">>),
    timer:sleep(100).

sub_ok(_) ->
    Host = <<"127.0.0.1">>,
    Port = 4222,
    {ok, C} = nats:connect(Host, Port),
    receive {C, ready} -> ok end,
    {ok, Sid} = nats:sub(C, <<"foo.*">>),
    timer:sleep(100),
    send_tcp_msg(Host, Port, <<"PUB foo.bar 0\r\n\r\n">>),
    receive
        {C, Sid, {msg, <<"foo.bar">>, <<>>, _}} -> ok
    after 1000 ->
            throw(did_not_receive_a_msg)
    end.

sub_verbose_ok(_) ->
    Host = <<"127.0.0.1">>,
    Port = 4222,
    {ok, C} = nats:connect(Host, Port, #{verbose => true}),
    receive {C, ready} -> ok end,

    {ok, Sid} = nats:sub(C, <<"foo.*">>),
    send_tcp_msg(Host, Port, <<"PUB foo.bar 0\r\n\r\n">>),
    receive
        {C, Sid, {msg, <<"foo.bar">>, <<>>, _}} -> ok
    after 1000 ->
            throw(did_not_receive_a_msg)
    end.

unsub_verbose_ok(_) ->
    {ok, C} = nats:connect(<<"127.0.0.1">>, 4222,
                           #{verbose => true}),
    receive {C, ready} -> ok end,
    {ok, Sid} = nats:sub(C, <<"foo.*">>),
    nats:unsub(C, Sid),
    nats:pub(C, <<"foo.bar">>, <<>>),
    receive
        {C, _, {msg, _, _, _}} ->
            throw(didnt_expect_a_msg)
    after 1000 ->
            ok
    end.

request_no_responders(_) ->
    Host = <<"127.0.0.1">>,
    Port = 4222,
    {ok, C} = nats:connect(Host, Port),
    receive {C, ready} -> ok end,

    Response = nats:request(C, ~"FOOBAR.echo", ~"Hello World", #{}),
    ?assertEqual({error,no_responders}, Response),
    ok.

request_verbose_no_responders(_) ->
    Host = <<"127.0.0.1">>,
    Port = 4222,
    {ok, C} = nats:connect(Host, Port),
    receive {C, ready} -> ok end,

    Response = nats:request(C, ~"FOOBAR.echo", ~"Hello World", #{}),
    ?assertEqual({error,no_responders}, Response),
    ok.
micro_ok(_) ->
    Host = <<"127.0.0.1">>,
    Port = 4222,
    {ok, C} = nats:connect(Host, Port),
    receive {C, ready} -> ok end,

    Svc = nats:service(#{name => ~"FOOBAR", version => ~"2.0.0"},
                       [nats:endpoint(#{name => ~"echo"}, echo)], ?MODULE, []),
    ok = nats:serve(C, Svc),

    Response = nats:request(C, ~"FOOBAR.echo", ~"Hello World", #{}),
    ?assertEqual({ok,{<<"Hello World">>, #{}}}, Response),

    receive
        Msg ->
            ct:pal("Unexpected Msg: ~p", [Msg]),
            throw(didnt_expect_a_msg)
    after 1000 ->
            ok
    end.

micro_verbose_ok(_) ->
    Host = <<"127.0.0.1">>,
    Port = 4222,
    {ok, C} = nats:connect(Host, Port, #{verbose => true}),
    receive {C, ready} -> ok end,

    Svc = nats:service(#{name => ~"FOOBAR", version => ~"2.0.0"},
                       [nats:endpoint(#{name => ~"echo"}, echo)], ?MODULE, []),
    ok = nats:serve(C, Svc),

    Response = nats:request(C, ~"FOOBAR.echo", ~"Hello World", #{}),
    ?assertEqual({ok,{<<"Hello World">>, #{}}}, Response),

    receive
        Msg ->
            ct:pal("Unexpected Msg: ~p", [Msg]),
            throw(didnt_expect_a_msg)
    after 1000 ->
            ok
    end.

send_tcp_msg(BinHost, Port, BinMsg) ->
    Host = binary_to_list(BinHost),
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 0}]),
    ok = gen_tcp:send(Socket, BinMsg),
    ok = gen_tcp:close(Socket).

%%%===================================================================
%%% Echo Server Callback
%%%===================================================================

echo(_SvcName, _Op, Payload, _, CbState) ->
    {reply, Payload, CbState}.
