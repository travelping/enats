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

suite() ->
    [{timetrap, {minutes,1}}].

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
     sub_notify_ok,
     unsub_verbose_ok,
     request_no_responders,
     micro_ok,
     micro_verbose_ok,
     multi_server_reconnect].

init_per_suite(Config) ->
    Level = ct:get_config(log_level, info),
    _ = logger:set_primary_config(level, Level),
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

    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port),
    receive
        {C, ready} -> ok
    after 1000 ->
            throw(ready_msg_not_sent)
    end,

    ?assertMatch({ok, #{server_name := _}}, nats:server_info(C)),
    ?assertMatch({ok, #{family := _}}, nats:peername(C)),
    ?assertMatch({ok, #{family := _}}, nats:sockname(C)),

    ok.

connect_fail_no_host(_) ->
    %% connect returns a new connection process,
    %% If if there are no valid servers left,
    %% a {Connection, {error, no_more_candidates}} message is sent to the owner

    {ok, _Host, Port} = nats_addr(),
    {ok, C} = nats:connect(<<"doesnt-exist.google.com">>, Port),
    receive
        {C, {error, no_more_candidates}} -> ok
    after 1000 ->
            throw(error_on_fail_not_sent)
    end.

connect_fail_no_port(_) ->
    %% connect returns a new connection process,
    %% If the connection fails (port is not open),
    %% a {Connection, {error, econnrefused}} message is sent to the owner

    {ok, Host, _Port} = nats_addr(),
    NonExistingPort = 4444,
    {ok, C} = nats:connect(Host, NonExistingPort),
    receive
        {C, {error, econnrefused}} -> ok
    after 1000 ->
            throw(error_on_fail_not_sent)
    end.

connect_verbose_ok(_) ->
    %% connect returns a new connection process, once the connection succeeds,
    %% a {Connection, ready} message is sent to the owner

    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port, #{verbose => true}),
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

    {ok, Host, _Port} = nats_addr(),
    NonExistingPort = 4444,
    {ok, C} = nats:connect(Host, NonExistingPort),
    receive
        {C, {error, econnrefused}} -> ok
    after 1000 ->
            throw(error_on_fail_not_sent)
    end.

disconnect_ok(_) ->
    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port, #{verbose => true}),
    ok = nats:disconnect(C),
    {error, not_found} = nats:is_ready(C).


pub_ok(_) ->
    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port),
    receive {C, ready} -> ok end,
    %% binaries
    nats:pub(C, <<"foo.bar">>, <<"My payload">> ),
    %% iolist
    nats:pub(C, [<<"foo">>, <<".">>, <<"bar">>], [<<"My">>, [<<" ">>], <<"payload">>]),
    %% improper iolist
    nats:pub(C, [<<"foo">>, <<".">> | <<"bar">>], [<<"My">>, [<<" ">>], <<"payload">>]),
    timer:sleep(100).

pub_verbose_ok(_) ->
    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port, #{verbose => true}),
    ok = nats:pub(C, <<"foo.bar">>, <<"My payload">>).

pub_with_buffer_size(_) ->
    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port, #{buffer_size => 1}),
    nats:pub(C, <<"foo.bar">>, <<"My payload">>),
    timer:sleep(100).

sub_ok(_) ->
    {ok, Host, Port} = nats_addr(),
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
    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port, #{verbose => true}),
    receive {C, ready} -> ok end,

    {ok, Sid} = nats:sub(C, <<"foo.*">>),
    send_tcp_msg(Host, Port, <<"PUB foo.bar 0\r\n\r\n">>),
    receive
        {C, Sid, {msg, <<"foo.bar">>, <<>>, _}} -> ok
    after 1000 ->
            throw(did_not_receive_a_msg)
    end.

sub_notify_ok(_) ->
    Self = self(),
    Ref = make_ref(),
    Notify =
        fun(Sid, Subject, Payload, MsgOpts) ->
                Self ! {Ref, Sid, Subject, Payload, MsgOpts}
        end,

    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port),
    receive {C, ready} -> ok end,
    {ok, Sid} = nats:sub(C, <<"foo.*">>, #{notify => Notify}),
    timer:sleep(100),
    send_tcp_msg(Host, Port, <<"PUB foo.bar 0\r\n\r\n">>),
    receive
        {Ref, Sid, <<"foo.bar">>, <<>>, _} -> ok
    after 1000 ->
            throw(did_not_receive_a_msg)
    end.

unsub_verbose_ok(_) ->
    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port, #{verbose => true}),
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
    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port),
    receive {C, ready} -> ok end,

    Response = nats:request(C, ~"FOOBAR.echo", ~"Hello World", #{}),
    ?assertEqual({error,no_responders}, Response),
    ok.

request_verbose_no_responders(_) ->
    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port),
    receive {C, ready} -> ok end,

    Response = nats:request(C, ~"FOOBAR.echo", ~"Hello World", #{}),
    ?assertEqual({error,no_responders}, Response),
    ok.
micro_ok(_) ->
    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port),
    receive {C, ready} -> ok end,

    Svc = #{name => ~"FOOBAR",
            version => ~"2.0.0",
            module => ?MODULE,
            state => [],
            endpoints =>
                [#{name => ~"echo", function => echo}]},
    ok = nats_service:serve(C, Svc),

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
    {ok, Host, Port} = nats_addr(),
    {ok, C} = nats:connect(Host, Port, #{verbose => true}),
    receive {C, ready} -> ok end,

    Svc = #{name => ~"FOOBAR",
            version => ~"2.0.0",
            module => ?MODULE,
            state => [],
            endpoints =>
                [#{name => ~"echo", function => echo}]},
    ok = nats_service:serve(C, Svc),

    Response = nats:request(C, ~"FOOBAR.echo", ~"Hello World", #{}),
    ?assertEqual({ok,{<<"Hello World">>, #{}}}, Response),

    receive
        Msg ->
            ct:pal("Unexpected Msg: ~p", [Msg]),
            throw(didnt_expect_a_msg)
    after 1000 ->
            ok
    end.

multi_server_reconnect_server(Owner) ->
    nats_msg:init(),

    {ok, S1} = nats_fake_server:start_link(),
    {ok, S2} = nats_fake_server:start_link(),

    Server = #{scheme => ~"nats", host => ~"localhost", family => inet},
    Server1 = Server#{port => nats_fake_server:port(S1)},
    Server2 = Server#{port => nats_fake_server:port(S2)},
    Owner ! {ready, self(), [Server1, Server2]},
    multi_server_reconnect_server_loop(Owner, undefined).

multi_server_reconnect_server_loop(Owner, Connected) ->
    receive
        {closed, Pid, Socket} when Connected =:= {Pid, Socket} ->
            Owner ! {closed, self(), Pid, Socket},
            multi_server_reconnect_server_loop(Owner, undefined);
        {accepted, Pid, Socket} when Connected =:= undefined ->
            Info =
                #{server_id => ~"Server-Id",
                  server_name => ~"Server-Name",
                  version => ~"2.11.0",
                  proto => 1,
                  go => ~"go1.24.1",
                  host => ~"0.0.0.0",
                  port => 4222,
                  headers => true,
                  max_payload => 1048576,
                  jetstream => true
                 },
            Msg = iolist_to_binary([~"INFO ", json:encode(Info), ~"\r\n"]),
            nats_fake_server:send(Pid, Socket, Msg),
            multi_server_reconnect_server_loop(Owner, {Pid, Socket});
        {recv, Pid, Socket, Data} when Connected =:= {Pid, Socket} ->
            try nats_msg:decode_all(Data) of
                {Msgs, <<>>} ->
                    lists:foreach(
                      fun({connect, _ClientInfo}) ->
                              Owner ! {connected, self(), Pid, Socket};
                         ({sub, Sub}) ->
                              ct:pal("NATS-SUB: ~0p", [Sub]),
                              Owner ! {sub, self(), Pid, Socket, Sub};
                         ({unsub, Unsub}) ->
                              ct:pal("NATS-UNSUB: ~0p", [Unsub]),
                              Owner ! {unsub, self(), Pid, Socket, Unsub};
                         (Msg) ->
                              ct:pal("NATS-Server Msg: ~0p", [Msg])
                      end, Msgs)
            catch
                C:E:St ->
                    ct:pal("NATS decode error: ~p:~p~nStack: ~p~nMsg: ~0p", [C, E, St, Data])
            end,
            multi_server_reconnect_server_loop(Owner, Connected);
        Msg ->
            ct:pal("got unexpected msg: ~0p", [Msg]),
            error(exit)
    end.

-define(assertNoMessage(X_Wait),
        begin
            ((fun() ->
                      receive X_M ->
                              ct:pal("got unexpected message: ~0p", [X_M]),
                              throw(unexpected_message)
                      after X_Wait -> ok
                      end
              end)())
        end).

multi_server_reconnect_collect_subs(0, _, Acc) ->
    lists:reverse(Acc);
multi_server_reconnect_collect_subs(Cnt, SPid, Acc) ->
    receive
        {Tag, SPid, Pid, Socket, Msg}
          when Tag =:= sub; Tag =:= unsub ->
            multi_server_reconnect_collect_subs(
              Cnt - 1, SPid, [{Tag, Msg, {Pid, Socket}}|Acc])
    after 10_000 -> throw(sub_msg_not_received)
    end.

multi_server_reconnect(_) ->
    %% the test sequence can be slightly hard to follow as it gets messages from
    %% nats client and from the test server. However, interleaving them in a single
    %% sequential flow is much simpler for the test than having to synchronize
    %% multiple processes.

    SPid = proc_lib:spawn_link(?MODULE, multi_server_reconnect_server, [self()]),

    Servers =
        receive {ready, SPid, S} -> S
        after 1000 -> throw(servers_not_ready)
        end,

    {ok, C} = nats:connect(#{servers => Servers, verbose => false}),
    receive {C, ready} -> ok
    after 1000 -> throw(ready_msg_not_sent)
    end,

    %% connect message from our test server loop
    {connected, _, Srv1Pid, Srv1Socket} =
        receive {connected, SPid, _, _} = Msg1 -> Msg1
        after 1000 -> throw(connected_msg_not_sent)
        end,

    %% there should be no pending messages
    ?assertNoMessage(1000),

    %% send a sequence, of SUB foo.*, UNSUB Sid MaxMsgs, SUB zzz.*
    {ok, NatsSid1} = nats:sub(C, ~"foo.*"),
    ok = nats:unsub(C, NatsSid1, #{max_messages => 1000}),
    {ok, _NatsSid2} = nats:sub(C, ~"zzz.*"),

    %% activate the inbox subscription, cheat and call the server directly
    %% we could instead do a nats:request(), but that would make it more complex
    {ok, _Inbox} = gen_statem:call(C, get_inbox_topic),

    %% collect the sub, unsub, sub from the fake server
    Subs1 = multi_server_reconnect_collect_subs(4, SPid, []),
    ?assertMatch(
       [{sub, {~"foo.*", _, _}, {Srv1Pid, Srv1Socket}},
        {unsub, {_, 1000}, {Srv1Pid, Srv1Socket}},
        {sub, {<<"zzz.*">>, _ , _}, {Srv1Pid, Srv1Socket}},
        {sub, {<<"_INBOX.", _/binary>>, _, _}, {Srv1Pid, Srv1Socket}}
       ],
       Subs1),
    {_, {_, _, BinSid}, _} = hd(Subs1),

    %% set a msg ...
    SendMsg1 = nats_msg:encode({msg, {~"foo.bar", BinSid, undefined, ~"Hello"}}),
    ct:pal("SendMsg1: ~0p", [SendMsg1]),
    nats_fake_server:send(Srv1Pid, Srv1Socket, SendMsg1),

    %% ... and check that the subscription works
    receive {C, NatsSid1, {msg, _, _, _}} -> ok
    after 1000 -> throw(sub_msg_not_received)
    end,

    %% there should be no more pending messages
    ?assertNoMessage(1000),

    %% kill the socket on the test server
    nats_fake_server:close(Srv1Pid, Srv1Socket),

    %% close message from our test server loop
    {closed, _, Srv1Pid, Srv1Socket} =
        receive {closed, SPid, _, _} = Msg2 -> Msg2
        after 1000 -> throw(close_msg_not_sent)
        end,

    %% closed
    receive {C, closed} -> ok
    after 1000 -> throw(closed_msg_not_sent)
    end,

    %% wait for the reconnect
    receive {C, ready} -> ok
    after 1000 -> throw(ready_msg_not_sent)
    end,

    %% reconnect message from our test server loop
    {connected, _, Srv2Pid, Srv2Socket} =
        receive {connected, SPid, _, _} = Msg3 -> Msg3
        after 1000 -> throw(connected_msg_not_sent)
        end,

    %% it should have connected to the other test server socket
    ?assertNotEqual(Srv1Pid, Srv2Pid),

    %% collect the sub, unsub, sub restoration messages from the fake server
    Subs2 = multi_server_reconnect_collect_subs(4, SPid, []),
    %% ct:pal("Srv2Pid: ~p~nSrv2Socket: ~p", [Srv2Pid, Srv2Socket]),
    ?assertMatch(
       [{sub, {~"foo.*", _, _}, {Srv2Pid, Srv2Socket}},
        {unsub, {_, 999}, {Srv2Pid, Srv2Socket}},
        {sub, {<<"zzz.*">>, _ , _}, {Srv2Pid, Srv2Socket}},
        {sub, {<<"_INBOX.", _/binary>>, _, _}, {Srv2Pid, Srv2Socket}}
       ],
       Subs2),

    %% there should be no more pending messages
    ?assertNoMessage(1000),

    %% activate a microserver with its subscriptions
    Svc = #{name => ~"FOOBAR",
            version => ~"2.0.0",
            module => ?MODULE,
            state => [],
            endpoints =>
                [#{name => ~"echo", function => echo}]},
    ok = nats_service:serve(C, Svc),

    %% should result in 10 additional subscriptions
    multi_server_reconnect_collect_subs(10, SPid, []),

    %% there should be no more pending messages
    ?assertNoMessage(1000),

    %% subscription should still work
    nats_fake_server:send(Srv2Pid, Srv2Socket, SendMsg1),
    receive {C, NatsSid1, {msg, _, _, _}} -> ok
    after 1000 -> throw(sub_msg_not_received)
    end,

    %% there should be no more pending messages
    ?assertNoMessage(1000),

    %% kill the socket on the test server
    nats_fake_server:close(Srv2Pid, Srv2Socket),

    %% close message from our test server loop
    {closed, _, Srv2Pid, Srv2Socket} =
        receive {closed, SPid, _, _} = Msg4 -> Msg4
        after 1000 -> throw(close_msg_not_sent)
        end,

    %% closed
    receive {C, closed} -> ok
    after 1000 -> throw(closed_msg_not_sent)
    end,

    %% wait for the reconnect
    receive {C, ready} -> ok
    after 1000 -> throw(ready_msg_not_sent)
    end,

    %% reconnect message from our test server loop
    {connected, _, Srv3Pid, Srv3Socket} =
        receive {connected, SPid, _, _} = Msg5 -> Msg5
        after 1000 -> throw(connected_msg_not_sent)
        end,

    %% it should now be back on the first test server socket
    ?assertEqual(Srv1Pid, Srv3Pid),

    %% collect the sub, unsub, sub restoration messages from the fake server
    Subs3 = multi_server_reconnect_collect_subs(14, SPid, []),
    %% ct:pal("Srv3Pid: ~p~nSrv3Socket: ~p", [Srv3Pid, Srv3Socket]),
    ?assertMatch(
       [{sub, {~"foo.*", _, _}, {Srv3Pid, Srv3Socket}},
        {unsub, {_, 998}, {Srv3Pid, Srv3Socket}},
        {sub, {<<"zzz.*">>, _ , _}, {Srv3Pid, Srv3Socket}},
        {sub, {<<"_INBOX.", _/binary>>, _, _}, {Srv3Pid, Srv3Socket}}
       | _
       ],
       Subs3),

    %% there should be no more pending messages
    ?assertNoMessage(1000),

    %% disconnect should be clean
    ok = nats:disconnect(C),
    {error, not_found} = nats:is_ready(C),

    %% closed
    receive {C, closed} -> ok
    after 1000 -> throw(closed_msg_not_sent)
    end,

    %% close message from our test server loop
    {closed, _, Srv3Pid, Srv3Socket} =
        receive {closed, SPid, _, _} = Msg6 -> Msg6
        after 1000 -> throw(close_msg_not_sent)
        end,

    %% there should be no more pending messages
    ?assertNoMessage(1000),

    ok.

send_tcp_msg(BinHost, Port, BinMsg) ->
    Host = binary_to_list(BinHost),
    {ok, Socket} = gen_tcp:connect(Host, Port, [binary, {packet, 0}]),
    ok = gen_tcp:send(Socket, BinMsg),
    ok = gen_tcp:close(Socket).

%%%===================================================================
%%% Echo Server Callback
%%%===================================================================

echo(_ReplyKey, _SvcName, _Op, Payload, _, CbState) ->
    {reply, Payload, CbState}.

nats_addr() ->
    Host = ct:get_config(nats_host, ~"localhost"),
    {ok, Host, 4222}.
