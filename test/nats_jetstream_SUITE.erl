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

-module(nats_jetstream_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("enats/include/nats_stream.hrl").

-compile([export_all, nowarn_export_all]).

%% NOTE: a gnatsd instance must be running at 127.0.0.1:4222

-define(SUBJECT, ~"CT-SUBJECT").
-define(STREAM, ~"CT-STREAM").
-define(PUSH_CONSUMER, ~"CT-PUSH").
-define(PULL_CONSUMER, ~"CT-PUSH").
-define(PUSH_SUBJECT, ~"CT-PUSH-SUBJECT").
-define(PULL_HANDLER, nats_js_pull_consumer).

all() ->
    [jetstream].

init_per_suite(Config) ->
    _ = logger:set_primary_config(level, debug),
    application:ensure_started(enats),
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    {ok, Con} = nats:connect(<<"127.0.0.1">>, 4222),
    receive
        {Con, ready} -> ok
    after 1000 ->
            ct:pal("end_per_testcase: ready_msg_not_sent")
    end,

    try
        nats_consumer:delete(Con, ?STREAM, ?PUSH_CONSUMER),
        nats_consumer:delete(Con, ?STREAM, ?PULL_CONSUMER),
        nats_stream:delete(Con, ?STREAM),
        ok
    catch C:E ->
            ct:pal("end_per_testcase: ~0p:~0p", [C, E])
    after
        nats:disconnect(Con)
    end,
    Config.

jetstream(Config) ->
    Client = connect(),
    Con = connect(),

    try
        jetstream(Client, Con, Config)
    after
        nats:disconnect(Con),
        nats:disconnect(Client)
    end,
    ok.

jetstream(Client, Con, _Config) ->
    StreamCreateR = nats_stream:create(Con, ?STREAM, #{subjects => [?SUBJECT]}),
    ct:pal("StreamCreateR: ~p", [StreamCreateR]),
    ?assertMatch(#{type := ?JS_API_V1_STREAM_CREATE_RESPONSE,
                   did_create := true}, StreamCreateR),

    StreamListR = nats_stream:list(Con),
    ?assertMatch(#{type := ?JS_API_V1_STREAM_LIST_RESPONSE,
                   total := _, streams :=[_|_]}, StreamListR),

    StreamNamesR = nats_stream:names(Con),
    ?assertMatch(#{type := ?JS_API_V1_STREAM_NAMES_RESPONSE,
                   total := _, streams :=[_|_]}, StreamNamesR),

    %% store a message in the stream
    ok = nats:pub(Client, ?SUBJECT, ~"Hello."),

    %% subscribe to push consumer delivery subject
    {ok, PushSid} = nats:sub(Con, ?PUSH_SUBJECT),

    %%
    %% consumer with ACK none policy
    %%
    Push1Config = #{config => #{deliver_subject => ?PUSH_SUBJECT}},
    Push1Consumer = nats_consumer:create(Con, ?STREAM, ?PUSH_CONSUMER, Push1Config),
    ct:pal("Push1Consumer: ~p", [Push1Consumer]),
    ?assertMatch(#{type := ?JS_API_V1_CONSUMER_CREATE_RESPONSE,
                   name := _}, Push1Consumer),

    receive
        {Con, PushSid, {msg, ?SUBJECT, _, _Push1Msg1Opts}} ->
            ct:pal("Push1Msg1Opts: ~p", [_Push1Msg1Opts]),
            ok
    after 1000 ->
            throw(did_not_receive_a_msg)
    end,

    nats_consumer:delete(Con, Push1Consumer),

    %%
    %% consumer with ACK all policy
    %%
    Push2Config = #{config =>
                        #{deliver_subject => ?PUSH_SUBJECT,
                          ack_policy => explicit
                         }
                   },
    Push2Consumer = nats_consumer:create(Con, ?STREAM, ?PUSH_CONSUMER, Push2Config),
    ct:pal("Push2Consumer: ~p", [Push2Consumer]),
    ?assertMatch(#{type := ?JS_API_V1_CONSUMER_CREATE_RESPONSE,
                   name := _}, Push2Consumer),

    receive
        {Con, PushSid, {msg, ?SUBJECT, _, Push2Msg1Opts}} ->
            nats_consumer:ack_msg(Con, ack, false, Push2Msg1Opts),
            ok
    after 1000 ->
            throw(did_not_receive_a_msg)
    end,

    nats_consumer:delete(Con, Push2Consumer),

    %% unsubscribe to push consumer delivery subject
    nats:unsub(Con, PushSid),

    %%
    %% consumer with ACK none policy
    %%
    Pull1Config = #{},
    _Pull1Consumer = nats_consumer:create(Con, ?STREAM, ?PULL_CONSUMER, Pull1Config),
    {ok, Pull1Srv} = nats_pull_consumer:start_link(
                       ?PULL_HANDLER, Con,
                       ?STREAM, ?PULL_CONSUMER, #{owner => self()}, []),
    receive
        {?PULL_HANDLER, ?SUBJECT, _, _Pull1Msg1Opts} ->
            ok
    after 1000 ->
            throw(did_not_receive_a_msg)
    end,
    nats_pull_consumer:stop(Pull1Srv),

    ok.

%%%===================================================================
%%% Internal helpers
%%%===================================================================

connect() ->
    {ok, C} = nats:connect(<<"127.0.0.1">>, 4222, #{buffer_size => -1}),
    receive {C, ready} -> C
    after 1000 ->
            ct:fail(ready_msg_not_sent)
    end.
