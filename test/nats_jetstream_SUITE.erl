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
-define(KV_BUCKET, ~"TEST_KV").
-define(KV_KEY_1, ~"FOO-1").
-define(KV_KEY_2, ~"FOO-2").
-define(KV_KEY_DEL, ~"FOO-DEL").

all() ->
    [jetstream, kv].

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
        nats_kv:delete_bucket(Con, ?KV_BUCKET),
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
    ?assertMatch({ok, #{did_create := true}}, StreamCreateR),

    StreamListR = nats_stream:list(Con),
    ?assertMatch({ok, #{total := _, streams :=[_|_]}}, StreamListR),

    StreamNamesR = nats_stream:names(Con),
    ?assertMatch({ok, #{total := _, streams :=[_|_]}}, StreamNamesR),

    %% store a message in the stream
    ok = nats:pub(Client, ?SUBJECT, ~"Hello."),

    %% subscribe to push consumer delivery subject
    {ok, PushSid} = nats:sub(Con, ?PUSH_SUBJECT),

    %%
    %% consumer with ACK none policy
    %%
    Push1Config = #{config => #{deliver_subject => ?PUSH_SUBJECT}},
    {ok, Push1Consumer} = nats_consumer:create(Con, ?STREAM, ?PUSH_CONSUMER, Push1Config),
    ct:pal("Push1Consumer: ~p", [Push1Consumer]),
    ?assertMatch(#{name := _}, Push1Consumer),

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
    {ok, Push2Consumer} = nats_consumer:create(Con, ?STREAM, ?PUSH_CONSUMER, Push2Config),
    ct:pal("Push2Consumer: ~p", [Push2Consumer]),
    ?assertMatch(#{name := _}, Push2Consumer),

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

kv(Config) ->
    Client = connect(),
    Con = connect(),

    try
        kv(Client, Con, Config)
    after
        nats:disconnect(Con),
        nats:disconnect(Client)
    end,
    ok.

kv(_Client, Con, _Config) ->
    BucketCreateR1 = nats_kv:create_bucket(Con, ?KV_BUCKET),
    ct:pal("R1: ~p", [BucketCreateR1]),
    ?assertMatch({ok, #{config := _}}, BucketCreateR1),
    {ok, #{config := BucketCfg}} = BucketCreateR1,

    BucketGetR1 = nats_kv:get_bucket(Con, ?KV_BUCKET),
    ct:pal("BucketGetR1: ~p", [BucketGetR1]),
    ?assertMatch({ok, #{config := _}}, BucketGetR1),

    BucketCreateR2 = nats_kv:create_bucket(Con, ?KV_BUCKET, #{ttl => 1_000_000_000}, #{}),
    ct:pal("R2: ~p", [BucketCreateR2]),
    ?assertMatch({error, #{code := _}}, BucketCreateR2),

    BucketUpdateR1 = nats_kv:update_bucket(Con, ?KV_BUCKET, #{ttl => 60 * 1_000_000_000}, #{}),
    ct:pal("R1: ~p", [BucketUpdateR1]),
    ?assertMatch({ok, #{config := _}}, BucketUpdateR1),
    {ok, #{config := NewBucketCfg}} = BucketUpdateR1,
    ?assertNotEqual(BucketCfg, NewBucketCfg),

    ?assertMatch({ok, #{stream := _, seq := _}},
                 nats_kv:put(Con, ?KV_BUCKET, ?KV_KEY_1, ~"BAR")),

    KvGetR1 = nats_kv:get(Con, ?KV_BUCKET, ?KV_KEY_1),
    ct:pal("KvGetR1: ~p", [KvGetR1]),
    ?assertMatch({ok, #{message := #{data := _}}}, KvGetR1),

    {ok, #{message := #{data := Data1}}} = KvGetR1,
    ct:pal("Data1: ~p", [Data1]),

    KvGetR2 = nats_kv:get(Con, ?KV_BUCKET, ?KV_KEY_1, BucketCfg),
    ct:pal("KvGetR2: ~p", [KvGetR2]),
    ?assertMatch({ok, #{message := #{data := _}}}, KvGetR2),

    {ok, #{message := #{data := Data2}}} = KvGetR2,
    ct:pal("Data2: ~p", [Data2]),

    KvDeleteR1 = nats_kv:delete(Con, ?KV_BUCKET, ?KV_KEY_2),
    ct:pal("KvDeleteR1: ~p", [KvDeleteR1]),
    ?assertMatch({ok, #{stream := _, seq := _}}, KvDeleteR1),

    KvCreateR1 = nats_kv:create(Con, ?KV_BUCKET, ?KV_KEY_2, ~"BOO"),
    ct:pal("KvCreateR1: ~p", [KvCreateR1]),
    ?assertMatch({ok, #{stream := _, seq := _}}, KvCreateR1),

    KvCreateR2 = nats_kv:create(Con, ?KV_BUCKET, ?KV_KEY_2, ~"BOO-1"),
    ct:pal("KvCreateR2: ~p", [KvCreateR2]),
    ?assertMatch({error, exists}, KvCreateR2),

    KvCreateRDel = nats_kv:create(Con, ?KV_BUCKET, ?KV_KEY_DEL, ~"DEL"),
    ?assertMatch({ok, #{stream := _, seq := _}}, KvCreateRDel),
    KvDeleteRDel = nats_kv:delete(Con, ?KV_BUCKET, ?KV_KEY_DEL),
    ?assertMatch({ok, #{stream := _, seq := _}}, KvDeleteRDel),

    KvListR1 = nats_kv:list_keys(Con, ?KV_BUCKET, #{}, #{}),
    ct:pal("KvListR1: ~p", [KvListR1]),
    ?assertMatch({ok, [_|_]}, KvListR1),
    {ok, Keys} = KvListR1,
    ?assertEqual(2, length(Keys)),

    KvPurgeR1 = nats_kv:purge(Con, ?KV_BUCKET, ?KV_KEY_2),
    ct:pal("KvPurgeR1: ~p", [KvPurgeR1]),
    ?assertMatch({ok, #{stream := _, seq := _}}, KvPurgeR1),

    KvCreateR3 = nats_kv:create(Con, ?KV_BUCKET, ?KV_KEY_2, ~"BOO"),
    ct:pal("KvCreateR3: ~p", [KvCreateR3]),
    ?assertMatch({ok, #{stream := _, seq := _}}, KvCreateR3),

    {ok, #{seq := SeqNoR3}} = KvCreateR3,

    KvUpdateR1 = nats_kv:update(Con, ?KV_BUCKET, ?KV_KEY_2, ~"BOO-2", SeqNoR3 + 100),
    ct:pal("KvUpdateR1: ~p", [KvUpdateR1]),
    ?assertMatch({error, #{code := _}}, KvUpdateR1),

    KvUpdateR2 = nats_kv:update(Con, ?KV_BUCKET, ?KV_KEY_2, ~"BOO-2", SeqNoR3),
    ct:pal("KvUpdateR2: ~p", [KvUpdateR2]),
    ?assertMatch({ok, #{stream := _, seq := _}}, KvUpdateR2),

    KvPurgeR2 = nats_kv:purge(Con, ?KV_BUCKET, ?KV_KEY_2),
    ct:pal("KvPurgeR2: ~p", [KvPurgeR2]),
    ?assertMatch({ok, #{stream := _, seq := _}}, KvPurgeR2),

    BucketDeleteR1 = nats_kv:delete_bucket(Con, ?KV_BUCKET),
    ct:pal("R1: ~p", [BucketDeleteR1]),
    ?assertMatch({ok, #{success := true}}, BucketDeleteR1),

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
