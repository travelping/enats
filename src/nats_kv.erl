%% Copyright (c) 2024, Travelping GmbH <info@travelping.com>.
%% All rights reserved.
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are permitted provided that the following conditions are
%% met:
%%
%% * Redistributions of source code must retain the above copyright
%%   notice, this list of conditions and the following disclaimer.
%%
%% * Redistributions in binary form must reproduce the above copyright
%%   notice, this list of conditions and the following disclaimer in the
%%   documentation and/or other materials provided with the distribution.
%%
%% * The names of its contributors may not be used to endorse or promote
%%   products derived from this software without specific prior written
%%   permission.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
%% "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
%% LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
%% A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
%% OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
%% SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
%% LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
%% DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
%% THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
%% (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
%% OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

-module(nats_kv).

%% K/V bucket API
-export([
         get_bucket/2, get_bucket/3,
         create_bucket/2, create_bucket/3, create_bucket/4,
         update_bucket/2, update_bucket/3, update_bucket/4,
         delete_bucket/2, delete_bucket/3
        ]).

%% K/V store API
-export([
         watch/4,
         list_keys/3, list_keys/4,
         get/3, get/4, get/5, get_msg/4,
         put/4,
         create/4, create/5,
         update/5,
         delete/3, delete/4,
         purge/3, purge/4
        ]).
-ignore_xref([types/1]).

-include_lib("kernel/include/logger.hrl").
-include_lib("enats/include/nats_stream.hrl").
-include("nats_kv.hrl").

-export_type([config/0]).

-type config() ::
        #{
          %% Bucket is the name of the KeyValue store. Bucket name has to be
          %% unique and can only contain alphanumeric characters, dashes, and
          %% underscores.
          bucket         := binary(),

          %% Description is an optional description for the KeyValue store.
          description    => binary(),

          %% MaxValueSize is the maximum size of a value in bytes. If not
          %% specified, the default is -1 (unlimited).
          max_value_size => non_neg_integer(),

          %% History is the number of historical values to keep per key. If not
          %% specified, the default is 1. Max is 64.
          history        => 1 .. 64,

          %% TTL is the expiry time for keys in nanoseconds. By default, keys do not expire.
          ttl            => non_neg_integer(),

          %% MaxBytes is the maximum size in bytes of the KeyValue store. If not
          %% specified, the default is -1 (unlimited).
          max_bytes      => non_neg_integer(),

          %% Storage is the type of storage to use for the KeyValue store. If not
          %% specified, the default is FileStorage.
          storage        => 'memory' | 'file',

          %% Replicas is the number of replicas to keep for the KeyValue store in
          %% clustered jetstream. Defaults to 1, maximum is 5.
          num_replicas   => 1 .. 5,

          %% Placement is used to declare where the stream should be placed via
          %% tags and/or an explicit cluster name.
          placement      => nats_stream:placement(),

          %% RePublish allows immediate republishing a message to the configured
          %% subject after it's stored.
          republish      => nats_stream:republish(),

          %% Mirror defines the consiguration for mirroring another KeyValue
          %% store.
          mirror         => nats_stream:stream_source(),

          %% Sources defines the configuration for sources of a KeyValue store.
          sources        => [nats_stream:stream_source()],

          %% Compression sets the underlying stream compression.
          %% NOTE: Compression is supported for nats-server 2.10.0+
          compression    => boolean()
         }.

%%%===================================================================
%%% API
%%%===================================================================

get_bucket(Conn, Bucket)
  when is_binary(Bucket) ->
    get_bucket(Conn, Bucket, #{}).

get_bucket(Conn, Bucket, Opts)
  when is_binary(Bucket) ->
    nats_stream:get(Conn, ?BUCKET_NAME(Bucket), Opts).

-doc """
CreateKeyValue will create a KeyValue store with the given
configuration.

If a KeyValue store with the same name already exists and the
configuration is different, ErrBucketExists will be returned.
""".
create_bucket(Conn, Bucket)
  when is_binary(Bucket) ->
    create_bucket(Conn, #{bucket => Bucket}, #{}).

-spec create_bucket(Conn :: pid(), Config :: config(), Opts :: map()) -> term().
create_bucket(Conn, #{bucket := Bucket} = Config, Opts)
  when is_binary(Bucket) ->
    StreamCfg = prepare_key_value_config(Config),
    nats_stream:create(Conn, StreamCfg, Opts).

create_bucket(Conn, Bucket, Config, Opts)
  when is_binary(Bucket), is_map(Config), is_map(Opts) ->
    create_bucket(Conn, Config#{bucket => Bucket}, Opts).

-doc """
UpdateKeyValue will update an existing KeyValue store with the given
configuration.

If a KeyValue store with the given name does not exist, ErrBucketNotFound
will be returned.
""".
update_bucket(Conn, Bucket)
  when is_binary(Bucket) ->
    update_bucket(Conn, #{bucket => Bucket}, #{}).

-spec update_bucket(Conn :: pid(), Config :: config(), Opts :: map()) -> term().
update_bucket(Conn, #{bucket := Bucket} = Config, Opts)
  when is_binary(Bucket) ->
    StreamCfg = prepare_key_value_config(Config),
    nats_stream:update(Conn, StreamCfg, Opts).

update_bucket(Conn, Bucket, Config, Opts)
  when is_binary(Bucket), is_map(Config), is_map(Opts) ->
    update_bucket(Conn, Config#{bucket => Bucket}, Opts).

-doc """
DeleteKeyValue will delete this KeyValue store.

If the KeyValue store with given name does not exist,
ErrBucketNotFound will be returned.
""".
delete_bucket(Conn, Bucket)
  when is_binary(Bucket) ->
    delete_bucket(Conn, Bucket, #{}).


delete_bucket(Conn, Bucket, Opts)
  when is_binary(Bucket), is_map(Opts) ->
    nats_stream:delete(Conn, ?BUCKET_NAME(Bucket), Opts).

-doc """
Watch for any updates to keys that match the keys argument which
could include wildcards. By default, the watcher will send the latest
value for each key and all future updates. Watch will send a nil
entry when it has received all initial values. There are a few ways
to configure the watcher:

- IncludeHistory will have the key watcher send all historical values
for each key (up to KeyValueMaxHistory).
- IgnoreDeletes will have the key watcher not pass any keys with
delete markers.
- UpdatesOnly will have the key watcher only pass updates on values
(without latest values when started).
- MetaOnly will have the key watcher retrieve only the entry meta
data, not the entry value.
- ResumeFromRevision instructs the key watcher to resume from a
specific revision number.
""".
watch(Conn, Bucket, WatchOpts, Opts)
  when is_binary(Bucket), is_map(WatchOpts), is_map(Opts) ->
    watch(Conn, Bucket, WatchOpts, Opts, [{spawn_opt, [link]}]).

watch(Conn, Bucket, WatchOpts, Opts, StartOpts)
  when is_binary(Bucket), is_map(WatchOpts), is_map(Opts) ->
    nats_kv_watch:start(Conn, Bucket, WatchOpts, Opts, StartOpts).

-doc """
ListKeys will return KeyLister, allowing to retrieve all keys from
the key value store in a streaming fashion (on a channel).
""".
list_keys(Conn, Bucket, WatchOpts) ->
    list_keys(Conn, Bucket, WatchOpts, #{}).

list_keys(Conn, Bucket, WatchOpts0, Opts) ->
    WatchOpts = WatchOpts0#{ignore_deletes => true,
                            cb => fun list_keys_watch_cb/3},
    {ok, Pid} = watch(Conn, Bucket, WatchOpts, Opts),
    list_keys_loop(Pid, []).

list_keys_watch_cb({init, Owner}, _Conn, _) ->
    {continue, Owner};
list_keys_watch_cb(init_done, _Conn, Owner) ->
    Owner ! {done, self()},
    {stop, normal};
list_keys_watch_cb({msg, _, _, _} = Msg, _Conn, Owner) ->
    Owner ! {'WATCH', self(), Msg},
    {continue, Owner}.

list_keys_loop(Pid, Acc) ->
    receive
        {done, Pid} ->
            {ok, lists:reverse(Acc)};
        {'WATCH', Pid, {msg, Key, _Value, _Opts}} ->
            list_keys_loop(Pid, [Key | Acc])
    end.

-doc """
Get returns the latest value for the key. If the key does not exist,
ErrKeyNotFound will be returned.
""".
get(Conn, Bucket, Key)
  when is_binary(Bucket), is_binary(Key) ->
    get(Conn, Bucket, Key, last, #{}).

get(Conn, Bucket, Key, SeqNo)
  when SeqNo =:= last; is_integer(SeqNo) ->
    get(Conn, Bucket, Key, SeqNo, #{});
get(Conn, Bucket, Key, Opts)
  when is_binary(Bucket), is_binary(Key), is_map(Opts) ->
    get(Conn, Bucket, Key, last, Opts).

get(Conn, Bucket, Key, SeqNo, Opts) ->
    case get_msg(Conn, Bucket, Key, SeqNo, Opts) of
        {ok, #{message := #{hdrs := Headers}} = Response} ->
            case lists:keyfind(?KV_OP, 1, Headers) of
                {?KV_OP, Op} when Op =:= ?KV_DEL; Op =:= ?KV_PURGE ->
                    {deleted, Response};
                _ ->
                    {ok, Response}
            end;
        Response ->
            Response
    end.

get_msg(Conn, Bucket, Key, last, Opts) ->
    get_last_msg_for_subject(Conn, Bucket, Key, Opts);
get_msg(Conn, Bucket, Key, SeqNo, Opts)
  when is_integer(SeqNo) ->
    get_msg(Conn, Bucket, #{last_by_subject => Key, seq => SeqNo}, Opts).

get_last_msg_for_subject(Conn, Bucket, Key, #{allow_direct := true} = Opts) ->
    GetStr =  <<?BUCKET_NAME(Bucket)/binary, $., ?SUBJECT_NAME(Bucket, Key)/binary>>,
    Topic = make_js_direct_api_topic(~"GET", GetStr, Opts),
    case nats:request(Conn, Topic, <<>>, #{}) of
        {ok, Response} ->
            direct_msg_response(Response);
        Other ->
            Other
    end;
get_last_msg_for_subject(Conn, Bucket, Key, Opts) ->
    get_msg(Conn, Bucket, #{last_by_subj => Key}, Opts).

get_msg(Conn, Bucket, Req, #{allow_direct := true} = Opts) ->
    Topic = make_js_direct_api_topic(~"GET", ?BUCKET_NAME(Bucket), Opts),
    JSON = json:encode(marshal_get_request(Bucket, Req)),
    case nats:request(Conn, Topic, JSON, Opts) of
        {ok, Response} ->
            direct_msg_response(Response);
        Other ->
            Other
    end;
get_msg(Conn, Bucket, Req, Opts) ->
    Name = ?BUCKET_NAME(Bucket),
    JSON = marshal_get_request(Bucket, Req),
    case nats_stream:msg_get(Conn, Name, JSON, Opts) of
        {ok, #{message := Msg} = Response} ->
            {ok, Response#{message := get_response_msg(Msg)}};
        Other ->
            Other
    end.

-doc """
Put will place the new value for the key into the store. If the key
does not exist, it will be created. If the key exists, the value will
be updated.

A key has to consist of alphanumeric characters, dashes, underscores,
equal signs, and dots.
""".
put(Conn, Bucket, Key, Value)
  when is_binary(Bucket), is_binary(Key) ->
    case nats:request(Conn, ?SUBJECT_NAME(Bucket, Key), Value, #{}) of
        {ok, Response} ->
            unmarshal_response(Response);
        Other ->
            Other
    end.

-doc """
Create will add the key/value pair if it does not exist. If the key
already exists, ErrKeyExists will be returned.

A key has to consist of alphanumeric characters, dashes, underscores,
equal signs, and dots.
""".
create(Conn, Bucket, Key, Value) ->
    create(Conn, Bucket, Key, Value, #{}).

create(Conn, Bucket, Key, Value, Opts)
  when is_binary(Bucket), is_binary(Key) ->
    case update(Conn, Bucket, Key, Value, 0) of
        {error, #{err_code := ?JS_ERR_CODE_STREAM_WRONG_LAST_SEQUENCE}} ->
            case get(Conn, Bucket, Key, last, Opts) of
                {deleted, #{message := #{seq := LastRev}}}->
                    update(Conn, Bucket, Key, Value, LastRev);
                _ ->
                    {error, exists}
            end;
        {error, _} = Error ->
            Error;
        {ok, #{stream := _, seq := _}} = Result ->
            Result
    end.

-doc """
Update will update the value if the latest revision matches.
If the provided revision is not the latest, Update will return an error.
""".
update(Conn, Bucket, Key, Value, SeqNo)
  when is_binary(Bucket), is_binary(Key) ->
    Header =
        nats_hd:header([{?EXPECTED_LAST_SUBJ_SEQ_HDR, integer_to_binary(SeqNo)}]),
    case nats:request(Conn, ?SUBJECT_NAME(Bucket, Key), Value, #{header => Header}) of
        {ok, Response} ->
            unmarshal_response(Response);
        Other ->
            Other
    end.

-doc """
Delete will place a delete marker and leave all revisions. A history
of a deleted key can still be retrieved by using the History method
or a watch on the key. [Delete] is a non-destructive operation and
will not remove any previous revisions from the underlying stream.

[LastRevision] option can be specified to only perform delete if the
                                                                 latest revision the provided one.
""".
delete(Conn, Bucket, Key) ->
    delete(Conn, Bucket, Key, #{}).

delete(Conn, Bucket, Key, Opts)
  when is_binary(Bucket), is_binary(Key), is_map(Opts) ->
    Headers0 =
        case Opts of
            #{purge := true} ->
                [{?KV_OP, ?KV_PURGE}, {?MSG_ROLLUP, ?MSG_ROLLUP_SUBJECT}];
            _ ->
                [{?KV_OP, ?KV_DEL}]
        end,
    Headers =
        case Opts of
            #{revision := Revision} when Revision =/= 0 ->
                [{?EXPECTED_LAST_SUBJ_SEQ_HDR, integer_to_binary(Revision)} | Headers0];
            _ ->
                Headers0
        end,
    Header = nats_hd:header(Headers),
    case nats:request(Conn, ?SUBJECT_NAME(Bucket, Key), <<>>, #{header => Header}) of
        {ok, Response} ->
            unmarshal_response(Response);
        Other ->
            Other
    end.

-doc """
Purge will place a delete marker and remove all previous revisions.
Only the latest revision will be preserved (with a delete marker).
Unlike [Delete], Purge is a destructive operation and will remove all
previous revisions from the underlying streams.

[LastRevision] option can be specified to only perform purge if the
                                                                latest revision the provided one.
""".
purge(Conn, Bucket, Key) ->
    purge(Conn, Bucket, Key, #{}).

purge(Conn, Bucket, Key, Opts) ->
    delete(Conn, Bucket, Key, Opts#{purge => true}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% make_js_api_topic(Op, #{domain := Domain}) ->
%%     <<"$JS.", Domain/binary, ".API.STREAM.", Op/binary>>;
%% make_js_api_topic(Op, _) ->
%%     <<"$JS.API.STREAM.", Op/binary>>.

make_js_direct_api_topic(Op, Stream, #{domain := Domain}) ->
    <<"$JS.", Domain/binary, ".API.DIRECT.", Op/binary, $., Stream/binary>>;
make_js_direct_api_topic(Op, Stream, _) ->
    <<"$JS.API.DIRECT.", Op/binary, $., Stream/binary>>.

marshal_get_request(Bucket, Req) ->
    maps:map(
      fun(K, V)
            when K =:= last_by_subj; K =:= next_by_subj ->
              ?SUBJECT_NAME(Bucket, V);
         (K, V)
            when K =:= start_time; K =:= up_to_time ->
              if V < 0 ->
                      %% NATS uses this mean `never`
                      ~"0001-01-01T00:00:00Z";
                 V >= 0 ->
                      iolist_to_binary(
                        calendar:system_time_to_rfc3339(V, [{unit, nanosecond}, {offset, "Z"}]))
              end;
         (multi_last, Subjects) ->
              lists:map(fun(S) -> ?SUBJECT_NAME(Bucket, S) end, Subjects);
         (batch, V) when V > 1 ->
              ?LOG(critical, "enats: batch get requests are not supported"),
              V;
         (_K, V) ->
              V
      end, Req).

to_atom(Bin) when is_binary(Bin) ->
    try binary_to_existing_atom(Bin) catch _:_ -> Bin end.

json_object_push(<<"type">>, Value, Acc)
  when is_binary(Value) ->
    [{type, to_atom(Value)} | Acc];
json_object_push(Key, Value, Acc)
  when Key =:= <<"created">>;
       Key =:= <<"ts">>;
       Key =:= <<"first_ts">>;
       Key =:= <<"last_ts">> ->
    TS = calendar:rfc3339_to_system_time(binary_to_list(Value), [{unit, nanosecond}]),
    [{binary_to_atom(Key), TS} | Acc];
json_object_push(Key, Value, Acc) ->
    [{to_atom(Key), Value} | Acc].

unmarshal_response({Response, _Opts}) ->
    Decoders = #{object_push => fun json_object_push/3},
    try json:decode(Response, ok, Decoders) of
        {#{error := Error}, ok, _} ->
            {error, Error};
        {JSON, ok, _} ->
            {ok, JSON};
        _ ->
            {error, invalid_msg_payload}
    catch
        C:E:St ->
            {error, {C, E, St}}
    end.

direct_msg_response({#{error := Error}, _}) ->
    {error, Error};

direct_msg_response({_Content,
                     #{header := <<"NATS/1.0 ", Code:3/bytes, " ", Rest/binary>>}}) ->
    {Pos, _} = binary:match(Rest, <<"\r">>),
    <<Msg:Pos/binary, "\r\n", _HdrStr/binary>> = Rest,

    %% Headers = nats_hd:parse_headers(HdrStr),
    Error0 = #{code => binary_to_integer(Code), description => Msg},
    Error =
        case Msg of
            <<"Message Not Found">> -> Error0#{err_code => ?JS_ERR_CODE_MESSAGE_NOT_FOUND};
            _ -> Error0
        end,
    {error, Error};

direct_msg_response({Content, #{header := <<"NATS/1.0\r\n", HdrStr/binary>>}}) ->
    Headers = nats_hd:parse_headers(HdrStr),
    Msg = lists:foldl(fun direct_msg_response_f/2,
                      #{data => Content, hdrs => Headers}, Headers),
    {ok, #{message => Msg}}.

direct_msg_response_f({<<"Nats-Subject">>, Subject}, Msg) ->
    Msg#{subject => Subject};
direct_msg_response_f({<<"Nats-Sequence">>, SeqNo}, Msg) ->
    Msg#{seq => binary_to_integer(SeqNo)};
direct_msg_response_f({<<"Nats-Time-Stamp">>, TimeStamp}, Msg) ->
    Msg#{time => TimeStamp};
direct_msg_response_f(_, Msg) ->
    Msg.

get_response_msg(Msg) ->
    maps:map(fun get_response_msg/2, Msg).

get_response_msg(data, V) ->
    base64:decode(V);
get_response_msg(hdrs, V) ->
    case base64:decode(V) of
        <<"NATS/1.0\r\n", HdrStr/binary>> ->
            nats_hd:parse_headers(HdrStr);
        Other ->
            Other
    end;
get_response_msg(_K, V) ->
    V.

prepare_key_value_config(#{bucket := Bucket} = Config)
  when is_binary(Bucket) ->
    DuplicateWindow =
        case Config of
            #{ttl := TTL} when TTL > 0, TTL < 2 * ?MINUTE_NS ->
                TTL;
            _ ->
                2 * ?MINUTE_NS
        end,
    Compression =
        case Config of
            #{compression := true} -> ~"s2";
            _                      -> ~"none"
        end,
    StreamCfg =
        #{
          name                  => ?BUCKET_NAME(Bucket),
          max_msgs_per_subject  => 1,
          max_bytes             => -1,
          max_age               => maps:get(ttl, Config, 0),
          max_msg_size          => maps:get(max_value_size, Config, -1),
          replicas              => 1,
          allow_rollup_hdrs     => true,
          deny_delete           => true,
          duplicates            => DuplicateWindow,
          max_msgs              => -1,
          max_consumers         => -1,
          allow_direct          => true,
          compression           => Compression,
          subjects              => [?SUBJECT_NAME(Bucket, ~">")]
          %% discard            => discardnew,
         },
    maps:merge(StreamCfg,
               maps:with([description, histroy, max_bytes, storage,
                          replicas, placement, republish], Config)).
%% TBD: mirror and sources configuration
