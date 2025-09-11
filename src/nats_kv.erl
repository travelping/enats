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
         watch/5, watch/6,
         watch_all/4, watch_all/5,
         select_keys/4, select_keys/5,
         list_keys/3, list_keys/4,
         get/3, get/4, get/5, get_msg/4,
         put/4,
         create/4, create/5,
         update/5,
         delete/3, delete/4,
         purge/3, purge/4,
         request/5, pub/5
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
          bucket         := iodata(),

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

-doc #{equiv => get_bucket(Conn, Bucket, #{})}.
-spec get_bucket(Conn :: nats:conn(), Bucket :: iodata()) -> {ok, map()} | {error, term()}.
get_bucket(Conn, Bucket) ->
    get_bucket(Conn, Bucket, #{}).

-doc """
Retrieves the configuration of a KeyValue store bucket with additional options.

Allows specifying options for the underlying NATS request.
Returns `{ok, Config}` or `{error, Reason}`.
""".
-spec get_bucket(Conn :: nats:conn(), Bucket :: iodata(), Opts :: map()) ->
          {ok, map()} | {error, term()}.
get_bucket(Conn, Bucket, Opts) ->
    nats_stream:get(Conn, ?BUCKET_NAME(Bucket), Opts).

-doc #{equiv => create_bucket(Conn, #{bucket => Bucket}, #{})}.
-spec create_bucket(Conn :: nats:conn(), Bucket :: iodata()) -> {ok, map()} | {error, term()}.
create_bucket(Conn, Bucket) ->
    create_bucket(Conn, #{bucket => Bucket}, #{}).

-doc """
Creates a KeyValue store bucket with the specified configuration and options.

`Config` is a map containing the bucket configuration (see `t:config/0`).
`Opts` allows specifying options for the underlying NATS request.
Returns `{ok, map()}` on success or `{error, Reason}` on failure.
""".
-spec create_bucket(Conn :: nats:conn(), Config :: config(), Opts :: map()) ->
          {ok, map()} | {error, term()}.
create_bucket(Conn, Config, Opts) ->
    StreamCfg = prepare_key_value_config(Config),
    nats_stream:create(Conn, StreamCfg, Opts).

-doc """
Creates a KeyValue store bucket with the specified bucket name, configuration, and options.

This is an alternative way to specify the bucket name alongside the configuration map.
Returns `{ok, map()}` on success or `{error, Reason}` on failure.

Equivalent to [`create_bucket(Conn, Config#{bucket => Bucket}, Opts)`](`create_bucket/3`).
""".
-spec create_bucket(Conn :: nats:conn(), Bucket :: iodata(), Config :: map(), Opts :: map()) ->
          {ok, map()} | {error, term()}.
create_bucket(Conn, Bucket, Config, Opts)
  when is_map(Config), is_map(Opts) ->
    create_bucket(Conn, Config#{bucket => Bucket}, Opts).

-doc #{equiv => update_bucket(Conn, #{bucket => Bucket}, #{})}.
-spec update_bucket(Conn :: nats:conn(), Bucket :: iodata()) -> {ok, map()} | {error, term()}.
update_bucket(Conn, Bucket) ->
    update_bucket(Conn, #{bucket => Bucket}, #{}).

-doc """
Updates an existing KeyValue store bucket with the specified configuration and options.

`Config` is a map containing the bucket configuration (see `t:config/0`).
`Opts` allows specifying options for the underlying NATS request.
Returns `{ok, map()}` on success or `{error, Reason}` on failure.
""".
-spec update_bucket(Conn :: nats:conn(), Config :: config(), Opts :: map()) -> {ok, map()} | {error, term()}.
update_bucket(Conn, Config, Opts) ->
    StreamCfg = prepare_key_value_config(Config),
    nats_stream:update(Conn, StreamCfg, Opts).

-doc """
Updates an existing KeyValue store bucket with the specified bucket name, configuration, and options.

This is an alternative way to specify the bucket name alongside the configuration map.
Returns `{ok, map()}` on success or `{error, Reason}` on failure.

Equivalent to [`update_bucket(Conn, Config#{bucket => Bucket}, Opts)`](`update_bucket/3`).
""".
-spec update_bucket(Conn :: nats:conn(), Bucket :: iodata(), Config :: map(), Opts :: map()) ->
          {ok, map()} | {error, term()}.
update_bucket(Conn, Bucket, Config, Opts)
  when is_map(Config), is_map(Opts) ->
    update_bucket(Conn, Config#{bucket => Bucket}, Opts).

-doc #{equiv => delete_bucket(Conn, Bucket, #{})}.
-spec delete_bucket(Conn :: nats:conn(), Bucket :: iodata()) -> {ok, map()} | {error, term()}.
delete_bucket(Conn, Bucket) ->
    delete_bucket(Conn, Bucket, #{}).

-doc """
Deletes a KeyValue store bucket with the specified options.

Allows specifying options for the underlying NATS request.
Returns `{ok, map()}` on success or `{error, Reason}` on failure.
""".
-spec delete_bucket(Conn :: nats:conn(), Bucket :: iodata(), Opts :: map()) ->
          {ok, map()} | {error, term()}.
delete_bucket(Conn, Bucket, Opts)
  when is_map(Opts) ->
    nats_stream:delete(Conn, ?BUCKET_NAME(Bucket), Opts).

-doc """
Starts a watcher process that monitors updates to keys matching the given `Keys` pattern(s).

`Keys` can be a single binary subject or a list of binary subjects, potentially containing wildcards.
`WatchOpts` is a map of options for the watcher behavior (e.g., `include_history`, `ignore_deletes`, `updates_only`, `meta_only`, `resume_from_revision`).
`Opts` allows specifying options for the underlying NATS request.

The watcher process will send messages to the calling process's mailbox.
Returns `{ok, Pid}` of the watcher process or `{error, Reason}` on failure.
""".
-spec watch(Conn :: nats:conn(), Bucket :: iodata(), Keys :: binary() | [binary()], WatchOpts :: map(), Opts :: map()) -> {ok, pid()} | {error, term()}.
watch(Conn, Bucket, Keys, WatchOpts, Opts)
  when is_map(WatchOpts), is_map(Opts) ->
    watch(Conn, Bucket, Keys, WatchOpts, Opts, [{spawn_opt, [link]}]).

-doc """
Starts a watcher process with additional start options for the watcher process.

See `watch/5` for details on `Conn`, `Bucket`, `Keys`, `WatchOpts`, and `Opts`.
`StartOpts` are options passed to `proc_lib:spawn_opt/4` when starting the watcher process.
Returns `{ok, Pid}` or `{error, Reason}`.
""".
-spec watch(Conn :: nats:conn(), Bucket :: iodata(), Keys :: binary() | [binary()], WatchOpts :: map(), Opts :: map(), StartOpts :: [term()]) -> {ok, pid()} | {error, term()}.
watch(Conn, Bucket, Keys, WatchOpts, Opts, StartOpts)
  when is_map(WatchOpts), is_map(Opts) ->
    nats_kv_watch:start(Conn, Bucket, Keys, WatchOpts, Opts, StartOpts).

-doc """
Starts a watcher process that monitors all updates in the bucket.

This is a convenience function equivalent to calling `watch/5` with `Keys` set to `~">"`.
`WatchOpts` and `Opts` are as described in `watch/5`.
Returns `{ok, Pid}` or `{error, Reason}`.
""".
-spec watch_all(Conn :: nats:conn(), Bucket :: iodata(), WatchOpts :: map(), Opts :: map()) -> {ok, pid()} | {error, term()}.
watch_all(Conn, Bucket, WatchOpts, Opts)
  when is_map(WatchOpts), is_map(Opts) ->
    watch(Conn, Bucket, ~">", WatchOpts, Opts, [{spawn_opt, [link]}]).

-doc """
Starts a watcher process that monitors all updates in the bucket with additional start options.

See `watch_all/4` for details on `Conn`, `Bucket`, `WatchOpts`, and `Opts`.
`StartOpts` are options passed to `proc_lib:spawn_opt/4`.
Returns `{ok, Pid}` or `{error, Reason}`.
""".
-spec watch_all(Conn :: nats:conn(), Bucket :: iodata(), WatchOpts :: map(), Opts :: map(), StartOpts :: [term()]) -> {ok, pid()} | {error, term()}.
watch_all(Conn, Bucket, WatchOpts, Opts, StartOpts)
  when is_map(WatchOpts), is_map(Opts) ->
    nats_kv_watch:start(Conn, Bucket, ~">", WatchOpts, Opts, StartOpts).

-doc #{equiv => select_keys(Conn, Bucket, Keys, WatchOpts, #{})}.
-spec select_keys(Conn :: nats:conn(), Bucket :: iodata(), Keys :: binary() | [binary()], WatchOpts :: map()) -> {ok, list()} | {error, term()}.
select_keys(Conn, Bucket, Keys, WatchOpts) ->
    select_keys(Conn, Bucket, Keys, WatchOpts, #{}).

-doc """
Retrieves selected keys from the key value store with additional options.

This function starts a temporary watcher process to fetch the current values
for keys matching the `Keys` pattern(s). It waits for the initial values
and then stops the watcher.
`Keys` can be a single binary subject or a list of binary subjects, potentially containing wildcards.
`WatchOpts` is a map of options for the watcher behavior (e.g., `include_history`, `meta_only`).
Note that `ignore_deletes` is implicitly set to `true` for this function.
`Opts` allows specifying options for the underlying NATS request.

Returns `{ok, List}` where `List` is a list of keys (binary()) or key-value pairs ({binary(), binary()}),
or `{error, Reason}` on failure.
""".
-spec select_keys(Conn :: nats:conn(), Bucket :: iodata(), Keys :: binary() | [binary()], WatchOpts :: map(), Opts :: map()) -> {ok, list()} | {error, term()}.
select_keys(Conn, Bucket, Keys, WatchOpts0, Opts) ->
    WatchOpts = WatchOpts0#{ignore_deletes => true,
                            cb => fun select_keys_watch_cb/3},
    {ok, Pid} = watch(Conn, Bucket, Keys, WatchOpts, Opts),
    RetFun = select_ret_fun(WatchOpts, Opts),
    select_keys_loop(Pid, RetFun, []).

select_ret_fun(#{headers_only := false}, #{return_meta := true}) ->
    fun(Key, Data, Opts) -> {Key, Data, Opts} end;
select_ret_fun(_, #{return_meta := true}) ->
    fun(Key, _Data, Opts) -> {Key, Opts} end;
select_ret_fun(#{headers_only := false}, _) ->
    fun(Key, Data, _Opts) -> {Key, Data} end;
select_ret_fun(_, _) ->
    fun(Key, _Data, _Opts) -> Key end.

select_keys_watch_cb({init, Owner}, _Conn, _) ->
    {continue, Owner};
select_keys_watch_cb(init_done, _Conn, Owner) ->
    Owner ! {done, self()},
    {stop, normal};
select_keys_watch_cb({msg, _, _, _} = Msg, _Conn, Owner) ->
    Owner ! {'WATCH', self(), Msg},
    {continue, Owner}.

select_keys_loop(Pid, RetFun, Acc) ->
    receive
        {done, Pid} ->
            nats_kv_watch:done(Pid),
            {ok, lists:reverse(Acc)};
        {'WATCH', Pid, {msg, Key, Data, Opts}} ->
            select_keys_loop(Pid, RetFun, [RetFun(Key, Data, Opts) | Acc])
    end.

-doc #{equiv => list_keys(Conn, Bucket, WatchOpts, #{})}.
-spec list_keys(Conn :: nats:conn(), Bucket :: iodata(), WatchOpts :: map()) -> {ok, list()} | {error, term()}.
list_keys(Conn, Bucket, WatchOpts) ->
    list_keys(Conn, Bucket, WatchOpts, #{}).

-doc """
Retrieves all keys from the key value store with additional options.

This is a convenience function equivalent to calling `select_keys/4` with `Keys` set to `~">"`.
`WatchOpts` is a map of options for the watcher behavior (e.g., `include_history`, `meta_only`).
Note that `ignore_deletes` is implicitly set to `true` for this function.
`Opts` allows specifying options for the underlying NATS request.
Returns `{ok, List}` or `{error, Reason}`.
""".
-spec list_keys(Conn :: nats:conn(), Bucket :: iodata(), WatchOpts :: map(), Opts :: map()) -> {ok, list()} | {error, term()}.
list_keys(Conn, Bucket, WatchOpts, Opts) ->
    select_keys(Conn, Bucket, ~">", WatchOpts, Opts).

-doc """
Retrieves the latest value for a key.

Equivalent to [`get(Conn, Bucket, Key, last, #{})`](`get/5`).
""".
-spec get(Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata()) ->
          {ok, map()} |
          {'deleted', #{'message':=#{'hdrs':=maybe_improper_list(), _=>_}, _=>_}} |
          {error, term()}.
get(Conn, Bucket, Key) ->
    get(Conn, Bucket, Key, last, #{}).

-doc """
Retrieves a specific revision of a key by sequence number, or the latest value with options.

- `get(Conn, Bucket, Key, SeqNo)`: Retrieves the value at a specific `SeqNo` or the `last` revision.
- `get(Conn, Bucket, Key, Opts)`: Retrieves the latest value with additional `Opts`.

If the key does not exist, an error indicating "Message Not Found" will be returned.
If the key has been deleted, `{deleted, map()}` is returned.
Otherwise, `{ok, binary()}` is returned with the key's value.
""".
-spec get(Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata(), SeqNo :: integer() | last) ->
          {ok, map()} |
          {'deleted', #{'message':=#{'hdrs':=maybe_improper_list(), _=>_}, _=>_}} |
          {error, term()};
         (Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata(), Opts :: map()) ->
          {ok, map()} |
          {'deleted', #{'message':=#{'hdrs':=maybe_improper_list(), _=>_}, _=>_}} |
          {error, term()}.
get(Conn, Bucket, Key, SeqNo)
  when SeqNo =:= last; is_integer(SeqNo) ->
    get(Conn, Bucket, Key, SeqNo, #{});
get(Conn, Bucket, Key, Opts)
  when is_map(Opts) ->
    get(Conn, Bucket, Key, last, Opts).

-doc """
Retrieves a specific revision of a key by sequence number with additional options.

This is the most general `get` function.
`SeqNo` can be an integer revision number or the atom `last`.
`Opts` allows specifying options for the underlying NATS request.

If the key does not exist or the specific revision is not found, an error indicating "Message Not Found" will be returned.
If the key at the specified revision has a delete or purge marker, `{deleted, map()}` is returned.
Otherwise, `{ok, map()}` is returned containing the message details.
""".
-spec get(Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata(), SeqNo :: integer() | 'last', Opts :: map()) ->
          {'ok', map()} |
          {'deleted', #{'message':=#{'hdrs':=maybe_improper_list(), _=>_}, _=>_}} |
          {'error', term()}.
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

-doc """
Retrieves the raw message for a key at a specific sequence number or the last message.

This function interacts directly with the JetStream API to fetch a message by subject and sequence number.
It is typically used internally by the `get/3,4,5` functions.
Returns `{ok, map()}` containing the raw message details or `{error, Reason}`.
""".
-spec get_msg(Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata(), SeqNo :: integer() | last, Opts :: map()) ->
          {ok, map()} | {error, term()}.
get_msg(Conn, Bucket, Key, last, Opts) ->
    get_last_msg_for_subject(Conn, Bucket, Key, Opts);
get_msg(Conn, Bucket, Key, SeqNo, Opts)
  when is_integer(SeqNo) ->
    get_msg(Conn, Bucket, #{last_by_subject => Key, seq => SeqNo}, Opts).

get_last_msg_for_subject(Conn, Bucket, Key, #{allow_direct := true} = Opts) ->
    GetStr =  [?BUCKET_NAME(Bucket), $., ?SUBJECT_NAME(Bucket, Key)],
    Topic = make_js_direct_api_topic(~"GET", GetStr, Opts),
    case nats:request(Conn, Topic, <<>>, #{}) of
        {ok, Response} ->
            direct_msg_response(Response);
        {error, _} = Error ->
            Error
    end;
get_last_msg_for_subject(Conn, Bucket, Key, Opts) ->
    get_msg(Conn, Bucket, #{last_by_subj => Key}, Opts).

get_msg(Conn, Bucket, Req, #{allow_direct := true} = Opts) ->
    Topic = make_js_direct_api_topic(~"GET", ?BUCKET_NAME(Bucket), Opts),
    JSON = json:encode(marshal_get_request(Bucket, Req)),
    case nats:request(Conn, Topic, JSON, Opts) of
        {ok, Response} ->
            direct_msg_response(Response);
        {error, _} = Error ->
            Error
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
Puts a new value for a key into the store.

If the key does not exist, it will be created. If the key exists, the value will be updated.
A key must consist of alphanumeric characters, dashes, underscores, equal signs, and dots.
Returns `{ok, map()}` containing the publish response details or `{error, Reason}` on failure.
""".
-spec put(Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata(), Value :: iodata()) -> {ok, map()} | {error, term()}.
put(Conn, Bucket, Key, Value) ->
    request(Conn, Bucket, Key, Value, #{}).

-doc #{equiv => create(Conn, Bucket, Key, Value, #{})}.
-spec create(Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata(), Value :: iodata()) -> {ok, map()} | {error, term()}.
create(Conn, Bucket, Key, Value) ->
    create(Conn, Bucket, Key, Value, #{}).

-doc """
Creates a key/value pair if the key does not exist with additional options.

See `create/4` for details on `Conn`, `Bucket`, `Key`, and `Value`.
`Opts` allows specifying options, including `return_existing` which, if true,
will return `{error, {exists, OldObj}}` where `OldObj` is the existing message details.
Returns `{ok, map()}` or `{error, Reason}`.
""".
-spec create(Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata(), Value :: iodata(), Opts :: map()) -> {ok, map()} | {error, term()}.
create(Conn, Bucket, Key, Value, Opts) ->
    create_do(Conn, Bucket, Key, Value, maps:merge(#{retry => true}, Opts)).

create_do(Conn, Bucket, Key, Value, #{retry := Retry} = Opts) ->
    case update(Conn, Bucket, Key, Value, 0) of
        {error, #{err_code := ?JS_ERR_CODE_STREAM_WRONG_LAST_SEQUENCE}} ->
            case get(Conn, Bucket, Key, last, Opts) of
                {deleted, #{message := #{seq := LastRev}}}->
                    update(Conn, Bucket, Key, Value, LastRev);
                {ok, #{message := OldObj}}
                  when is_map_key(return_existing, Opts) andalso
                       map_get(return_existing, Opts) =:= true ->
                    {error, {exists, OldObj}};
                {error, #{err_code := ?JS_ERR_CODE_MESSAGE_NOT_FOUND}}
                  when Retry ->
                    %% race between us and stream purge on the message, retry
                    create_do(Conn, Bucket, Key, Value, Opts#{retry := false});
                Other ->
                    ?LOG(debug, "key '~0p' already exists: ~0p", [Key, Other]),
                    {error, exists}
            end;
        {error, _} = Error ->
            Error;
        {ok, #{stream := _, seq := _}} = Result ->
            Result
    end.

-doc """
Updates the value for a key if the latest revision matches the provided sequence number.

This function performs a conditional update. If the current latest revision
of the key does not match `SeqNo`, the update will fail with an error
indicating a wrong last sequence.
Returns `{ok, map()}` containing the publish response details or `{error, Reason}` on failure.
""".
-spec update(Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata(), Value :: iodata(), SeqNo :: integer()) ->
          {ok, map()} | {error, term()}.
update(Conn, Bucket, Key, Value, SeqNo) ->
    Header =
        nats_hd:header([{?EXPECTED_LAST_SUBJ_SEQ_HDR, integer_to_binary(SeqNo)}]),
    request(Conn, Bucket, Key, Value, #{header => Header}).

-doc #{equiv => delete(Conn, Bucket, Key, #{})}.
-spec delete(Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata()) -> {ok, map()} | {error, term()}.
delete(Conn, Bucket, Key) ->
    delete(Conn, Bucket, Key, #{}).

-doc """
Deletes a key by placing a delete marker with additional options.

This is a non-destructive operation; all previous revisions are kept.
A history of a deleted key can still be retrieved using `get/4,5` with a specific revision
or by using a watcher without the `ignore_deletes` option.

`Opts` can include:
- `revision`: An integer sequence number. The delete will only succeed if the latest
  revision matches this number (conditional delete).
- `purge`: If `true`, performs a purge instead of a regular delete (see `purge/3`).
Returns `{ok, map()}` or `{error, Reason}`.
""".
-spec delete(Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata(), Opts :: map()) -> {ok, map()} | {error, term()}.
delete(Conn, Bucket, Key, Opts)
  when is_map(Opts) ->
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
    request(Conn, Bucket, Key, <<>>, #{header => Header}).

-doc """
Purges a key by placing a delete marker and removing all previous revisions.

This is a destructive operation. Only the latest revision (the purge marker)
will be preserved.
Returns `{ok, map()}` containing the publish response details for the purge marker
or `{error, Reason}` on failure.
""".
-spec purge(Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata()) -> {ok, map()} | {error, term()}.
purge(Conn, Bucket, Key) ->
    purge(Conn, Bucket, Key, #{}).

-doc """
Purges a key with additional options.

See `purge/3` for details on `Conn`, `Bucket`, and `Key`.
`Opts` can include:
- `revision`: An integer sequence number. The purge will only succeed if the latest
  revision matches this number (conditional purge).
Returns `{ok, map()}` or `{error, Reason}`.
""".
-spec purge(Conn :: nats:conn(), Bucket :: iodata(), Key :: iodata(), Opts :: map()) -> {ok, map()} | {error, term()}.
purge(Conn, Bucket, Key, Opts) ->
    delete(Conn, Bucket, Key, Opts#{purge => true}).

-doc """
Perform a NATS request on Bucket/Key with a given Value and raw NATS request opts.

Returns `{ok, map()}` containing the publish response details for the purge marker
or `{error, Reason}` on failure.
""".
request(Conn, Bucket, Key, Value, Opts) ->
    case nats:request(Conn, ?SUBJECT_NAME(Bucket, Key), Value, Opts) of
        {ok, Response} ->
            unmarshal_response(Response);
        {error, _} = Error ->
            Error
    end.

-doc """
Perform a NATS publish on Bucket/Key with a given Value and raw NATS request opts.

Returns `ok` or `{error, Reason}` on failure.
""".
pub(Conn, Bucket, Key, Value, Opts) ->
    nats:pub(Conn, ?SUBJECT_NAME(Bucket, Key), Value, Opts).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% make_js_api_topic(Op, #{domain := Domain}) ->
%%     <<"$JS.", Domain/binary, ".API.STREAM.", Op/binary>>;
%% make_js_api_topic(Op, _) ->
%%     <<"$JS.API.STREAM.", Op/binary>>.

make_js_direct_api_topic(Op, Stream, #{domain := Domain}) ->
    [~"$JS.", Domain, ~".API.DIRECT.", Op, $., Stream];
make_js_direct_api_topic(Op, Stream, _) ->
    [~"$JS.API.DIRECT.", Op, $., Stream].

marshal_get_request(Bucket, Req) ->
    maps:map(
      fun(K, V)
            when K =:= last_by_subj; K =:= next_by_subj ->
              iolist_to_binary(?SUBJECT_NAME(Bucket, V));
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
              lists:map(fun(S) -> iolist_to_binary(?SUBJECT_NAME(Bucket, S)) end, Subjects);
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

prepare_key_value_config(#{bucket := Bucket} = Config) ->
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
          name                  => iolist_to_binary(?BUCKET_NAME(Bucket)),
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
          subjects              => [iolist_to_binary(?SUBJECT_NAME(Bucket, ~">"))]
          %% discard            => discardnew,
         },
    maps:merge(StreamCfg,
               maps:with([description, histroy, max_bytes, storage,
                          replicas, placement, republish, deny_delete,
                          allow_msg_ttl, subject_delete_marker_ttl,
                          allow_msg_counter, allow_atomic], Config)).
%% TBD: mirror and sources configuration
