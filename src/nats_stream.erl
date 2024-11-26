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

-module(nats_stream).

-export([types/1,
         create/3, create/4,
         get/2, get/3,
         update/3,
         delete/2, delete/3,
         purge/2, purge/3,
         list/1, list/2, list/3,
         names/1, names/2, names/3,
         msg_get/3, msg_get/4,
         msg_delete/3, msg_delete/4, msg_delete/5
        ]).
-ignore_xref([types/1]).
-ignore_xref([create/3, create/4,
              get/2, get/3,
              update/3,
              delete/2, delete/3,
              purge/2, purge/3,
              list/1, list/2, list/3,
              names/1, names/2, names/3,
              msg_get/3, msg_get/4,
              msg_delete/3, msg_delete/4, msg_delete/5
             ]).

-include_lib("enats/include/nats_stream.hrl").

-export_type([
              subject_transform_config/0,
              republish/0,
              placement/0,
              stream_source/0,
              external_stream/0
             ]).

-doc """
SubjectTransformConfig is for applying a subject transform (to matching messages) before doing anything else when a new message is received.
""".
-type subject_transform_config() ::
        #{
          src => binary(),
          dest => binary()
         }.

-doc """
RePublish is for republishing messages once committed to a stream. The
original subject is remapped from the subject pattern to the destination
pattern.
""".
-type republish() ::
        #{
          %% Source is the subject pattern to match incoming messages against.
          src => binary(),

          %% Destination is the subject pattern to republish the subject to.
          dest => binary(),

          %% HeadersOnly is a flag to indicate that only the headers should be
          %% republished.
          headers_only => boolean()
         }.

-doc """
Placement is used to guide placement of streams in clustered JetStream.
""".
-type placement() ::
        #{
          %% Cluster is the name of the cluster to which the stream should be
          %% assigned.
          cluster := binary(),

          %% Tags are used to match streams to servers in the cluster. A stream
          %% will be assigned to a server with a matching tag.
          tags => [binary()]
         }.

-doc """
StreamSource dictates how streams can source from other streams.
""".
-type stream_source() ::
        #{
          name               := binary(),
          opt_start_seq      => non_neg_integer(),
          opt_start_time     => binary(),
          filter_subject     => binary(),
          subject_transforms => [subject_transform_config()],
          external           => external_stream(),
          domain             => binary()
         }.

-doc """
ExternalStream allows you to qualify access to a stream source in another account.
""".
-type external_stream() ::
        #{
          api     := binary(),
          deliver => binary()
         }.


-define(API_OPTS, [domain]).
-define(MSG_GET_REQUEST_FIELDS, [seq, last_by_subj, next_by_subj, batch, max_bytes,
                                 start_time, multi_last, up_to_seq, up_to_time]).

%%%===================================================================
%%% API
%%%===================================================================

%% the primary purpose of this function is ensure that all
%% JSON messages types values exists as atom.
types(v1) ->
    [?JS_API_V1_STREAM_CREATE_RESPONSE,
     ?JS_API_V1_STREAM_DELETE_RESPONSE,
     ?JS_API_V1_STREAM_INFO_RESPONSE,
     ?JS_API_V1_STREAM_LIST_RESPONSE,
     ?JS_API_V1_STREAM_MSG_DELETE_RESPONSE,
     ?JS_API_V1_STREAM_MSG_GET_RESPONSE,
     ?JS_API_V1_STREAM_NAMES_RESPONSE,
     ?JS_API_V1_STREAM_PURGE_RESPONSE,
     ?JS_API_V1_STREAM_REMOVE_PEER_RESPONSE,
     ?JS_API_V1_STREAM_RESTORE_RESPONSE,
     ?JS_API_V1_STREAM_SNAPSHOT_RESPONSE,
     ?JS_API_V1_STREAM_TEMPLATE_CREATE_RESPONSE,
     ?JS_API_V1_STREAM_TEMPLATE_DELETE_RESPONSE,
     ?JS_API_V1_STREAM_TEMPLATE_INFO_RESPONSE,
     ?JS_API_V1_STREAM_TEMPLATE_NAMES_RESPONSE,
     ?JS_API_V1_STREAM_UPDATE_RESPONSE].

create(Conn, Name, Opts)
  when is_binary(Name), is_map(Opts) ->
    create(Conn, Name, maps:without(?API_OPTS, Opts), maps:with(?API_OPTS, Opts));

create(Conn, #{name := Name} = Config, Opts)
  when is_binary(Name), is_map(Opts) ->
    case Config of
        #{pedantic := true,
          retention := _,
          max_consumers := _,
          max_msgs := _,
          max_bytes := _,
          max_age := _,
          storage := _,
          num_replicas := _} -> ok;
        #{pedantic := true} ->
            erlang:error(badarg, [Config]);
        _ ->
            ok
    end,
    Topic = make_js_api_topic(~"CREATE", Name, Opts),
    case nats:request(Conn, Topic, json:encode(Config#{name => Name}), #{}) of
        {ok, Response} ->
            unmarshal_response(?JS_API_V1_STREAM_CREATE_RESPONSE, Response);
        Other ->
            Other
    end.

create(Conn, Name, Config, Opts) ->
    create(Conn, Config#{name => Name}, Opts).

get(Conn, Name) ->
    get(Conn, Name, #{}, #{}).

get(Conn, Name, Opts) ->
    get(Conn, Name, maps:without(?MSG_GET_REQUEST_FIELDS, Opts), maps:with(?API_OPTS, Opts)).

get(Conn, Name, Request, Opts) ->
    Topic = make_js_api_topic(~"INFO", Name, Opts),
    case nats:request(Conn, Topic, json_encode(Request), #{}) of
        {ok, Response} ->
            unmarshal_response(?JS_API_V1_STREAM_INFO_RESPONSE, Response);
        Other ->
            Other
    end.

update(Conn, Name, Opts)
  when is_binary(Name), is_map(Opts) ->
    update(Conn, Name, maps:without(?API_OPTS, Opts), maps:with(?API_OPTS, Opts));

update(Conn, #{name := Name} = Config, Opts)
  when is_map(Opts) ->
    case Config of
        #{pedantic := true,
          retention := _,
          max_consumers := _,
          max_msgs := _,
          max_bytes := _,
          max_age := _,
          storage := _,
          num_replicas := _} -> ok;
        #{pedantic := true} ->
            erlang:error(badarg, [Config]);
        _ ->
            ok
    end,
    Topic = make_js_api_topic(~"UPDATE", Name, Opts),
    case nats:request(Conn, Topic, json:encode(Config#{name => Name}), #{}) of
        {ok, Response} ->
            unmarshal_response(?JS_API_V1_STREAM_UPDATE_RESPONSE, Response);
        Other ->
            Other
    end.

update(Conn, Name, Config, Opts) ->
    update(Conn, Config#{name => Name}, Opts).

delete(Conn, Name) ->
    delete(Conn, Name, #{}).

delete(Conn, Name, Opts) ->
    Topic = make_js_api_topic(~"DELETE", Name, Opts),
    case nats:request(Conn, Topic, <<>>, #{}) of
        {ok, Response} ->
            unmarshal_response(?JS_API_V1_STREAM_DELETE_RESPONSE, Response);
        Other ->
            Other
    end.

purge(Conn, Name) ->
    purge(Conn, Name, #{}).

purge(Conn, Name, Opts) ->
    Topic = make_js_api_topic(~"PURGE", Name, Opts),
    case nats:request(Conn, Topic, <<>>, #{}) of
        {ok, Response} ->
            unmarshal_response(?JS_API_V1_STREAM_PURGE_RESPONSE, Response);
        Other ->
            Other
    end.

list(Conn) ->
    list(Conn, #{}, #{}).

list(Conn, Opts) ->
    JsKeys = [subject, offset],
    list(Conn, maps:with(JsKeys, Opts), maps:without(JsKeys, Opts)).

list(Conn, Request, Opts) ->
    Topic = make_js_api_topic(~"LIST", Opts),
    case nats:request(Conn, Topic, json_encode(Request), #{}) of
        {ok, Response} ->
            unmarshal_response(?JS_API_V1_STREAM_LIST_RESPONSE, Response);
        Other ->
            Other
    end.

names(Conn) ->
    names(Conn, #{}, #{}).

names(Conn, Opts) ->
    JsKeys = [subject, offset],
    names(Conn, maps:with(JsKeys, Opts), maps:without(JsKeys, Opts)).

names(Conn, Request, Opts) ->
    Topic = make_js_api_topic(~"NAMES", Opts),
    case nats:request(Conn, Topic, json_encode(Request), #{}) of
        {ok, Response} ->
            unmarshal_response(?JS_API_V1_STREAM_NAMES_RESPONSE, Response);
        Other ->
            Other
    end.

msg_get(Conn, Name, SeqNo) when is_integer(SeqNo), SeqNo >= 0 ->
    msg_get(Conn, Name, #{seq => SeqNo}, #{});
msg_get(Conn, Name, Subject) when is_binary(Subject) ->
    msg_get(Conn, Name, #{last_by_subj => Subject}, #{}).

%% Msg must be "io.nats.jetstream.api.v1.stream_msg_get_request",
msg_get(Conn, Name, Msg, Opts)
  when is_map(Msg), is_map(Opts) ->
    Topic = make_js_api_topic(~"MSG.GET", Name, Opts),
    case nats:request(Conn, Topic, json_encode(Msg), #{}) of
        {ok, Response} ->
            unmarshal_response(?JS_API_V1_STREAM_MSG_GET_RESPONSE, Response);
        Other ->
            Other
    end.

msg_delete(Conn, Name, SeqNo) when is_integer(SeqNo), SeqNo >= 0 ->
    msg_delete(Conn, Name, #{seq => SeqNo}, #{}).

msg_delete(Conn, Name, SeqNo, Opts)
  when is_integer(SeqNo), SeqNo >= 0, is_map(Opts) ->
    msg_delete(Conn, Name, #{seq => SeqNo}, Opts);

msg_delete(Conn, Name, SeqNo, NoErase)
  when is_integer(SeqNo), SeqNo >= 0, is_boolean(NoErase) ->
    msg_delete(Conn, Name, #{seq => SeqNo, no_erase => NoErase}, #{});

%% Msg must be "io.nats.jetstream.api.v1.stream_msg_delete_request",
msg_delete(Conn, Name, Msg, Opts)
  when is_map(Msg), is_map(Opts) ->
    Topic = make_js_api_topic(~"MSG.DELETE", Name, Opts),
    case nats:request(Conn, Topic, json_encode(Msg), #{}) of
        {ok, Response} ->
            unmarshal_response(?JS_API_V1_STREAM_MSG_DELETE_RESPONSE, Response);
        Other ->
            Other
    end.

msg_delete(Conn, Name, SeqNo, NoErase, Opts) ->
    msg_delete(Conn, Name, #{seq => SeqNo, no_erase => NoErase}, Opts).

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_js_api_topic(Op, #{domain := Domain}) ->
    <<"$JS.", Domain/binary, ".API.STREAM.", Op/binary>>;
make_js_api_topic(Op, _) ->
    <<"$JS.API.STREAM.", Op/binary>>.

make_js_api_topic(Op, Stream, #{domain := Domain}) ->
    <<"$JS.", Domain/binary, ".API.STREAM.", Op/binary, $., Stream/binary>>;
make_js_api_topic(Op, Stream, _) ->
    <<"$JS.API.STREAM.", Op/binary, $., Stream/binary>>.

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

json_encode(Msg)
  when map_size(Msg) =:= 0 ->
    <<>>;
json_encode(Msg) ->
    json:encode(Msg).

init_decoders(_) ->
    #{object_push => fun json_object_push/3}.

unmarshal_response(Type, {Response, _Opts}) ->
    Decoders = init_decoders(Type),
    try json:decode(Response, ok, Decoders) of
        {#{type := Type, error := Error}, ok, _} ->
            {error, Error};
        {#{type := Type} = JSON, ok, _} ->
            {ok, maps:remove(type, JSON)};
        _ ->
            {error, invalid_msg_payload}
    catch
        C:E:St ->
            {error, {C, E, St}}
    end.
