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

-define(API_OPTS, [domain]).

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

create(Conn, Name, Opts) ->
    create(Conn, Name, maps:without(?API_OPTS, Opts), maps:with(?API_OPTS, Opts)).

create(Conn, Name, Config, Opts) ->
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
        {ok, {JSON, _}} ->
            decode_response(JSON);
        Other ->
            Other
    end.

get(Conn, Name) ->
    get(Conn, Name, #{}).

get(Conn, Name, Opts) ->
    Topic = make_js_api_topic(~"INFO", Name, Opts),
    case nats:request(Conn, Topic, <<>>, #{}) of
        {ok, {JSON, _}} ->
            decode_response(JSON);
        Other ->
            Other
    end.

update(Conn, Name, Opts) ->
    update(Conn, Name, maps:without(?API_OPTS, Opts), maps:with(?API_OPTS, Opts)).

update(Conn, Name, Config, Opts) ->
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
        {ok, {JSON, _}} ->
            decode_response(JSON);
        Other ->
            Other
    end.

delete(Conn, Name) ->
    delete(Conn, Name, #{}).

delete(Conn, Name, Opts) ->
    Topic = make_js_api_topic(~"DELETE", Name, Opts),
    case nats:request(Conn, Topic, <<>>, #{}) of
        {ok, {JSON, _}} ->
            decode_response(JSON);
        Other ->
            Other
    end.

purge(Conn, Name) ->
    purge(Conn, Name, #{}).

purge(Conn, Name, Opts) ->
    Topic = make_js_api_topic(~"PURGE", Name, Opts),
    case nats:request(Conn, Topic, <<>>, #{}) of
        {ok, {JSON, _}} ->
            decode_response(JSON);
        Other ->
            Other
    end.

list(Conn) ->
    list(Conn, #{}, #{}).

list(Conn, Opts) ->
    JsKeys = [subject, offset],
    list(Conn, maps:with(JsKeys), maps:without(Opts)).

list(Conn, Request, Opts) ->
    Topic = make_js_api_topic(~"LIST", Opts),
    BinReq = if map_size(Request) =:= 0 ->
                     <<>>;
                true ->
                     json:encode(Request)
             end,
    case nats:request(Conn, Topic, BinReq, #{}) of
        {ok, {JSON, _}} ->
            decode_response(JSON);
        Other ->
            Other
    end.

names(Conn) ->
    names(Conn, #{}, #{}).

names(Conn, Opts) ->
    JsKeys = [subject, offset],
    names(Conn, maps:with(JsKeys), maps:without(Opts)).

names(Conn, Request, Opts) ->
    Topic = make_js_api_topic(~"NAMES", Opts),
    BinReq = if map_size(Request) =:= 0 ->
                     <<>>;
                true ->
                     json:encode(Request)
             end,
    case nats:request(Conn, Topic, BinReq, #{}) of
        {ok, {JSON, _}} ->
            decode_response(JSON);
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
    case nats:request(Conn, Topic, json:encode(Msg), #{}) of
        {ok, {JSON, _}} ->
            decode_response(JSON);
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
    case nats:request(Conn, Topic, json:encode(Msg), #{}) of
        {ok, {JSON, _}} ->
            decode_response(JSON);
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

json_object_push(Key, Value, Acc) ->
    K = try binary_to_existing_atom(Key) catch _:_ -> Key end,
    [{K, Value} | Acc].

decode_response(Response) ->
    try json:decode(Response, ok, #{object_push => fun json_object_push/3}) of
        {#{type := Type} = JSON, ok, _} ->
            JSON#{type := binary_to_existing_atom(Type)};
        {JSON, ok, _} ->
            JSON
    catch
        C:E ->
            {error, {C, E}}
    end.
