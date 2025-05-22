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

-module(nats_consumer).

-export([types/1,
         create/2, create/3, create/4, create/5,
         get/2, get/3, get/4,
         delete/2, delete/3, delete/4,
         msg_next/3, msg_next/5, msg_next/6,
         ack_msg/4,
         subscribe/3, subscribe/4,
         unsubscribe/3, unsubscribe/4
        ]).
-ignore_xref([types/1]).

-include_lib("enats/include/nats_stream.hrl").

-define(API_OPTS, [domain, header]).

%%%===================================================================
%%% API
%%%===================================================================

%% the primary purpose of this function is ensure that all
%% JSON messages types values exists as atom.
types(v1) ->
    [?JS_API_V1_CONSUMER_CREATE_RESPONSE,
     ?JS_API_V1_CONSUMER_DELETE_RESPONSE,
     ?JS_API_V1_CONSUMER_INFO_RESPONSE,
     ?JS_API_V1_CONSUMER_LIST_RESPONSE,
     ?JS_API_V1_CONSUMER_NAMES_RESPONSE,
     ?JS_API_V1_CONSUMER_PAUSE_RESPONSE].

-spec create(Conn :: nats:conn(), Stream :: iodata()) -> {ok, map()} | {error, any()}.
create(Conn, Stream) ->
    create(Conn, Stream, #{}, #{}).

-spec create(Conn :: nats:conn(), Stream :: iodata(), Opts :: map()) ->
          {ok, map()} | {error, any()};
            (Conn :: nats:conn(), Stream :: iodata(), Name :: iodata()) ->
          {ok, map()} | {error, any()}.
create(Conn, Stream, Opts)
  when is_map(Opts) ->
    create(Conn, Stream, maps:without(?API_OPTS, Opts), maps:with(?API_OPTS, Opts));

create(Conn, Stream, Name) ->
    create(Conn, Stream, Name, #{}).

-spec create(Conn :: nats:conn(), Stream :: iodata(), Config :: map(), Opts :: map()) ->
          {ok, map()} | {error, any()};
            (Conn :: nats:conn(), Stream :: iodata(), Name :: iodata(), Opts :: map()) ->
          {ok, map()} | {error, any()}.
create(Conn, Stream, Config, Opts)
  when is_map(Config), is_map(Opts) ->
    create_req(Conn, Stream, Config#{stream_name => to_bin(Stream)}, Opts);

create(Conn, Stream, Name, Opts)
  when is_map(Opts) ->
    create(Conn, Stream, Name, maps:without(?API_OPTS, Opts), maps:with(?API_OPTS, Opts)).

-spec create(Conn :: nats:conn(), Stream :: iodata(), Name :: iodata(),
             Config :: map(), Opts :: map()) -> {ok, map()} | {error, any()}.
create(Conn, Stream, Name, Config, Opts)
  when is_map(Config), is_map(Opts) ->
    ConsumerId = [Stream, $., Name],
    create_req(
      Conn, ConsumerId, Config#{stream_name => to_bin(Stream), name => to_bin(Name)}, Opts).

create_req(Conn, ConsumerId, Config, Opts) ->
    Topic = make_js_api_create_topic(ConsumerId, Opts),
    case nats:request(Conn, Topic, json:encode(Config), #{}) of
        {ok, Response} ->
            unmarshal_response(?JS_API_V1_CONSUMER_CREATE_RESPONSE, Response);
        Other ->
            Other
    end.

-spec get(Conn :: nats:conn(), #{name := iodata(), stream_name := iodata(), _=>_}) ->
          {ok, map()} | {error, any()}.
get(Conn, #{stream_name := Stream, name := Name}) ->
    get(Conn, Stream, Name, #{}).

-spec get(Conn :: nats:conn(),
          #{name := iodata(), stream_name := iodata(), _=>_}, Opts :: map()) ->
          {ok, map()} | {error, any()};
         (Conn :: nats:conn(), Stream :: iodata(), Name :: iodata()) ->
          {ok, map()} | {error, any()}.
get(Conn, #{stream_name := Stream, name := Name}, Opts)
  when is_map(Opts) ->
    get(Conn, Stream, Name, #{});
get(Conn, Stream, Name) ->
    get(Conn, Stream, Name, #{}).

-spec get(Conn :: nats:conn(), Stream :: iodata(), Name :: iodata(), Opts :: map()) ->
          {ok, map()} | {error, any()}.
get(Conn, Stream, Name, Opts) ->
    Topic = make_js_api_topic(~"INFO", [Stream, $., Name], Opts),
    case nats:request(Conn, Topic, <<>>, #{}) of
        {ok, Response} ->
            unmarshal_response(?JS_API_V1_CONSUMER_INFO_RESPONSE, Response);
        {error, _} = Error ->
            Error
    end.

-spec delete(Conn :: nats:conn(), Config :: map()) -> {ok, map()} | {error, timeout}.
delete(Conn, #{stream_name := Stream, name := Name}) ->
    delete(Conn, Stream, Name, #{}).

-spec delete(Conn :: nats:conn(),
             #{name := iodata(), stream_name := iodata(), _=>_}, Opts :: map()) ->
          {ok, map()} | {error, any()};
            (Conn :: nats:conn(), Stream :: iodata(), Name :: iodata()) ->
          {ok, map()} | {error, any()}.
delete(Conn, #{stream_name := Stream, name := Name}, Opts)
  when is_map(Opts) ->
    delete(Conn, Stream, Name, Opts);

delete(Conn, Stream, Name) ->
    delete(Conn, Stream, Name, #{}).

-spec delete(Conn :: nats:conn(), Stream :: iodata(), Name :: iodata(), Opts :: map()) ->
          {ok, map()} | {error, any()}.
delete(Conn, Stream, Name, Opts) ->
    Topic = make_js_api_topic(~"DELETE", [Stream, $., Name], Opts),
    case nats:request(Conn, Topic, <<>>, #{}) of
        {ok, Response} ->
            unmarshal_response(?JS_API_V1_CONSUMER_DELETE_RESPONSE, Response);
        Other ->
            Other
    end.

%% the caller is responsible for setting up a subscription on reply_to / deliver_subject.
-spec msg_next(Conn :: nats:conn(),
               #{config := #{deliver_subject := iodata(), _=>_},
                 name := iodata(), stream_name := iodata(), _=>_},
               Opts :: map()) -> ok | {error, timeout}.
msg_next(Conn, #{stream_name := Stream, name := Name,
                 config := #{deliver_subject := ReplyTo}}, Opts)
  when is_map(Opts) ->
    msg_next(Conn, Stream, Name, ReplyTo, Opts).

-spec msg_next(Conn :: nats:conn(), Stream :: iodata(), Name :: iodata(),
               ReplyTo :: iodata(), Opts :: map()) -> ok | {error, timeout}.
msg_next(Conn, Stream, Name, ReplyTo, Opts)
  when is_map(Opts) ->
    msg_next(Conn, Stream, Name, ReplyTo,
             maps:without(?API_OPTS, Opts), maps:with(?API_OPTS, Opts)).

%% msg_next/6
msg_next(Conn, Stream, Name, ReplyTo, Request, Opts)
  when is_map(Request), is_map(Opts) ->
    msg_next_pub(Conn, Stream, Name, ReplyTo, json:encode(Request), Opts);
msg_next(Conn, Stream, Name, ReplyTo, Request, Opts)
  when is_integer(Request), is_map(Opts) ->
    msg_next_pub(Conn, Stream, Name, ReplyTo, integer_to_binary(Request), Opts).

-spec msg_next(Conn :: nats:conn(), Stream :: iodata(), Name :: iodata(),
               ReplyTo :: iodata(), Request :: map() | integer(), Opts :: map()) ->
          ok | {error, any()}.
msg_next_pub(Conn, Stream, Name, ReplyTo, Request, Opts) ->
    Topic = make_js_api_topic(~"MSG.NEXT", [Stream, $., Name], Opts),
    nats:pub(Conn, Topic, Request, Opts#{reply_to => ReplyTo}).

-spec subscribe(Conn :: nats:conn(), Config :: map(), Opts :: map()) ->
          {ok, pid()} | {error, any()} | {error, not_deliver_subject}.
subscribe(Conn, #{stream_name := _, name := _,
                  config := #{deliver_subject := DeliverSubject}}, _Opts) ->
    nats:sub(Conn, DeliverSubject);
subscribe(_Conn, #{stream_name := _, name := _}, _Opts) ->
    {error, not_deliver_subject}.

-spec subscribe(Conn :: nats:conn(), Stream :: iodata(), Name :: iodata(), Opts :: map()) ->
          {ok, pid()} | {error, any()} | {error, not_a_push_consumer}.
subscribe(Conn, Stream, Name, _Opts) ->
    maybe
        {ok, #{type := ?JS_API_V1_CONSUMER_INFO_RESPONSE} = Config} ?= get(Conn, Stream, Name),
        case Config of
            #{config := #{deliver_subject := DeliverSubject}} ->
                nats:sub(Conn, DeliverSubject);
            _ ->
                {error, not_a_push_consumer}
        end
    end.

-spec unsubscribe(Conn :: nats:conn(), Config :: map(), Opts :: map()) ->
          ok | {error, any()} | {error, not_a_a_push_consumer}.
unsubscribe(Conn, #{stream_name := _, name := _,
                    config := #{deliver_subject := DeliverSubject}}, _Opts) ->
    nats:unsub(Conn, DeliverSubject);
unsubscribe(_Conn, #{stream_name := _, name := _}, _Opts) ->
    {error, not_a_push_consumer}.

-spec unsubscribe(Conn :: nats:conn(), Stream :: iodata(), Name :: iodata(), Opts :: map()) ->
          ok | {error, any()} | {error, not_a_push_consumer}.
unsubscribe(Conn, Stream, Name, _Opts) ->
    maybe
        {ok, #{type := ?JS_API_V1_CONSUMER_INFO_RESPONSE} = Config} ?= get(Conn, Stream, Name),
        case Config of
            #{config := #{deliver_subject := DeliverSubject}} ->
                nats:unsub(Conn, DeliverSubject);
            _ ->
                {error, not_a_push_consumer}
        end
    end.

-spec ack_msg(Conn :: nats:conn(), AckType :: atom() | tuple(),
              Sync :: boolean(), Opts :: map()) -> map().
ack_msg(_Conn, _AckType, _Sync, #{acked := true} = Opts) ->
    Opts;
ack_msg(Conn, AckType, Sync, #{reply_to := ReplyTo} = Opts) ->
    Ack = ack(AckType),
    %% TBD: swallowing the result of the ACK might be wrong, check with Golang NATS client
    _ = case Sync of
            true ->
                nats:request(Conn, ReplyTo, Ack, #{});
            false ->
                nats:pub(Conn, ReplyTo, Ack, #{})
        end,
    case AckType of
        _ when AckType =:= ack; AckType =:= next; AckType =:= term ->
            Opts#{acked => true};
        {TplType, _} when TplType =:= next; TplType =:= term ->
            Opts#{acked => true};
        _ -> Opts
    end;
ack_msg(_Conn, _AckType, _Sync, Opts) ->
    Opts.

%%%===================================================================
%%% Internal functions
%%%===================================================================

make_js_api_create_topic(ConsumerId, #{durable := true} = Opts) ->
    make_js_api_topic(~"CREATE.DURABLE", ConsumerId, Opts);
make_js_api_create_topic(ConsumerId, Opts) ->
    make_js_api_topic(~"CREATE", ConsumerId, Opts).

make_js_api_topic(Op, Topic, #{domain := Domain}) ->
    [~"$JS.", Domain, ~".API.CONSUMER.", Op, $., Topic];
make_js_api_topic(Op, Topic, _) ->
    [~"$JS.API.CONSUMER.", Op, $., Topic].

to_atom(Bin) when is_binary(Bin) ->
    try binary_to_existing_atom(Bin) catch _:_ -> Bin end.

to_bin(Bin) when is_binary(Bin) -> Bin;
to_bin(List) when is_list(List) -> iolist_to_binary(List).

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

ack(ack) -> ~"+ACK";
ack(next) -> ~"+NXT";
ack({next, Payload}) when is_integer(Payload) ->
    [~"+NXT ", integer_to_binary(Payload)];
ack({next, Payload}) when is_map(Payload) ->
    [~"+NXT ", json:encode(Payload)];
ack({next, Payload}) ->
    [~"+NXT ", Payload];
ack(nak) -> ~"-NAK";
ack({nak, Delay})
  when is_integer(Delay), Delay > 0 ->
    [~"-NAK ", json:encode(#{delay => Delay})];
ack(inProgress) -> ~"+WPI";
ack(term) -> ~"+TERM";
ack({term, Reason}) ->
    [~"+TERM ", Reason].
