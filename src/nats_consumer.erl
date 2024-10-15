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

create(Conn, Stream)
  when is_binary(Stream) ->
    create(Conn, Stream, #{}, #{}).

create(Conn, Stream, Name)
  when is_binary(Stream), is_binary(Name) ->
    create(Conn, Stream, Name, #{});

create(Conn, Stream, Opts)
  when is_binary(Stream), is_map(Opts) ->
    create(Conn, Stream, maps:without(?API_OPTS, Opts), maps:with(?API_OPTS, Opts)).

create(Conn, Stream, Name, Opts)
  when is_binary(Stream), is_binary(Name), is_map(Opts) ->
    create(Conn, Stream, Name, maps:without(?API_OPTS, Opts), maps:with(?API_OPTS, Opts));

create(Conn, Stream, Config, Opts)
  when is_binary(Stream), is_map(Config), is_map(Opts) ->
    create_req(Conn, Stream, Config#{stream_name => Stream}, Opts).

create(Conn, Stream, Name, Config, Opts)
  when is_binary(Stream), is_binary(Name), is_map(Config), is_map(Opts) ->
    ConsumerId = <<Stream/binary, $., Name/binary>>,
    create_req(Conn, ConsumerId, Config#{stream_name => Stream, name => Name}, Opts).

create_req(Conn, ConsumerId, Config, Opts) ->
    Topic = make_js_api_create_topic(ConsumerId, Opts),
    case nats:request(Conn, Topic, json:encode(Config), #{}) of
        {ok, {JSON, _}} ->
            decode_response(JSON);
        Other ->
            Other
    end.

get(Conn, #{stream_name := Stream, name := Name}) ->
    get(Conn, Stream, Name, #{}).

get(Conn, #{stream_name := Stream, name := Name}, Opts)
  when is_map(Opts) ->
    get(Conn, Stream, Name, #{});

get(Conn, Stream, Name)
  when is_binary(Stream), is_binary(Name) ->
    get(Conn, Stream, Name, #{}).

get(Conn, Stream, Name, Opts)
  when is_binary(Stream), is_binary(Name) ->
    Topic = make_js_api_topic(~"INFO", <<Stream/binary, $., Name/binary>>, Opts),
    case nats:request(Conn, Topic, <<>>, #{}) of
        {ok, {JSON, _}} ->
            decode_response(JSON);
        Other ->
            Other
    end.

delete(Conn, #{stream_name := Stream, name := Name}) ->
    delete(Conn, Stream, Name, #{}).

delete(Conn, #{stream_name := Stream, name := Name}, Opts)
  when is_map(Opts) ->
    delete(Conn, Stream, Name, #{});

delete(Conn, Stream, Name)
  when is_binary(Stream), is_binary(Name) ->
    delete(Conn, Stream, Name, #{}).

delete(Conn, Stream, Name, Opts)
  when is_binary(Stream), is_binary(Name) ->
    Topic = make_js_api_topic(~"DELETE", <<Stream/binary, $., Name/binary>>, Opts),
    case nats:request(Conn, Topic, <<>>, #{}) of
        {ok, {JSON, _}} ->
            decode_response(JSON);
        Other ->
            Other
    end.

%% the caller is responsible for setting up a subscription on reply_to / deliver_subject.
msg_next(Conn, #{stream_name := Stream, name := Name,
                 config := #{deliver_subject := ReplyTo}}, Opts)
  when is_binary(Stream), is_binary(Name), is_binary(ReplyTo), is_map(Opts) ->
    msg_next(Conn, Stream, Name, ReplyTo, Opts).

msg_next(Conn, Stream, Name, ReplyTo, Opts)
  when is_binary(Stream), is_binary(Name), is_binary(ReplyTo), is_map(Opts) ->
    msg_next(Conn, Stream, Name, ReplyTo,
             maps:without(?API_OPTS, Opts), maps:with(?API_OPTS, Opts)).

%% msg_next/6
msg_next(Conn, Stream, Name, ReplyTo, Request, Opts)
  when is_binary(Stream), is_binary(Name), is_binary(ReplyTo),
       is_map(Request), is_map(Opts) ->
    msg_next_pub(Conn, Stream, Name, ReplyTo, json:encode(Request), Opts);
msg_next(Conn, Stream, Name, ReplyTo, Request, Opts)
  when is_binary(Stream), is_binary(Name), is_binary(ReplyTo),
       is_integer(Request), is_map(Opts) ->
    msg_next_pub(Conn, Stream, Name, ReplyTo, integer_to_binary(Request), Opts).

msg_next_pub(Conn, Stream, Name, ReplyTo, Request, Opts) ->
    Topic = make_js_api_topic(~"MSG.NEXT", <<Stream/binary, $., Name/binary>>, Opts),
    nats:pub(Conn, Topic, Request, Opts#{reply_to => ReplyTo}).

subscribe(Conn, #{stream_name := _, name := _,
                  config := #{deliver_subject := DeliverSubject}}, _Opts) ->
    nats:sub(Conn, DeliverSubject);
subscribe(_Conn, #{stream_name := _, name := _}, _Opts) ->
    {error, not_deliver_subject}.

subscribe(Conn, Stream, Name, _Opts) ->
    maybe
        #{type := ?JS_API_V1_CONSUMER_INFO_RESPONSE} = Config ?= get(Conn, Stream, Name),
        case Config of
            #{config := #{deliver_subject := DeliverSubject}} ->
                nats:sub(Conn, DeliverSubject);
            _ ->
                {error, not_a_push_consumer}
        end
    end.

unsubscribe(Conn, #{stream_name := _, name := _,
                    config := #{deliver_subject := DeliverSubject}}, _Opts) ->
    nats:unsub(Conn, DeliverSubject);
unsubscribe(_Conn, #{stream_name := _, name := _}, _Opts) ->
    {error, not_a_push_consumer}.

unsubscribe(Conn, Stream, Name, _Opts) ->
    maybe
        #{type := ?JS_API_V1_CONSUMER_INFO_RESPONSE} = Config ?= get(Conn, Stream, Name),
        case Config of
            #{config := #{deliver_subject := DeliverSubject}} ->
                nats:unsub(Conn, DeliverSubject);
            _ ->
                {error, not_a_push_consumer}
        end
    end.

ack_msg(_Conn, _AckType, _Sync, #{acked := true} = Opts) ->
    Opts;
ack_msg(Conn, AckType, Sync, #{reply_to := ReplyTo} = Opts) ->
    Ack = ack(AckType),
    case Sync of
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
    <<"$JS.", Domain/binary, ".API.CONSUMER.", Op/binary, $., Topic/binary>>;
make_js_api_topic(Op, Topic, _) ->
    <<"$JS.API.CONSUMER.", Op/binary, $., Topic/binary>>.

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
