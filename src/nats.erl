%% Copyright 2016, Travelping GmbH <info@travelping.com>

%% This program is free software; you can redistribute it and/or
%% modify it under the terms of the GNU General Public License
%% as published by the Free Software Foundation; either version
%% 2 of the License, or (at your option) any later version.

-module(nats).

-behaviour(gen_statem).

%% API
-export([connect/2,
         connect/3]).
-export([pub/3,
         pub/4,
         sub/2,
         sub/3,
         unsub/2,
         unsub/3]).
%%          disconnect/1,
%%          is_ready/1]).

%% debug functions
-export([dump_subs/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([handle_event/4]).

-define(MSG, ?MODULE).
-define(VERSION, <<"0.4.1">>).
-define(DEFAULT_SEND_TIMEOUT, 10).
-define(DEFAULT_MAX_BATCH_SIZE, 100).
-define(CONNECT_TIMEOUT, 1000).

-define(DEFAULT_OPTS,
        #{verbose => false,
          pedantic => false,
          ssl_required => false,
          auth_token => undefined,
          user => undefined,
          pass => undefined,
          name => <<"nats">>,
          lang => <<"Erlang">>,
          version => ?VERSION,
          buffer_size => 0,
          max_batch_size => ?DEFAULT_MAX_BATCH_SIZE,
          send_timeout => ?DEFAULT_SEND_TIMEOUT,
          reconnect => {undefined, 0}}).
-define(DEFAULT_SOCKET_OPTS,
        #{reuseaddr => true}).

-include_lib("kernel/include/logger.hrl").

-type socket_opts() :: #{netns => string(),
                         netdev => binary(),
                         rcvbuf => non_neg_integer(),
                         sndbuf => non_neg_integer(),
                         reuseaddr => boolean()}.
%% -type nats_host() :: inet:socket_address() | inet:hostname().
-type nats_host() :: inet:socket_address().

-type opts() :: #{socket         => socket_opts(),
                  verbose        => boolean(),
                  pedantic       => boolean(),
                  ssl_required   => boolean(),
                  auth_token     => binary(),
                  user           => binary(),
                  pass           => binary(),
                  name           => binary(),
                  lang           => binary(),
                  version        => binary(),
                  buffer_size    => non_neg_integer(),
                  max_batch_size => non_neg_integer(),
                  send_timeout   => non_neg_integer(),
                  reconnect      => term()}.

-type data() :: #{socket         := undefined | gen_tcp:socket() | ssl:socket(),
                  tls            := boolean,
                  server_info    := map(),
                  recv_buffer    := binary(),
                  batch          := iolist(),
                  batch_size     := non_neg_integer(),
                  batch_timer    := undefined | reference(),

                  tid            := ets:tid(),

                  %% Opts
                  host           := nats_host(),
                  port           := inet:port_number(),
                  socket         := socket_opts(),
                  verbose        := boolean(),
                  pedantic       := boolean(),
                  ssl_required   := boolean(),
                  auth_token     := undefined | binary(),
                  user           := undefined | binary(),
                  pass           := undefined | binary(),
                  name           := binary(),
                  lang           := binary(),
                  version        := binary(),
                  buffer_size    := non_neg_integer(),
                  max_batch_size := non_neg_integer(),
                  send_timeout   := non_neg_integer(),
                  reconnect      := term()
                 }.

-record(pid, {pid}).
-record(sub, {pid, sid}).
-record(sub_session, {pid, max_msgs}).

%%%===================================================================
%%% API
%%%===================================================================

connect(Host, Port) ->
    connect(Host, Port, #{}).

-spec connect(Host :: nats_host(),
              Port :: inet:port_number(),
              Opts :: opts()) ->
          {ok, Pid :: pid()} |
          ignore |
          {error, Error :: term()}.
connect(Host, Port, Opts) ->
    logger:set_primary_config(level, debug),
    gen_statem:start_link(?MODULE, [Host, Port, Opts], []).

pub(Server, Subject, Payload) ->
    pub(Server, Subject, Payload, #{}).

pub(Server, Subject, Payload, Opts) ->
    gen_statem:call(Server, {pub, Subject, Payload, Opts}).

sub(Server, Subject) ->
    sub(Server, Subject, #{}).

sub(Server, Subject, Opts) ->
    gen_statem:call(Server, {sub, Subject, Opts, self()}).

unsub(Server, SRef) ->
    unsub(Server, SRef, #{}).
unsub(Server, SRef, Opts) ->
    gen_statem:call(Server, {unsub, SRef, Opts}).

dump_subs(Server) ->
    gen_statem:call(Server, dump_subs).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> [handle_event_function, state_enter].

-spec init(Args :: term()) ->
          gen_statem:init_result(data()).
init([Host, Port, Opts0]) ->
    process_flag(trap_exit, true),

    nats_msg:init(),

    Opts = maps:merge(?DEFAULT_OPTS, Opts0),
    ?LOG(debug, "SocketOpts: ~p", [socket_opts(Opts)]),
    case gen_tcp:connect(Host, Port, socket_opts(Opts), ?CONNECT_TIMEOUT) of
        {ok, Socket} ->
            Data = init_data(Host, Port, Socket, Opts),
            {ok, connected, Data};
        {error, _} = Error ->
            ?LOG(debug, "NATS client failed to open ~p TCP socket for connecting to ~s:~w with ~p",
                 [family(Host), inet:ntoa(Host), Port, Error]),
            Error
    end.

-spec handle_event('enter',
                   OldState :: term(),
                   State :: term(),
                   Data :: term()) ->
          gen_statem:state_enter_result(data());
                  (gen_statem:event_type(),
                   Msg :: term(),
                   State :: term(),
                   Data :: term()) ->
          gen_statem:event_handler_result(data()).

handle_event(enter, _, connected, #{socket := Socket}) ->
    inet:setopts(Socket, [{active,once}]),
    keep_state_and_data;
handle_event(enter, _, ready, #{socket := Socket}) ->
    inet:setopts(Socket, [{active, true}]),
    keep_state_and_data;

handle_event(info, {tcp_error, Socket, Reason}, _, #{socket := Socket} = Data) ->
    log_connection_error(Reason, Data),
    {next_state, disconnected, close_socket(Data)};
handle_event(info, {tcp_closed, Socket}, _, #{socket := Socket} = Data) ->
    {next_state, disconnected, close_socket(Data)};

handle_event(info, {tcp, Socket, Bin}, State, #{socket := Socket} = Data)
  when State =:= connected; State =:= ready ->
    ?LOG(debug, "got data: ~p", [Bin]),
    handle_message(Bin, State, Data);

handle_event(info, batch_timeout, ready, #{batch := Batch} = Data) ->
    send(Batch, Data),
    {keep_state, Data#{batch_size := 0, batch := [], batch_timer := undefined}};

handle_event(info, {'DOWN', _MRef, process, Pid, normal}, _, Data) ->
    del_pid_monitor(Pid, Data),
    Sids = get_pid_subs(Pid, Data),
    lists:foreach(fun(X) -> del_sid_session(X, Data) end, Sids),
    true = length(Sids) =:= del_pid_subs(Pid, Data),
    keep_state_and_data;

handle_event({call, _From}, _, connected, _Data) ->
    postpone;
handle_event({call, From}, _, State, _Data) when State /= ready ->
    {keep_state_and_data, [{reply, From, {error, not_connected}}]};

handle_event({call, From}, {pub, Subject, Payload, Opts}, _State, Data) ->
    ReplyTo = maps:get(reply_to, Opts, undefined),
    Msg = nats_msg:pub(Subject, ReplyTo, Payload),
    {keep_state, enqueue_msg(Msg, Data), [{reply, From, ok}]};

handle_event({call, From}, {sub, Subject, Opts, NotifyPid}, _State, Data0) ->
    {NatsSid, Sid, Data} = make_sub_id(Data0),
    monitor_sub_pid(NotifyPid, Data),
    put_pid_sub(NotifyPid, Sid, Data),
    put_sid_session(Sid, NotifyPid, 0, Data),

    QueueGrp = maps:get(queue_group, Opts, undefined),
    Msg = nats_msg:sub(Subject, QueueGrp, NatsSid),
    {keep_state, enqueue_msg(Msg, Data), [{reply, From, {ok, Sid}}]};

handle_event({call, From}, {unsub, {'$sid', NatsSid} = Sid, Opts}, _State, Data) ->
    case get_sid_session(Sid, Data) of
        [#sub_session{pid = NotifyPid}] ->
            case Opts of
                #{max_messages := MaxMsgsOpt} when is_integer(MaxMsgsOpt) ->
                    put_sid_session(Sid, NotifyPid, MaxMsgsOpt, Data);
                _ ->
                    del_sid_session(Sid, Data),
                    del_pid_sub(NotifyPid, Sid, Data),
                    demonitor_sub_pid(NotifyPid, Data)
            end,

            MaxMsgs = maps:get(max_messages, Opts, undefined),
            Msg = nats_msg:unsub(integer_to_binary(NatsSid), MaxMsgs),
            {keep_state, enqueue_msg(Msg, Data), [{reply, From, ok}]};
        _ ->
            {keep_state_and_data, [{reply, From, {error, invalid_session_ref}}]}
    end;

handle_event({call, From}, dump_subs, _State, #{tid := Tid}) ->
    {keep_state_and_data, [{reply, From, ets:tab2list(Tid)}]};

handle_event(Event, EventContent, State, Data) ->
    ?LOG(debug, "NATS:~nEvent: ~p~nContent: ~p~nState: ~p~nData: ~p",
         [Event, EventContent, State, Data]),
    keep_state_and_data.

terminate(_Reason, _State, _Data) ->
    void.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%%===================================================================
%%% State Helper functions
%%%===================================================================

handle_message(Bin, State0, #{recv_buffer := Acc0} = Data0) ->
    {{State, Data}, Acc} =
        nats_msg:decode(<<Acc0/binary, Bin/binary>>, {fun handle_nats_msg/2, {State0, Data0}}),
    {next_state, State, Data#{recv_buffer := Acc}}.

handle_nats_msg(stop, DecState) ->
    {stop, DecState};

handle_nats_msg(ping, {connected, Data} = DecState) ->
    send(nats_msg:pong(), Data),
    {continue, DecState};

handle_nats_msg(ping, {ready, Data0}) ->
    Data = flush_batch(enqueue_msg_no_check(nats_msg:pong(), Data0)),
    {continue, {ready, Data}};

handle_nats_msg({info, Payload} = Msg, {connected, Data0}) ->
   ?LOG(debug, "NATS Info Msg: ~p", [Msg]),
    case handle_nats_info(Payload, Data0) of
        {ok, Data} ->
            {stop, {ready, Data}};
        {error, _} ->
            {stop, {disconnected, Data0}}
    end;

handle_nats_msg({msg, {Subject, NatsSid, ReplyTo, Payload}} = Msg, {ready, Data}) ->
    ?LOG(debug, "got msg: ~p", [Msg]),
    Sid = {'$sid', binary_to_integer(NatsSid)},
    case get_sid_session(Sid, Data) of
        [#sub_session{pid = NotifyPid, max_msgs = MaxMsgs}] ->
            Resp = {msg, Subject, ReplyTo, Payload},
            NotifyPid ! {Sid, Resp},

            case MaxMsgs of
                0 ->
                    %% do nothing, no subscription limit
                    ok;
                1 ->
                    ?LOG(debug, "NATS: Auto-removing subscription, limit reached for sid '~s'", [NatsSid]),
                    del_sid_session(Sid, Data),
                    del_pid_sub(NotifyPid, Sid, Data),
                    demonitor_sub_pid(NotifyPid, Data);
                _ ->
                    put_sid_session(Sid, NotifyPid, MaxMsgs - 1, Data)
            end;
        _ ->
            ok
    end,
    {continue, {ready, Data}};

handle_nats_msg(Msg, DecState) ->
    ?LOG("NATS Msg: ~p", [Msg]),
    {continue, DecState}.

handle_nats_info(Payload, Data0) ->
    try json:decode(Payload, ok, #{object_push => fun json_object_push/3}) of
        {JSON, ok, _} ->
            maybe
                ?LOG(debug, "NATS Info JSON: ~p", [JSON]),
                {ok, Data} = ssl_upgrade(JSON, Data0#{server_info := JSON}),
                ?LOG(debug, "NATS Client Info: ~p", [client_info(Data)]),
                send(client_info(Data), Data),
                {ok, Data}
            end
    catch
        E:C ->
            ?LOG(debug, "NATS Info Error: ~p:~p", [E, C]),
            {error, C}
    end.

ssl_upgrade(#{tls_required := true}, #{socket := Socket} = Data) ->
    case ssl:connect(Socket, []) of
        {ok, NewSocket} ->
            {ok, Data#{socket := NewSocket, tls := true}};
        {error, _Reason} = Error ->
            Error
    end;
ssl_upgrade(_, State) ->
    {ok, State}.

send(Bin, #{socket := Socket, tls := false}) ->
    gen_tcp:send(Socket, Bin);
send(Bin, #{socket := Socket, tls := true}) ->
    ssl:send(Socket, Bin).

enqueue_msg_no_check(Msg, #{batch := Batch, batch_size := BSz} = Data) ->
    Data#{batch := [Batch, Msg],
          batch_size := BSz + iolist_size(Msg)}.

enqueue_msg(Msg, Data) ->
    check_batch_queue(enqueue_msg_no_check(Msg, Data)).

check_batch_queue(#{batch_size := BatchSz, buffer_size := BufferSz, batch := Batch} = Data)
  when BufferSz /= 0, BatchSz >= BufferSz ->
    send(Batch, Data),
    stop_batch_timer(Data#{batch_size := 0, batch := []});
check_batch_queue(#{batch_size := BatchSz} = Data) when BatchSz /= 0 ->
    start_batch_timer(Data);
check_batch_queue(Data) ->
    Data.

start_batch_timer(#{batch_timer := undefined, send_timeout := SendTimeout} = Data) ->
    TRef = erlang:send_after(SendTimeout, self(), batch_timeout),
    Data#{batch_timer := TRef};
start_batch_timer(Data) ->
    Data.

stop_batch_timer(#{batch_timer := TRef} = Data) when is_reference(TRef) ->
    erlang:cancel_timer(TRef),
    Data#{batch_timer := undefined};
stop_batch_timer(Data) ->
    Data.

flush_batch(Data0) ->
    Data = stop_batch_timer(Data0),
    self() ! batch_timeout,
    Data.

client_info(#{server_info := ServerInfo} = Data) ->
    %% Include user and name iff the server requires it
    FieldsList = [verbose, pedantic, ssl_required, auth_token, name, lang, version],
    NewFieldsList =
        case maps:get(auth_required, ServerInfo, false) of
            true -> [user, pass | FieldsList];
            _ -> FieldsList
        end,
    Nats = maps:with(NewFieldsList, Data),
    nats_msg:connect(json:encode(Nats)).

log_connection_error(Error, #{host := Host, port := Port}) ->
    ?LOG(debug, "NATS connection to ~s:~w failed with ~p", [inet:ntoa(Host), Port, Error]),
    ok.

close_socket(#{socket := Socket, tls := TLS} = Data) ->
    case TLS of
        true  -> ssl:close(Socket);
        false -> gen_tcp:close(Socket)
    end,
    Data#{socket := undefined,
          tls := false,
          recv_select := undefined,
          send_select := undefined
         }.

make_sub_id(#{sid := Sid} = Data) ->
    {integer_to_binary(Sid), {'$sid', Sid}, Data#{sid := Sid + 1}}.

monitor_sub_pid(NotifyPid, #{tid := Tid}) ->
    PKey = #pid{pid = NotifyPid},
    case ets:member(Tid, PKey) of
        true  -> ok;
        false ->
            MRef = monitor(process, NotifyPid),
            true = ets:insert(Tid, {PKey, MRef})
    end.

del_pid_monitor(NotifyPid, #{tid := Tid}) ->
    case ets:take(Tid, #pid{pid = NotifyPid}) of
        [{_, MRef}] ->
            [MRef];
        _ ->
            []
    end.

demonitor_sub_pid(NotifyPid, Data) ->
    case count_pid_sub(NotifyPid, Data) of
        0 ->
            maybe
                [MRef] ?= del_pid_monitor(NotifyPid, Data),
                demonitor(MRef)
            end,
            ok;
        _ ->
            ok
    end.

put_pid_sub(Pid, Sid, #{tid := Tid}) ->
    true = ets:insert(Tid, {#sub{pid = Pid, sid = Sid}}).

del_pid_sub(Pid, Sid, #{tid := Tid}) ->
    true = ets:delete(Tid, #sub{pid = Pid, sid = Sid}).

get_pid_subs(Pid, #{tid := Tid}) ->
    Ms = [{{#sub{pid = Pid, sid = '$1'}}, [], ['$1']}],
    ets:select(Tid, Ms).

del_pid_subs(Pid, #{tid := Tid}) ->
    Ms = [{{#sub{pid = Pid, _ = '_'}}, [], [true]}],
    ets:select_delete(Tid, Ms).

count_pid_sub(Pid, #{tid := Tid}) ->
    Ms = [{{#sub{pid = Pid, _ = '_'}}, [], [true]}],
    ets:select_count(Tid,  Ms).

put_sid_session(Sid, Pid, MaxMsgs, #{tid := Tid}) ->
    true = ets:insert(Tid, {Sid, #sub_session{pid = Pid, max_msgs = MaxMsgs}}).

get_sid_session({'$sid', _} = Sid, #{tid := Tid}) ->
    Ms = [{{Sid, '$1'}, [], ['$1']}],
    ets:select(Tid, Ms).

del_sid_session({'$sid', _} = Sid, #{tid := Tid}) ->
    true = ets:delete(Tid, Sid).

%%%===================================================================
%%% Internal functions
%%%===================================================================

json_object_push(Key, Value, Acc) ->
    K = try binary_to_existing_atom(Key) catch _:_ -> Key end,
    [{K, Value} | Acc].

family({_,_,_,_}) -> inet;
family({_,_,_,_,_,_,_,_}) -> inet6.

socket_opts(Opts) ->
    SockOpts = maps:merge(?DEFAULT_SOCKET_OPTS, maps:get(socket, Opts, #{})),
    maps:fold(fun make_socket_opt/3, [{active, false}, binary, {packet, 0}], SockOpts).

make_socket_opt(netns, NetNs, Opts) ->
    [{netns, NetNs} | Opts];
make_socket_opt(netdev, NetDev, Opts) ->
    [{bind_to_device, NetDev} | Opts];
make_socket_opt(rcvbuf, Sz, Opts) ->
    [{recbuf, Sz} | Opts];
make_socket_opt(sndbuf, Sz, Opts) ->
    [{sndbuf, Sz} | Opts];
make_socket_opt(reuseaddr, V, Opts) ->
    [{reuseaddr, V} | Opts];
make_socket_opt(_, _, Opts) ->
    Opts.

init_data(Host, Port, Socket, Opts) ->
    Data = #{socket => Socket,
             tls => false,
             server_info => undefined,

             recv_buffer => <<>>,

             batch => [],
             batch_size => 0,
             batch_timer => undefined,

             sid => 1,
             tid => ets:new(?MODULE, [private, set]),

             host => Host,
             port => Port},
    maps:merge(Opts, Data).
