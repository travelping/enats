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

-module(nats).

-behaviour(gen_statem).

%% API
-export([connect/2,
         connect/3]).
-export([pub/2,
         pub/3,
         pub/4,
         sub/2,
         sub/3,
         unsub/2,
         unsub/3,
         disconnect/1,
         is_ready/1]).

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
          tls_required => false,
          auth_token => undefined,
          user => undefined,
          pass => undefined,
          name => <<"nats">>,
          lang => <<"Erlang">>,
          version => ?VERSION,
          headers => true,
          no_responders => true,
          buffer_size => 0,
          max_batch_size => ?DEFAULT_MAX_BATCH_SIZE,
          send_timeout => ?DEFAULT_SEND_TIMEOUT
         }).
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

-type opts() :: #{socket_opts    => socket_opts(),
                  verbose        => boolean(),
                  pedantic       => boolean(),
                  tls_required   => boolean(),
                  auth_token     => binary(),
                  user           => binary(),
                  pass           => binary(),
                  name           => binary(),
                  lang           => binary(),
                  version        => binary(),
                  headers        => boolean(),
                  no_responders  => boolean(),
                  buffer_size    => non_neg_integer(),
                  max_batch_size => non_neg_integer(),
                  send_timeout   => non_neg_integer()
                 }.

-type data() :: #{socket         := undefined | gen_tcp:socket() | ssl:socket(),
                  tls            := boolean,
                  server_info    := map(),
                  recv_buffer    := binary(),
                  batch          := iolist(),
                  batch_size     := non_neg_integer(),
                  batch_timer    := undefined | reference(),

                  tid            := ets:tid(),

                  %% Opts
                  parent         := pid(),
                  host           := nats_host(),
                  port           := inet:port_number(),
                  socket_opts    := socket_opts(),
                  verbose        := boolean(),
                  pedantic       := boolean(),
                  tls_required   := boolean(),
                  auth_token     := undefined | binary(),
                  user           := undefined | binary(),
                  pass           := undefined | binary(),
                  name           := binary(),
                  lang           := binary(),
                  version        := binary(),
                  headers        := boolean(),
                  no_responders  := boolean(),
                  buffer_size    := non_neg_integer(),
                  max_batch_size := non_neg_integer(),
                  send_timeout   := non_neg_integer()
                 }.

-record(pid, {pid}).
-record(sub, {pid, sid}).
-record(sub_session, {pid, max_msgs}).
-record(ready, {pending}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Host, Port, Opts) ->
    logger:set_primary_config(level, debug),
    gen_statem:start_link(?MODULE, [Host, Port, maps:merge(#{parent => self()}, Opts)], []).

connect(Host, Port) ->
    connect(Host, Port, #{}).

-spec connect(Host :: nats_host(),
              Port :: inet:port_number(),
              Opts :: opts()) ->
          {ok, Pid :: pid()} |
          ignore |
          {error, Error :: term()}.

connect(Host, Port, Opts) ->
    start_link(Host, Port, Opts).

pub(Server, Subject) ->
    pub(Server, Subject, <<>>, #{}).

pub(Server, Subject, Opts)
  when is_map(Opts) ->
    pub(Server, Subject, <<>>, Opts);
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

disconnect(Server) ->
    gen_statem:call(Server, disconnect).

is_ready(Server) ->
    try
        gen_statem:call(Server, is_ready)
    catch
        exit:{timeout, _} ->
            {error, timeout};
        exit:_ ->
            {error, not_found}
    end.

dump_subs(Server) ->
    gen_statem:call(Server, dump_subs).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================
-define(CONNECT_TAG, '$connect').

-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> [handle_event_function, state_enter].

-spec init(Args :: term()) ->
          gen_statem:init_result(data()).
init([Host, Port, Opts0]) ->
    process_flag(trap_exit, true),

    nats_msg:init(),

    Opts = maps:merge(?DEFAULT_OPTS, Opts0),
    ?LOG(debug, "SocketOpts: ~p", [socket_opts(Opts)]),

    Data = init_data(Host, Port, Opts),
    {ok, connecting, Data}.

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

handle_event({call, From}, is_ready, State, _Data) ->
    {keep_state_and_data, [{reply, From, is_record(State, ready)}]};

handle_event({call, From}, disconnect, State, Data)
  when State =:= connected; is_record(State, ready) ->
    {next_state, closed, close_socket(Data), [{reply, From, ok}]};
handle_event({call, From}, disconnect, _State, Data) ->
    {next_state, closed, Data, [{reply, From, ok}]};

handle_event(enter, _, connecting, _) ->
    self() ! ?CONNECT_TAG,
    keep_state_and_data;

handle_event(info, {?CONNECT_TAG, Pid, {ok, Socket}},
             connecting, #{host := Host, port := Port, socket := Pid} = Data) ->
    ?LOG(debug, "NATS client connected to ~s:~w", [fmt_host(Host), Port]),
    {next_state, connected, Data#{socket := Socket}};

handle_event(info, {{'DOWN', ?CONNECT_TAG}, _, process, Pid, Reason},
             connecting, #{host := Host, port := Port, socket := Pid} = Data) ->
    ?LOG(debug, "NATS client failed to open TCP socket for connecting to ~s:~w unexpectedly with ~p",
         [fmt_host(Host), Port, Reason]),
    notify_parent({error, Reason}, Data),
    {next_state, closed, Data#{socket := undefined}};

handle_event(info, {{'DOWN', ?CONNECT_TAG}, _, process, _, _}, _, _) ->
    keep_state_and_data;

handle_event(info, {?CONNECT_TAG, Pid, {error, _} = Error},
             connecting, #{host := Host, port := Port, socket := Pid} = Data) ->
    ?LOG(debug, "NATS client failed to open TCP socket for connecting to ~s:~w with ~p",
         [fmt_host(Host), Port, Error]),
    notify_parent(Error, Data),
    {next_state, closed, Data#{socket := undefined}};

handle_event(info, ?CONNECT_TAG, connecting, #{host := Host, port := Port} = Data) ->
    case get_host_addr(Host) of
        {ok, IP} ->
            Owner = self(),
            SocketOpts = socket_opts(Data),
            {Pid, _} =
                proc_lib:spawn_opt(
                  fun() ->
                          Result = gen_tcp:connect(IP, Port, SocketOpts, ?CONNECT_TIMEOUT),
                          case Result of
                              {ok, Socket} ->
                                  ok = gen_tcp:controlling_process(Socket, Owner);
                              _ ->
                                  ok
                          end,
                          Owner ! {?CONNECT_TAG, self(), Result}
                  end,
                  [{monitor, [{tag, {'DOWN', ?CONNECT_TAG}}]}]),
            {keep_state, Data#{socket := Pid}};
        {error, _} = Error ->
            ?LOG(debug, "NATS client failed to open TCP socket for connecting to ~s:~w with ~p",
                 [fmt_host(Host), Port, Error]),
            notify_parent(Error, Data),
            {next_state, closed, Data}
    end;

handle_event(enter, _, closed, Data) ->
    notify_parent(closed, Data),
    {stop, normal};

handle_event(enter, _, connected, #{socket := Socket}) ->
    ?LOG(debug, "NATS enter connected state, socket ~p", [Socket]),
    ok = inet:setopts(Socket, [{active,once}]),
    keep_state_and_data;
handle_event(enter, OldState, State, #{socket := Socket} = Data)
  when not is_record(OldState, ready), is_record(State, ready) ->
    ?LOG(debug, "NATS enter ready state"),
    ok = inet:setopts(Socket, [{active, true}]),
    notify_parent(ready, Data),
    keep_state_and_data;
handle_event(enter, _, State, _Data)
  when is_record(State, ready) ->
    keep_state_and_data;

handle_event(info, {tcp_error, Socket, Reason}, _, #{socket := Socket} = Data) ->
    log_connection_error(Reason, Data),
    {next_state, closed, close_socket(Data)};
handle_event(info, {tcp_closed, Socket}, _, #{socket := Socket} = Data) ->
    {next_state, closed, close_socket(Data)};

handle_event(info, {tcp, Socket, Bin}, State, #{socket := Socket} = Data)
  when State =:= connected; is_record(State, ready) ->
    ?LOG(debug, "got data: ~p", [Bin]),
    handle_message(Bin, State, Data);

handle_event(info, batch_timeout, State, #{batch := Batch} = Data)
  when is_record(State, ready) ->
    send(Batch, Data),
    {keep_state, Data#{batch_size := 0, batch := [], batch_timer := undefined}};

handle_event(info, {'DOWN', _MRef, process, Pid, normal}, _, Data) ->
    del_pid_monitor(Pid, Data),
    Sids = get_pid_subs(Pid, Data),
    lists:foreach(fun(X) -> del_sid_session(X, Data) end, Sids),
    true = length(Sids) =:= del_pid_subs(Pid, Data),
    keep_state_and_data;

handle_event({call, _From}, _, State, _Data)
  when State =:= connecting; State =:= connected  ->
    {keep_state_and_data, [postpone]};
handle_event({call, _From}, _, #ready{pending = Pending}, _Data)
  when Pending /= undefined ->
    {keep_state_and_data, [postpone]};
handle_event({call, From}, _, State, _Data)
  when not is_record(State, ready) ->
    {keep_state_and_data, [{reply, From, {error, not_ready}}]};

handle_event({call, From}, {pub, Subject, Payload, #{header := Header} = Opts}, State, Data) ->
    HdrToSend =
        if is_binary(Header) orelse is_list(Header) ->
                Header;
           true ->
                <<>>
        end,
    ReplyTo = maps:get(reply_to, Opts, undefined),
    Msg = nats_msg:hpub(Subject, ReplyTo, HdrToSend, Payload),
    send_msg_with_reply(From, ok, Msg, State, Data);

handle_event({call, From}, {pub, Subject, Payload, Opts}, State, Data) ->
    ReplyTo = maps:get(reply_to, Opts, undefined),
    Msg = nats_msg:pub(Subject, ReplyTo, Payload),
    send_msg_with_reply(From, ok, Msg, State, Data);

handle_event({call, From}, {sub, Subject, Opts, NotifyPid}, State, Data0) ->
    {NatsSid, Sid, Data} = make_sub_id(Data0),
    monitor_sub_pid(NotifyPid, Data),
    put_pid_sub(NotifyPid, Sid, Data),
    put_sid_session(Sid, NotifyPid, 0, Data),

    QueueGrp = maps:get(queue_group, Opts, undefined),
    Msg = nats_msg:sub(Subject, QueueGrp, NatsSid),
    send_msg_with_reply(From, {ok, Sid}, Msg, State, Data);

handle_event({call, From}, {unsub, {'$sid', NatsSid} = Sid, Opts}, State, Data) ->
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
            send_msg_with_reply(From, ok, Msg, State, Data);
        _ ->
            {keep_state_and_data, [{reply, From, {error, {invalid_session_ref, Sid}}}]}
    end;
handle_event({call, From}, {unsub, Sid, _Opts}, _State, _Data) ->
    {keep_state_and_data, [{reply, From, {error, {invalid_session_ref, Sid}}}]};

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

send_msg_with_reply(From, Reply, Msg,
                    #ready{pending = undefined} = State, #{verbose := true} = Data) ->
    Action = {reply, From, Reply},
    {next_state, State#ready{pending = Action}, enqueue_msg(Msg, Data)};
send_msg_with_reply(From, Reply, Msg, #ready{pending = undefined}, Data) ->
    {keep_state, enqueue_msg(Msg, Data), [{reply, From, Reply}]}.

socket_active(connected, connected, #{socket := Socket}) ->
    inet:setopts(Socket, [{active,once}]);
socket_active(_, _, _) ->
    ok.

notify_parent(Msg, #{parent := Parent}) when is_pid(Parent) ->
    Parent ! {self(), Msg};
notify_parent(_, _) ->
    ok.

handle_message(Bin, State0, #{recv_buffer := Acc0} = Data0) ->
    {{State, Data}, Acc} =
        nats_msg:decode(<<Acc0/binary, Bin/binary>>, {fun handle_nats_msg/2, {State0, Data0}}),
    socket_active(State0, State, Data),
    {next_state, State, Data#{recv_buffer := Acc}}.

handle_nats_msg(ok, {#ready{pending = ok} = State, Data}) ->
    {stop, {State#ready{pending = undefined}, Data}};
handle_nats_msg(ok, {#ready{pending = {reply, From, Reply}} = State, Data}) ->
    gen_statem:reply(From, Reply),
    {stop, {State#ready{pending = undefined}, Data}};

handle_nats_msg(stop, DecState) ->
    {stop, DecState};

handle_nats_msg(ping, {connected, Data} = DecState) ->
    send(nats_msg:pong(), Data),
    {continue, DecState};

handle_nats_msg(ping, {State, Data0})
  when is_record(State, ready) ->
    Data = flush_batch(enqueue_msg_no_check(nats_msg:pong(), Data0)),
    {continue, {State, Data}};

handle_nats_msg({info, Payload} = Msg, {connected, Data0}) ->
   ?LOG(debug, "NATS Info Msg: ~p", [Msg]),
    case handle_nats_info(Payload, Data0) of
        {ok, State, Data} ->
            {stop, {State, Data}};
        {error, _} ->
            {stop, {closed, Data0}}
    end;

handle_nats_msg({msg, {Subject, NatsSid, ReplyTo, Payload}} = Msg, {State, Data})
  when is_record(State, ready) ->
    ?LOG(debug, "got msg: ~p", [Msg]),
    Opts = reply_opt(ReplyTo, #{}),
    handle_nats_msg_msg(Subject, NatsSid, Payload, Opts, Data),
    {continue, {State, Data}};

handle_nats_msg({hmsg, {Subject, NatsSid, ReplyTo, Header, Payload}} = Msg, {State, Data})
  when is_record(State, ready) ->
    ?LOG(debug, "got msg: ~p", [Msg]),
    Opts = reply_opt(ReplyTo, #{header => Header}),
    handle_nats_msg_msg(Subject, NatsSid, Payload, Opts, Data),
    {continue, {State, Data}};
handle_nats_msg(Msg, DecState) ->
    ?LOG("NATS Msg: ~p", [Msg]),
    {continue, DecState}.

reply_opt(ReplyTo, Opts) when is_binary(ReplyTo) ->
    Opts#{reply_to => ReplyTo};
reply_opt(_, Opts) ->
    Opts.

handle_nats_msg_msg(Subject, NatsSid, Payload, Opts, Data) ->
    Sid = {'$sid', binary_to_integer(NatsSid)},
    case get_sid_session(Sid, Data) of
        [#sub_session{pid = NotifyPid, max_msgs = MaxMsgs}] ->
            Resp = {msg, Subject, Payload, Opts},
            NotifyPid ! {self(), Sid, Resp},

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
    end.

handle_nats_info(Payload, Data0) ->
    try json:decode(Payload, ok, #{object_push => fun json_object_push/3}) of
        {JSON, ok, _} ->
            maybe
                ?LOG(debug, "NATS Info JSON: ~p", [JSON]),
                {ok, Data} = ssl_upgrade(JSON, Data0#{server_info := JSON}),
                ?LOG(debug, "NATS Client Info: ~p", [client_info(Data)]),
                send(client_info(Data), Data),
                case Data of
                    #{verbose := true} ->
                        {ok, #ready{pending = ok}, Data};
                    _ ->
                        {ok, #ready{}, Data}
                end
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
    FieldsList = [verbose, pedantic, tls_required, auth_token, name, lang,
                  version, headers, no_responders],
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

fmt_host(IP)
  when is_tuple(IP) andalso (tuple_size(IP) =:= 4 orelse tuple_size(IP) =:= 8) ->
    inet:ntoa(IP);
fmt_host(Host) when is_list(Host); is_binary(Host) ->
    Host.

get_host_addr({_, _, _, _} = IP) ->
    {ok, IP};
get_host_addr({_, _, _, _, _, _, _, _} = IP) ->
    {ok, IP};
get_host_addr(Bin) when is_binary(Bin) ->
    get_host_addr(binary_to_list(Bin));
get_host_addr(Host) when is_list(Host) ->
    maybe
        {error, _} ?= inet:getaddrs(Host, inet6),
        {error, _} ?= inet:getaddrs(Host, inet)
    else
        {ok, IPs} ->
            {ok, lists:nth(rand:uniform(length(IPs)), IPs)}
    end.

json_object_push(Key, Value, Acc) ->
    K = try binary_to_existing_atom(Key) catch _:_ -> Key end,
    [{K, Value} | Acc].

socket_opts(Opts) ->
    SockOpts = maps:merge(?DEFAULT_SOCKET_OPTS, maps:get(socket_opts, Opts, #{})),
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

init_data(Host, Port, Opts) ->
    Data = #{socket => undefined,
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
