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
         is_ready/1,
         request/4,
         serve/2
        ]).
-export([service/4, endpoint/2]).

%% debug functions
-export([dump_subs/1]).

-ignore_xref([connect/2, connect/3,
              pub/2, pub/3, pub/4,
              sub/2, sub/3,
              unsub/2, unsub/3,
              disconnect/1,
              is_ready/1,
              request/4,
              serve/2, service/4, endpoint/2,
              dump_subs/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4, format_status/1]).
-export([handle_event/4]).

-define(MSG, ?MODULE).
-define(VERSION, <<"0.4.1">>).
-define(DEFAULT_SEND_TIMEOUT, 1).
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
-include_lib("stdlib/include/ms_transform.hrl").

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

-type endpoint() :: #{name        := binary(),
                      group_name  => binary(),
                      queue_group => binary(),
                      metadata    => map()
                     }.
-type service() :: #{name        := binary(),
                     version     := binary(),
                     description => binary(),
                     metadata    => map()
                    }.

-type data() :: #{socket         := undefined | gen_tcp:socket() | ssl:socket(),
                  tls            := boolean(),
                  server_info    := undefined | map(),
                  recv_buffer    := binary(),
                  batch          := iolist(),
                  batch_size     := non_neg_integer(),
                  batch_timer    := undefined | reference(),

                  tid            := ets:tid(),
                  inbox          := undefined | binary(),
                  srv_init       := boolean(),

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
                  send_timeout   := non_neg_integer(),
                  _              => _
                 }.

-record(pid, {pid}).
-record(sub, {pid, sid}).
-record(sub_session, {pid, max_msgs}).
-record(ready, {pending}).
-record(inbox, {}).
-record(req, {req_id}).
-record(svc_id, {name, id}).
-record(service, {svc, op, endp, queue_group, subject}).
-record(svc, {module, state, id, service, endpoints, started}).
-record(endpoint, {function, id, endpoint}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Host, Port, Opts) ->
    _ = logger:set_primary_config(level, debug),
    gen_statem:start_link(?MODULE, {Host, Port, maps:merge(#{parent => self()}, Opts)}, []).

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

%% request(Server, Subject, Payload, Opts) ->
%%     gen_statem:call(Server, {request, Subject, Payload, Opts}).

request(Server, Subject, Payload, Opts) ->
    maybe
        {ok, Inbox} ?= gen_statem:call(Server, get_inbox_topic),
        case gen_statem:call(Server, {request, Subject, Inbox, Payload, Opts}) of
            {ok, {_, #{header := <<"NATS/1.0 503\r\n", _/binary>>}}} ->
                    {error, no_responders};
            Reply ->
                Reply
        end
    end.

serve(Server, Service) ->
    gen_statem:call(Server, {serve, Service}).

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

-spec service(service(),  nonempty_list(#endpoint{}), module(), any()) -> #svc{}.
service(SvcDesc, EndpDesc, Module, State) ->
    #svc{module = Module, state = State, service = SvcDesc, endpoints = EndpDesc}.

-spec endpoint(endpoint(), atom()) -> #endpoint{}.
endpoint(EndpDesc, Function) ->
    #endpoint{function = Function,
              endpoint = maps:merge(#{queue_group => ~"q", metadata => null}, EndpDesc)}.

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

-define(CONNECT_TAG, '$connect').

-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> [handle_event_function, state_enter].

-spec init({Host :: nats_host(),
            Port :: inet:port_number(),
            Opts :: opts()}) ->
          gen_statem:init_result(connecting, data()).
init({Host, Port, Opts0}) ->
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
          gen_statem:state_enter_result(term(), data());
                  (gen_statem:event_type(),
                   Msg :: term(),
                   State :: term(),
                   Data :: term()) ->
          gen_statem:event_handler_result(term(), data()).
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
    _ = del_pid_monitor(Pid, Data),
    Sids = get_pid_subs(Pid, Data),
    lists:foreach(fun(X) -> del_sid(X, Data) end, Sids),
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

handle_event({call, From}, {pub, Subject, Payload, Opts}, State, Data) ->
    Msg = mk_pub_msg(Subject, Payload, Opts),
    send_msg_with_reply(From, ok, Msg, State, Data);

handle_event({call, From}, {sub, Subject, Opts, NotifyPid}, State, Data) ->
    {NatsSid, Sid} = make_sub_id(Data),
    monitor_sub_pid(NotifyPid, Data),
    put_pid_sub(NotifyPid, Sid, Data),
    put_sid_session(Sid, NotifyPid, 0, Data),

    QueueGrp = maps:get(queue_group, Opts, undefined),
    Msg = nats_msg:sub(Subject, QueueGrp, NatsSid),
    send_msg_with_reply(From, {ok, Sid}, Msg, State, Data);

handle_event({call, From}, {unsub, {'$sid', NatsSid} = Sid, Opts}, State, Data) ->
    case get_sid(Sid, Data) of
        [#sub_session{pid = NotifyPid}] ->
            case Opts of
                #{max_messages := MaxMsgsOpt} when is_integer(MaxMsgsOpt) ->
                    put_sid_session(Sid, NotifyPid, MaxMsgsOpt, Data);
                _ ->
                    del_sid(Sid, Data),
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

handle_event({call, From}, get_inbox_topic, _State, #{inbox := Inbox})
  when is_binary(Inbox) ->
    {keep_state_and_data, [{reply, From, {ok, Inbox}}]};
handle_event({call, From}, get_inbox_topic, State, Data) ->
    Inbox = <<"_INBOX.", (rnd_topic_id())/binary, $.>>,
    {NatsSid, Sid} = make_sub_id(Data),
    put_sid_inbox(Sid, Data),

    Subject = <<Inbox/binary, $*>>,
    Msg = nats_msg:sub(Subject, NatsSid),
    send_msg_with_reply(From, {ok, Inbox}, Msg, State, Data#{inbox => Inbox});

handle_event({call, From}, {request, Subject, Inbox, Payload, Opts}, State, Data) ->
    ReqId = rnd_request_id(),
    ReqInbox = <<Inbox/binary, ReqId/binary>>,

    put_inbox_reqid(ReqId, From, Data),

    Msg = mk_pub_msg(Subject, Payload, Opts#{reply_to => ReqInbox}),
    send_msg(Msg, State, Data);

handle_event({call, From},
             {serve, #svc{service = #{name := SvcName}, endpoints = EndPs} = Svc0},
             State, Data) ->
    Id = rnd_topic_id(),
    Svc = Svc0#svc{started = erlang:system_time(second)},

    Subs0 = lists:map(
              fun (#endpoint{endpoint = #{name := OpName, queue_group := QueueGrp}} = EndP) ->
                      service_sub_msg(SvcName, OpName, EndP#endpoint{id = Id},
                                      <<SvcName/binary, $., OpName/binary>>, QueueGrp, Data)
              end, EndPs),
    SvcIdName = <<SvcName/binary, $., Id/binary>>,
    Subs1 =
        [service_sub_msg(SvcName, '$stats', Id, <<"$SRV.STATS.", SvcIdName/binary>>, Data),
         service_sub_msg(SvcName, '$info', Id, <<"$SRV.INFO.", SvcIdName/binary>>, Data),
         service_sub_msg(SvcName, '$ping', Id, <<"$SRV.PING.", SvcIdName/binary>>, Data)
        | Subs0],

    Subs2 =
        case get_svcs(SvcName, Data) of
            [] ->
                [service_sub_msg(SvcName, '$stats', '$service',
                                 <<"$SRV.STATS.", SvcName/binary>>, Data),
                 service_sub_msg(SvcName, '$info', '$service',
                                 <<"$SRV.INFO.", SvcName/binary>>, Data),
                 service_sub_msg(SvcName, '$ping', '$service',
                                 <<"$SRV.PING.", SvcName/binary>>, Data)
                | Subs1];
            _ ->
                Subs1
        end,
    Subs =
        case Data of
            #{srv_init := false} ->
                [service_sub_msg('$srv', '$stats', '$all', ~"$SRV.STATS", Data),
                 service_sub_msg('$srv', '$info', '$all', ~"$SRV.INFO", Data),
                 service_sub_msg('$srv', '$ping', '$all', ~"$SRV.PING", Data)
                | Subs2];
            _ ->
                Subs2
        end,
    put_svc(SvcName, Id, Svc#svc{id = Id}, Data),

    Action = {reply, From, ok},
    next_state_enqueue_batch(Subs, Action, State, Data);

handle_event({call, From}, dump_subs, _State, #{tid := Tid}) ->
    {keep_state_and_data, [{reply, From, ets:tab2list(Tid)}]};

handle_event(Event, EventContent, State, Data) ->
    ?LOG(debug, "NATS:~nEvent: ~p~nContent: ~p~nState: ~p~nData: ~p",
         [Event, EventContent, State, Data]),
    keep_state_and_data.

format_status(Status) ->
  maps:map(
    fun(data, Data) ->
            Data#{pass := redacted, auth_token := redacted};
       (_, Value) ->
            Value
    end, Status).

terminate(_Reason, _State, _Data) ->
    void.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%%===================================================================
%%% State Helper functions
%%%===================================================================

service_sub_msg(SvcName, Op, Id, Subject, Data) ->
    service_sub_msg(SvcName, Op, Id, Subject, undefined, Data).

service_sub_msg(SvcName, Op, Id, Subject, QueueGrp, Data) ->
    {NatsSid, Sid} = make_sub_id(Data),
    Svc = #service{
             svc = SvcName,
             op = Op,
             endp = Id,
             queue_group = QueueGrp,
             subject = Subject},
    put_sid_service(Sid, Svc, Data),
    nats_msg:sub(Subject, QueueGrp, NatsSid).

mk_pub_msg(Subject, Payload, #{header := Header} = Opts) ->
    HdrToSend =
        if is_binary(Header) orelse is_list(Header) ->
                Header;
           true ->
                <<>>
        end,
    ReplyTo = maps:get(reply_to, Opts, undefined),
    nats_msg:hpub(Subject, ReplyTo, HdrToSend, Payload);

mk_pub_msg(Subject, Payload, Opts) ->
    ReplyTo = maps:get(reply_to, Opts, undefined),
    nats_msg:pub(Subject, ReplyTo, Payload).

send_msg(Msg, #ready{pending = undefined} = State, #{verbose := true} = Data) ->
    {next_state, State#ready{pending = ok}, enqueue_msg(Msg, Data)};
send_msg(Msg, #ready{pending = undefined}, Data) ->
    {keep_state, enqueue_msg(Msg, Data)}.

send_msg_with_reply(From, Reply, Msg,
                    #ready{pending = undefined} = State, #{verbose := true} = Data) ->
    Action = {reply, From, Reply},
    {next_state, State#ready{pending = Action}, enqueue_msg(Msg, Data)};
send_msg_with_reply(From, Reply, Msg, #ready{pending = undefined}, Data) ->
    {keep_state, enqueue_msg(Msg, Data), [{reply, From, Reply}]}.

socket_active(connected, connected, #{socket := Socket}) ->
    _ = inet:setopts(Socket, [{active,once}]),
    ok;
socket_active(_, _, _) ->
    ok.

notify_parent(Msg, #{parent := Parent}) when is_pid(Parent) ->
    Parent ! {self(), Msg},
    ok;
notify_parent(_, _) ->
    ok.

handle_message(Bin, State0, #{recv_buffer := Acc0} = Data0) ->
    {{State, Data1}, Acc} =
        nats_msg:decode(<<Acc0/binary, Bin/binary>>, {fun handle_nats_msg/2, {State0, Data0}}),
    Data = Data1#{recv_buffer := Acc},
    socket_active(State0, State, Data),
    {next_state, State, Data}.

handle_nats_msg(stop, DecState) ->
    {stop, DecState};

handle_nats_msg(ok, {#ready{pending = Pending} = State, #{send_q := [Msg|More]} = Data})
  when Pending /= undefined ->
    {continue, {State, enqueue_msg(Msg, Data#{send_q := More})}};

handle_nats_msg(ok, {#ready{pending = ok} = State, Data}) ->
    {continue, {State#ready{pending = undefined}, Data}};
handle_nats_msg(ok, {#ready{pending = {reply, From, Reply}} = State, Data}) ->
    gen_statem:reply(From, Reply),
    {continue, {State#ready{pending = undefined}, Data}};

handle_nats_msg({error, _} = Error, {_, Data} = DecState) ->
    notify_parent(Error, Data),
    {continue, DecState};

handle_nats_msg(ping, {connected, Data} = DecState) ->
    send(nats_msg:pong(), Data),
    {continue, DecState};

handle_nats_msg(ping, {State, Data0})
  when is_record(State, ready) ->
    Data = flush_batch(enqueue_msg_no_check(nats_msg:pong(), Data0)),
    {continue, {State, Data}};

handle_nats_msg({info, Payload} = Msg, {connected, Data}) ->
   ?LOG(debug, "NATS Info Msg: ~p", [Msg]),
    handle_nats_info(Payload, Data);

handle_nats_msg({msg, {Subject, NatsSid, ReplyTo, Payload}} = Msg, {State, _} = DecState)
  when is_record(State, ready) ->
    ?LOG(debug, "got msg: ~p", [Msg]),
    Opts = reply_opt(ReplyTo, #{}),
    handle_nats_msg_msg(Subject, NatsSid, Payload, Opts, DecState);

handle_nats_msg({hmsg, {Subject, NatsSid, ReplyTo, Header, Payload}} = Msg,
                {State, _} = DecState)
  when is_record(State, ready) ->
    ?LOG(debug, "got msg: ~p", [Msg]),
    Opts = reply_opt(ReplyTo, #{header => Header}),
    handle_nats_msg_msg(Subject, NatsSid, Payload, Opts, DecState);
handle_nats_msg(Msg, DecState) ->
    ?LOG(debug, "NATS Msg: ~p", [Msg]),
    {continue, DecState}.

reply_opt(ReplyTo, Opts) when is_binary(ReplyTo) ->
    Opts#{reply_to => ReplyTo};
reply_opt(_, Opts) ->
    Opts.

handle_nats_msg_msg(Subject, NatsSid, Payload, Opts, {_, Data} = DecState) ->
    Sid = {'$sid', binary_to_integer(NatsSid)},
    case get_sid(Sid, Data) of
        [#sub_session{pid = NotifyPid, max_msgs = MaxMsgs}] ->
            Resp = {msg, Subject, Payload, Opts},
            NotifyPid ! {self(), Sid, Resp},

            case MaxMsgs of
                0 ->
                    %% do nothing, no subscription limit
                    ok;
                1 ->
                    ?LOG(debug, "NATS: Auto-removing subscription, limit reached for sid '~s'", [NatsSid]),
                    del_sid(Sid, Data),
                    del_pid_sub(NotifyPid, Sid, Data),
                    demonitor_sub_pid(NotifyPid, Data);
                _ ->
                    put_sid_session(Sid, NotifyPid, MaxMsgs - 1, Data)
            end,
            {continue, DecState};
        [#inbox{}] ->
            ?LOG(debug, "NATS msg for global INBOX"),
            handle_nats_inbox_msg(Subject, Payload, Opts, DecState);
        [#service{svc = SvcName} = Service] ->
            ?LOG(debug, "## SERVICE: ~p", [Service]),
            ?LOG(debug, "NATS msg for service '~p'", [SvcName]),
            handle_nats_service_msg(Service, Subject, Payload, Opts, DecState);
        _Other ->
            ?LOG(debug, "NATS msg for unexpected sid ~w: ~p", [NatsSid, _Other]),
            {continue, DecState}
    end.

handle_nats_inbox_msg(Subject, Payload, Opts, {_, #{inbox := Inbox} = Data} = DecState) ->
    case Subject of
        <<Inbox:(byte_size(Inbox))/bytes, ReqId/binary>> ->
            ?LOG(debug, "NATS inbox msg with request id: ~0p", [ReqId]),
            case take_inbox_reqid(ReqId, Data) of
                [From] ->
                    gen_statem:reply(From, {ok, {Payload, Opts}});
                [] ->
                    ok
            end;
        _ ->
            ?LOG(debug, "NATS inbox msg with invalid structure: ~0p, Inbox: ~0p",
                 [Subject, Inbox]),
            ok
    end,
    {continue, DecState}.

service_info_msg(ReplyTo, #svc{id = Id, service = Service, endpoints = EndPs}) ->
    {ok, VSN} = application:get_key(nats, vsn),
    Endpoints =
        lists:map(
          fun(#endpoint{endpoint = #{name := Name} = EndP}) ->
                  maps:with(
                    [name, subject, queue_group, metadata],
                    EndP#{subject => <<Id/binary, $., Name/binary>>})
          end, EndPs),
    Resp0 =
        Service#{id => Id,
                 type => ~"io.nats.micro.v1.info_response",
                 metadata =>
                     maps:merge(
                       #{'_nats.client.created.library' => ~"natserl",
                         '_nats.client.created.version' =>
                             iolist_to_binary(VSN)},
                       maps:get(metadata, Service, #{})),
                 endpoints => Endpoints},
    Response = maps:with([name, id, version, metadata,
                          type, description, endpoints], Resp0),
    ?LOG(debug, "SvcRespPayload: ~p", [Response]),
    nats_msg:pub(ReplyTo, undefined, json:encode(Response)).

service_stats_msg(ReplyTo, #svc{id = Id, service = Service, endpoints = EndPs,
                                started = Started}) ->
    {ok, VSN} = application:get_key(nats, vsn),
    Endpoints =
        lists:map(
          fun(#endpoint{endpoint = #{name := Name} = EndP}) ->
                  EndpInfo =
                      maps:with(
                        [name, subject, queue_group],
                        EndP#{subject => <<Id/binary, $., Name/binary>>}),
                  EndpStats =
                      #{
                        num_requests => 0,
                        num_errors => 0,
                        last_error => ~"",
                        processing_time => 0,
                        average_processing_time => 0,
                        data => #{total_payload => 0}},
                  maps:merge(EndpInfo, EndpStats)
          end, EndPs),
    Resp0 =
        Service#{id => Id,
                 type => ~"io.nats.micro.v1.stats_response",
                 started => iolist_to_binary(
                              calendar:system_time_to_rfc3339(Started, [{offset, "Z"}])),
                 metadata =>
                     maps:merge(
                       #{'_nats.client.created.library' => ~"natserl",
                         '_nats.client.created.version' =>
                             iolist_to_binary(VSN)},
                       maps:get(metadata, Service, #{})),
                 endpoints => Endpoints},
    Response = maps:with([name, id, version, metadata,
                          type, description, started, endpoints], Resp0),
    ?LOG(debug, "SvcRespPayload: ~p", [Response]),
    nats_msg:pub(ReplyTo, undefined, json:encode(Response)).

service_ping_msg(ReplyTo, #svc{id = Id, service = Service}) ->
    {ok, VSN} = application:get_key(nats, vsn),
    Resp0 =
        Service#{id => Id,
                 type => ~"io.nats.micro.v1.ping_response",
                 metadata =>
                     maps:merge(
                       #{'_nats.client.created.library' => ~"natserl",
                         '_nats.client.created.version' =>
                             iolist_to_binary(VSN)},
                       maps:get(metadata, Service, #{}))},
    Response = maps:with([name, id, version, metadata,
                          type, description], Resp0),
    ?LOG(debug, "SvcRespPayload: ~p", [Response]),
    nats_msg:pub(ReplyTo, undefined, json:encode(Response)).

handle_nats_service_msg(#service{svc = '$srv', op = '$info', endp = '$all'},
                        _Subject, _Payload, #{reply_to := ReplyTo}, {State, Data}) ->
    ?LOG(debug, "NATS service message, browse services"),
    Batch = lists:map(fun(X) -> service_info_msg(ReplyTo, X) end, get_svcs(Data)),
    continue_enqueue_batch(Batch, State, Data);

handle_nats_service_msg(#service{svc = SvcName, op = '$info', endp = '$service'},
                        _Subject, _Payload, #{reply_to := ReplyTo}, {State, Data}) ->
    ?LOG(debug, "NATS service message, get service ~p", [SvcName]),
    Batch = lists:map(fun(X) -> service_info_msg(ReplyTo, X) end, get_svcs(SvcName, Data)),
    continue_enqueue_batch(Batch, State, Data);

handle_nats_service_msg(#service{svc = SvcName, op = '$info', endp = Id},
                        _Subject, _Payload, #{reply_to := ReplyTo}, {State, Data}) ->
    ?LOG(debug, "NATS service message, get service instance ~p / ~p", [SvcName, Id]),
    Batch = lists:map(fun(X) -> service_info_msg(ReplyTo, X) end, get_svc(SvcName, Id, Data)),
    continue_enqueue_batch(Batch, State, Data);

handle_nats_service_msg(#service{svc = '$srv', op = '$stats', endp = '$all'},
                        _Subject, _Payload, #{reply_to := ReplyTo}, {State, Data}) ->
    ?LOG(debug, "NATS service message, browse stats"),
    Batch = lists:map(fun(X) -> service_stats_msg(ReplyTo, X) end, get_svcs(Data)),
    continue_enqueue_batch(Batch, State, Data);

handle_nats_service_msg(#service{svc = SvcName, op = '$stats', endp = '$service'},
                        _Subject, _Payload, #{reply_to := ReplyTo}, {State, Data}) ->
    ?LOG(debug, "NATS service message, get stats ~p", [SvcName]),
    Batch = lists:map(fun(X) -> service_stats_msg(ReplyTo, X) end, get_svcs(SvcName, Data)),
    continue_enqueue_batch(Batch, State, Data);

handle_nats_service_msg(#service{svc = SvcName, op = '$stats', endp = Id},
                        _Subject, _Payload, #{reply_to := ReplyTo}, {State, Data}) ->
    ?LOG(debug, "NATS service message, get stats instance ~p / ~p", [SvcName, Id]),
    Batch = lists:map(fun(X) -> service_stats_msg(ReplyTo, X) end, get_svc(SvcName, Id, Data)),
    continue_enqueue_batch(Batch, State, Data);

handle_nats_service_msg(#service{svc = '$srv', op = '$ping', endp = '$all'},
                        _Subject, _Payload, #{reply_to := ReplyTo}, {State, Data}) ->
    ?LOG(debug, "NATS service message, wildcard ping"),
    Batch = lists:map(fun(X) -> service_ping_msg(ReplyTo, X) end, get_svcs(Data)),
    continue_enqueue_batch(Batch, State, Data);

handle_nats_service_msg(#service{svc = SvcName, op = '$ping', endp = '$service'},
                        _Subject, _Payload, #{reply_to := ReplyTo}, {State, Data}) ->
    ?LOG(debug, "NATS service message, get ping ~p", [SvcName]),
    Batch = lists:map(fun(X) -> service_ping_msg(ReplyTo, X) end, get_svcs(SvcName, Data)),
    continue_enqueue_batch(Batch, State, Data);

handle_nats_service_msg(#service{svc = SvcName, op = '$ping', endp = Id},
                        _Subject, _Payload, #{reply_to := ReplyTo}, {State, Data}) ->
    ?LOG(debug, "NATS service message, get ping instance ~p / ~p", [SvcName, Id]),
    Batch = lists:map(fun(X) -> service_ping_msg(ReplyTo, X) end, get_svc(SvcName, Id, Data)),
    continue_enqueue_batch(Batch, State, Data);

handle_nats_service_msg(#service{svc = SvcName, op = Op,
                                 endp = #endpoint{function = F, id = Id}},
                        Subject, Payload, Opts, {State, Data} = DecState) ->
    ?LOG(debug, "~s:~s service request, ~p, subject: ~p, payload: ~p, opts: ~p",
         [SvcName, Op, Id, Subject, Payload, Opts]),

    case get_svc(SvcName, Id, Data) of
        [#svc{module = M, state = CbState} = Svc] ->
            ?LOG(debug, "Svc: ~p", [Svc]),
            try M:F(SvcName, Op, Payload, Opts, CbState) of
                {reply, Reply, CbStateNew} ->
                    put_svc(SvcName, Id, Svc#svc{state = CbStateNew}, Data),
                    case Opts of
                        #{reply_to := ReplyTo} ->
                            ReplyMsg = nats_msg:pub(ReplyTo, undefined, Reply),
                            continue_enqueue_batch([ReplyMsg], State, Data);
                        _ ->
                            {continue, DecState}
                    end;
                {reply, Header, Reply, CbStateNew} ->
                    put_svc(SvcName, Id, Svc#svc{state = CbStateNew}, Data),
                    case Opts of
                        #{reply_to := ReplyTo} ->
                            ReplyMsg = nats_msg:hpub(ReplyTo, undefined, Header, Reply),
                            continue_enqueue_batch([ReplyMsg], State, Data);
                        _ ->
                            {continue, DecState}
                    end;
                {batch, Batch, CbStateNew} ->
                    put_svc(SvcName, Id, Svc#svc{state = CbStateNew}, Data),
                    continue_enqueue_batch([Batch], State, Data);
                {noreply, CbStateNew} ->
                    put_svc(SvcName, Id, Svc#svc{state = CbStateNew}, Data),
                    {continue, {State, Data}};
                Other ->
                    ?LOG(debug, "Unexpected return from Svc: ~p", [Other]),
                    {continue, DecState}
            catch
                C:E:St ->
                    ?LOG(debug, "service handler ~s:~s crashed with ~p:~p~nStacktrace ~p",
                         [M, F, C, E, St]),
                    {continue, DecState}
            end;
        _Other ->
            ?LOG(debug, "Unexpected service request: ~p", [_Other]),
            {continue, DecState}
    end;

handle_nats_service_msg(_Service, Subject, Payload, Opts, DecState) ->
    ?LOG(debug, "unexpected service request, subject: ~p, payload: ~p, opts: ~p",
         [Subject, Payload, Opts]),
    {continue, DecState}.

handle_nats_info(Payload, Data0) ->
    try json:decode(Payload, ok, #{object_push => fun json_object_push/3}) of
        {JSON, ok, _} ->
            maybe
                ?LOG(debug, "NATS Info JSON: ~p", [JSON]),
                {ok, Data} ?= ssl_upgrade(JSON, Data0#{server_info := JSON}),
                ?LOG(debug, "NATS Client Info: ~p", [client_info(Data)]),
                Msg = client_info(Data),
                continue_enqueue_batch([Msg], #ready{}, Data)
            end
    catch
        C:E ->
            ?LOG(debug, "NATS Info Error: ~p:~p", [C, E]),
            notify_parent({error, {C, E}}, Data0),
            {stop, {closed, Data0}}
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

continue_enqueue_batch([Msg|More],
                       #ready{pending = undefined} = State, #{verbose := true} = Data) ->
    {continue, {State#ready{pending = ok}, enqueue_msg(Msg, Data#{send_q := More})}};
continue_enqueue_batch(Batch, State, Data) ->
    {continue, {State, enqueue_msg(Batch, Data)}}.

next_state_enqueue_batch([Msg|More], Action,
                         #ready{pending = undefined} = State, #{verbose := true} = Data) ->
    {next_state, State#ready{pending = Action}, enqueue_msg(Msg, Data#{send_q := More})};
next_state_enqueue_batch(Batch, Action, State, Data) ->
    {next_state, State, enqueue_msg(Batch, Data), [Action]}.

enqueue_msg_no_check(Msg, #{verbose := true, batch := Batch} = Data) ->
    send([Batch, Msg], Data),
    stop_batch_timer(Data#{batch := <<>>});
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
    _ = erlang:cancel_timer(TRef),
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
          server_info := undefined
         }.

make_sub_id(#{tid := Tid}) ->
    Sid = ets:update_counter(Tid, '$sid', 1, {'$sid', 0}),
    {integer_to_binary(Sid), {'$sid', Sid}}.

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

put_inbox_reqid(ReqId, From, #{tid := Tid}) ->
    true = ets:insert(Tid, {#req{req_id = ReqId}, From}).

take_inbox_reqid(ReqId, #{tid := Tid}) ->
    case ets:take(Tid, #req{req_id = ReqId}) of
        [{_, From}] -> [From];
        [] -> []
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

put_sid_inbox(Sid, #{tid := Tid}) ->
    true = ets:insert(Tid, {Sid, #inbox{}}).

put_sid_service(Sid, #service{} = Svc, #{tid := Tid}) ->
    true = ets:insert(Tid, {Sid, Svc}).

get_sid({'$sid', _} = Sid, #{tid := Tid}) ->
    Ms = [{{Sid, '$1'}, [], ['$1']}],
    ets:select(Tid, Ms).

del_sid({'$sid', _} = Sid, #{tid := Tid}) ->
    true = ets:delete(Tid, Sid).

get_svcs(#{tid := Tid}) ->
    %% Ms = ets:fun2ms(fun({#svc_id{}, X}) -> X end),
    Ms = [{{#svc_id{_ = '_'}, '$1'}, [], ['$1']}],
    ets:select(Tid, Ms).

get_svcs(SvcName, #{tid := Tid}) ->
    Ms = [{{#svc_id{name = SvcName, _ = '_'}, '$1'}, [], ['$1']}],
    ets:select(Tid, Ms).

get_svc(SvcName, Id, #{tid := Tid}) ->
    Ms = [{{#svc_id{name = SvcName, id = Id}, '$1'}, [], ['$1']}],
    ets:select(Tid, Ms).

put_svc(SvcName, Id, #svc{} = Svc, #{tid := Tid}) ->
    true = ets:insert(Tid, {#svc_id{name = SvcName, id = Id}, Svc}).


%%%===================================================================
%%% Internal functions
%%%===================================================================

rnd_topic_id() ->
    base62enc(rand:uniform(16#ffffffffffffffffffffffffffffffff)).

rnd_request_id() ->
    base62enc(rand:uniform(16#ffffffffffffffff)).

base62(I) when I < 10 ->
    $0 + I;
base62(I) when I >= 10, I < 36 ->
    $A - 10 + I;
base62(I) when I >= 36, I < 62 ->
    $a - 36 + I.

base62enc(0) ->
    <<>>;
base62enc(I) ->
    << (base62enc(I div 62))/binary, (base62(I rem 62))>>.

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

-spec init_data(Host :: nats_host(), Port :: inet:port_number(), Opts :: opts()) -> data().
init_data(Host, Port, Opts) ->
    Data = #{socket => undefined,
             tls => false,
             server_info => undefined,

             recv_buffer => <<>>,
             send_q => undefined,

             batch => [],
             batch_size => 0,
             batch_timer => undefined,

             tid => ets:new(?MODULE, [private, set]),

             inbox => undefined,
             srv_init => false,

             host => Host,
             port => Port},
    maps:merge(Opts, Data).
