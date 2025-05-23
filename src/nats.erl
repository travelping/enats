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
-export([connect/1,
         connect/2,
         connect/3,
         flush/1,
         pub/2,
         pub/3,
         pub/4,
         sub/2,
         sub/3,
         unsub/2,
         unsub/3,
         disconnect/1,
         is_ready/1,
         request/4,
         server_info/1,
         sockname/1,
         peername/1,
         is_alive/1,
         controlling_process/2
        ]).

%% internal API
-export([serve/2,
         service_reply/2,
         service_reply/3,
         service/4, endpoint/2,
         monitor/1]).
-export([rnd_topic_id/0]).

%% debug functions
-export([dump_subs/1]).

-ignore_xref([connect/1, connect/2, connect/3,
              flush/2,
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
-define(DEFAULT_SEND_TIMEOUT, 1).
-define(DEFAULT_MAX_BATCH_SIZE, 100).
-define(CONNECT_TIMEOUT, 1000).
-define(DEFAULT_EXPLICIT_HOST_PREFERENCE, 0).
-define(DEFAULT_EXPLICIT_HOST_ORDER, 0).
-define(DEFAULT_IMPLICIT_HOST_PREFERENCE, 0).
-define(DEFAULT_IMPLICIT_HOST_ORDER, 0).

-define(DEFAULT_OPTS,
        #{socket_opts => #{},
          verbose => false,
          pedantic => false,
          tls_required => false,
          tls_first => false,
          tls_opts => [],
          auth_required => optional,
          name => <<"nats">>,
          lang => <<"Erlang">>,
          headers => true,
          no_responders => true,
          buffer_size => 0,
          max_batch_size => ?DEFAULT_MAX_BATCH_SIZE,
          send_timeout => ?DEFAULT_SEND_TIMEOUT,
          stop_on_error => false,
          default_explicit_host_preference => ?DEFAULT_EXPLICIT_HOST_PREFERENCE,
          default_explicit_host_order => ?DEFAULT_EXPLICIT_HOST_ORDER,
          default_implicit_host_preference => ?DEFAULT_IMPLICIT_HOST_PREFERENCE,
          default_implicit_host_order => ?DEFAULT_IMPLICIT_HOST_ORDER
         }).
-define(DEFAULT_SOCKET_OPTS,
        #{reuseaddr => true}).

%% set optional server info values to defaults
-define(DEFAULT_SERVER_INFO,
        #{auth_required => false,
          tls_required => false,
          tls_verify => false,
          tls_available => false,
          ldm => false,
          jetstream => false}).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("opentelemetry_api/include/otel_tracer.hrl").

-type socket_opts() :: #{netns => string(),
                         netdev => binary(),
                         rcvbuf => non_neg_integer(),
                         sndbuf => non_neg_integer(),
                         reuseaddr => boolean()}.
-type nats_host() :: inet:socket_address() | inet:hostname() | binary().
%% -type nats_host() :: inet:socket_address().

-type nats_server_info() :: #{scheme    => binary(),
                              family    => 'undefined' | 'inet' | 'inet6',
                              host      => nats_host(),
                              port      => non_neg_integer() | undefined}.

-type nats_server_opts() :: #{scheme     := binary(),
                              family     := 'undefined' | 'inet' | 'inet6',
                              preference := 0..65535,
                              order      := 0..65535,
                              explicit   := boolean()}.

-type nats_server() :: #{scheme     := binary(),
                         host       := nats_host(),
                         port       := non_neg_integer() | undefined,
                         family     := 'undefined' | 'inet' | 'inet6',
                         preference := 0..65535,
                         host       := 0..65535,
                         explicit   := boolean()}.

-type v0_opts() :: #{socket_opts    => socket_opts(),
                     verbose        => boolean(),
                     pedantic       => boolean(),
                     tls_required   => boolean(),
                     tls_first      => boolean(),
                     tls_opts       => [ssl:tls_client_option()],
                     auth_required  => 'optional' | boolean(),
                     auth_token     => binary(),
                     user           => binary(),
                     pass           => binary(),
                     nkey_seed      => binary(),
                     jwt            => binary(),
                     name           => binary(),
                     lang           => binary(),
                     version        => binary(),
                     headers        => boolean(),
                     no_responders  => boolean(),
                     buffer_size    => -1 | non_neg_integer(),
                     max_batch_size => non_neg_integer(),
                     send_timeout   => non_neg_integer()
                    }.

-type v1_opts() :: #{socket_opts    => socket_opts(),
                     verbose        => boolean(),
                     pedantic       => boolean(),
                     tls_required   => boolean(),
                     tls_first      => boolean(),
                     tls_opts       => [ssl:tls_client_option()],
                     auth_required  => 'optional' | boolean(),
                     auth_token     => binary(),
                     user           => binary(),
                     pass           => binary(),
                     name           => binary(),
                     lang           => binary(),
                     version        => binary(),
                     headers        => boolean(),
                     no_responders  => boolean(),
                     buffer_size    => -1 | non_neg_integer(),
                     max_batch_size => non_neg_integer(),
                     send_timeout   => non_neg_integer(),
                     stop_on_error  => boolean(),
                     default_explicit_host_preference => 0..65535,
                     default_explicit_host_order => 0..65535,
                     default_implicit_host_preference => 0..65535,
                     default_implicit_host_order => 0..65535,
                     servers        => [nats_server_info()]
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

-type server_opts() :: #{socket_opts    := socket_opts(),
                         verbose        := boolean(),
                         pedantic       := boolean(),
                         tls_required   := boolean(),
                         tls_first      := boolean(),
                         tls_opts       := [ssl:tls_client_option()],
                         auth_required  := 'optional' | boolean(),
                         auth_token     => binary(),
                         user           => binary(),
                         pass           => binary(),
                         name           := binary(),
                         lang           := binary(),
                         version        => binary(),
                         headers        := boolean(),
                         no_responders  := boolean(),
                         buffer_size    := -1 | non_neg_integer(),
                         max_batch_size := non_neg_integer(),
                         send_timeout   := non_neg_integer(),
                         stop_on_error  := boolean(),
                         default_server_opts := nats_server_opts(),
                         default_explicit_host_preference := 0..65535,
                         default_explicit_host_order      := 0..65535,
                         default_implicit_host_preference := 0..65535,
                         default_implicit_host_order      := 0..65535,
                         servers        := [nats_server()],
                         _              => _
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
                  server         := undefined | nats_server(),
                  socket_opts    := socket_opts(),
                  verbose        := boolean(),
                  pedantic       := boolean(),
                  tls_first      := boolean(),
                  tls_required   := boolean(),
                  tls_opts       := [ssl:tls_client_option()],
                  auth_required  := 'optional' | boolean(),
                  auth_token     => binary(),
                  user           => binary(),
                  pass           => binary(),
                  name           := binary(),
                  lang           := binary(),
                  version        := binary(),
                  headers        := boolean(),
                  no_responders  := boolean(),
                  buffer_size    := non_neg_integer(),
                  max_batch_size := non_neg_integer(),
                  send_timeout   := non_neg_integer(),
                  stop_on_error  := boolean(),
                  default_server_opts := nats_server_opts(),
                  servers        := [nats_server()],
                  candidates     := [nats_server()],
                  _              => _
                 }.

-export_type([sid/0, notify_fun/0, conn/0]).

-opaque sid() :: {'$sid', integer()}.
-opaque conn() :: pid().

-type notify_fun() :: fun((Sid :: sid(),
                           Subject :: binary(),
                           Payload :: binary(),
                           MsgOpts :: map()) -> any()).

-record(nats_req, {conn     :: conn(),
                   svc      :: binary(),
                   id       :: binary(),
                   reply_to :: undefined | binary()
                  }).
-record(pid, {pid}).
-record(sub, {owner, sid}).
-record(sub_session, {owner    :: pid(),
                      type     :: 'sub' | 'unsub',
                      subject  :: binary(),
                      queue_group :: undefined | binary(),
                      notify   :: notify_fun(),
                      max_msgs :: integer()}).
-record(ready, {pending}).
-record(inbox, {subject :: binary()}).
-record(req, {req_id}).
-record(svc_id, {name, id}).
-record(service, {svc, op, endp, queue_group, subject}).
-record(svc, {module, state, id, service, endpoints, started}).
-record(endpoint, {function, id, endpoint}).

%%%===================================================================
%%% API
%%%===================================================================

-doc """
Starts the NATS client process.
This function is typically used internally by the `connect/1,2,3` functions.
""".
-spec start_link(Opts :: server_opts()) ->
          {ok, Conn :: conn()} |
          ignore |
          {error, Error :: term()}.
start_link(Opts) ->
    gen_statem:start_link(?MODULE, {maps:merge(#{parent => self()}, Opts)}, []).

-doc """
Connects to a NATS server or cluster using the specified options.
This function supports the v1 options format, including a list of servers.
""".
-spec connect(Opts :: v1_opts()) ->
          {ok, Conn :: conn()} |
          ignore |
          {error, Error :: term()}.
connect(#{servers := Servers0} = Opts0) ->
    Opts = init_opts(Opts0),
    Servers = lists:map(fun(Server) -> init_explicit_host(Server, Opts) end, Servers0),
    start_link(finalize_server_opts(Opts#{servers := Servers})).

-spec connect(Server :: nats_server_info(),
              Opts :: v0_opts()) ->
          {ok, Conn :: conn()} |
          ignore |
          {error, Error :: term()};
             (Servers :: [nats_server_info()],
              Opts :: v0_opts()) ->
          {ok, Conn :: conn()} |
          ignore |
          {error, Error :: term()};
             (Host :: nats_host(),
              Port :: inet:port_number()) ->
          {ok, Conn :: conn()} |
          ignore |
          {error, Error :: term()}.
-doc """
Connects to a NATS server or cluster using the specified server information and v0 options.
Supports connecting to a single server or a list of servers.
""".
connect(#{scheme := _, host := _} = Server, Opts) when is_map(Opts) ->
    connect([Server], Opts);
connect(Servers, Opts) when is_list(Servers), is_map(Opts) ->
    connect(Opts#{servers => Servers});
connect(Host, Port) when is_integer(Port) ->
    connect(Host, Port, #{}).

-spec connect(Host :: nats_host(),
              Port :: inet:port_number(),
              Opts :: v0_opts()) ->
          {ok, Conn :: conn()} |
          ignore |
          {error, Error :: term()}.
-doc """
Connects to a NATS server at the given host and port with specified v0 options.
""".
connect(Host, Port, Opts) ->
    connect(init_host_scheme(Host, Port, Opts), Opts).

-doc """
Flushes the pending messages to the NATS server.
""".
-spec flush(Server :: conn()) -> ok | {error, timeout}.
flush(Server) ->
    gen_statem:call(Server, flush).

-doc """
Publishes a message with no payload and default options to the specified subject.
""".
-spec pub(Server :: conn(), Subject :: iodata()) -> ok | {error, timeout}.
pub(Server, Subject) ->
    pub(Server, Subject, <<>>, #{}).

-doc """
Publishes a message with either no payload and specified options to the specified subject, or
with the specified payload and default options to the specified subject.
""".
-spec pub(Server :: conn(), Subject :: iodata(), Payload :: iodata()) -> ok | {error, timeout};
         (Server :: conn(), Subject :: iodata(), Opts :: map()) -> ok | {error, timeout}.
pub(Server, Subject, Opts)
  when is_map(Opts) ->
    pub(Server, Subject, <<>>, Opts);
pub(Server, Subject, Payload) ->
    pub(Server, Subject, Payload, #{}).

-doc """
Publishes a message with the specified payload and options to the specified subject.
""".
-spec pub(Server :: conn(), Subject :: iodata(), Payload :: iodata(), Opts :: map()) -> ok | {error, timeout}.
pub(Server, Subject, Payload, Opts) ->
    call_with_ctx(
      ~"nats: pub", #{},
      Server, {pub, Subject, Payload, Opts}).

-doc """
Subscribes to the specified subject with default options.
""".
-spec sub(Server :: conn(), Subject :: iodata()) -> {ok, sid()} | {error, timeout | not_ready}.
sub(Server, Subject) ->
    sub(Server, Subject, #{}).

-doc """
Subscribes to the specified subject with the given options.
""".
-spec sub(Server :: conn(), Subject :: iodata(), Opts :: map()) -> {ok, sid()} | {error, timeout | not_ready}.
sub(Server, Subject, Opts) ->
    call_with_ctx(
      ~"nats: sub", #{},
      Server, {sub, Subject, self(), Opts}).

-doc """
Unsubscribes from the specified subscription reference.
""".
-spec unsub(Server :: conn(), SRef :: sid()) -> ok | {error, timeout | not_ready | invalid_session_ref}.
unsub(Server, SRef) ->
    unsub(Server, SRef, #{}).
-doc """
Unsubscribes from the specified subscription reference with options (e.g., max messages).
""".
-spec unsub(Server :: conn(), SRef :: sid(), Opts :: map()) -> ok | {error, timeout | not_ready | invalid_session_ref}.
unsub(Server, SRef, Opts) ->
    call_with_ctx(
      ~"nats: unsub", #{},
      Server, {unsub, SRef, Opts}).

%% request(Server, Subject, Payload, Opts) ->
%%     gen_statem:call(Server, {request, Subject, Payload, Opts}).

-doc """
Sends a request message and waits for a single reply.
""".
-spec request(Server :: conn(), Subject :: iodata(), Payload :: iodata(), Opts :: map()) ->
          {ok, {Payload :: iodata(), MsgOpts :: map()}} |
          {error, timeout | not_ready | no_responders}.
request(Server, Subject, Payload, Opts) ->
    maybe
        {ok, Inbox} ?= gen_statem:call(Server, get_inbox_topic),
        try
            call_with_ctx(
              ~"nats: request", #{},
              Server, {request, Subject, Inbox, Payload, Opts})
        of
            {ok, {_, #{header := <<"NATS/1.0 503\r\n", _/binary>>}}} ->
                {error, no_responders};
            Reply ->
                Reply
        catch
            exit:{timeout, _} ->
                {error, timeout};
            exit:{_, _} ->
                {error, no_responders}
        end
    end.

-doc """
Registers a service with the NATS server.
""".
-spec serve(Server :: conn(), Service :: #svc{}) -> ok | {error, timeout | not_ready}.
serve(Server, Service) ->
    gen_statem:call(Server, {serve, Service}).

-doc """
Sends a reply to a service request.
""".
-spec service_reply(ReplyKey :: #nats_req{}, Payload :: iodata()) -> ok.
service_reply(#nats_req{reply_to = undefined}, _Payload) ->
    ok;
service_reply(#nats_req{conn = Server, reply_to = ReplyTo} = ReplyKey, Payload) ->
    Msg = nats_msg:pub(ReplyTo, undefined, Payload),
    (catch gen_statem:call(Server, {service_reply, ReplyKey, Msg})),
    ok.

-doc """
Sends a reply with headers to a service request.
""".
-spec service_reply(ReplyKey :: #nats_req{}, Header :: binary(), Payload :: iodata()) -> ok.
service_reply(#nats_req{reply_to = undefined}, _Header, _Payload) ->
    ok;
service_reply(#nats_req{conn = Server, reply_to = ReplyTo} = ReplyKey, Header, Payload) ->
    Msg = nats_msg:hpub(ReplyTo, undefined, Header, Payload),
    (catch gen_statem:call(Server, {service_reply, ReplyKey, Msg})),
    ok.

-doc """
Disconnects the client from the NATS server.
""".
-spec disconnect(Server :: conn()) -> ok | {error, timeout}.
disconnect(Server) ->
    gen_statem:call(Server, disconnect).

-doc """
Checks if the client is ready to send and receive messages.
""".
-spec is_ready(Server :: conn()) -> boolean() | {error, timeout | not_found}.
is_ready(Server) ->
    try
        gen_statem:call(Server, is_ready)
    catch
        exit:{timeout, _} ->
            {error, timeout};
        exit:_ ->
            {error, not_found}
    end.

-doc """
Dumps the current subscriptions for debugging purposes.
""".
-spec dump_subs(Server :: conn()) -> list().
dump_subs(Server) ->
    gen_statem:call(Server, dump_subs).

-doc """
Retrieves the server information received during connection.
""".
-spec server_info(Server :: conn()) -> {ok, map()} | {error, timeout | closed}.
server_info(Server) ->
    gen_statem:call(Server, server_info).

-doc """
Retrieves the local socket address of the connection.
""".
-spec sockname(Server :: conn()) -> {ok, map()} | {error, timeout | closed}.
sockname(Server) ->
    gen_statem:call(Server, sockname).

-doc """
Retrieves the remote peer address of the connection.
""".
-spec peername(Server :: conn()) -> {ok, map()} | {error, timeout | closed}.
peername(Server) ->
    gen_statem:call(Server, peername).

-doc """
Creates a service record for use with `serve/2`.
""".
-spec service(service(),  nonempty_list(#endpoint{}), module(), any()) -> #svc{}.
service(SvcDesc, EndpDesc, Module, State) ->
    #svc{module = Module, state = State, service = SvcDesc, endpoints = EndpDesc}.

-doc """
Creates an endpoint record for use within a service.
""".
-spec endpoint(endpoint(), atom()) -> #endpoint{}.
endpoint(EndpDesc, Function) ->
    #endpoint{function = Function,
              endpoint = maps:merge(#{queue_group => ~"q", metadata => null}, EndpDesc)}.

-doc """
Generates a random topic ID.
""".
-spec rnd_topic_id() -> binary().
rnd_topic_id() ->
    base62enc(rand:uniform(16#ffffffffffffffffffffffffffffffff)).

-doc false.
-spec monitor(Server :: conn()) -> reference().
monitor(Server) ->
    monitor(process, Server).

-doc """
Return true is the connection process is alive
""".
-spec is_alive(Server :: conn()) -> boolean().
is_alive(Server) ->
    is_process_alive(Server).

-doc """
Change the controlling process (owner) of a socket.

Assigns a new controlling process Pid to Conn. The controlling process is the process that
the connection sends connection events to. If this function is called from any other process
 than the current controlling process, {error, not_owner} is returned.
""".
-spec controlling_process(Conn :: nats:conn(), Pid :: pid()) -> ok | {error, Reason :: atom()}.
controlling_process(Conn, Pid) ->
      gen_statem:call(Conn, {controlling_process, self(), Pid}).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

-define(CONNECT_TAG, '$connect').

-doc false.
-spec callback_mode() -> gen_statem:callback_mode_result().
callback_mode() -> [handle_event_function, state_enter].

-doc false.
-spec init({Opts :: server_opts()}) ->
          gen_statem:init_result(connecting, data()).
init({Opts}) ->
    process_flag(trap_exit, true),

    nats_msg:init(),
    Data0 = init_data(Opts),
    Data = init_connection_candidates(Data0),
    {ok, connecting, Data}.

-doc false.
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
    notify_parent(closed, Data),
    {stop_and_reply, normal, [{reply, From, ok}], close_socket(Data)};
handle_event({call, From}, disconnect, _State, Data) ->
    notify_parent(closed, Data),
    {stop_and_reply, normal, [{reply, From, ok}], Data};
handle_event(enter, OldState, connecting, Data)
  when is_record(OldState, ready) ->
    %% re-initialize the connection candiates when we had a working connection before
    self() ! ?CONNECT_TAG,
    {keep_state, init_connection_candidates(Data)};
handle_event(enter, _, connecting, _Data) ->
    self() ! ?CONNECT_TAG,
    keep_state_and_data;

handle_event(info, {?CONNECT_TAG, Pid, TLS, {ok, Socket}},
             connecting, #{server := #{host := Host, port := Port}, socket := Pid} = Data) ->
    ?LOG(debug, "NATS client connected to ~s:~w", [fmt_host(Host), Port]),
    {next_state, connected, Data#{socket := Socket, tls := TLS}};

handle_event(info, {{'DOWN', ?CONNECT_TAG}, _, process, Pid, Reason},
             connecting, #{server := #{host := Host, port := Port}, socket := Pid} = Data) ->
    ?LOG(debug, "NATS client failed to open TCP socket for connecting to ~s:~w unexpectedly with ~0p",
         [fmt_host(Host), Port, Reason]),
    notify_parent({error, Reason}, Data),
    handle_socket_closed(Data#{socket := undefined});

handle_event(info, {{'DOWN', ?CONNECT_TAG}, _, process, _, _} = Msg, _, _) ->
    ?LOG(debug, "Monitor Message: ~p", [Msg]),
    keep_state_and_data;

handle_event(info, {?CONNECT_TAG, Pid, _, {error, _} = Error},
             connecting,
             #{server := #{host := Host, port := Port}, socket := Pid} = Data) ->
    ?LOG(debug, "NATS client failed to open TCP socket for connecting to ~s:~w with ~0p",
         [fmt_host(Host), Port, Error]),
    notify_parent(Error, Data),
    handle_socket_closed(Data#{socket := undefined});

handle_event(info, ?CONNECT_TAG, connecting, Data0) ->
    case select_connection_candidates(Data0) of
        {error, _} = Error ->
            notify_parent(Error, Data0),
            %% give up
            handle_socket_closed(Data0#{server := undefined});
        {#{host := Host, port := Port, family := Family}, Data} ->
            case get_host_addr(Family, Host) of
                {ok, IP} ->
                    Owner = self(),
                    {Pid, _} =
                        proc_lib:spawn_opt(
                          fun() -> async_connect(Owner, IP, Port, Data) end,
                          [{monitor, [{tag, {'DOWN', ?CONNECT_TAG}}]}]),
                    {keep_state, Data#{socket := Pid}};
                {error, _} = Error ->
                    ?LOG(debug, "NATS client failed to open TCP socket for connecting to ~s:~w with ~p",
                         [fmt_host(Host), Port, Error]),

                    %% try again
                    self() ! ?CONNECT_TAG,
                    {keep_state, Data}
            end
    end;

handle_event(enter, _, connected, #{socket := Socket} = Data) ->
    ?LOG(debug, "NATS enter connected state, socket ~p", [Socket]),
    ok = setopts(Data, [{active,once}]),
    keep_state_and_data;
handle_event(enter, OldState, State, #{socket := _} = Data)
  when not is_record(OldState, ready), is_record(State, ready) ->
    ?LOG(debug, "NATS enter ready state"),
    ok = setopts(Data, [{active, true}]),
    notify_parent(ready, Data),

    Subs = get_sub_sessions_reverse(Data),
    SubMsgs =
        lists:foldl(
          fun([Sid, #sub_session{type = Type, subject = Subject,
                                 queue_group = QueueGrp, max_msgs = MaxMsgs}], Acc0) ->
                  NatsSid = integer_to_binary(Sid),
                  Acc =
                      case Type of
                          unsub ->
                              [nats_msg:unsub(NatsSid, MaxMsgs) | Acc0];
                          _ ->
                              Acc0
                      end,
                  [nats_msg:sub(Subject, QueueGrp, NatsSid) | Acc];
             ([Sid, #inbox{subject = Subject}], Acc) ->
                  NatsSid = integer_to_binary(Sid),
                  [nats_msg:sub(Subject, NatsSid) | Acc];
             ([Sid, #service{subject = Subject, queue_group = QueueGrp}], Acc) ->
                  NatsSid = integer_to_binary(Sid),
                  [nats_msg:sub(Subject, QueueGrp, NatsSid) | Acc]
          end, [], Subs),

    {keep_state, enqueue_msg(SubMsgs, Data)};

handle_event(enter, _, State, _Data)
  when is_record(State, ready) ->
    keep_state_and_data;

handle_event(info, {tcp_error, Socket, Reason}, _, #{socket := Socket} = Data) ->
    log_connection_error(Reason, Data),
    handle_socket_closed(close_socket(Data));
handle_event(info, {tcp_closed, Socket}, _, #{socket := Socket} = Data) ->
    handle_socket_closed(close_socket(Data));

handle_event(info, {tcp, Socket, Bin}, State, #{socket := Socket} = Data)
  when State =:= connected; is_record(State, ready) ->
    ?LOG(debug, "got data: ~p", [Bin]),
    handle_message(Bin, State, Data);

handle_event(info, {ssl_error, Socket, Reason}, _, #{socket := Socket} = Data) ->
    log_connection_error(Reason, Data),
    handle_socket_closed(close_socket(Data));
handle_event(info, {ssl_closed, Socket}, _, #{socket := Socket} = Data) ->
    handle_socket_closed(close_socket(Data));

handle_event(info, {ssl, Socket, Bin}, State, #{socket := Socket} = Data)
  when State =:= connected; is_record(State, ready) ->
    ?LOG(debug, "got data: ~p", [Bin]),
    handle_message(Bin, State, Data);

handle_event(info, batch_timeout, State, #{batch := Batch} = Data)
  when is_record(State, ready) ->
    send(Batch, Data),
    {keep_state, Data#{batch_size := 0, batch := [], batch_timer := undefined}};

handle_event(info, {'DOWN', _MRef, process, Owner, normal}, _, Data) ->
    _ = del_pid_monitor(Owner, Data),
    Sids = get_owner_subs(Owner, Data),
    lists:foreach(fun(X) -> del_sid(X, Data) end, Sids),
    true = length(Sids) =:= del_owner_subs(Owner, Data),
    keep_state_and_data;

handle_event(EventType, {with_ctx, Ctx, SpanCtx, Msg}, State, Data) ->
    Ctx1 = otel_tracer:set_current_span(Ctx, SpanCtx),
    Token = otel_ctx:attach(Ctx1),
    try handle_event(EventType, Msg, State, Data)
    after
        _ = otel_span:end_span(SpanCtx),
        otel_ctx:detach(Token)
    end;

handle_event({call, From}, {controlling_process, OldParent, NewParent},
             _State, #{parent := Parent} = Data) ->
    case Parent =:= OldParent of
        true ->
            unlink(OldParent),
            link(NewParent),
            {keep_state, Data#{parent := NewParent}, [{reply, From, ok}]};
        false ->
            {keep_state, Data#{parent := NewParent}, [{reply, From, {error, not_owner}}]}
    end;

handle_event({call, _From}, _, State, _Data)
  when State =:= connecting; State =:= connected  ->
    {keep_state_and_data, [postpone]};
handle_event({call, _From}, _, #ready{pending = Pending}, _Data)
  when Pending /= undefined ->
    {keep_state_and_data, [postpone]};
handle_event({call, From}, _, State, _Data)
  when not is_record(State, ready) ->
    {keep_state_and_data, [{reply, From, {error, not_ready}}]};

handle_event({call, From}, flush, _State, #{batch := Batch} = Data) ->
    send(Batch, Data),
    {keep_state, Data#{batch_size := 0, batch := [], batch_timer := undefined}, [{reply, From, ok}]};

handle_event({call, From}, {pub, Subject, Payload, Opts}, State, Data) ->
    Msg = mk_pub_msg(Subject, Payload, Opts),
    send_msg_with_reply(From, ok, Msg, State, Data);

handle_event({call, From}, {sub, Subject, Owner, Opts}, State, Data) ->
    {NatsSid, Sid} = make_sub_id(Data),
    QueueGrp = maps:get(queue_group, Opts, undefined),
    monitor_owner_sub(Owner, Data),
    put_owner_sub(Owner, Sid, Data),
    put_sid_session(Sid, Owner, Subject, QueueGrp, notify_fun(Owner, Opts), 0, Data),
    Msg = nats_msg:sub(Subject, QueueGrp, NatsSid),
    send_msg_with_reply(From, {ok, Sid}, Msg, State, Data);

handle_event({call, From}, {unsub, {'$sid', NatsSid} = Sid, Opts}, State, Data) ->
    case get_sid(Sid, Data) of
        [#sub_session{owner = Owner} = Session] ->
            case Opts of
                #{max_messages := MaxMsgsOpt} when is_integer(MaxMsgsOpt) ->
                    put_sid_session(
                      Sid, Session#sub_session{type = unsub, max_msgs = MaxMsgsOpt}, Data);
                _ ->
                    del_sid(Sid, Data),
                    del_owner_sub(Owner, Sid, Data),
                    demonitor_owner_sub(Owner, Data)
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
    Subject = <<Inbox/binary, $*>>,

    {NatsSid, Sid} = make_sub_id(Data),
    put_sid_inbox(Sid, Subject, Data),

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

handle_event({call, From},
             {service_reply, #nats_req{svc = SvcName, id = Id}, Msg},
             State, Data) ->
    case get_svc(SvcName, Id, Data) of
        [#svc{} = _Svc] ->
            Action = {reply, From, ok},
            next_state_enqueue_batch([Msg], Action, State, Data);
        [] ->
            {keep_state_and_data, [{reply, From, {error, not_found}}]}
    end;

handle_event({call, From}, dump_subs, _State, #{tid := Tid}) ->
    {keep_state_and_data, [{reply, From, ets:tab2list(Tid)}]};

handle_event({call, From}, server_info, State, #{server_info := ServerInfo})
  when State =:= connected; is_record(State, ready) ->
    Reply = {ok, ServerInfo},
    {keep_state_and_data, [{reply, From, Reply}]};
handle_event({call, From}, server_info, _, _) ->
    Reply = {error, closed},
    {keep_state_and_data, [{reply, From, Reply}]};

handle_event({call, From}, sockname, State, #{socket := Socket, tls := TLS})
  when State =:= connected; is_record(State, ready) ->
    Mod = case TLS of
              false -> inet;
              true  -> ssl
          end,
    Reply =
        case Mod:sockname(Socket) of
            {ok, {IP, Port}} when tuple_size(IP) =:= 4 ->
                {ok, #{family => inet, addr => IP, port => Port}};
            {ok, {IP, Port}} when tuple_size(IP) =:= 8 ->
                {ok, #{family => inet6, addr => IP, port => Port}};
            {error, _} = Error ->
                Error
        end,
    {keep_state_and_data, [{reply, From, Reply}]};
handle_event({call, From}, sockname, _, _) ->
    Reply = {error, closed},
    {keep_state_and_data, [{reply, From, Reply}]};

handle_event({call, From}, peername, State, #{socket := Socket, tls := TLS})
  when State =:= connected; is_record(State, ready) ->
    Mod = case TLS of
              false -> inet;
              true  -> ssl
          end,
    Reply =
        case Mod:peername(Socket) of
            {ok, {IP, Port}} when tuple_size(IP) =:= 4 ->
                {ok, #{family => inet, addr => IP, port => Port}};
            {ok, {IP, Port}} when tuple_size(IP) =:= 8 ->
                {ok, #{family => inet6, addr => IP, port => Port}};
            {error, _} = Error ->
                Error
        end,
    {keep_state_and_data, [{reply, From, Reply}]};
handle_event({call, From}, peername, _, _) ->
    Reply = {error, closed},
    {keep_state_and_data, [{reply, From, Reply}]};


handle_event(Event, EventContent, State, Data) ->
    ?LOG(debug, "NATS-#1:~nEvent: ~p~nContent: ~p~nState: ~p~nData: ~p",
         [Event, EventContent, State, Data]),
    keep_state_and_data.

-doc false.
format_status(Status) ->
    maps:map(
      fun(data, Data) ->
              maps:map(
                fun(pass, _) -> redacted;
                   (auth_token, _) -> redacted;
                   (nkey_seed, _) -> redacted;
                   (_, V) -> V
                end, Data);
         (_, Value) ->
              Value
      end, Status).

-doc false.
terminate(_Reason, _State, _Data) ->
    void.

-doc false.
code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%%===================================================================
%%% State Helper functions
%%%===================================================================

handle_socket_closed(#{stop_on_error := Stop} = Data) ->
    notify_parent(closed, Data),
    case Stop of
        true ->
            {stop, normal, Data};
        false ->
            {next_state, connecting, Data}
    end.

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

socket_active(connected, connected, Data) ->
    _ = setopts(Data, [{active,once}]),
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
    ?with_span(
       ~"nats: msg", #{},
       fun (_) ->
               Opts = reply_opt(ReplyTo, #{}),
               handle_nats_msg_msg(Subject, NatsSid, Payload, Opts, DecState)
       end);

handle_nats_msg({hmsg, {Subject, NatsSid, ReplyTo, Header, Payload}} = Msg,
                {State, _} = DecState)
  when is_record(State, ready) ->
    ?LOG(debug, "got msg: ~p", [Msg]),
    ?with_span(
       ~"nats: hmsg", #{},
       fun (_) ->
               Opts = reply_opt(ReplyTo, #{header => Header}),
               handle_nats_msg_msg(Subject, NatsSid, Payload, Opts, DecState)
       end);
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
        [#sub_session{owner = Owner, notify = Notify, max_msgs = MaxMsgs} = Session] ->
            Notify(Sid, Subject, Payload, Opts),

            case MaxMsgs of
                0 ->
                    %% do nothing, no subscription limit
                    ok;
                1 ->
                    ?LOG(debug, "NATS: Auto-removing subscription, limit reached for sid '~s'", [NatsSid]),
                    del_sid(Sid, Data),
                    del_owner_sub(Owner, Sid, Data),
                    demonitor_owner_sub(Owner, Data);
                _ ->
                    put_sid_session(Sid, Session#sub_session{max_msgs = MaxMsgs - 1}, Data)
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
                         '_nats.client.created.version' => lib_version()},
                       maps:get(metadata, Service, #{})),
                 endpoints => Endpoints},
    Response = maps:with([name, id, version, metadata,
                          type, description, endpoints], Resp0),
    ?LOG(debug, "SvcRespPayload: ~p", [Response]),
    nats_msg:pub(ReplyTo, undefined, json:encode(Response)).

service_stats_msg(ReplyTo, #svc{id = Id, service = Service, endpoints = EndPs,
                                started = Started}) ->
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
                         '_nats.client.created.version' => lib_version()},
                       maps:get(metadata, Service, #{})),
                 endpoints => Endpoints},
    Response = maps:with([name, id, version, metadata,
                          type, description, started, endpoints], Resp0),
    ?LOG(debug, "SvcRespPayload: ~p", [Response]),
    nats_msg:pub(ReplyTo, undefined, json:encode(Response)).

service_ping_msg(ReplyTo, #svc{id = Id, service = Service}) ->
    Resp0 =
        Service#{id => Id,
                 type => ~"io.nats.micro.v1.ping_response",
                 metadata =>
                     maps:merge(
                       #{'_nats.client.created.library' => ~"natserl",
                         '_nats.client.created.version' => lib_version()},
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
            ReplyKey = #nats_req{conn = self(), svc = SvcName, id = Id,
                                 reply_to = maps:get(reply_to, Opts, undefined)},
            try M:F(ReplyKey, SvcName, Op, Payload, Opts, CbState) of
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
                ServerInfo = maps:merge(?DEFAULT_SERVER_INFO, JSON),
                {ok, Data} ?= ssl_upgrade(JSON, Data0#{server_info := ServerInfo}),
                ?LOG(debug, "NATS Client Info: ~p", [client_info(Data)]),
                {ok, Msg} ?= client_info(Data),
                continue_enqueue_batch([Msg], #ready{}, Data)
            else
                Error ->
                    ?LOG(debug, "NATS Info Error: ~p", [Error]),
                    notify_parent(Error, Data0),
                    {stop, {closed, Data0}}
            end
    catch
        C:E ->
            ?LOG(debug, "NATS Info Error: ~p:~p", [C, E]),
            notify_parent({error, {C, E}}, Data0),
            {stop, {closed, Data0}}
    end.

tls_server_name_indication(Host, TlsOpts) ->
    case proplists:is_defined(server_name_indication, TlsOpts) of
        false when is_list(Host); is_tuple(Host) ->
            [{server_name_indication, Host}|TlsOpts];
        false when is_binary(Host) ->
            [{server_name_indication, binary_to_list(Host)}|TlsOpts];
        _ ->
            TlsOpts
    end.

tls_customize_hostname_check(TlsOpts) ->
    case proplists:is_defined(customize_hostname_check, TlsOpts) of
        true -> TlsOpts;
        false ->
            [{customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]} |
             TlsOpts]
    end.

tls_opts(#{server := #{host := Host}, tls_opts := TlsOpts0}) ->
    TlsOpts1 = tls_server_name_indication(Host, TlsOpts0),
    _TlsOpts = tls_customize_hostname_check(TlsOpts1).

ssl_upgrade(#{tls_required := true}, #{tls := true} = Data) ->
    {ok, Data};
ssl_upgrade(#{tls_required := true}, #{socket := Socket} = Data) ->
    case ssl:connect(Socket, tls_opts(Data)) of
        {ok, NewSocket} ->
            {ok, Data#{socket := NewSocket, tls := true}};
        {error, _Reason} = Error ->
            Error
    end;
ssl_upgrade(_, State) ->
    {ok, State}.

setopts(#{socket := Socket, tls := true}, SockOpts) ->
    ssl:setopts(Socket, SockOpts);
setopts(#{socket := Socket, tls := false}, SockOpts) ->
    inet:setopts(Socket, SockOpts);
setopts(_, _) ->
    {error, not_connected}.

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
    FieldsList = [verbose, pedantic, tls_required, name, lang,
                  version, headers, no_responders],
    Nats0 = maps:with(FieldsList, Data),
    maybe
        {ok, Nats} ?= client_auth(ServerInfo, Data, Nats0),
        {ok, nats_msg:connect(json:encode(Nats))}
    end.

%% Include authentication properties iff the server requires it
client_auth(#{auth_required := true}, #{auth_required := false}, _) ->
    {error, server_requires_auth};
client_auth(#{auth_required := false}, #{auth_required := true}, _) ->
    {error, client_mandates_auth};
client_auth(#{auth_required := true}, #{user := User, pass := Pass}, Nats)
  when is_binary(User), is_binary(Pass) ->
    {ok, Nats#{user => User, pass => Pass}};
client_auth(#{auth_required := true}, #{auth_token := Token}, Nats)
  when is_binary(Token) ->
    {ok, Nats#{auth_token => Token}};
client_auth(#{auth_required := true, nonce := Nonce},
            #{nkey_seed := <<"SU", _/binary>> = Seed, jwt := JWT}, Nats) ->
    case nats_nkey:from_seed(Seed) of
        {ok, NKey} ->
            SignatureBin = nats_nkey:sign(NKey, Nonce),
            Signature = base64:encode(SignatureBin, #{mode => urlsafe, padding => false}),
            {ok, Nats#{jwt => JWT, sig => Signature}};
        {error, _} ->
            {error, invalid_seed}
    end;
client_auth(#{auth_required := true, nonce := Nonce},
            #{nkey_seed := <<"SU", _/binary>> = Seed}, Nats) ->
    case nats_nkey:from_seed(Seed) of
        {ok, NKey} ->
            SignatureBin = nats_nkey:sign(NKey, Nonce),
            Signature = base64:encode(SignatureBin, #{mode => urlsafe, padding => false}),
            Public = nats_nkey:public(NKey),
            {ok, Nats#{nkey => Public, sig => Signature}};
        {error, _} ->
            {error, invalid_seed}
    end;
client_auth(#{auth_required := true}, _, _) ->
    %% server requires authentication, but no valid credentials provided
    {error, server_requires_auth};
client_auth(_, _, Nats) ->
    {ok, Nats}.

log_connection_error(Error, #{server := #{host := Host, port := Port}}) ->
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

monitor_owner_sub(Owner, #{tid := Tid}) ->
    PKey = #pid{pid = Owner},
    case ets:member(Tid, PKey) of
        true  -> ok;
        false ->
            MRef = monitor(process, Owner),
            true = ets:insert(Tid, {PKey, MRef})
    end.

del_pid_monitor(Pid, #{tid := Tid}) ->
    case ets:take(Tid, #pid{pid = Pid}) of
        [{_, MRef}] ->
            [MRef];
        _ ->
            []
    end.

demonitor_owner_sub(Owner, Data) ->
    case count_owner_subs(Owner, Data) of
        0 ->
            maybe
                [MRef] ?= del_pid_monitor(Owner, Data),
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

put_owner_sub(Owner, Sid, #{tid := Tid}) ->
    true = ets:insert(Tid, {#sub{owner = Owner, sid = Sid}}).

del_owner_sub(Owner, Sid, #{tid := Tid}) ->
    true = ets:delete(Tid, #sub{owner = Owner, sid = Sid}).

get_owner_subs(Owner, #{tid := Tid}) ->
    Ms = [{{#sub{owner = Owner, sid = '$1'}}, [], ['$1']}],
    ets:select(Tid, Ms).

del_owner_subs(Owner, #{tid := Tid}) ->
    Ms = [{{#sub{owner = Owner, _ = '_'}}, [], [true]}],
    ets:select_delete(Tid, Ms).

count_owner_subs(Owner, #{tid := Tid}) ->
    Ms = [{{#sub{owner = Owner, _ = '_'}}, [], [true]}],
    ets:select_count(Tid,  Ms).

put_sid_session(Sid, #sub_session{} = Session, #{tid := Tid}) ->
    Obj = {Sid, Session},
    true = ets:insert(Tid, Obj).

put_sid_session(Sid, Owner, Subject, QueueGroup, Notify, MaxMsgs, #{tid := Tid}) ->
    Session = #sub_session{
                 owner = Owner,
                 type = sub,
                 subject = Subject,
                 queue_group = QueueGroup,
                 notify = Notify,
                 max_msgs = MaxMsgs
                },
    true = ets:insert(Tid, {Sid, Session}).

get_sub_sessions_reverse(#{tid := Tid}) ->
    Ms = [{{{'$sid', '$1'}, '$2'}, [], ['$$']}],
    ets:select_reverse(Tid, Ms).

put_sid_inbox(Sid, Subject, #{tid := Tid}) ->
    true = ets:insert(Tid, {Sid, #inbox{subject = Subject}}).

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

init_opts(Opts) ->
    maps:merge(?DEFAULT_OPTS, Opts).

finalize_server_opts(#{default_implicit_host_preference := Pref,
                       default_implicit_host_order := Order} = Opts0) ->
    Scheme = case Opts0 of
                 #{tls_first := true} -> ~"tls";
                 _ -> ~"nats"
             end,
    ServerOpts =
        #{scheme => Scheme,
          preference => Pref,
          order => Order,
          explicit => false,
          family => undefined},
    _Opts = Opts0#{default_server_opts => ServerOpts}.

normalize_server(#{port := _} = Server) ->
    Server;
normalize_server(Server) ->
    Server#{port => 4222}.

init_explicit_host(#{scheme := _, host := _} = Server,
                   #{default_explicit_host_preference := Pref, default_explicit_host_order := Order}) ->
    maps:merge(#{preference => Pref, order => Order, family => undefined},
               normalize_server(Server#{explicit => true})).

init_host_scheme(Host, Port, #{tls_first := true}) ->
    #{scheme => ~"tls", host => Host, port => Port};
init_host_scheme(Host, Port, _) ->
    #{scheme => ~"nats", host => Host, port => Port}.

filter_connection_candidates(#{host := PrevHost, port := PrevPort}, Servers) ->
    lists:filter(
      fun(#{host := Host, port := Port}) ->
              Host =/= PrevHost orelse Port =/= PrevPort
      end, Servers);
filter_connection_candidates(_, Servers) ->
    Servers.

init_connection_candidates(#{server := Server, servers := Servers} = Data) ->
    Candidates0 = filter_connection_candidates(Server, Servers),
    Candidates = maps:groups_from_list(
                   fun(#{order := Order}) -> Order end,
                   Candidates0),
    CandIter = maps:iterator(Candidates, ordered),
    Data#{server := undefined, candidates := maps:to_list(CandIter)}.

select_by_preference(L) ->
    %% number between 0 <= Rnd <= Max
    Total = lists:foldl(
              fun(#{preference := Pref}, Sum) -> Sum + Pref end, 0, L),
    Rnd = rand:uniform() * Total,
    fun F([Last], _) ->
            Last;
        F([#{preference := Pref} = Server|_], Weight)
          when Weight - Pref < 0 ->
            Server;
        F([#{preference := Pref}|T], Weight) ->
            F(T, Weight - Pref)
    end(L, Rnd).

%% select candidates in increasing order, pref is use as statiscal weight for selecting
%% see 3GPP TS 29.303, Annex C.1 for the algorithm
select_connection_candidates(#{candidates := []}) ->
    {error, no_more_candidates};
select_connection_candidates(#{candidates :=
                                   [{Order, Candidates}|NextOrderCandidates]} = Data) ->
    NextCandidates =
        case Candidates of
            [Server] ->
                %% last server in this order
                NextOrderCandidates;
            _ ->
                PrefL0 = lists:map(
                           fun(#{preference := 0} = X) -> {0, X};
                              (X) -> {rand:uniform(), X}
                           end, Candidates),
                PrefL = [N || {_,N} <- lists:sort(PrefL0)],
                Server = select_by_preference(PrefL),
                [{Order, lists:delete(Server, Candidates)}|NextOrderCandidates]
        end,
    {Server, Data#{server := Server, candidates := NextCandidates}}.

async_connect(Owner, IP, Port, #{tls_first := true} = Data) ->
    Opts = socket_opts(Data) ++ tls_opts(Data),
    Result = ssl:connect(IP, Port, Opts, ?CONNECT_TIMEOUT),
    case Result of
        {ok, Socket} ->
            ok = ssl:controlling_process(Socket, Owner);
        _ ->
            ok
    end,
    Owner ! {?CONNECT_TAG, self(), true, Result};
async_connect(Owner, IP, Port, Data) ->
    SocketOpts = socket_opts(Data),
    Result = gen_tcp:connect(IP, Port, SocketOpts, ?CONNECT_TIMEOUT),
    case Result of
        {ok, Socket} ->
            ok = gen_tcp:controlling_process(Socket, Owner);
        _ ->
            ok
    end,
    Owner ! {?CONNECT_TAG, self(), false, Result}.


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

get_host_addr(_, {_, _, _, _} = IP) ->
    {ok, IP};
get_host_addr(_, {_, _, _, _, _, _, _, _} = IP) ->
    {ok, IP};
get_host_addr(Family, Bin) when is_binary(Bin) ->
    get_host_addr(Family, binary_to_list(Bin));
get_host_addr(Family, Host) when is_list(Host); is_atom(Host) ->
    get_host_addr([inet6, inet], Family, Host).

get_host_addr([], _Family, _Host) ->
    {error, nxdomain};
get_host_addr([WantFamily|More], Family, Host)
  when Family =:= undefined;
       Family =:= WantFamily ->
    case inet:getaddrs(Host, WantFamily) of
        {ok, IPs} ->
            {ok, lists:nth(rand:uniform(length(IPs)), IPs)};
        {error, _} ->
            get_host_addr(More, Family, Host)
    end;
get_host_addr([_|More], Family, Host) ->
    get_host_addr(More, Family, Host).

json_object_push(Key, Value, Acc) ->
    K = try binary_to_existing_atom(Key) catch _:_ -> Key end,
    [{K, Value} | Acc].

socket_opts(#{socket_opts := SockOpts0}) ->
    SockOpts = maps:merge(?DEFAULT_SOCKET_OPTS, SockOpts0),
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

lib_version() ->
    {ok, VSN} = application:get_key(enats, vsn),
    iolist_to_binary(VSN).

-spec init_data(Opts :: server_opts()) -> data().
init_data(Opts) ->
    Data = #{%version => maps:get(version, Opts, lib_version()),
             socket => undefined,
             tls => false,
             server_info => undefined,

             recv_buffer => <<>>,
             send_q => undefined,

             batch => [],
             batch_size => 0,
             batch_timer => undefined,

             tid => ets:new(?MODULE, [private, ordered_set]),

             inbox => undefined,
             srv_init => false,

             server => undefined,
             candidates => []
            },
    maps:merge(Opts, Data).

notify_fun(_, #{notify := Notify})
  when is_function(Notify, 4) ->
    Notify;
notify_fun(Owner, _) ->
    Self = self(),
    fun(Sid, Subject, Payload, MsgOpts) ->
            Resp = {msg, Subject, Payload, MsgOpts},
            Owner ! {Self, Sid, Resp}
    end.

call_with_ctx(Name, Opts, ServerRef, Request) ->
    SpanCtx = ?start_span(Name, Opts),
    Ctx = otel_ctx:get_current(),
    gen_statem:call(ServerRef, {with_ctx, Ctx, SpanCtx, Request}).
