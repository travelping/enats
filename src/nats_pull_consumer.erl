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

-module(nats_pull_consumer).

-behaviour(gen_server).

%% API
-export([start_link/6, start_link/7, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).
-ignore_xref([start_link/6, start_link/7, stop/1]).

-include_lib("kernel/include/logger.hrl").
-include_lib("enats/include/nats_stream.hrl").

-record(state, {module            :: module(),
                modstate          :: term(),
                conn              :: pid(),
                durable           :: boolean(),
                config            :: map(),
                stream            :: binary(),
                name              :: binary(),
                nats_opts         :: map(),
                inbox             :: binary(),
                sid               :: term(),
                batch_size        :: integer(),
                batch_outstanding :: integer()
               }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(Module, Conn, Stream, Name, NatsOpts, Options) ->
    gen_server:start_link(?MODULE, [Module, Conn, Stream, Name, NatsOpts], Options).

start_link(ServerName, Module, Conn, Stream, Name, NatsOpts, Options) ->
    gen_server:start_link(ServerName, ?MODULE, [Module, Conn, Stream, Name, NatsOpts], Options).

stop(ServerRef) ->
    gen_server:stop(ServerRef).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Module, Conn, Stream, Name, NatsOpts]) ->
    process_flag(trap_exit, true),

    monitor(process, Conn),

    maybe
        {ok, Config} ?= nats_consumer:get(Conn, Stream, Name, NatsOpts),
        ok ?= case Config of
                  #{config := Config} when is_map_key(deliver_subject, Config) ->
                      {error, not_a_pull_consumer};
                  _ ->
                      ok
              end,
        Durable = is_map_key(durable_name, Config),

        {ok, BatchSize, ModState} ?= Module:init(Stream, Name, Durable, NatsOpts),

        Inbox = <<"_INBOX.", (nats:rnd_topic_id())/binary, $., (nats:rnd_topic_id())/binary>>,
        {ok, Sid} ?= nats:sub(Conn, Inbox),

        nats_consumer:msg_next(Conn, Stream, Name, Inbox, #{batch => BatchSize}, NatsOpts),

        State = #state{
                   module = Module,
                   modstate = ModState,
                   conn = Conn,
                   durable = Durable,
                   config = Config,
                   stream = Stream,
                   name = Name,
                   nats_opts = NatsOpts,
                   inbox = Inbox,
                   sid = Sid,
                   batch_size = BatchSize,
                   batch_outstanding = BatchSize
                  },
        {ok, State}
    else
        {error, _} = Error ->
            Error
    end.

handle_call(_Request, _From, State) ->
    ?LOG(info, "~s: call ~0p", [?MODULE, _Request]),
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    ?LOG(info, "~s: cast ~0p", [?MODULE, _Request]),
    {noreply, State}.

handle_info({Conn, Sid, {msg, Subject, Message, Opts}},
            #state{module = Module, modstate = ModStateIn, conn = Conn, sid = Sid} = State) ->
    ?LOG(info, "~s: Subject: ~0p, Message: ~0p", [Subject, Message]),
    {AckType, ModStateOut} = Module:handle_message(Subject, Message, Opts, ModStateIn),
    nats_consumer:ack_msg(Conn, AckType, false, Opts),

    BatchOutstanding =
        case State of
            #state{stream = Stream, name = Name, inbox = Inbox, nats_opts = NatsOpts,
                   batch_size = BatchSize, batch_outstanding = 1} ->
                nats_consumer:msg_next(
                  Conn, Stream, Name, Inbox, #{batch => BatchSize}, NatsOpts),
                BatchSize;
            #state{batch_outstanding = Outstanding} ->
                Outstanding
        end,
    {noreply, State#state{modstate = ModStateOut, batch_outstanding = BatchOutstanding}};

handle_info(_Info, State) ->
    ?LOG(info, "~s: info ~0p, state: ~p0", [?MODULE, _Info, State]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
