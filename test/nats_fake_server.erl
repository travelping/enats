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

%% this test suite is based on the test suite of nats_teacup (https://github.com/yuce/teacup_nats)

-module(nats_fake_server).
-behaviour(gen_server).

%% API
-export([start_link/0, start_link/1, port/1, send/3, close/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/logger.hrl").

-define(SERVER, ?MODULE).

-record(state, {owner, listen, accept, sockets = #{}}).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
    start_link(0).

start_link(Port) ->
    gen_server:start_link(?MODULE, [self(), Port], []).

port(Server) ->
    gen_server:call(Server, port).

send(Server, Socket, Data) ->
    gen_server:call(Server, {send, Socket, Data}).

close(Server, Socket) ->
    gen_server:call(Server, {close, Socket}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Owner, Port]) ->
    process_flag(trap_exit, true),
    {ok, LSock} = socket:open(inet, stream, tcp),
    ok = socket:setopt(LSock, {socket, reuseaddr}, true),
    ok = socket:bind(LSock, #{family => inet, port => Port, addr => any}),
    ok = socket:listen(LSock),

    State = #state{owner = Owner, listen = LSock, accept = make_ref()},
    select(LSock, State#state.accept),
    ?LOG(debug, "Accepting on ~0p", [LSock]),

    {ok, State}.

handle_call(port, _From, #state{listen = LSock} = State) ->
    {ok, #{port := Port}} = socket:sockname(LSock),
    {reply, Port, State};

handle_call({send, Socket, Data}, _From,  #state{sockets = Sockets} = State0)
  when is_map_key(Socket, Sockets) ->
    State =
        case socket:send(Socket, Data, [], infinity) of
            ok ->
                State0;
            {error, _} ->
                close_socket(Socket, State0)
        end,
    {reply, ok, State};

handle_call({close, Socket}, _From,  #state{sockets = Sockets} = State0)
  when is_map_key(Socket, Sockets) ->
    State = close_socket(Socket, State0),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    Reply = {error, invalid_call},
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'$socket', LSock, select, SelectHandle} = _Info,
            #state{listen = LSock, accept = SelectHandle} = State0) ->
    ?LOG(debug, "Socket server Accept Info ~0p", [_Info]),
    State = accept(State0),
    {noreply, State};

handle_info({'$socket', Socket, select, SelectHandle} = _Info,
            #state{sockets = Sockets} = State0)
  when is_map_key(Socket, Sockets) ->
    ?LOG(debug, "Socket server Socket Info ~0p", [_Info]),
    State =
        case map_get(Socket, Sockets) of
            #{select := SelectHandle} = SocketInfo0 ->
                case read(Socket, SocketInfo0, State0) of
                    {ok, SocketInfo} ->
                        State0#state{sockets = Sockets#{Socket := SocketInfo}};
                    {error, _} ->
                        close_socket(Socket, State0)
                end;
            Other ->
                ?LOG(error, "invalid select on ~0p: ~0p", [Socket, Other]),
                State0
        end,
    {noreply, State};

handle_info({'$socket', _Socket, abort, {_Handle, closed}} = _Info, State) ->
    {noreply, State};

handle_info(_Info, State) ->
    ?LOG(error, "Socket server unknown Info ~0p", [_Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ?LOG(debug, "Socket server terminated with ~0p", [_Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

fmt_sockaddr(#{family := Family, port := Port, addr := Addr})
  when is_tuple(Addr) ->
    io_lib:format("~s: ~s:~w", [Family, inet:ntoa(Addr), Port]);
fmt_sockaddr(#{family := Family, port := Port, addr := Addr})
  when is_atom(Addr) ->
    io_lib:format("~s: ~s:~w", [Family, Addr, Port]).

select(Socket, Handle) ->
    self() ! {'$socket', Socket, select, Handle}.

accept(#state{owner = Owner, listen = LSock, accept = Handle, sockets = Sockets} = State0) ->
    case socket:accept(LSock, Handle) of
        {select, {select_info, accept, SelectHandle}} ->
            ?LOG(debug, "Accept SelectHandle ~0p", [SelectHandle]),
            State0#state{accept = SelectHandle};
        {ok, Socket} ->
            ?LOG(debug, "Accepted ~0p", [Socket]),
            Owner ! {accepted, self(), Socket},
            State = State0#state{sockets = Sockets#{Socket => init_socket(Socket)}},
            accept(State)
    end.

init_socket(Socket) ->
    {ok, SockAddr} = socket:peername(Socket),
    ?LOG(debug, "accepted connection from ~s\n", [fmt_sockaddr(SockAddr)]),
    Handle = make_ref(),
    select(Socket, Handle),
    #{peer => SockAddr, select => Handle}.

read(Socket, #{select := Handle} = Info, #state{owner = Owner} = State) ->
    case socket:recv(Socket, 0, [], Handle) of
        {select, {select_info, recv, SelectHandle}} ->
            ?LOG(debug, "recv select ~0p", [SelectHandle]),
            {ok, Info#{select := SelectHandle}};
        {ok, Data} ->
            Owner ! {recv, self(), Socket, Data},
            ?LOG(debug, "read data ~0p", [Data]),
            read(Socket, Info, State);
        {error, _} = Error ->
            ?LOG(debug, "read error ~0p", [Error]),
            Error
    end.

close_socket(Socket, #state{owner = Owner, sockets = Sockets} = State) ->
    _ = socket:close(Socket),
    Owner ! {closed, self(), Socket},
    State#state{sockets = maps:remove(Socket, Sockets)}.
