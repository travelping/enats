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

-module(nats_service).

-export([serve/2, reply/2, reply/3]).

%%%===================================================================
%%% API
%%%===================================================================

serve(Conn, #{name := _, version := _,
              module := Module, state := State, endpoints := Endpoints} = Svc) ->
    Exports = try
                  sets:from_list(Module:module_info(exports), [{version, 2}])
              catch _:_ -> erlang:exit(badarg, [Svc]) end,
    SvcEndpoints =
        lists:map(
          fun(#{name := _, function := Function} = EndP) ->
                  case sets:is_element({Function, 6}, Exports) of
                      true -> ok;
                      false -> erlang:exit(badarg, [EndP])
                  end,
                  nats:endpoint(
                    maps:with([name, group_name, queue_group, metadata], EndP), Function);
             (EndP) ->
                  erlang:exit(badarg, [EndP])
          end, Endpoints),
    Service =
        nats:service(
          maps:with([name, version, description, metadata], Svc), SvcEndpoints, Module, State),
    nats:serve(Conn, Service).

reply(Key, Payload) ->
    nats:service_reply(Key, Payload).

reply(Key, Header, Payload) ->
    nats:service_reply(Key, Header, Payload).
