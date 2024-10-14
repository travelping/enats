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

-module(nats_js_pull_consumer).

-export([init/4, handle_message/4]).

-include_lib("kernel/include/logger.hrl").

init(_Stream, _Name, _Durable, Opts) ->
    {ok, 10, Opts}.

handle_message(Subject, Message, MsgOpts, #{owner := Owner} = State) ->
    ?LOG(info, "~s: Subject: ~0p, Message: ~0p", [?MODULE, Subject, Message]),
    Owner ! {?MODULE, Subject, Message, MsgOpts},
    {ack, State}.
