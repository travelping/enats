%%%-------------------------------------------------------------------
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

-module(nats_kv_watch).

-behaviour(gen_statem).

%% API
-export([start/6, stop/1, done/1]).

%% gen_statem callbacks
-export([callback_mode/0, init/1, terminate/3, code_change/4]).
-export([handle_event/4]).

-include_lib("kernel/include/logger.hrl").
-include_lib("enats/include/nats_stream.hrl").
-include("nats_kv.hrl").

-define(SERVER, ?MODULE).

-record(data, {owner, subj_pre, conn, watch, sid, cb, cb_state, init_cnt,
               ignore_deletes}).

-define(VALID_KEYS(Keys), (is_binary(Keys) orelse (is_list(Keys) andalso length(Keys) > 0))).

%%%===================================================================
%%% API
%%%===================================================================

start(Conn, Bucket, Keys, #{owner := Pid} = WatchOpts, Opts, StartOpts)
  when is_pid(Pid) andalso ?VALID_KEYS(Keys) ->
    gen_statem:start(?MODULE, [Conn, Bucket, Keys, WatchOpts, Opts], StartOpts);
start(Conn, Bucket, Keys, WatchOpts, Opts, StartOpts)
  when ?VALID_KEYS(Keys) ->
    gen_statem:start(?MODULE, [Conn, Bucket, Keys, WatchOpts#{owner => self()}, Opts], StartOpts).

stop(Watch) ->
    gen_statem:stop(Watch).

done(Pid) ->
    unlink(Pid),
    receive {'EXIT', Pid, _} -> true
    after 0 -> true
    end.

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

callback_mode() -> [handle_event_function, state_enter].

init([Conn, Bucket, Keys, #{owner := Owner} = WatchOpts, Opts]) ->
    process_flag(trap_exit, true),

    %% use a ephemeral consumer

    %% {
    %%   "stream_name": "KV_FOO",
    %%   "config": {
    %%     "deliver_policy": "last_per_subject",
    %%     "ack_policy": "none",
    %%     "ack_wait": 79200000000000,
    %%     "max_deliver": 1,
    %%     "filter_subject": "$KV.FOO.\\u003e",
    %%     "replay_policy": "instant",
    %%     "flow_control": true,
    %%     "idle_heartbeat": 5000000000,
    %%     "headers_only": true,
    %%     "deliver_subject": "_INBOX.IYYrsHiLQxScla5idhSjPl",
    %%     "num_replicas": 1,
    %%     "mem_storage": true
    %%   }
    %% }

    %% wild card SUB for the deliver_subject doesn't work:
    %% https://github.com/nats-io/nats-server/issues/6016
    %%
    %% Inbox = <<"_MY_KV_SUB.", (nats:rnd_topic_id())/binary>>,
    %% SubSubject = <<Inbox/binary, ".*">>,
    %% DeliverSubject = <<Inbox/binary, $. , (nats:rnd_topic_id())/binary>>,
    %% {ok, Sid} = nats:sub(Conn, SubSubject),

    DeliverSubject =  <<"_INBOX.", (nats:rnd_topic_id())/binary>>,
    {ok, Sid} = nats:sub(Conn, DeliverSubject),
    WatchConfig0 =
        #{deliver_policy => last_per_subject,
          ack_policy => none,
          ack_wait => 79200000000000,
          max_deliver => 1,
          replay_policy => instant,
          flow_control => true,
          idle_heartbeat => 5000000000,
          headers_only => maps:get(headers_only, WatchOpts, true),
          deliver_subject => DeliverSubject,
          num_replicas => 1,
          mem_storage => true
         },
    WatchConfig = filter_subjects(Bucket, Keys, WatchConfig0),

    ?LOG(debug, "Sid: ~p~nSubj: ~p~nWatchConfig: ~p~n", [Sid, DeliverSubject, WatchConfig]),
    {ok, Watch} =
        nats_consumer:create(Conn, ?BUCKET_NAME(Bucket), #{config => WatchConfig}, Opts),

    Cb = maps:get(cb, WatchOpts, fun default_cb/3),
    InitCnt = maps:get(num_pending, Watch, 0),
    Data0 = #data{
              owner = Owner,
              subj_pre = ?SUBJECT_NAME(Bucket, <<>>),
              conn = Conn,
              watch = Watch,
              sid = Sid,
              cb = Cb,
              ignore_deletes = maps:get(ignore_deletes, WatchOpts, false),
              init_cnt = InitCnt
             },

    case Cb({init, Owner}, Conn, InitCnt) of
        {continue, CbState} ->
            Data = Data0#data{cb_state = CbState},
            State = if InitCnt > 0 -> init;
                       true        -> watching
                    end,
            {ok, State, Data};
        {stop, Reason} ->
            {stop, Reason}
    end.

handle_event(enter, _, watching, #data{conn = Conn,
                                       cb = Cb, cb_state = CbStateIn} = Data) ->
    case Cb(init_done, Conn, CbStateIn) of
        {continue, CbStateOut} ->
            {keep_state, Data#data{cb_state = CbStateOut}};
        {stop, Reason} ->
            {stop, Reason}
    end;

handle_event(enter, _, _, _Data) ->
    keep_state_and_data;

handle_event(info, {Conn, Sid, Msg0}, State, #data{conn = Conn, sid = Sid} = Data) ->
    Msg = parse_msg(Msg0, Data),
    handle_message(Msg, State, Data).

terminate(_Reason, _State,
          #data{conn = Conn,
                watch = #{config := #{deliver_subject := DeliverSubject}} = Watch}) ->
    nats:unsub(Conn, DeliverSubject),
    nats_consumer:delete(Conn, Watch),
    ok.

code_change(_OldVsn, State, Data, _Extra) ->
    {ok, State, Data}.

handle_message({msg, _, _, #{status := 100, reply_to := ReplyTo}} = _Msg,
               _State, #data{conn = Conn}) ->
    %% FlowControl Request
    nats:pub(Conn, ReplyTo),
    keep_state_and_data;
handle_message({msg, _, <<>>,
                #{status := 100, description := <<"Idle", _/binary>>,
                  header := _Header} = Opts} = _Msg,
               _State, #data{conn = _Conn})
  when not is_map_key(reply_to, Opts) ->
    ?LOG(critical, "HeartBeat Message: ~0p", [_Msg]),
    keep_state_and_data;
handle_message({msg, _, <<>>,
                #{status := 100, description := <<"Flow", _/binary>>,
                  header := _Header} = Opts} = _Msg,
               _State, #data{conn = _Conn})
  when not is_map_key(reply_to, Opts) ->
    ?LOG(critical, "Flow control Message: ~0p", [_Msg]),
    keep_state_and_data;
handle_message({msg, _, <<>>, #{status := 100, header := _Header} = Opts} = _Msg,
               _State, _Data)
  when not is_map_key(reply_to, Opts) ->
    ?LOG(critical, "unexpected control Message: ~0p", [_Msg]),
    keep_state_and_data;

handle_message({msg, _, _, MsgOpts} = Msg,
               State,
               #data{conn = Conn, cb = Cb, cb_state = CbStateIn, init_cnt = InitCnt} = Data) ->
    Headers = maps:get(header, MsgOpts, []),
    KvOp = proplists:get_value(?KV_OP, Headers, none),

    Next =
        if Data#data.ignore_deletes andalso (KvOp =:= ?KV_DEL orelse KvOp =:= ?KV_PURGE) ->
                {continue, CbStateIn};
           true ->
                Cb(Msg, Conn, CbStateIn)
        end,
    case Next of
        {continue, CbStateOut} ->
            case {State, InitCnt} of
                {init, Cnt} when Cnt > 1 ->
                    {keep_state, Data#data{cb_state = CbStateOut, init_cnt = Cnt - 1}};
                {init, 1} ->
                    {next_state, watching, Data#data{cb_state = CbStateOut,
                                                     init_cnt = undefined}}
            end;
        {stop, Reason} ->
            {stop, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

filter_subjects(Bucket, Key, WatchConfig)
  when is_binary(Key) ->
    WatchConfig#{filter_subject => ?SUBJECT_NAME(Bucket, Key)};
filter_subjects(Bucket, Keys, WatchConfig)
  when is_list(Keys) ->
    WatchConfig#{filter_subjects => [?SUBJECT_NAME(Bucket, Key) || Key <- Keys]}.

parse_msg({msg, Subject, Value, Opts}, #data{subj_pre = Pre}) ->
    Key = case Subject of
              <<Pre:(byte_size(Pre))/binary, K/binary>> -> K;
              _ -> Subject
          end,
    {msg, Key, Value, parse_opts(Opts)}.

parse_opts(#{header := <<"NATS/1.0\r\n", HdrStr/binary>>} = Opts) ->
    Header = nats_hd:parse_headers(HdrStr),
    Opts#{header := Header};
parse_opts(#{header := <<"NATS/1.0 ", Status:3/bytes, Rest/binary>>} = Opts0) ->
    Opts = Opts0#{status => binary_to_integer(Status),
                  header := []},
    case binary:split(Rest, ~"\r\n") of
        [Description, More] ->
            Header = nats_hd:parse_headers(More),
            Opts#{description => string:trim(Description), header := Header};
        _Other ->
            Opts#{description => string:trim(Rest)}
    end;
parse_opts(Opts) ->
    Opts.

default_cb({init, Owner}, _Conn, _) ->
    {continue, Owner};
default_cb(init_done, Conn, Owner) ->
    Owner ! {init_done, self(), Conn},
    {continue, Owner};
default_cb({msg, _, _, _} = Msg, Conn, Owner) ->
    Owner ! {'WATCH', self(), Conn, Msg},
    {continue, Owner}.
