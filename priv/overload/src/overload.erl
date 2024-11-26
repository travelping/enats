-module(overload).

-export([overload/2, load/0, run/2]).

-define(KV_BUCKET, ~"SESSIONS").
-define(DEPTH, 6).

permutations(Depth, Items) ->
    permutations(Depth, Items, [[]]).

permutations(0, _Items, Acc) ->
    Acc;
permutations(Depth, Items, Acc) ->
    permutations(Depth - 1, Items, [ [A|B] || A <- Items, B <- Acc]).

overload(Iter, Threads) ->
    load(),
    run(Iter, Threads).

load() ->
    application:ensure_all_started([enats, plists]),

    Permutations = permutations(?DEPTH, lists:seq(0, 9)),
    Subjects = [begin
                    K = iolist_to_binary(
                          lists:join($., [<<(integer_to_binary(A))/binary, "foobar">> || A <- X])),
                    UUID = uuid:uuid_to_string(uuid:get_v5(K), binary_standard),
                    <<UUID/binary, $., K/binary>>
                end || X <- Permutations],

    {ok, C} = nats:connect({127,0,0,1}, 4222, #{verbose => false}),

    Config = #{buffer_size => -1, deny_delete => false},
    %% nats_kv:create_bucket(C, ?KV_BUCKET, Config#{storage => memory}, #{}),
    nats_kv:create_bucket(C, ?KV_BUCKET, Config, #{}),

    plists:foreach(
      fun(K) -> nats_kv:put(C, ?KV_BUCKET, K, <<>>) end, Subjects, [{processes, 1000}]),
    ok.

run(Iter, Threads) ->
    {ok, C} = nats:connect({127,0,0,1}, 4222, #{verbose => false}),
    Self = self(),

    Pids = [proc_lib:spawn_link(fun() -> run_fun(Self, C, Iter) end) || _ <- lists:seq(1, Threads)],
    [ receive
          {P, ready} -> ok
      after 100 -> io:format("~p failed~n", [P]), erlang:exit(1)
      end
      || P <- Pids ],

    [ P ! run || P <- Pids ],

    Times0 =
        [ receive
              {P, done, Time} -> Time
          end
          || P <- Pids ],
    Times = lists:sort(Times0),
    Min = hd(Times) / 1000,
    Max = lists:last(Times) / 1000,
    Avg = lists:sum(Times) / Threads / 1000,
    io:format("min: ~w ms / ~f ms/op, max: ~w ms / ~f ms/op, avg: ~w ms / ~f ms/op~n",
              [Min, Min/Iter, Max, Max/Iter, Avg, Avg/Iter]),
    ok.

run_fun(Owner, C, Iter) ->
    K = [rand:uniform(10) - 1 || _ <- lists:seq(1, ?DEPTH)],
    Key = iolist_to_binary(
            lists:join($., [<<(integer_to_binary(A))/binary, "foobar">> || A <- K])),

    Owner ! {self(), ready},

    receive run -> ok end,
    {Time, _} = timer:tc(fun() -> select(C, <<"*.", Key/binary>>, Iter), ok end),

    Owner ! {self(), done, Time},
    ok.

select(_C, _Subj, 0) ->
    ok;
select(C, Subj, Iter) ->
    nats_kv:select_keys(C, ?KV_BUCKET, Subj, #{}, #{}),
    select(C, Subj, Iter - 1).
