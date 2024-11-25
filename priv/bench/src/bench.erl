-module(bench).

-export([bench/0, load/0, run/0]).

permutations(Depth, Items) ->
    permutations(Depth, Items, [[]]).

permutations(0, _Items, Acc) ->
    Acc;
permutations(Depth, Items, Acc) ->
    permutations(Depth - 1, Items, [ [A|B] || A <- Items, B <- Acc]).

bench() ->
    load(),
    run().

load() ->
    application:ensure_all_started([enats, plists]),

    Permutations = permutations(6, lists:seq(0, 9)),
    Subjects = [ iolist_to_binary(lists:join($., [<<(integer_to_binary(A))/binary, "foobar">> || A <- X])) || X <- Permutations],

    {ok, C} = nats:connect({127,0,0,1}, 4222, #{verbose => false}),
    %% nats_kv:create_bucket(C, ~"SESSIONS", #{storage => memory}, #{}),
    nats_kv:create_bucket(C, ~"SESSIONS", #{}, #{}),

    plists:foreach(
      fun(K) -> nats_kv:put(C, ~"SESSIONS", K, <<>>) end, Subjects, [{processes, 1000}]),
    ok.


run() ->
    {ok, C} = nats:connect({127,0,0,1}, 4222, #{verbose => false}),

    select(C, ~"0foobar.0foobar.0foobar.0foobar.0foobar.0foobar", 1000),
    select(C, ~"*.0foobar.0foobar.0foobar.0foobar.0foobar", 10),
    select(C, ~"0foobar.*.0foobar.0foobar.0foobar.0foobar", 10),
    select(C, ~"0foobar.0foobar.*.0foobar.0foobar.0foobar", 10),
    select(C, ~"0foobar.0foobar.0foobar.*.0foobar.0foobar", 10),
    select(C, ~"0foobar.0foobar.0foobar.0foobar.*.0foobar", 10),
    select(C, ~"0foobar.0foobar.0foobar.0foobar.0foobar.*", 10),
    ok.

select_fun(_C, _Subj, 0) ->
    ok;
select_fun(C, Subj, Cnt) ->
    nats_kv:select_keys(C, ~"SESSIONS", Subj, #{}),
    select_fun(C, Subj, Cnt - 1).

select(C, Subj, Cnt) ->
    {Time1, _} = timer:tc(fun() -> select_fun(C, Subj, Cnt) end),
    io:format("first:  ~-60s ~f ms, ~w ops, ~f ms/op~n", [Subj, Time1 / 1000, Cnt, Time1 / Cnt / 1000]),
    {Time2, _} = timer:tc(fun() -> select_fun(C, Subj, Cnt) end),
    io:format("second: ~-60s ~f ms, ~w ops, ~f ms/op~n~n", [Subj, Time2 / 1000, Cnt, Time2 / Cnt / 1000]),
    ok.
