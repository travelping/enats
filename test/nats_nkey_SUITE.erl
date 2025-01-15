%% Copyright 2025, Travelping GmbH <info@travelping.com>
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

-module(nats_nkey_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [decode, from_seed, from_invalid_seed,
     %% from_seed_valid_prefix  %% seeds copied from gnats, but have broken CRCs and gnats has broken CRC checks....
     sign
    ].

init_per_suite(Config) ->
    Level = ct:get_config(log_level, info),
    _ = logger:set_primary_config(level, Level),
    application:ensure_started(enats),
    Config.

end_per_suite(Config) ->
    Config.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

decode() ->
    [{doc, "roperly decodes a key used by golang tests"}].
decode(_) ->
    Public = ~"AACKDD7DWAJM2K76WMDHTHTIN2WZLKA7MGSLNHIHSZ3ZRSEBZG6GWECF",

    {ok, Decoded} = nats_nkey:decode_base32(Public),
    Encoded = nats_nkey:encode_base32(Decoded),

    ?assertEqual(Public, Encoded).

from_seed() ->
    [{doc, "creates a struct from a valid seed"}].
from_seed(_) ->
    ?assertMatch({ok, _}, nats_nkey:from_seed(
                            ~"SUAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU")).

from_invalid_seed() ->
    [{doc, "should raise error when seed is invalid in any way"}].
from_invalid_seed(_) ->
    %% pad padding
    ?assertMatch({error, invalid_base32},
                 nats_nkey:from_seed(
                   ~"UAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU")),
    ?assertMatch({error, invalid_crc},
                 nats_nkey:from_seed(
                   ~"AUAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU")),
    ?assertMatch({error, invalid_seed}, nats_nkey:from_seed(~"")),
    ?assertMatch({error, invalid_base32}, nats_nkey:from_seed(~" ")).

from_seed_valid_prefix() ->
    [{doc, "hould validate prefix bytes"}].
from_seed_valid_prefix(_) ->
    Seeds =
        [~"SNAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU",
         ~"SCAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU",
         ~"SOAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU",
         ~"SUAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU"],
    lists:foreach(
      fun(X) ->
              ct:pal("Seed: ~s", [X]),
              ?assertMatch({ok, _}, nats_nkey:from_seed(X))
      end, Seeds).

sign() ->
    [{doc, "sign nonces"}].
sign(_) ->
    %% seed, nonces and signatures copies from gnats.
    %% Note: gnats is inconsistent with its use of url_encode64

    Seed = ~"SUAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU",
    {ok, NKey} = nats_nkey:from_seed(Seed),

    Nonce1 = ~"PXoWU7zWAMt75FY",
    Signed1 = nats_nkey:sign(NKey, Nonce1),
    Encoded1 = base64:encode(Signed1),

    ?assertEqual(
       ~"ZaAiVDgB5CeYoXoQ7cBCmq+ZllzUnGUoDVb8C7PilWvCs8XKfUchAUhz2P4BYAF++Dg3w05CqyQFRDiGL6LrDw==",
       Encoded1),

    Nonce2 = ~"iBFByN3zQjAT7dQ",
    Signed2 = nats_nkey:sign(NKey, Nonce2),
    Encoded2 = base64:encode(Signed2, #{mode => urlsafe}),

    ?assertEqual(
       ~"kagPGrixaWS5yuHqw9nTQrda1Q376fK3fRCGtYdF4_w2aSk-4O7Ca0JM0qvzm69HH6MoMps2yF6Q0Qs830JZCA==",
       Encoded2).
