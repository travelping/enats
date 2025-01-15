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

%% PrefixByteSeed is the version byte used for encoded NATS Seeds
%% Base32-encodes to 'S...'
-define(NKEY_PREFIX_BYTE_SEED, 18).

%% PrefixBytePrivate is the version byte used for encoded NATS Private keys
%% Base32-encodes to 'P...'
-define(NKEY_PREFIX_BYTE_PRIVATE, 15).

%% PrefixByteServer is the version byte used for encoded NATS Servers
%% Base32-encodes to 'N...'
-define(NKEY_PREFIX_BYTE_SERVER, 13).

%% PrefixByteCluster is the version byte used for encoded NATS Clusters
%% Base32-encodes to 'C...'
-define(NKEY_PREFIX_BYTE_CLUSTER, 2).

%% PrefixByteOperator is the version byte used for encoded NATS Operators
%% Base32-encodes to 'O...'
-define(NKEY_PREFIX_BYTE_OPERATOR, 14).

%% PrefixByteAccount is the version byte used for encoded NATS Accounts
%% Base32-encodes to 'A...'
-define(NKEY_PREFIX_BYTE_ACCOUNT, 0).

%% PrefixByteUser is the version byte used for encoded NATS Users
%% Base32-encodes to 'U...'
-define(NKEY_PREFIX_BYTE_USER, 20).

%% PrefixByteCurve is the version byte used for encoded CurveKeys (X25519)
%% Base32-encodes to 'X...'
-define(NKEY_PREFIX_BYTE_CURVE, 23).

%% PrefixByteUnknown is for unknown prefixes.
%% Base32-encodes to 'Z...'
-define(NKEY_PREFIX_BYTE_UNKNOWN, 25).
