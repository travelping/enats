%% one minute in nanoseconds (10^-9)
-define(MINUTE_NS, 60 * 1_000_000_000).
-define(BUCKET_NAME(Bucket), [~"KV_", Bucket]).
-define(SUBJECT_NAME(Bucket), [~"$KV.", Bucket]).
-define(SUBJECT_NAME(Bucket, KeyPart), [~"$KV.", Bucket, $., KeyPart]).

%% Headers for published messages.
-define(MSG_ID_HDR,                 ~"Nats-Msg-Id").
-define(EXPECTED_STREAM_HDR,        ~"Nats-Expected-Stream").
-define(EXPECTED_LAST_SEQ_HDR,      ~"Nats-Expected-Last-Sequence").
-define(EXPECTED_LAST_SUBJ_SEQ_HDR, ~"Nats-Expected-Last-Subject-Sequence").
-define(EXPECTED_LAST_MSG_IDu_Hdr,   ~"Nats-Expected-Last-Msg-Id").
-define(MSG_ROLLUP, ~"Nats-Rollup").

%% Rollups, can be subject only or all messages.
-define(MSG_ROLLUP_ALL, ~"all").
-define(MSG_ROLLUP_SUBJECT, ~"sub").

%% K/V operations
-define(KV_OP,    ~"KV-Operation").
-define(KV_DEL,   ~"DEL").
-define(KV_PURGE, ~"PURGE").
