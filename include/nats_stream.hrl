-ifndef(NATS_STREAM_HRL).
-define(NATS_STREAM_HRL, true).

%% yes, the lines are too long, but breaking them is worse
-elvis([{elvis_text_style, line_length, disable}]).

-define(JS_API_V1_ACCOUNT_INFO_RESPONSE, 'io.nats.jetstream.api.v1.account_info_response').
-define(JS_API_V1_ACCOUNT_PURGE_RESPONSE, 'io.nats.jetstream.api.v1.account_purge_response').
-define(JS_API_V1_CONSUMER_CREATE_RESPONSE, 'io.nats.jetstream.api.v1.consumer_create_response').
-define(JS_API_V1_CONSUMER_DELETE_RESPONSE, 'io.nats.jetstream.api.v1.consumer_delete_response').
-define(JS_API_V1_CONSUMER_INFO_RESPONSE, 'io.nats.jetstream.api.v1.consumer_info_response').
-define(JS_API_V1_CONSUMER_LEADER_STEPDOWN_RESPONSE, 'io.nats.jetstream.api.v1.consumer_leader_stepdown_response').
-define(JS_API_V1_CONSUMER_LIST_RESPONSE, 'io.nats.jetstream.api.v1.consumer_list_response').
-define(JS_API_V1_CONSUMER_NAMES_RESPONSE, 'io.nats.jetstream.api.v1.consumer_names_response').
-define(JS_API_V1_CONSUMER_PAUSE_RESPONSE, 'io.nats.jetstream.api.v1.consumer_pause_response').
-define(JS_API_V1_META_LEADER_STEPDOWN_RESPONSE, 'io.nats.jetstream.api.v1.meta_leader_stepdown_response').
-define(JS_API_V1_META_SERVER_REMOVE_RESPONSE, 'io.nats.jetstream.api.v1.meta_server_remove_response').
-define(JS_API_V1_STREAM_CREATE_RESPONSE, 'io.nats.jetstream.api.v1.stream_create_response').
-define(JS_API_V1_STREAM_DELETE_RESPONSE, 'io.nats.jetstream.api.v1.stream_delete_response').
-define(JS_API_V1_STREAM_INFO_RESPONSE, 'io.nats.jetstream.api.v1.stream_info_response').
-define(JS_API_V1_STREAM_LEADER_STEPDOWN_RESPONSE, 'io.nats.jetstream.api.v1.stream_leader_stepdown_response').
-define(JS_API_V1_STREAM_LIST_RESPONSE, 'io.nats.jetstream.api.v1.stream_list_response').
-define(JS_API_V1_STREAM_MSG_DELETE_RESPONSE, 'io.nats.jetstream.api.v1.stream_msg_delete_response').
-define(JS_API_V1_STREAM_MSG_GET_RESPONSE, 'io.nats.jetstream.api.v1.stream_msg_get_response').
-define(JS_API_V1_STREAM_NAMES_RESPONSE, 'io.nats.jetstream.api.v1.stream_names_response').
-define(JS_API_V1_STREAM_PURGE_RESPONSE, 'io.nats.jetstream.api.v1.stream_purge_response').
-define(JS_API_V1_STREAM_REMOVE_PEER_RESPONSE, 'io.nats.jetstream.api.v1.stream_remove_peer_response').
-define(JS_API_V1_STREAM_RESTORE_RESPONSE, 'io.nats.jetstream.api.v1.stream_restore_response').
-define(JS_API_V1_STREAM_SNAPSHOT_RESPONSE, 'io.nats.jetstream.api.v1.stream_snapshot_response').
-define(JS_API_V1_STREAM_TEMPLATE_CREATE_RESPONSE, 'io.nats.jetstream.api.v1.stream_template_create_response').
-define(JS_API_V1_STREAM_TEMPLATE_DELETE_RESPONSE, 'io.nats.jetstream.api.v1.stream_template_delete_response').
-define(JS_API_V1_STREAM_TEMPLATE_INFO_RESPONSE, 'io.nats.jetstream.api.v1.stream_template_info_response').
-define(JS_API_V1_STREAM_TEMPLATE_NAMES_RESPONSE, 'io.nats.jetstream.api.v1.stream_template_names_response').
-define(JS_API_V1_STREAM_UPDATE_RESPONSE, 'io.nats.jetstream.api.v1.stream_update_response').


%% JetStream API errors

-define(JS_ERR_CODE_JET_STREAM_NOT_ENABLED_FOR_ACCOUNT, 10039).
-define(JS_ERR_CODE_JET_STREAM_NOT_ENABLED,             10076).

-define(JS_ERR_CODE_STREAM_NOT_FOUND,   10059).
-define(JS_ERR_CODE_STREAM_NAME_IN_USE, 10058).

-define(JS_ERR_CODE_CONSUMER_CREATE,             10012).
-define(JS_ERR_CODE_CONSUMER_NOT_FOUND,          10014).
-define(JS_ERR_CODE_CONSUMER_NAME_EXISTS,        10013).
-define(JS_ERR_CODE_CONSUMER_ALREADY_EXISTS,     10105).
-define(JS_ERR_CODE_CONSUMER_EXISTS,             10148).
-define(JS_ERR_CODE_DUPLICATE_FILTER_SUBJECTS,   10136).
-define(JS_ERR_CODE_OVERLAPPING_FILTER_SUBJECTS, 10138).
-define(JS_ERR_CODE_CONSUMER_EMPTY_FILTER,       10139).
-define(JS_ERR_CODE_CONSUMER_DOES_NOT_EXIST,     10149).

-define(JS_ERR_CODE_MESSAGE_NOT_FOUND, 10037).

-define(JS_ERR_CODE_BAD_REQUEST, 10003).

-define(JS_ERR_CODE_STREAM_WRONG_LAST_SEQUENCE, 10071).

-endif.
