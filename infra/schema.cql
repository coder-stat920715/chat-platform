CREATE KEYSPACE IF NOT EXISTS chat
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE chat;

CREATE TABLE IF NOT EXISTS messages_by_conversation (
  conversation_id bigint,
  bucket_date     int,
  message_id      bigint,
  sender_id       bigint,
  client_msg_id   uuid,
  type            tinyint,
  body            text,
  media_ref       text,
  reply_to_id     bigint,
  deleted         boolean,
  PRIMARY KEY ((conversation_id, bucket_date), message_id)
) WITH CLUSTERING ORDER BY (message_id DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS', 'compaction_window_size': '1'};

CREATE TABLE IF NOT EXISTS conversation_buckets (
  conversation_id bigint,
  bucket_date     int,
  PRIMARY KEY (conversation_id, bucket_date)
) WITH CLUSTERING ORDER BY (bucket_date DESC);

-- Offline buffer (fan-out-on-write conversations). 30-day TTL.
CREATE TABLE IF NOT EXISTS user_inbox (
  user_id         bigint,
  message_id      bigint,
  conversation_id bigint,
  PRIMARY KEY (user_id, message_id)
) WITH CLUSTERING ORDER BY (message_id ASC)
  AND default_time_to_live = 2592000;

-- Per-recipient receipts for small chats (optional, not written by this reference build).
CREATE TABLE IF NOT EXISTS message_status (
  conversation_id bigint,
  message_id      bigint,
  user_id         bigint,
  status          tinyint,
  updated_at      timestamp,
  PRIMARY KEY ((conversation_id, message_id), user_id)
);

-- Scalable read state: one row per user per conversation.
CREATE TABLE IF NOT EXISTS read_cursors (
  user_id           bigint,
  conversation_id   bigint,
  last_delivered_id bigint,
  last_read_id      bigint,
  PRIMARY KEY (user_id, conversation_id)
);
