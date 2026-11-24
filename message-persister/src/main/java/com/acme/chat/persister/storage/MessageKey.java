package com.acme.chat.persister.storage;

import java.util.Objects;
import org.springframework.data.cassandra.core.cql.Ordering;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyClass;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;

/** Partition = (conversation_id, bucket_date) keeps partitions bounded; clustering message_id DESC. */
@PrimaryKeyClass
public class MessageKey {

    @PrimaryKeyColumn(name = "conversation_id", ordinal = 0, type = PrimaryKeyType.PARTITIONED)
    private final long conversationId;

    @PrimaryKeyColumn(name = "bucket_date", ordinal = 1, type = PrimaryKeyType.PARTITIONED)
    private final int bucketDate;

    @PrimaryKeyColumn(name = "message_id", ordinal = 2, type = PrimaryKeyType.CLUSTERED,
            ordering = Ordering.DESCENDING)
    private final long messageId;

    public MessageKey(long conversationId, int bucketDate, long messageId) {
        this.conversationId = conversationId;
        this.bucketDate = bucketDate;
        this.messageId = messageId;
    }

    public long getConversationId() { return conversationId; }
    public int getBucketDate() { return bucketDate; }
    public long getMessageId() { return messageId; }

    @Override
    public boolean equals(Object o) {
        return o instanceof MessageKey k && k.conversationId == conversationId
                && k.bucketDate == bucketDate && k.messageId == messageId;
    }

    @Override
    public int hashCode() { return Objects.hash(conversationId, bucketDate, messageId); }
}
