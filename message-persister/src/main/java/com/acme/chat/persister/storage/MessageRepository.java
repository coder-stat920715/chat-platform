package com.acme.chat.persister.storage;

import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.ReactiveCassandraRepository;
import reactor.core.publisher.Flux;

public interface MessageRepository extends ReactiveCassandraRepository<MessageEntity, MessageKey> {

    /** Keyset pagination inside one partition: newest first, strictly older than the cursor. */
    @Query("SELECT * FROM messages_by_conversation WHERE conversation_id = ?0 AND bucket_date = ?1 "
            + "AND message_id < ?2 LIMIT ?3")
    Flux<MessageEntity> pageBefore(long conversationId, int bucketDate, long beforeId, int limit);
}
