package com.acme.chat.persister.storage;

import com.acme.chat.common.id.SnowflakeIdGenerator;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlOperations;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
public class HistoryService {

    private final MessageRepository repo;
    private final ReactiveCqlOperations cql;

    public HistoryService(MessageRepository repo, ReactiveCassandraTemplate cassandra) {
        this.repo = repo;
        this.cql = cassandra.getReactiveCqlOperations();
    }

    /**
     * Newest-first keyset pagination across daily buckets. before<=0 means "latest".
     * Buckets are read lazily, so walking stops as soon as `limit` rows were produced.
     */
    public Flux<MessageEntity> page(long conversationId, long before, int limit) {
        int cap = Math.min(Math.max(limit, 1), 200);
        long beforeId = before <= 0 ? Long.MAX_VALUE : before;
        int startBucket = before <= 0
                ? SnowflakeIdGenerator.bucketDateOfMillis(System.currentTimeMillis())
                : SnowflakeIdGenerator.bucketDate(before);

        return cql.queryForFlux("SELECT bucket_date FROM conversation_buckets "
                        + "WHERE conversation_id = ? AND bucket_date <= ?", Integer.class, conversationId, startBucket)
                .concatMap(bucket -> repo.pageBefore(conversationId, bucket, beforeId, cap))
                .take(cap);
    }
}
