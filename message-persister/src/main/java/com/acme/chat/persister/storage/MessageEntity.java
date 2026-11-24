package com.acme.chat.persister.storage;

import com.acme.chat.common.id.SnowflakeIdGenerator;
import com.acme.chat.domain.ChatMessage;
import java.util.UUID;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

@Table("messages_by_conversation")
public class MessageEntity {

    @PrimaryKey
    private final MessageKey key;

    @Column("sender_id")
    private final long senderId;

    @Column("client_msg_id")
    private final UUID clientMsgId;

    private final byte type;
    private final String body;

    @Column("media_ref")
    private final String mediaRef;

    @Column("reply_to_id")
    private final Long replyToId;

    private final boolean deleted;

    public MessageEntity(MessageKey key, long senderId, UUID clientMsgId, byte type, String body,
                         String mediaRef, Long replyToId, boolean deleted) {
        this.key = key;
        this.senderId = senderId;
        this.clientMsgId = clientMsgId;
        this.type = type;
        this.body = body;
        this.mediaRef = mediaRef;
        this.replyToId = replyToId;
        this.deleted = deleted;
    }

    public static MessageEntity from(ChatMessage m) {
        var key = new MessageKey(m.conversationId(), SnowflakeIdGenerator.bucketDate(m.messageId()), m.messageId());
        return new MessageEntity(key, m.senderId(), m.clientMsgId(), (byte) m.type().ordinal(), m.body(),
                m.mediaRef(), m.replyToId(), false);
    }

    public MessageKey getKey() { return key; }
    public long getSenderId() { return senderId; }
    public UUID getClientMsgId() { return clientMsgId; }
    public byte getType() { return type; }
    public String getBody() { return body; }
    public String getMediaRef() { return mediaRef; }
    public Long getReplyToId() { return replyToId; }
    public boolean isDeleted() { return deleted; }
}
