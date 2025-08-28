package tech.ydb.apps.entity;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.hibernate.annotations.DynamicUpdate;
import org.springframework.data.domain.Persistable;

/**
 *
 * @author Aleksandr Gorshenin
 */
@Entity
@DynamicUpdate
@Table(name = "app_token_log")
public class TokenLog implements Serializable, Persistable<String> {
    private static final long serialVersionUID = -3643491448443852677L;

    @Id
    private String id;

    @ManyToOne
    private Token token;

    @Column
    private Long globalVersion;

    @Column
    private Instant updatedAt;

    @Column
    private Integer updatedTo;

    @Transient
    private final boolean isNew;

    @Override
    public String getId() {
        return this.id;
    }

    public Token getToken() {
        return this.token;
    }

    public Long getGlobalVersion() {
        return this.globalVersion;
    }

    public Instant getUpdatedAt() {
        return this.updatedAt;
    }

    public Integer getUpdatedTo() {
        return this.updatedTo;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public TokenLog() {
        this.isNew = false;
    }

    public TokenLog(Token token, long version) {
        this.id = hash256(token.getId(), version);
        this.globalVersion = version;
        this.token = token;
        this.updatedAt = Instant.now();
        this.updatedTo = token.getVersion();
        this.isNew = true;
    }

    @Override
    public String toString() {
        return "TokenLog{version=" + globalVersion + ", token=" + token.getId() +
                ", updateAt='" + updatedAt + "', updateTo=" + updatedTo + "}";
    }

    private static String hash256(UUID uuid, long version) {
        Hasher hasher = Hashing.sha256().newHasher(24);
        hasher.putLong(uuid.getMostSignificantBits());
        hasher.putLong(uuid.getLeastSignificantBits());
        hasher.putLong(version);
        return hasher.hash().toString();
    }
}
