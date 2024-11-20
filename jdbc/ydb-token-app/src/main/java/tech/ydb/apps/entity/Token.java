package tech.ydb.apps.entity;

import java.io.Serializable;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Type;
import org.springframework.data.domain.Persistable;

/**
 *
 * @author Aleksandr Gorshenin
 */
@Entity
@DynamicUpdate
@Table(name = "app_token")
public class Token implements Serializable, Persistable<UUID> {
    private static final long serialVersionUID = -1533981732225785339L;

    @Id
    @Type(type="uuid-char")
    private UUID id;

    @Column
    private String username;

    @Column
    private Integer version;

    @Transient
    private final boolean isNew;

    @Override
    public UUID getId() {
        return this.id;
    }

    public String getUserName() {
        return this.username;
    }

    public Integer getVersion() {
        return this.version;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public Token() {
        this.isNew = false;
    }

    public Token(String username) {
        this.id = getKey(username);
        this.username = username;
        this.version = 1;
        this.isNew = true;
    }

    public void incVersion() {
        this.version ++;
    }

    @Override
    public String toString() {
        return "Token{id=" + id.toString() + ", username='" + username + "', version=" + version + "}";
    }

    public static UUID getKey(String username) {
        // UUID based on MD5 hash
        return UUID.nameUUIDFromBytes((username + "_v").getBytes());
    }
}
