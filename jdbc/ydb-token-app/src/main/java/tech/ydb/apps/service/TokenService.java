package tech.ydb.apps.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import tech.ydb.apps.annotation.YdbRetryable;
import tech.ydb.apps.entity.Token;
import tech.ydb.apps.repo.TokenRepository;

/**
 *
 * @author Aleksandr Gorshenin
 */
@Service
public class TokenService {
    private final static Logger logger = LoggerFactory.getLogger(TokenService.class);
    private final TokenRepository repository;

    public TokenService(TokenRepository repository) {
        this.repository = repository;
    }

    private UUID getKey(int id) {
        return Token.getKey("user_" + id);
    }

    @YdbRetryable
    @Transactional
    public void insertBatch(int firstID, int lastID) {
        List<Token> batch = new ArrayList<>();
        for (int id = firstID; id < lastID; id++) {
            batch.add(new Token("user_" + id));
        }
        repository.saveAll(batch);
    }

    @YdbRetryable
    @Transactional
    public Token fetchToken(int id) {
        Optional<Token> token = repository.findById(getKey(id));

        if (!token.isPresent()) {
            logger.warn("token {} is not found", id);
            return null;
        }

        return token.get();
    }

    @YdbRetryable
    @Transactional
    public void updateToken(int id) {
        Token token = fetchToken(id);
        if (token != null) {
            token.incVersion();
            repository.save(token);
            logger.trace("updated token {} -> {}", id, token.getVersion());
        }
    }

    @YdbRetryable
    @Transactional
    public void updateBatch(List<Integer> ids) {
        List<UUID> uuids = ids.stream().map(this::getKey).collect(Collectors.toList());

        Iterable<Token> batch = repository.findAllById(uuids);
        for (Token token: batch) {
            logger.trace("update token {}", token);
            token.incVersion();
        }

        repository.saveAllAndFlush(batch);
    }

    @YdbRetryable
    @Transactional
    public void removeBatch(List<Integer> ids) {
        List<UUID> uuids = ids.stream().map(this::getKey).collect(Collectors.toList());
        repository.deleteAllByIdInBatch(uuids);
    }

    @YdbRetryable
    @Transactional
    public void listManyRecords() {
        long count = 0;
        for (String id : repository.scanFindAll()) {
            count ++;
            if (count % 1000 == 0) {
                logger.info("scan readed {} records", count);
            }
        }
    }
}
