package tech.ydb.apps.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import tech.ydb.apps.annotation.YdbRetryable;
import tech.ydb.apps.entity.Token;
import tech.ydb.apps.entity.TokenLog;
import tech.ydb.apps.repo.TokenLogRepository;
import tech.ydb.apps.repo.TokenRepository;

/**
 *
 * @author Aleksandr Gorshenin
 */
@Service
public class WorkloadService {
    private final static Logger logger = LoggerFactory.getLogger(WorkloadService.class);
    private final TokenRepository tokenRepo;
    private final TokenLogRepository tokenLogRepo;

    public WorkloadService(TokenRepository tokenRepo, TokenLogRepository tokenLogRepo) {
        this.tokenRepo = tokenRepo;
        this.tokenLogRepo = tokenLogRepo;
    }

    private UUID getKey(int id) {
        return Token.getKey("user_" + id);
    }

    @YdbRetryable
    @Transactional
    public void loadData(int firstID, int lastID) {
        List<Token> batch = new ArrayList<>();
        for (int id = firstID; id < lastID; id++) {
            batch.add(new Token("user_" + id));
        }
        tokenRepo.saveAll(batch);
    }

    @YdbRetryable
    @Transactional
    public Token fetchToken(int id) {
        Optional<Token> token = tokenRepo.findById(getKey(id));

        if (!token.isPresent()) {
            logger.warn("token {} is not found", id);
            return null;
        }

        return token.get();
    }

    @YdbRetryable
    @Transactional
    public void updateToken(int id, long counter) {
        Token token = fetchToken(id);
        if (token != null) {
            token.incVersion();
            tokenRepo.save(token);
            tokenLogRepo.save(new TokenLog(token, counter));
            logger.trace("updated token {} -> {}", id, token.getVersion());
        } else {
            logger.warn("token {} is not found", id);
        }
    }

    @YdbRetryable
    @Transactional
    public void updateBatch(Set<Integer> ids, long counterFrom) {
        List<UUID> uuids = ids.stream().map(this::getKey).collect(Collectors.toList());

        Iterable<Token> batch = tokenRepo.findAllById(uuids);
        List<TokenLog> logs = new ArrayList<>();
        for (Token token: batch) {
            logger.trace("update token {}", token);
            token.incVersion();
            logs.add(new TokenLog(token, counterFrom++));
        }

        tokenRepo.saveAll(batch);
        tokenLogRepo.saveAll(logs);
    }

    @YdbRetryable
    @Transactional
    public void removeBatch(List<Integer> ids) {
        List<UUID> uuids = ids.stream().map(this::getKey).collect(Collectors.toList());
        tokenRepo.deleteAllByIdInBatch(uuids);
    }

    @YdbRetryable
    public long readLastGlobalVersion() {
        return tokenLogRepo.findTopGlobalVersion().orElse(0l);
    }
}
