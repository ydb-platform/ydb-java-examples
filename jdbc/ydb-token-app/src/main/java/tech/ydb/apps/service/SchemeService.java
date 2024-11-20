package tech.ydb.apps.service;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import javax.persistence.EntityManager;

import com.google.common.io.CharStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import tech.ydb.apps.annotation.YdbRetryable;

/**
 *
 * @author Aleksandr Gorshenin
 */
@Service
public class SchemeService {
    private static final Logger logger = LoggerFactory.getLogger(SchemeService.class);

    private final EntityManager em;
    private final ResourceLoader rl;

    public SchemeService(EntityManager em, ResourceLoader rl) {
        this.em = em;
        this.rl = rl;
    }

    @Transactional
    @YdbRetryable
    public void executeClean() {
        String script = readResourceFile("sql/drop.sql");
        if (script == null) {
            logger.warn("cannot find drop sql in classpath");
            return;
        }

        em.createNativeQuery(script).executeUpdate();
    }

    @Transactional
    @YdbRetryable
    public void executeInit() {
        String script = readResourceFile("sql/init.sql");
        if (script == null) {
            logger.warn("cannot find init sql in classpath");
            return;
        }

        em.createNativeQuery(script).executeUpdate();
    }

    private String readResourceFile(String location) {
        Resource resource = rl.getResource("classpath:" + location);
        try (InputStream is = resource.getInputStream()) {
            return CharStreams.toString(new InputStreamReader(is, StandardCharsets.UTF_8));
        } catch (IOException e) {
            return null;
        }
    }
}
