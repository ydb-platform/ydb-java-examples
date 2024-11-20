package tech.ydb.apps.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.SQLRecoverableException;

import org.springframework.core.annotation.AliasFor;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;

/**
 * @author Aleksandr Gorshenin
 * @see https://github.com/spring-projects/spring-retry/blob/main/README.md#further-customizations for details
 */
@Target({ ElementType.METHOD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Retryable(
        retryFor = SQLRecoverableException.class,
        maxAttempts = 5,
        backoff = @Backoff(delay = 100, multiplier = 2.0, maxDelay = 5000, random = true)
)
public @interface YdbRetryable {
    @AliasFor(annotation = Retryable.class, attribute = "recover")
    String recover() default "";

    @AliasFor(annotation = Retryable.class, attribute = "label")
    String label() default "";

    @AliasFor(annotation = Retryable.class, attribute = "stateful")
    boolean stateful() default false;

    @AliasFor(annotation = Retryable.class, attribute = "listeners")
    String[] listeners() default {};
}
