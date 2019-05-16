package tech.ydb.examples.basic_example.exceptions;

import java.util.Arrays;

import tech.ydb.core.Status;


/**
 * @author Sergey Polovko
 */
public class NonRetriableErrorException extends RuntimeException {

    public NonRetriableErrorException(Status status) {
        super("code: " + status.getCode() + ", issues: " + Arrays.toString(status.getIssues()));
    }
}
