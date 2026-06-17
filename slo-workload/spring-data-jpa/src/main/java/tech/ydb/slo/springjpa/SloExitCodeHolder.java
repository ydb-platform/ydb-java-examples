package tech.ydb.slo.springjpa;

import org.springframework.boot.ExitCodeGenerator;
import org.springframework.stereotype.Component;

@Component
public class SloExitCodeHolder implements ExitCodeGenerator {
    private int exitCode;

    void setExitCode(int exitCode) {
        this.exitCode = exitCode;
    }

    @Override
    public int getExitCode() {
        return exitCode;
    }
}
