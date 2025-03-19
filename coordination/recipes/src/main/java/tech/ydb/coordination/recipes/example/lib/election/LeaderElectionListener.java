package tech.ydb.coordination.recipes.example.lib.election;

public interface LeaderElectionListener {
    void takeLeadership() throws Exception;
}
