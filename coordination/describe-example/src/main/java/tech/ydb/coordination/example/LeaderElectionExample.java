package tech.ydb.coordination.example;

import java.net.InetAddress;
import java.net.UnknownHostException;

import tech.ydb.coordination.CoordinationClient;
import tech.ydb.coordination.recipes.leader_election.LeaderElection;
import tech.ydb.core.grpc.GrpcTransport;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class LeaderElectionExample {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: java -jar jdbc-coordination-describe.jar <connection_url>");
            return;
        }

        try (GrpcTransport transport = GrpcTransport.forConnectionString(args[0]).build()) {
            CoordinationClient client = CoordinationClient.newClient(transport);

            LeaderElection election = new LeaderElection(
                    client,
                    "/example/leader",
                    "leaderSemaphore",
                    InetAddress.getLocalHost().getHostName().getBytes()
            );

            election.start(new LeaderElection.Listener() {
                @Override
                public void takeLeadership() {
                }

                @Override
                public void suspendLeadership() {
                }

                @Override
                public void resumeLeadership() {
                }

                @Override
                public void lostLeadership() {
                }
            });

            // wait for app finish
            // ...

            // close election and release leadership
            election.close();
        } catch (UnknownHostException ex) {
            ex.printStackTrace(System.err);
        }
    }

}
