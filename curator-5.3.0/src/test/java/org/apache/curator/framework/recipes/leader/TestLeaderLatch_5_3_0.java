package org.apache.curator.framework.recipes.leader;

import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.FailedServerStartException;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.test.compatibility.Timing2;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.BindException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLeaderLatch_5_3_0 {

    protected volatile TestingServer server;

    private static final int MAX_LOOPS = 5;

    @BeforeEach
    public void setup() throws Exception {
        try {
            createServer();
        } catch (FailedServerStartException ignore) {
            System.err.println("Failed to start server - retrying 1 more time");
            closeServer();
            createServer();
        }
    }

    protected void createServer() throws Exception {
        while (server == null) {
            try {
                server = new TestingServer();
            } catch (BindException e) {
                server = null;
                throw new FailedServerStartException("Getting bind exception - retrying to allocate server");
            }
        }
    }

    @AfterEach
    public void teardown() {
        closeServer();
    }

    private void closeServer() {
        if (server != null) {
            try {
                server.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                server = null;
            }
        }
    }

    @Test
    public void testLeadershipElectionWhenNodeDisappearsAfterChildrenAreRetrieved() throws Exception {
        final String latchPath = "/foo/bar";
        final Timing2 timing = new Timing2();
        final Duration pollInterval = Duration.ofMillis(100);
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1))) {
            client.start();
            LeaderLatch latchInitialLeader = new LeaderLatch(client, latchPath, "initial-leader");
            LeaderLatch latchCandidate0 = new LeaderLatch(client, latchPath, "candidate-0");
            LeaderLatch latchCandidate1 = new LeaderLatch(client, latchPath, "candidate-1");

            latchInitialLeader.start();

            // we want to make sure that the leader gets leadership before other instances are going to join the party
            waitForALeader(Collections.singletonList(latchInitialLeader), new Timing());
            // candidate #0 will wait for the leader to go away - this should happen after the child nodes are retrieved by candidate #0
            latchCandidate0.debugCheckLeaderShipLatch = new CountDownLatch(1);
            latchCandidate0.start();

            final int expectedChildrenAfterCandidate0Joins = 2;
            Awaitility.await("There should be " + expectedChildrenAfterCandidate0Joins + " child nodes created after candidate #0 joins the leader election.")
                    .pollInterval(pollInterval)
                    .pollInSameThread()
                    .until(() -> client.getChildren().forPath(latchPath).size() == expectedChildrenAfterCandidate0Joins);
            // no extra CountDownLatch needs to be set here because candidate #1 will rely on candidate #0
            latchCandidate1.start();

            final int expectedChildrenAfterCandidate1Joins = 3;
            Awaitility.await("There should be " + expectedChildrenAfterCandidate1Joins + " child nodes created after candidate #1 joins the leader election.")
                    .pollInterval(pollInterval)
                    .pollInSameThread()
                    .until(() -> client.getChildren().forPath(latchPath).size() == expectedChildrenAfterCandidate1Joins);

            // triggers the removal of the corresponding child node after candidate #0 retrieved the children
            latchInitialLeader.close();

            latchCandidate0.debugCheckLeaderShipLatch.countDown();

            waitForALeader(Arrays.asList(latchCandidate0, latchCandidate1), new Timing());

            assertTrue(latchCandidate0.hasLeadership() ^ latchCandidate1.hasLeadership());
        }
    }

    private List<LeaderLatch> waitForALeader(List<LeaderLatch> latches, Timing timing) throws InterruptedException {
        for (int i = 0; i < MAX_LOOPS; ++i) {
            List<LeaderLatch> leaders = getLeaders(latches);
            if (leaders.size() != 0) {
                return leaders;
            }
            timing.sleepABit();
        }
        return Lists.newArrayList();
    }

    private List<LeaderLatch> getLeaders(Collection<LeaderLatch> latches) {
        List<LeaderLatch> leaders = Lists.newArrayList();
        for (LeaderLatch latch : latches) {
            if (latch.hasLeadership()) {
                leaders.add(latch);
            }
        }
        return leaders;
    }
}
