import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.requests.AbstractRequest;

public final class GroupWireSmoke {
    private static final String TOPIC = "rusta-group-data";
    private static final String GROUP = "rusta-wire-group";

    record Coordinator(int id, String endpoint) {}
    private record Session(String member, int generation) {}

    public static void main(String[] args) throws Exception {
        var scratch = Path.of(args[2]);
        if (args[0].equals("minority")) {
            var old = load(scratch);
            retryable(fetch(args[1], 9).groups().getFirst().errorCode());
            retryable(commit(args[1], 9, old, 2));
            System.out.println("Group coordinator minority rejected read and commit");
            return;
        }

        var coordinator = find(args[1], GROUP, 0);
        String endpoint = coordinator.endpoint();
        if (args[0].equals("prepare")) {
            for (int version = 6; version <= 9; version++) {
                var session = join(endpoint, version);
                sync(endpoint, 4 + version % 2, session);
                success(((HeartbeatResponseData) exchange(endpoint, 4,
                    new HeartbeatRequestData().setGroupId(GROUP).setMemberId(session.member())
                        .setGenerationId(session.generation()))).errorCode());
                success(commit(endpoint, 8 + version % 2, session, 1));
                verifyFetch(endpoint, version, 1);
                verifyAdmin(endpoint, version, session);
                if (version < 9) leave(endpoint, 4 + version % 2, session);
                else Files.write(scratch.resolve("wire-session"),
                    List.of(session.member(), Integer.toString(session.generation())));
            }
            Files.writeString(scratch.resolve("wire-coordinator"), Integer.toString(coordinator.id()));
            System.out.println("All advertised classic group API versions passed");
        } else if (args[0].equals("recover")) {
            var old = load(scratch);
            verifyFetch(endpoint, 9, 1);
            var session = join(endpoint, 9);
            GroupSmoke.check(session.generation() > old.generation(), "generation did not advance after failover");
            sync(endpoint, 5, session);
            short stale = commit(endpoint, 9, old, 2);
            GroupSmoke.check(stale == Errors.UNKNOWN_MEMBER_ID.code() || stale == Errors.ILLEGAL_GENERATION.code(),
                "stale generation accepted or returned an unexpected error: " + Errors.forCode(stale));
            verifyFetch(endpoint, 9, 1);
            success(commit(endpoint, 9, session, 2));
            success(commit(endpoint, 9, session, 2));
            verifyFetch(endpoint, 9, 2);
            leave(endpoint, 5, session);
            System.out.println("Group coordinator failover preserved offsets and fenced the old generation");
        } else if (args[0].equals("verify")) {
            verifyFetch(endpoint, 9, 2);
            System.out.println("Group offset recovered through " + endpoint);
        } else {
            throw new IllegalArgumentException("unknown mode " + args[0]);
        }
    }

    private static Session load(Path scratch) throws Exception {
        var session = Files.readAllLines(scratch.resolve("wire-session"));
        return new Session(session.get(0), Integer.parseInt(session.get(1)));
    }

    static Coordinator find(String endpoints, String groupId, int excluded) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(12).toNanos();
        while (System.nanoTime() < deadline) {
            for (var endpoint : endpoints.split(",")) {
                var single = (FindCoordinatorResponseData) exchange(endpoint, 3,
                    new FindCoordinatorRequestData().setKey(groupId));
                if (single.errorCode() != 0 || single.nodeId() == excluded) continue;
                var batch = (FindCoordinatorResponseData) exchange(endpoint, 4,
                    new FindCoordinatorRequestData().setCoordinatorKeys(List.of(groupId, "")));
                var target = batch.coordinators().getFirst();
                if (target.errorCode() == 0 && target.nodeId() == single.nodeId()) {
                    GroupSmoke.check(batch.coordinators().get(1).errorCode() == Errors.INVALID_GROUP_ID.code(),
                        "invalid group in batched coordinator lookup");
                    return new Coordinator(target.nodeId(), target.host() + ":" + target.port());
                }
            }
            Thread.sleep(75);
        }
        throw new AssertionError("coordinator did not become available");
    }

    private static Session join(String endpoint, int version) throws Exception {
        var protocols = new JoinGroupRequestData.JoinGroupRequestProtocolCollection();
        protocols.add(new JoinGroupRequestData.JoinGroupRequestProtocol().setName("range").setMetadata(new byte[0]));
        var request = new JoinGroupRequestData().setGroupId(GROUP).setSessionTimeoutMs(5000)
            .setRebalanceTimeoutMs(5000).setProtocolType("consumer").setProtocols(protocols);
        var required = (JoinGroupResponseData) exchange(endpoint, version, request);
        GroupSmoke.check(required.errorCode() == Errors.MEMBER_ID_REQUIRED.code(), "expected member-id handshake");
        var joined = (JoinGroupResponseData) exchange(endpoint, version, request.setMemberId(required.memberId()));
        success(joined.errorCode());
        return new Session(joined.memberId(), joined.generationId());
    }

    private static void sync(String endpoint, int version, Session session) throws Exception {
        var assignment = new byte[] {1, 2, 3};
        var data = new SyncGroupRequestData().setGroupId(GROUP).setMemberId(session.member())
            .setGenerationId(session.generation())
            .setAssignments(List.of(new SyncGroupRequestData.SyncGroupRequestAssignment()
                .setMemberId(session.member()).setAssignment(assignment)));
        if (version >= 5) data.setProtocolType("consumer").setProtocolName("range");
        var response = (SyncGroupResponseData) exchange(endpoint, version, data);
        success(response.errorCode());
        GroupSmoke.check(java.util.Arrays.equals(response.assignment(), assignment), "assignment changed");
    }

    private static short commit(String endpoint, int version, Session session, long offset) throws Exception {
        var data = new OffsetCommitRequestData().setGroupId(GROUP).setMemberId(session.member())
            .setGenerationIdOrMemberEpoch(session.generation())
            .setTopics(List.of(new OffsetCommitRequestData.OffsetCommitRequestTopic().setName(TOPIC)
                .setPartitions(List.of(new OffsetCommitRequestData.OffsetCommitRequestPartition()
                    .setPartitionIndex(0).setCommittedOffset(offset).setCommittedMetadata("wire")))));
        var response = (OffsetCommitResponseData) exchange(endpoint, version, data);
        return response.topics().getFirst().partitions().getFirst().errorCode();
    }

    private static OffsetFetchResponseData fetch(String endpoint, int version) throws Exception {
        var data = new OffsetFetchRequestData();
        if (version <= 7) {
            data.setGroupId(GROUP).setTopics(List.of(new OffsetFetchRequestData.OffsetFetchRequestTopic()
                .setName(TOPIC).setPartitionIndexes(List.of(0))));
        } else {
            data.setGroups(List.of(new OffsetFetchRequestData.OffsetFetchRequestGroup().setGroupId(GROUP)
                .setTopics(List.of(new OffsetFetchRequestData.OffsetFetchRequestTopics()
                    .setName(TOPIC).setPartitionIndexes(List.of(0))))));
        }
        return (OffsetFetchResponseData) exchange(endpoint, version, data);
    }

    private static void verifyFetch(String endpoint, int version, long expected) throws Exception {
        var response = fetch(endpoint, version);
        if (version <= 7) {
            success(response.errorCode());
            var offset = response.topics().getFirst().partitions().getFirst();
            success(offset.errorCode());
            GroupSmoke.check(offset.committedOffset() == expected && "wire".equals(offset.metadata()), "wrong per-group offset");
        } else {
            var group = response.groups().getFirst();
            success(group.errorCode());
            var offset = group.topics().getFirst().partitions().getFirst();
            success(offset.errorCode());
            GroupSmoke.check(offset.committedOffset() == expected && "wire".equals(offset.metadata()), "wrong batched offset");
        }
    }

    private static void verifyAdmin(String endpoint, int version, Session session) throws Exception {
        var described = (DescribeGroupsResponseData) exchange(endpoint, 5 + version % 2,
            new DescribeGroupsRequestData().setGroups(List.of(GROUP)));
        var group = described.groups().getFirst();
        success(group.errorCode());
        GroupSmoke.check(group.members().getFirst().memberId().equals(session.member()), "missing member in DescribeGroups");

        var request = new ListGroupsRequestData();
        if (version >= 7) request.setStatesFilter(List.of("Stable"));
        if (version >= 8) request.setTypesFilter(List.of("Classic"));
        var listed = (ListGroupsResponseData) exchange(endpoint, Math.min(5, version - 3), request);
        success(listed.errorCode());
        GroupSmoke.check(listed.groups().stream().anyMatch(entry -> entry.groupId().equals(GROUP)),
            "missing group in ListGroups");
    }

    private static void leave(String endpoint, int version, Session session) throws Exception {
        var response = (LeaveGroupResponseData) exchange(endpoint, version, new LeaveGroupRequestData()
            .setGroupId(GROUP).setMembers(List.of(new LeaveGroupRequestData.MemberIdentity()
                .setMemberId(session.member()))));
        success(response.errorCode());
        success(response.members().getFirst().errorCode());
    }

    private static ApiMessage exchange(String endpoint, int version, ApiMessage data) throws Exception {
        var bytes = MessageUtil.toByteBufferAccessor(data, (short) version);
        var request = AbstractRequest.parseRequest(ApiKeys.forId(data.apiKey()), (short) version, bytes).request;
        return Smoke.exchange(endpoint, request, 1).data();
    }

    private static void success(short code) {
        GroupSmoke.check(code == 0, "unexpected Kafka error: " + Errors.forCode(code));
    }

    private static void retryable(short code) {
        GroupSmoke.check(code == Errors.COORDINATOR_NOT_AVAILABLE.code()
            || code == Errors.NOT_COORDINATOR.code()
            || code == Errors.COORDINATOR_LOAD_IN_PROGRESS.code(), "minority returned " + Errors.forCode(code));
    }
}
