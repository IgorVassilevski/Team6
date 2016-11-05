package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.*;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.elasticsearch.cluster.SnapshotsInProgress.completed;

/**
 * Created by pahl on 2016-11-05.
 */
public abstract class SnapshotsServiceSplitDAT265 extends AbstractLifecycleComponent implements ClusterStateListener {
    protected final ClusterService clusterService;
    protected final IndexNameExpressionResolver indexNameExpressionResolver;
    protected final RepositoriesService repositoriesService;
    protected final ThreadPool threadPool;
    protected final CopyOnWriteArrayList<SnapshotCompletionListener> snapshotCompletionListeners = new CopyOnWriteArrayList<>();

    public SnapshotsServiceSplitDAT265(Settings settings, IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService, ThreadPool threadPool, RepositoriesService repositoriesService) {
        super(settings);
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.repositoriesService = repositoriesService;
    }

    private static void validate(final String repositoryName, final String snapshotName) {
        if (Strings.hasLength(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "cannot be empty");
        }
        if (snapshotName.contains(" ")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain whitespace");
        }
        if (snapshotName.contains(",")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain ','");
        }
        if (snapshotName.contains("#")) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not contain '#'");
        }
        if (snapshotName.charAt(0) == '_') {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must not start with '_'");
        }
        if (snapshotName.toLowerCase(Locale.ROOT).equals(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName, snapshotName, "must be lowercase");
        }
        if (Strings.validFileName(snapshotName) == false) {
            throw new InvalidSnapshotNameException(repositoryName,
                                                   snapshotName,
                                                   "must not contain the following characters " + Strings.INVALID_FILENAME_CHARS);
        }
    }

    /**
     * Initializes the snapshotting process.
     * <p>
     * This method is used by clients to start snapshot. It makes sure that there is no snapshots are currently running and
     * creates a snapshot record in cluster state metadata.
     *
     * @param request  snapshot request
     * @param listener snapshot creation listener
     */
    public void createSnapshot(final SnapshotRequest request, final CreateSnapshotListener listener) {
        final String repositoryName = request.repositoryName;
        final String snapshotName = request.snapshotName;
        validate(repositoryName, snapshotName);
        final SnapshotId snapshotId = new SnapshotId(snapshotName, UUIDs.randomBase64UUID()); // new UUID for the snapshot
        final RepositoryData repositoryData = repositoriesService.repository(repositoryName).getRepositoryData();

        clusterService.submitStateUpdateTask(request.cause(), new ClusterStateUpdateTask() {

            private SnapshotsInProgress.Entry newSnapshot = null;

            @Override
            public ClusterState execute(ClusterState currentState) {
                validate(request, currentState);

                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots == null || snapshots.entries().isEmpty()) {
                    // Store newSnapshot here to be processed in clusterStateProcessed
                    List<String> indices = Arrays.asList(indexNameExpressionResolver.concreteIndexNames(currentState, request.indicesOptions(), request.indices()));
                    logger.trace("[{}][{}] creating snapshot for indices [{}]", repositoryName, snapshotName, indices);
                    List<IndexId> snapshotIndices = repositoryData.resolveNewIndices(indices);
                    newSnapshot = new SnapshotsInProgress.Entry(new Snapshot(repositoryName, snapshotId),
                                                                request.includeGlobalState(),
                                                                request.partial(),
                                                                SnapshotsInProgress.State.INIT,
                                                                snapshotIndices,
                                                                System.currentTimeMillis(),
                                                                null);
                    snapshots = new SnapshotsInProgress(newSnapshot);
                } else {
                    // TODO: What should we do if a snapshot is already running?
                    throw new ConcurrentSnapshotExecutionException(repositoryName, snapshotName, "a snapshot is already running");
                }
                return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}][{}] failed to create snapshot", repositoryName, snapshotName), e);
                newSnapshot = null;
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, final ClusterState newState) {
                if (newSnapshot != null) {
                    threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() ->
                        beginSnapshot(newState, newSnapshot, request.partial(), listener)
                    );
                }
            }

            @Override
            public TimeValue timeout() {
                return request.masterNodeTimeout();
            }

        });
    }

    /**
     * Validates snapshot request
     *
     * @param request snapshot request
     * @param state   current cluster state
     */
    private void validate(SnapshotRequest request, ClusterState state) {
        RepositoriesMetaData repositoriesMetaData = state.getMetaData().custom(RepositoriesMetaData.TYPE);
        final String repository = request.repositoryName;
        if (repositoriesMetaData == null || repositoriesMetaData.repository(repository) == null) {
            throw new RepositoryMissingException(repository);
        }
        validate(repository, request.snapshotName);
    }

    /**
     * Starts snapshot.
     * <p>
     * Creates snapshot in repository and updates snapshot metadata record with list of shards that needs to be processed.
     *
     * @param clusterState               cluster state
     * @param snapshot                   snapshot meta data
     * @param partial                    allow partial snapshots
     * @param userCreateSnapshotListener listener
     */
    protected void beginSnapshot(final ClusterState clusterState,
                                 final SnapshotsInProgress.Entry snapshot,
                                 final boolean partial,
                                 final CreateSnapshotListener userCreateSnapshotListener) {
        boolean snapshotCreated = false;
        try {
            Repository repository = repositoriesService.repository(snapshot.snapshot().getRepository());

            MetaData metaData = clusterState.metaData();
            if (!snapshot.includeGlobalState()) {
                // Remove global state from the cluster state
                MetaData.Builder builder = MetaData.builder();
                for (IndexId index : snapshot.indices()) {
                    builder.put(metaData.index(index.getName()), false);
                }
                metaData = builder.build();
            }

            repository.initializeSnapshot(snapshot.snapshot().getSnapshotId(), snapshot.indices(), metaData);
            snapshotCreated = true;
            if (snapshot.indices().isEmpty()) {
                // No indices in this snapshot - we are done
                userCreateSnapshotListener.onResponse();
                endSnapshot(snapshot);
                return;
            }
            clusterService.submitStateUpdateTask("update_snapshot [" + snapshot.snapshot() + "]", new ClusterStateUpdateTask() {
                boolean accepted = false;
                SnapshotsInProgress.Entry updatedSnapshot;
                String failure = null;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                    List<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                    for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                        if (entry.snapshot().equals(snapshot.snapshot())) {
                            // Replace the snapshot that was just created
                            ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = shards(currentState, entry.indices());
                            if (!partial) {
                                Tuple<Set<String>, Set<String>> indicesWithMissingShards = indicesWithMissingShards(shards, currentState.metaData());
                                Set<String> missing = indicesWithMissingShards.v1();
                                Set<String> closed = indicesWithMissingShards.v2();
                                if (missing.isEmpty() == false || closed.isEmpty() == false) {
                                    StringBuilder failureMessage = new StringBuilder();
                                    updatedSnapshot = new SnapshotsInProgress.Entry(entry, SnapshotsInProgress.State.FAILED, shards);
                                    entries.add(updatedSnapshot);
                                    if (missing.isEmpty() == false ) {
                                        failureMessage.append("Indices don't have primary shards ");
                                        failureMessage.append(missing);
                                    }
                                    if (closed.isEmpty() == false ) {
                                        if (failureMessage.length() > 0) {
                                            failureMessage.append("; ");
                                        }
                                        failureMessage.append("Indices are closed ");
                                        failureMessage.append(closed);
                                    }
                                    failure = failureMessage.toString();
                                    continue;
                                }
                            }
                            updatedSnapshot = new SnapshotsInProgress.Entry(entry, SnapshotsInProgress.State.STARTED, shards);
                            entries.add(updatedSnapshot);
                            if (!completed(shards.values())) {
                                accepted = true;
                            }
                        } else {
                            entries.add(entry);
                        }
                    }
                    return ClusterState.builder(currentState)
                                       .putCustom(SnapshotsInProgress.TYPE, new SnapshotsInProgress(Collections.unmodifiableList(entries)))
                                       .build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to create snapshot", snapshot.snapshot().getSnapshotId()), e);
                    removeSnapshotFromClusterState(snapshot.snapshot(), null, e, new CleanupAfterErrorListener(snapshot, true, userCreateSnapshotListener, e));
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    // The userCreateSnapshotListener.onResponse() notifies caller that the snapshot was accepted
                    // for processing. If client wants to wait for the snapshot completion, it can register snapshot
                    // completion listener in this method. For the snapshot completion to work properly, the snapshot
                    // should still exist when listener is registered.
                    userCreateSnapshotListener.onResponse();

                    // Now that snapshot completion listener is registered we can end the snapshot if needed
                    // We should end snapshot only if 1) we didn't accept it for processing (which happens when there
                    // is nothing to do) and 2) there was a snapshot in metadata that we should end. Otherwise we should
                    // go ahead and continue working on this snapshot rather then end here.
                    if (!accepted && updatedSnapshot != null) {
                        endSnapshot(updatedSnapshot, failure);
                    }
                }
            });
        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to create snapshot [{}]", snapshot.snapshot().getSnapshotId()), e);
            removeSnapshotFromClusterState(snapshot.snapshot(), null, e, new CleanupAfterErrorListener(snapshot, snapshotCreated, userCreateSnapshotListener, e));
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        try {
            if (event.localNodeMaster()) {
                if (event.nodesRemoved()) {
                    processSnapshotsOnRemovedNodes(event);
                }
                if (event.routingTableChanged()) {
                    processStartedShards(event);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to update snapshot state ", e);
        }
    }

    /**
     * Cleans up shard snapshots that were running on removed nodes
     *
     * @param event cluster changed event
     */
    private void processSnapshotsOnRemovedNodes(ClusterChangedEvent event) {
        if (removedNodesCleanupNeeded(event)) {
            // Check if we just became the master
            final boolean newMaster = !event.previousState().nodes().isLocalNodeElectedMaster();
            clusterService.submitStateUpdateTask("update snapshot state after node removal", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    DiscoveryNodes nodes = currentState.nodes();
                    SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                    if (snapshots == null) {
                        return currentState;
                    }
                    boolean changed = false;
                    ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                    for (final SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                        SnapshotsInProgress.Entry updatedSnapshot = snapshot;
                        boolean snapshotChanged = false;
                        if (snapshot.state() == SnapshotsInProgress.State.STARTED || snapshot.state() == SnapshotsInProgress.State.ABORTED) {
                            ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
                            for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardEntry : snapshot.shards()) {
                                SnapshotsInProgress.ShardSnapshotStatus shardStatus = shardEntry.value;
                                if (!shardStatus.state().completed() && shardStatus.nodeId() != null) {
                                    if (nodes.nodeExists(shardStatus.nodeId())) {
                                        shards.put(shardEntry.key, shardEntry.value);
                                    } else {
                                        // TODO: Restart snapshot on another node?
                                        snapshotChanged = true;
                                        logger.warn("failing snapshot of shard [{}] on closed node [{}]", shardEntry.key, shardStatus.nodeId());
                                        shards.put(shardEntry.key, new SnapshotsInProgress.ShardSnapshotStatus(shardStatus.nodeId(), SnapshotsInProgress.State.FAILED, "node shutdown"));
                                    }
                                }
                            }
                            if (snapshotChanged) {
                                changed = true;
                                ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsMap = shards.build();
                                if (!snapshot.state().completed() && completed(shardsMap.values())) {
                                    updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, SnapshotsInProgress.State.SUCCESS, shardsMap);
                                    endSnapshot(updatedSnapshot);
                                } else {
                                    updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, snapshot.state(), shardsMap);
                                }
                            }
                            entries.add(updatedSnapshot);
                        } else if (snapshot.state() == SnapshotsInProgress.State.INIT && newMaster) {
                            // Clean up the snapshot that failed to start from the old master
                            deleteSnapshot(snapshot.snapshot(), new DeleteSnapshotListener() {
                                @Override
                                public void onResponse() {
                                    logger.debug("cleaned up abandoned snapshot {} in INIT state", snapshot.snapshot());
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    logger.warn("failed to clean up abandoned snapshot {} in INIT state", snapshot.snapshot());
                                }
                            });
                        } else if (snapshot.state() == SnapshotsInProgress.State.SUCCESS && newMaster) {
                            // Finalize the snapshot
                            endSnapshot(snapshot);
                        }
                    }
                    if (changed) {
                        snapshots = new SnapshotsInProgress(entries.toArray(new SnapshotsInProgress.Entry[entries.size()]));
                        return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
                    }
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn("failed to update snapshot state after node removal");
                }
            });
        }
    }

    private void processStartedShards(ClusterChangedEvent event) {
        if (waitingShardsStartedOrUnassigned(event)) {
            clusterService.submitStateUpdateTask("update snapshot state after shards started", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    RoutingTable routingTable = currentState.routingTable();
                    SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                    if (snapshots != null) {
                        boolean changed = false;
                        ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                        for (final SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                            SnapshotsInProgress.Entry updatedSnapshot = snapshot;
                            if (snapshot.state() == SnapshotsInProgress.State.STARTED) {
                                ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = processWaitingShards(snapshot.shards(), routingTable);
                                if (shards != null) {
                                    changed = true;
                                    if (!snapshot.state().completed() && completed(shards.values())) {
                                        updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, SnapshotsInProgress.State.SUCCESS, shards);
                                        endSnapshot(updatedSnapshot);
                                    } else {
                                        updatedSnapshot = new SnapshotsInProgress.Entry(snapshot, shards);
                                    }
                                }
                                entries.add(updatedSnapshot);
                            }
                        }
                        if (changed) {
                            snapshots = new SnapshotsInProgress(entries.toArray(new SnapshotsInProgress.Entry[entries.size()]));
                            return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
                        }
                    }
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to update snapshot state after shards started from [{}] ", source), e);
                }
            });
        }
    }

    private ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> processWaitingShards(
            ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> snapshotShards, RoutingTable routingTable) {
        boolean snapshotChanged = false;
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards = ImmutableOpenMap.builder();
        for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardEntry : snapshotShards) {
            SnapshotsInProgress.ShardSnapshotStatus shardStatus = shardEntry.value;
            ShardId shardId = shardEntry.key;
            if (shardStatus.state() == SnapshotsInProgress.State.WAITING) {
                IndexRoutingTable indexShardRoutingTable = routingTable.index(shardId.getIndex());
                if (indexShardRoutingTable != null) {
                    IndexShardRoutingTable shardRouting = indexShardRoutingTable.shard(shardId.id());
                    if (shardRouting != null && shardRouting.primaryShard() != null) {
                        if (shardRouting.primaryShard().started()) {
                            // Shard that we were waiting for has started on a node, let's process it
                            snapshotChanged = true;
                            logger.trace("starting shard that we were waiting for [{}] on node [{}]", shardId, shardStatus.nodeId());
                            shards.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(shardRouting.primaryShard().currentNodeId()));
                            continue;
                        } else if (shardRouting.primaryShard().initializing() || shardRouting.primaryShard().relocating()) {
                            // Shard that we were waiting for hasn't started yet or still relocating - will continue to wait
                            shards.put(shardId, shardStatus);
                            continue;
                        }
                    }
                }
                // Shard that we were waiting for went into unassigned state or disappeared - giving up
                snapshotChanged = true;
                logger.warn("failing snapshot of shard [{}] on unassigned shard [{}]", shardId, shardStatus.nodeId());
                shards.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(shardStatus.nodeId(), SnapshotsInProgress.State.FAILED, "shard is unassigned"));
            } else {
                shards.put(shardId, shardStatus);
            }
        }
        if (snapshotChanged) {
            return shards.build();
        } else {
            return null;
        }
    }

    private boolean waitingShardsStartedOrUnassigned(ClusterChangedEvent event) {
        SnapshotsInProgress curr = event.state().custom(SnapshotsInProgress.TYPE);
        if (curr != null) {
            for (SnapshotsInProgress.Entry entry : curr.entries()) {
                if (entry.state() == SnapshotsInProgress.State.STARTED && !entry.waitingIndices().isEmpty()) {
                    for (ObjectCursor<String> index : entry.waitingIndices().keys()) {
                        if (event.indexRoutingTableChanged(index.value)) {
                            IndexRoutingTable indexShardRoutingTable = event.state().getRoutingTable().index(index.value);
                            for (ShardId shardId : entry.waitingIndices().get(index.value)) {
                                ShardRouting shardRouting = indexShardRoutingTable.shard(shardId.id()).primaryShard();
                                if (shardRouting != null && (shardRouting.started() || shardRouting.unassigned())) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean removedNodesCleanupNeeded(ClusterChangedEvent event) {
        // Check if we just became the master
        boolean newMaster = !event.previousState().nodes().isLocalNodeElectedMaster();
        SnapshotsInProgress snapshotsInProgress = event.state().custom(SnapshotsInProgress.TYPE);
        if (snapshotsInProgress == null) {
            return false;
        }
        for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {
            if (newMaster && (snapshot.state() == SnapshotsInProgress.State.SUCCESS || snapshot.state() == SnapshotsInProgress.State.INIT)) {
                // We just replaced old master and snapshots in intermediate states needs to be cleaned
                return true;
            }
            for (DiscoveryNode node : event.nodesDelta().removedNodes()) {
                for (ObjectCursor<SnapshotsInProgress.ShardSnapshotStatus> shardStatus : snapshot.shards().values()) {
                    if (!shardStatus.value.state().completed() && node.getId().equals(shardStatus.value.nodeId())) {
                        // At least one shard was running on the removed node - we need to fail it
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns list of indices with missing shards, and list of indices that are closed
     *
     * @param shards list of shard statuses
     * @return list of failed and closed indices
     */
    private Tuple<Set<String>, Set<String>> indicesWithMissingShards(ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards, MetaData metaData) {
        Set<String> missing = new HashSet<>();
        Set<String> closed = new HashSet<>();
        for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> entry : shards) {
            if (entry.value.state() == SnapshotsInProgress.State.MISSING) {
                if (metaData.hasIndex(entry.key.getIndex().getName()) && metaData.getIndexSafe(entry.key.getIndex()).getState() == IndexMetaData.State.CLOSE) {
                    closed.add(entry.key.getIndex().getName());
                } else {
                    missing.add(entry.key.getIndex().getName());
                }
            }
        }
        return new Tuple<>(missing, closed);
    }

    /**
     * Finalizes the shard in repository and then removes it from cluster state
     * <p>
     * This is non-blocking method that runs on a thread from SNAPSHOT thread pool
     *
     * @param entry snapshot
     */
    void endSnapshot(SnapshotsInProgress.Entry entry) {
        endSnapshot(entry, null);
    }

    /**
     * Finalizes the shard in repository and then removes it from cluster state
     * <p>
     * This is non-blocking method that runs on a thread from SNAPSHOT thread pool
     *
     * @param entry   snapshot
     * @param failure failure reason or null if snapshot was successful
     */
    private void endSnapshot(final SnapshotsInProgress.Entry entry, final String failure) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new Runnable() {
            @Override
            public void run() {
                final Snapshot snapshot = entry.snapshot();
                try {
                    final Repository repository = repositoriesService.repository(snapshot.getRepository());
                    logger.trace("[{}] finalizing snapshot in repository, state: [{}], failure[{}]", snapshot, entry.state(), failure);
                    ArrayList<ShardSearchFailure> failures = new ArrayList<>();
                    ArrayList<SnapshotShardFailure> shardFailures = new ArrayList<>();
                    for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardStatus : entry.shards()) {
                        ShardId shardId = shardStatus.key;
                        SnapshotsInProgress.ShardSnapshotStatus status = shardStatus.value;
                        if (status.state().failed()) {
                            failures.add(new ShardSearchFailure(status.reason(), new SearchShardTarget(status.nodeId(), shardId.getIndex(), shardId.id())));
                            shardFailures.add(new SnapshotShardFailure(status.nodeId(), shardId, status.reason()));
                        }
                    }
                    SnapshotInfo snapshotInfo = repository.finalizeSnapshot(snapshot.getSnapshotId(), entry.indices(), entry.startTime(), failure, entry.shards().size(), Collections.unmodifiableList(shardFailures));
                    removeSnapshotFromClusterState(snapshot, snapshotInfo, null);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to finalize snapshot", snapshot), e);
                    removeSnapshotFromClusterState(snapshot, null, e);
                }
            }
        });
    }

    /**
     * Removes record of running snapshot from cluster state
     *  @param snapshot       snapshot
     * @param snapshotInfo   snapshot info if snapshot was successful
     * @param e              exception if snapshot failed
     */
    protected void removeSnapshotFromClusterState(final Snapshot snapshot, final SnapshotInfo snapshotInfo, final Exception e) {
        removeSnapshotFromClusterState(snapshot, snapshotInfo, e, null);
    }

    /**
     * Removes record of running snapshot from cluster state and notifies the listener when this action is complete
     *  @param snapshot   snapshot
     * @param failure          exception if snapshot failed
     * @param listener   listener to notify when snapshot information is removed from the cluster state
     */
    private void removeSnapshotFromClusterState(final Snapshot snapshot, final SnapshotInfo snapshotInfo, final Exception failure,
                                                @Nullable ActionListener<SnapshotInfo> listener) {
        clusterService.submitStateUpdateTask("remove snapshot metadata", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots != null) {
                    boolean changed = false;
                    ArrayList<SnapshotsInProgress.Entry> entries = new ArrayList<>();
                    for (SnapshotsInProgress.Entry entry : snapshots.entries()) {
                        if (entry.snapshot().equals(snapshot)) {
                            changed = true;
                        } else {
                            entries.add(entry);
                        }
                    }
                    if (changed) {
                        snapshots = new SnapshotsInProgress(entries.toArray(new SnapshotsInProgress.Entry[entries.size()]));
                        return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
                    }
                }
                return currentState;
            }

            @Override
            public void onFailure(String source, Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to remove snapshot metadata", snapshot), e);
                if (listener != null) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                for (SnapshotCompletionListener listener : snapshotCompletionListeners) {
                    try {
                        if (snapshotInfo != null) {
                            listener.onSnapshotCompletion(snapshot, snapshotInfo);
                        } else {
                            listener.onSnapshotFailure(snapshot, failure);
                        }
                    } catch (Exception t) {
                        logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to notify listener [{}]", listener), t);
                    }
                }
                if (listener != null) {
                    listener.onResponse(snapshotInfo);
                }
            }
        });
    }

    /**
     * Deletes snapshot from repository.
     * <p>
     * If the snapshot is still running cancels the snapshot first and then deletes it from the repository.
     *
     * @param snapshot  snapshot
     * @param listener  listener
     */
    public void deleteSnapshot(final Snapshot snapshot, final DeleteSnapshotListener listener) {
        clusterService.submitStateUpdateTask("delete snapshot", new ClusterStateUpdateTask() {

            boolean waitForSnapshot = false;

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                RestoreInProgress restoreInProgress = currentState.custom(RestoreInProgress.TYPE);
                if (restoreInProgress != null) {
                    // don't allow snapshot deletions while a restore is taking place,
                    // otherwise we could end up deleting a snapshot that is being restored
                    // and the files the restore depends on would all be gone
                    if (restoreInProgress.entries().isEmpty() == false) {
                        throw new ConcurrentSnapshotExecutionException(snapshot, "cannot delete snapshot during a restore");
                    }
                }
                SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
                if (snapshots == null) {
                    // No snapshots running - we can continue
                    return currentState;
                }
                SnapshotsInProgress.Entry snapshotEntry = snapshots.snapshot(snapshot);
                if (snapshotEntry == null) {
                    // This snapshot is not running - continue
                    if (!snapshots.entries().isEmpty()) {
                        // However other snapshots are running - cannot continue
                        throw new ConcurrentSnapshotExecutionException(snapshot, "another snapshot is currently running cannot delete");
                    }
                    return currentState;
                } else {
                    // This snapshot is currently running - stopping shards first
                    waitForSnapshot = true;
                    ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards;
                    if (snapshotEntry.state() == SnapshotsInProgress.State.STARTED && snapshotEntry.shards() != null) {
                        // snapshot is currently running - stop started shards
                        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardsBuilder = ImmutableOpenMap.builder();
                        for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shardEntry : snapshotEntry.shards()) {
                            SnapshotsInProgress.ShardSnapshotStatus status = shardEntry.value;
                            if (!status.state().completed()) {
                                shardsBuilder.put(shardEntry.key, new SnapshotsInProgress.ShardSnapshotStatus(status.nodeId(), SnapshotsInProgress.State.ABORTED));
                            } else {
                                shardsBuilder.put(shardEntry.key, status);
                            }
                        }
                        shards = shardsBuilder.build();
                    } else if (snapshotEntry.state() == SnapshotsInProgress.State.INIT) {
                        // snapshot hasn't started yet - end it
                        shards = snapshotEntry.shards();
                        endSnapshot(snapshotEntry);
                    } else {
                        boolean hasUncompletedShards = false;
                        // Cleanup in case a node gone missing and snapshot wasn't updated for some reason
                        for (ObjectCursor<SnapshotsInProgress.ShardSnapshotStatus> shardStatus : snapshotEntry.shards().values()) {
                            // Check if we still have shard running on existing nodes
                            if (shardStatus.value.state().completed() == false && shardStatus.value.nodeId() != null
                                    && currentState.nodes().get(shardStatus.value.nodeId()) != null) {
                                hasUncompletedShards = true;
                                break;
                            }
                        }
                        if (hasUncompletedShards) {
                            // snapshot is being finalized - wait for shards to complete finalization process
                            logger.debug("trying to delete completed snapshot - should wait for shards to finalize on all nodes");
                            return currentState;
                        } else {
                            // no shards to wait for - finish the snapshot
                            logger.debug("trying to delete completed snapshot with no finalizing shards - can delete immediately");
                            shards = snapshotEntry.shards();
                            endSnapshot(snapshotEntry);
                        }
                    }
                    SnapshotsInProgress.Entry newSnapshot = new SnapshotsInProgress.Entry(snapshotEntry, SnapshotsInProgress.State.ABORTED, shards);
                    snapshots = new SnapshotsInProgress(newSnapshot);
                    return ClusterState.builder(currentState).putCustom(SnapshotsInProgress.TYPE, snapshots).build();
                }
            }

            @Override
            public void onFailure(String source, Exception e) {
                listener.onFailure(e);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                if (waitForSnapshot) {
                    logger.trace("adding snapshot completion listener to wait for deleted snapshot to finish");
                    addListener(new SnapshotCompletionListener() {
                        @Override
                        public void onSnapshotCompletion(Snapshot completedSnapshot, SnapshotInfo snapshotInfo) {
                            if (completedSnapshot.equals(snapshot)) {
                                logger.trace("deleted snapshot completed - deleting files");
                                removeListener(this);
                                deleteSnapshotFromRepository(snapshot, listener);
                            }
                        }

                        @Override
                        public void onSnapshotFailure(Snapshot failedSnapshot, Exception e) {
                            if (failedSnapshot.equals(snapshot)) {
                                logger.trace("deleted snapshot failed - deleting files", e);
                                removeListener(this);
                                deleteSnapshotFromRepository(snapshot, listener);
                            }
                        }
                    });
                } else {
                    logger.trace("deleted snapshot is not running - deleting files");
                    deleteSnapshotFromRepository(snapshot, listener);
                }
            }
        });
    }

    /**
     * Deletes snapshot from repository
     *
     * @param snapshot   snapshot
     * @param listener   listener
     */
    private void deleteSnapshotFromRepository(final Snapshot snapshot, final DeleteSnapshotListener listener) {
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(() -> {
            try {
                Repository repository = repositoriesService.repository(snapshot.getRepository());
                repository.deleteSnapshot(snapshot.getSnapshotId());
                listener.onResponse();
            } catch (Exception t) {
                listener.onFailure(t);
            }
        });
    }

    /**
     * Calculates the list of shards that should be included into the current snapshot
     *
     * @param clusterState cluster state
     * @param indices      list of indices to be snapshotted
     * @return list of shard to be included into current snapshot
     */
    private ImmutableOpenMap<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shards(ClusterState clusterState, List<IndexId> indices) {
        ImmutableOpenMap.Builder<ShardId, SnapshotsInProgress.ShardSnapshotStatus> builder = ImmutableOpenMap.builder();
        MetaData metaData = clusterState.metaData();
        for (IndexId index : indices) {
            final String indexName = index.getName();
            IndexMetaData indexMetaData = metaData.index(indexName);
            if (indexMetaData == null) {
                // The index was deleted before we managed to start the snapshot - mark it as missing.
                builder.put(new ShardId(indexName, IndexMetaData.INDEX_UUID_NA_VALUE, 0), new SnapshotsInProgress.ShardSnapshotStatus(null, SnapshotsInProgress.State.MISSING, "missing index"));
            } else if (indexMetaData.getState() == IndexMetaData.State.CLOSE) {
                for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
                    ShardId shardId = new ShardId(indexMetaData.getIndex(), i);
                    builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(null, SnapshotsInProgress.State.MISSING, "index is closed"));
                }
            } else {
                IndexRoutingTable indexRoutingTable = clusterState.getRoutingTable().index(indexName);
                for (int i = 0; i < indexMetaData.getNumberOfShards(); i++) {
                    ShardId shardId = new ShardId(indexMetaData.getIndex(), i);
                    if (indexRoutingTable != null) {
                        ShardRouting primary = indexRoutingTable.shard(i).primaryShard();
                        if (primary == null || !primary.assignedToNode()) {
                            builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(null, SnapshotsInProgress.State.MISSING, "primary shard is not allocated"));
                        } else if (primary.relocating() || primary.initializing()) {
                            // The WAITING state was introduced in V1.2.0 - don't use it if there are nodes with older version in the cluster
                            builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId(), SnapshotsInProgress.State.WAITING));
                        } else if (!primary.started()) {
                            builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId(), SnapshotsInProgress.State.MISSING, "primary shard hasn't been started yet"));
                        } else {
                            builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(primary.currentNodeId()));
                        }
                    } else {
                        builder.put(shardId, new SnapshotsInProgress.ShardSnapshotStatus(null, SnapshotsInProgress.State.MISSING, "missing routing table"));
                    }
                }
            }
        }

        return builder.build();
    }

    /**
     * Adds snapshot completion listener
     *
     * @param listener listener
     */
    public void addListener(SnapshotCompletionListener listener) {
        this.snapshotCompletionListeners.add(listener);
    }

    /**
     * Removes snapshot completion listener
     *
     * @param listener listener
     */
    public void removeListener(SnapshotCompletionListener listener) {
        this.snapshotCompletionListeners.remove(listener);
    }

    /**
     * Listener for create snapshot operation
     */
    public interface CreateSnapshotListener {

        /**
         * Called when snapshot has successfully started
         */
        void onResponse();

        /**
         * Called if a snapshot operation couldn't start
         */
        void onFailure(Exception e);
    }

    /**
     * Listener for delete snapshot operation
     */
    public interface DeleteSnapshotListener {

        /**
         * Called if delete operation was successful
         */
        void onResponse();

        /**
         * Called if delete operation failed
         */
        void onFailure(Exception e);
    }

    public interface SnapshotCompletionListener {

        void onSnapshotCompletion(Snapshot snapshot, SnapshotInfo snapshotInfo);

        void onSnapshotFailure(Snapshot snapshot, Exception e);
    }

    /**
     * Snapshot creation request
     */
    public static class SnapshotRequest {

        private final String cause;

        private final String repositoryName;

        private final String snapshotName;

        private String[] indices;

        private IndicesOptions indicesOptions = IndicesOptions.strictExpandOpen();

        private boolean partial;

        private Settings settings;

        private boolean includeGlobalState;

        private TimeValue masterNodeTimeout;

        /**
         * Constructs new snapshot creation request
         *
         * @param repositoryName  repository name
         * @param snapshotName    snapshot name
         * @param cause           cause for snapshot operation
         */
        public SnapshotRequest(final String repositoryName, final String snapshotName, final String cause) {
            this.repositoryName = Objects.requireNonNull(repositoryName);
            this.snapshotName = Objects.requireNonNull(snapshotName);
            this.cause = Objects.requireNonNull(cause);
        }

        /**
         * Sets the list of indices to be snapshotted
         *
         * @param indices list of indices
         * @return this request
         */
        public SnapshotRequest indices(String[] indices) {
            this.indices = indices;
            return this;
        }

        /**
         * Sets repository-specific snapshot settings
         *
         * @param settings snapshot settings
         * @return this request
         */
        public SnapshotRequest settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        /**
         * Set to true if global state should be stored as part of the snapshot
         *
         * @param includeGlobalState true if global state should be stored as part of the snapshot
         * @return this request
         */
        public SnapshotRequest includeGlobalState(boolean includeGlobalState) {
            this.includeGlobalState = includeGlobalState;
            return this;
        }

        /**
         * Sets master node timeout
         *
         * @param masterNodeTimeout master node timeout
         * @return this request
         */
        public SnapshotRequest masterNodeTimeout(TimeValue masterNodeTimeout) {
            this.masterNodeTimeout = masterNodeTimeout;
            return this;
        }

        /**
         * Sets the indices options
         *
         * @param indicesOptions indices options
         * @return this request
         */
        public SnapshotRequest indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        /**
         * Set to true if partial snapshot should be allowed
         *
         * @param partial true if partial snapshots should be allowed
         * @return this request
         */
        public SnapshotRequest partial(boolean partial) {
            this.partial = partial;
            return this;
        }

        /**
         * Returns cause for snapshot operation
         *
         * @return cause for snapshot operation
         */
        public String cause() {
            return cause;
        }

        /**
         * Returns the repository name
         */
        public String repositoryName() {
            return repositoryName;
        }

        /**
         * Returns the snapshot name
         */
        public String snapshotName() {
            return snapshotName;
        }

        /**
         * Returns the list of indices to be snapshotted
         *
         * @return the list of indices
         */
        public String[] indices() {
            return indices;
        }

        /**
         * Returns indices options
         *
         * @return indices options
         */
        public IndicesOptions indicesOptions() {
            return indicesOptions;
        }

        /**
         * Returns repository-specific settings for the snapshot operation
         *
         * @return repository-specific settings
         */
        public Settings settings() {
            return settings;
        }

        /**
         * Returns true if global state should be stored as part of the snapshot
         *
         * @return true if global state should be stored as part of the snapshot
         */
        public boolean includeGlobalState() {
            return includeGlobalState;
        }

        /**
         * Returns true if partial snapshot should be allowed
         *
         * @return true if partial snapshot should be allowed
         */
        public boolean partial() {
            return partial;
        }

        /**
         * Returns master node timeout
         *
         * @return master node timeout
         */
        public TimeValue masterNodeTimeout() {
            return masterNodeTimeout;
        }

    }

    private class CleanupAfterErrorListener implements ActionListener<SnapshotInfo> {

        private final SnapshotsInProgress.Entry snapshot;
        private final boolean snapshotCreated;
        private final CreateSnapshotListener userCreateSnapshotListener;
        private final Exception e;

        public CleanupAfterErrorListener(SnapshotsInProgress.Entry snapshot, boolean snapshotCreated, CreateSnapshotListener userCreateSnapshotListener, Exception e) {
            this.snapshot = snapshot;
            this.snapshotCreated = snapshotCreated;
            this.userCreateSnapshotListener = userCreateSnapshotListener;
            this.e = e;
        }

        @Override
        public void onResponse(SnapshotInfo snapshotInfo) {
            cleanupAfterError(this.e);
        }

        @Override
        public void onFailure(Exception e) {
            e.addSuppressed(this.e);
            cleanupAfterError(e);
        }

        private void cleanupAfterError(Exception exception) {
            if(snapshotCreated) {
                try {
                    repositoriesService.repository(snapshot.snapshot().getRepository())
                                       .finalizeSnapshot(snapshot.snapshot().getSnapshotId(),
                                                         snapshot.indices(),
                                                         snapshot.startTime(),
                                                         ExceptionsHelper.detailedMessage(exception),
                                                         0,
                                                         Collections.emptyList());
                } catch (Exception inner) {
                    inner.addSuppressed(exception);
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("[{}] failed to close snapshot in repository", snapshot.snapshot()), inner);
                }
            }
            userCreateSnapshotListener.onFailure(e);
        }

    }
}