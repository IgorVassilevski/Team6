/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.ShardSnapshotStatus;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryMissingException;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.cluster.SnapshotsInProgress.completed;

/**
 * Service responsible for creating snapshots
 * <p>
 * A typical snapshot creating process looks like this:
 * <ul>
 * <li>On the master node the {@link #createSnapshot(SnapshotRequest, CreateSnapshotListener)} is called and makes sure that no snapshots is currently running
 * and registers the new snapshot in cluster state</li>
 * <li>When cluster state is updated the {@link #beginSnapshot(ClusterState, SnapshotsInProgress.Entry, boolean, CreateSnapshotListener)} method
 * kicks in and initializes the snapshot in the repository and then populates list of shards that needs to be snapshotted in cluster state</li>
 * <li>Each data node is watching for these shards and when new shards scheduled for snapshotting appear in the cluster state, data nodes
 * start processing them through {@link SnapshotShardsService#processIndexShardSnapshots(ClusterChangedEvent)} method</li>
 * <li>Once shard snapshot is created data node updates state of the shard in the cluster state using the {@link SnapshotShardsService#updateIndexShardSnapshotStatus} method</li>
 * <li>When last shard is completed master node in {@link SnapshotShardsService#innerUpdateSnapshotState} method marks the snapshot as completed</li>
 * <li>After cluster state is updated, the {@link #endSnapshot(SnapshotsInProgress.Entry)} finalizes snapshot in the repository,
 * notifies all {@link #snapshotCompletionListeners} that snapshot is completed, and finally calls {@link #removeSnapshotFromClusterState(Snapshot, SnapshotInfo, Exception)} to remove snapshot from cluster state</li>
 * </ul>
 */
public class SnapshotsService extends SnapshotsServiceSplitDAT265 {

    @Inject
    public SnapshotsService(Settings settings, ClusterService clusterService, IndexNameExpressionResolver indexNameExpressionResolver, RepositoriesService repositoriesService, ThreadPool threadPool) {
        super(settings, indexNameExpressionResolver, clusterService, threadPool, repositoriesService);

        if (DiscoveryNode.isMasterNode(settings)) {
            // addLast to make sure that Repository will be created before snapshot
            clusterService.addLast(this);
        }
    }

    /**
     * Retrieves list of snapshot ids that are present in a repository
     *
     * @param repositoryName repository name
     * @return list of snapshot ids
     */
    public List<SnapshotId> snapshotIds(final String repositoryName) {
        Repository repository = repositoriesService.repository(repositoryName);
        assert repository != null; // should only be called once we've validated the repository exists
        return repository.getRepositoryData().getSnapshotIds();
    }

    /**
     * Retrieves snapshot from repository
     *
     * @param repositoryName  repository name
     * @param snapshotId      snapshot id
     * @return snapshot
     * @throws SnapshotMissingException if snapshot is not found
     */
    public SnapshotInfo snapshot(final String repositoryName, final SnapshotId snapshotId) {
        List<SnapshotsInProgress.Entry> entries = currentSnapshots(repositoryName, Arrays.asList(snapshotId.getName()));
        if (!entries.isEmpty()) {
            return inProgressSnapshot(entries.iterator().next());
        }
        return repositoriesService.repository(repositoryName).getSnapshotInfo(snapshotId);
    }

    /**
     * Returns a list of snapshots from repository sorted by snapshot creation date
     *
     * @param repositoryName repository name
     * @param snapshotIds       snapshots for which to fetch snapshot information
     * @param ignoreUnavailable if true, snapshots that could not be read will only be logged with a warning,
     *                          if false, they will throw an error
     * @return list of snapshots
     */
    public List<SnapshotInfo> snapshots(final String repositoryName, List<SnapshotId> snapshotIds, final boolean ignoreUnavailable) {
        final Set<SnapshotInfo> snapshotSet = new HashSet<>();
        final Set<SnapshotId> snapshotIdsToIterate = new HashSet<>(snapshotIds);
        // first, look at the snapshots in progress
        final List<SnapshotsInProgress.Entry> entries =
            currentSnapshots(repositoryName, snapshotIdsToIterate.stream().map(SnapshotId::getName).collect(Collectors.toList()));
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotSet.add(inProgressSnapshot(entry));
            snapshotIdsToIterate.remove(entry.snapshot().getSnapshotId());
        }
        // then, look in the repository
        final Repository repository = repositoriesService.repository(repositoryName);
        for (SnapshotId snapshotId : snapshotIdsToIterate) {
            try {
                snapshotSet.add(repository.getSnapshotInfo(snapshotId));
            } catch (Exception ex) {
                if (ignoreUnavailable) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to get snapshot [{}]", snapshotId), ex);
                } else {
                    throw new SnapshotException(repositoryName, snapshotId, "Snapshot could not be read", ex);
                }
            }
        }
        final ArrayList<SnapshotInfo> snapshotList = new ArrayList<>(snapshotSet);
        CollectionUtil.timSort(snapshotList);
        return Collections.unmodifiableList(snapshotList);
    }

    /**
     * Returns a list of currently running snapshots from repository sorted by snapshot creation date
     *
     * @param repositoryName repository name
     * @return list of snapshots
     */
    public List<SnapshotInfo> currentSnapshots(final String repositoryName) {
        List<SnapshotInfo> snapshotList = new ArrayList<>();
        List<SnapshotsInProgress.Entry> entries = currentSnapshots(repositoryName, Collections.emptyList());
        for (SnapshotsInProgress.Entry entry : entries) {
            snapshotList.add(inProgressSnapshot(entry));
        }
        CollectionUtil.timSort(snapshotList);
        return Collections.unmodifiableList(snapshotList);
    }

    private SnapshotInfo inProgressSnapshot(SnapshotsInProgress.Entry entry) {
        return new SnapshotInfo(entry.snapshot().getSnapshotId(),
                                   entry.indices().stream().map(IndexId::getName).collect(Collectors.toList()),
                                   entry.startTime());
    }

    /**
     * Returns status of the currently running snapshots
     * <p>
     * This method is executed on master node
     * </p>
     *
     * @param repository repository id
     * @param snapshots  list of snapshots that will be used as a filter, empty list means no snapshots are filtered
     * @return list of metadata for currently running snapshots
     */
    public List<SnapshotsInProgress.Entry> currentSnapshots(final String repository, final List<String> snapshots) {
        SnapshotsInProgress snapshotsInProgress = clusterService.state().custom(SnapshotsInProgress.TYPE);
        if (snapshotsInProgress == null || snapshotsInProgress.entries().isEmpty()) {
            return Collections.emptyList();
        }
        if ("_all".equals(repository)) {
            return snapshotsInProgress.entries();
        }
        if (snapshotsInProgress.entries().size() == 1) {
            // Most likely scenario - one snapshot is currently running
            // Check this snapshot against the query
            SnapshotsInProgress.Entry entry = snapshotsInProgress.entries().get(0);
            if (entry.snapshot().getRepository().equals(repository) == false) {
                return Collections.emptyList();
            }
            if (snapshots.isEmpty() == false) {
                for (String snapshot : snapshots) {
                    if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                        return snapshotsInProgress.entries();
                    }
                }
                return Collections.emptyList();
            } else {
                return snapshotsInProgress.entries();
            }
        }
        List<SnapshotsInProgress.Entry> builder = new ArrayList<>();
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.entries()) {
            if (entry.snapshot().getRepository().equals(repository) == false) {
                continue;
            }
            if (snapshots.isEmpty() == false) {
                for (String snapshot : snapshots) {
                    if (entry.snapshot().getSnapshotId().getName().equals(snapshot)) {
                        builder.add(entry);
                        break;
                    }
                }
            } else {
                builder.add(entry);
            }
        }
        return Collections.unmodifiableList(builder);
    }

    /**
     * Returns status of shards  currently finished snapshots
     * <p>
     * This method is executed on master node and it's complimentary to the {@link SnapshotShardsService#currentSnapshotShards(Snapshot)} because it
     * returns similar information but for already finished snapshots.
     * </p>
     *
     * @param repositoryName  repository name
     * @param snapshotInfo    snapshot info
     * @return map of shard id to snapshot status
     */
    public Map<ShardId, IndexShardSnapshotStatus> snapshotShards(final String repositoryName,
                                                                 final SnapshotInfo snapshotInfo) throws IOException {
        Map<ShardId, IndexShardSnapshotStatus> shardStatus = new HashMap<>();
        Repository repository = repositoriesService.repository(repositoryName);
        RepositoryData repositoryData = repository.getRepositoryData();
        MetaData metaData = repository.getSnapshotMetaData(snapshotInfo, repositoryData.resolveIndices(snapshotInfo.indices()));
        for (String index : snapshotInfo.indices()) {
            IndexId indexId = repositoryData.resolveIndexId(index);
            IndexMetaData indexMetaData = metaData.indices().get(index);
            if (indexMetaData != null) {
                int numberOfShards = indexMetaData.getNumberOfShards();
                for (int i = 0; i < numberOfShards; i++) {
                    ShardId shardId = new ShardId(indexMetaData.getIndex(), i);
                    SnapshotShardFailure shardFailure = findShardFailure(snapshotInfo.shardFailures(), shardId);
                    if (shardFailure != null) {
                        IndexShardSnapshotStatus shardSnapshotStatus = new IndexShardSnapshotStatus();
                        shardSnapshotStatus.updateStage(IndexShardSnapshotStatus.Stage.FAILURE);
                        shardSnapshotStatus.failure(shardFailure.reason());
                        shardStatus.put(shardId, shardSnapshotStatus);
                    } else {
                        IndexShardSnapshotStatus shardSnapshotStatus =
                            repository.getShardSnapshotStatus(snapshotInfo.snapshotId(), snapshotInfo.version(), indexId, shardId);
                        shardStatus.put(shardId, shardSnapshotStatus);
                    }
                }
            }
        }
        return unmodifiableMap(shardStatus);
    }


    private SnapshotShardFailure findShardFailure(List<SnapshotShardFailure> shardFailures, ShardId shardId) {
        for (SnapshotShardFailure shardFailure : shardFailures) {
            if (shardId.getIndexName().equals(shardFailure.index()) && shardId.getId() == shardFailure.shardId()) {
                return shardFailure;
            }
        }
        return null;
    }


    /**
     * Deletes a snapshot from the repository, looking up the {@link Snapshot} reference before deleting.
     * If the snapshot is still running cancels the snapshot first and then deletes it from the repository.
     *
     * @param repositoryName  repositoryName
     * @param snapshotName    snapshotName
     * @param listener        listener
     */
    public void deleteSnapshot(final String repositoryName, final String snapshotName, final DeleteSnapshotListener listener) {
        // First, look for the snapshot in the repository
        final Repository repository = repositoriesService.repository(repositoryName);
        Optional<SnapshotId> matchedEntry = repository.getRepositoryData().getSnapshotIds()
                                                .stream()
                                                .filter(s -> s.getName().equals(snapshotName))
                                                .findFirst();
        // if nothing found by the same name, then look in the cluster state for current in progress snapshots
        if (matchedEntry.isPresent() == false) {
            matchedEntry = currentSnapshots(repositoryName, Collections.emptyList()).stream()
                               .map(e -> e.snapshot().getSnapshotId()).filter(s -> s.getName().equals(snapshotName)).findFirst();
        }
        if (matchedEntry.isPresent() == false) {
            throw new SnapshotMissingException(repositoryName, snapshotName);
        }
        deleteSnapshot(new Snapshot(repositoryName, matchedEntry.get()), listener);
    }

    /**
     * Checks if a repository is currently in use by one of the snapshots
     *
     * @param clusterState cluster state
     * @param repository   repository id
     * @return true if repository is currently in use by one of the running snapshots
     */
    public static boolean isRepositoryInUse(ClusterState clusterState, String repository) {
        SnapshotsInProgress snapshots = clusterState.custom(SnapshotsInProgress.TYPE);
        if (snapshots != null) {
            for (SnapshotsInProgress.Entry snapshot : snapshots.entries()) {
                if (repository.equals(snapshot.snapshot().getRepository())) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Check if any of the indices to be deleted are currently being snapshotted. Fail as deleting an index that is being
     * snapshotted (with partial == false) makes the snapshot fail.
     */
    public static void checkIndexDeletion(ClusterState currentState, Set<IndexMetaData> indices) {
        Set<Index> indicesToFail = indicesToFailForCloseOrDeletion(currentState, indices);
        if (indicesToFail != null) {
            throw new IllegalArgumentException("Cannot delete indices that are being snapshotted: " + indicesToFail +
                ". Try again after snapshot finishes or cancel the currently running snapshot.");
        }
    }

    /**
     * Check if any of the indices to be closed are currently being snapshotted. Fail as closing an index that is being
     * snapshotted (with partial == false) makes the snapshot fail.
     */
    public static void checkIndexClosing(ClusterState currentState, Set<IndexMetaData> indices) {
        Set<Index> indicesToFail = indicesToFailForCloseOrDeletion(currentState, indices);
        if (indicesToFail != null) {
            throw new IllegalArgumentException("Cannot close indices that are being snapshotted: " + indicesToFail +
                ". Try again after snapshot finishes or cancel the currently running snapshot.");
        }
    }

    private static Set<Index> indicesToFailForCloseOrDeletion(ClusterState currentState, Set<IndexMetaData> indices) {
        SnapshotsInProgress snapshots = currentState.custom(SnapshotsInProgress.TYPE);
        Set<Index> indicesToFail = null;
        if (snapshots != null) {
            for (final SnapshotsInProgress.Entry entry : snapshots.entries()) {
                if (entry.partial() == false) {
                    if (entry.state() == State.INIT) {
                        for (IndexId index : entry.indices()) {
                            IndexMetaData indexMetaData = currentState.metaData().index(index.getName());
                            if (indexMetaData != null && indices.contains(indexMetaData)) {
                                if (indicesToFail == null) {
                                    indicesToFail = new HashSet<>();
                                }
                                indicesToFail.add(indexMetaData.getIndex());
                            }
                        }
                    } else {
                        for (ObjectObjectCursor<ShardId, SnapshotsInProgress.ShardSnapshotStatus> shard : entry.shards()) {
                            if (!shard.value.state().completed()) {
                                IndexMetaData indexMetaData = currentState.metaData().index(shard.key.getIndex());
                                if (indexMetaData != null && indices.contains(indexMetaData)) {
                                    if (indicesToFail == null) {
                                        indicesToFail = new HashSet<>();
                                    }
                                    indicesToFail.add(shard.key.getIndex());
                                }
                            }
                        }
                    }
                }
            }
        }
        return indicesToFail;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {
        clusterService.remove(this);
    }

    public RepositoriesService getRepositoriesService() {
        return repositoriesService;
    }

}