package org.elasticsearch.index.store;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.index.engine.Engine;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.index.store.Store.digestToString;

/**
 * Created by hui on 2016-11-05.
 */
public  final class MetadataSnapshot implements Iterable<StoreFileMetaData>, Writeable {
    private final Map<String, StoreFileMetaData> metadata;

    public static final MetadataSnapshot EMPTY = new MetadataSnapshot();

    private final Map<String, String> commitUserData;

    private final long numDocs;

    public MetadataSnapshot(Map<String, StoreFileMetaData> metadata, Map<String, String> commitUserData, long numDocs) {
        this.metadata = metadata;
        this.commitUserData = commitUserData;
        this.numDocs = numDocs;
    }

    MetadataSnapshot() {
        metadata = emptyMap();
        commitUserData = emptyMap();
        numDocs = 0;
    }

    MetadataSnapshot(IndexCommit commit, Directory directory, ESLogger logger) throws IOException {
        LoadedMetadata loadedMetadata = loadMetadata(commit, directory, logger);
        metadata = loadedMetadata.fileMetadata;
        commitUserData = loadedMetadata.userData;
        numDocs = loadedMetadata.numDocs;
        assert metadata.isEmpty() || numSegmentFiles() == 1 : "numSegmentFiles: " + numSegmentFiles();
    }

    /**
     * Read from a stream.
     */
    public MetadataSnapshot(StreamInput in) throws IOException {
        final int size = in.readVInt();
        Map<String, StoreFileMetaData> metadata = new HashMap<>();
        for (int i = 0; i < size; i++) {
            StoreFileMetaData meta = new StoreFileMetaData(in);
            metadata.put(meta.name(), meta);
        }
        Map<String, String> commitUserData = new HashMap<>();
        int num = in.readVInt();
        for (int i = num; i > 0; i--) {
            commitUserData.put(in.readString(), in.readString());
        }

        this.metadata = unmodifiableMap(metadata);
        this.commitUserData = unmodifiableMap(commitUserData);
        this.numDocs = in.readLong();
        assert metadata.isEmpty() || numSegmentFiles() == 1 : "numSegmentFiles: " + numSegmentFiles();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.metadata.size());
        for (StoreFileMetaData meta : this) {
            meta.writeTo(out);
        }
        out.writeVInt(commitUserData.size());
        for (Map.Entry<String, String> entry : commitUserData.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue());
        }
        out.writeLong(numDocs);
    }

    /**
     * Returns the number of documents in this store snapshot
     */
    public long getNumDocs() {
        return numDocs;
    }

    static class LoadedMetadata {
        final Map<String, StoreFileMetaData> fileMetadata;
        final Map<String, String> userData;
        final long numDocs;

        LoadedMetadata(Map<String, StoreFileMetaData> fileMetadata, Map<String, String> userData, long numDocs) {
            this.fileMetadata = fileMetadata;
            this.userData = userData;
            this.numDocs = numDocs;
        }
    }

    static LoadedMetadata loadMetadata(IndexCommit commit, Directory directory, ESLogger logger) throws IOException {
        long numDocs;
        Map<String, StoreFileMetaData> builder = new HashMap<>();
        Map<String, String> commitUserDataBuilder = new HashMap<>();
        try {
            final SegmentInfos segmentCommitInfos = Store.readSegmentsInfo(commit, directory);
            numDocs = Lucene.getNumDocs(segmentCommitInfos);
            commitUserDataBuilder.putAll(segmentCommitInfos.getUserData());
            Version maxVersion = segmentCommitInfos.getMinSegmentLuceneVersion(); // we don't know which version was used to write so we take the max version.
            for (SegmentCommitInfo info : segmentCommitInfos) {
                final Version version = info.info.getVersion();
                if (version == null) {
                    // version is written since 3.1+: we should have already hit IndexFormatTooOld.
                    throw new IllegalArgumentException("expected valid version value: " + info.info.toString());
                }
                if (version.onOrAfter(maxVersion)) {
                    maxVersion = version;
                }
                for (String file : info.files()) {
                    if (version.onOrAfter(StoreFileMetaData.FIRST_LUCENE_CHECKSUM_VERSION)) {
                        checksumFromLuceneFile(directory, file, builder, logger, version, SEGMENT_INFO_EXTENSION.equals(IndexFileNames.getExtension(file)));
                    } else {
                        throw new IllegalStateException("version must be onOrAfter: " + StoreFileMetaData.FIRST_LUCENE_CHECKSUM_VERSION + " but was: " +  version);
                    }
                }
            }
            if (maxVersion == null) {
                maxVersion = StoreFileMetaData.FIRST_LUCENE_CHECKSUM_VERSION;
            }
            final String segmentsFile = segmentCommitInfos.getSegmentsFileName();
            if (maxVersion.onOrAfter(StoreFileMetaData.FIRST_LUCENE_CHECKSUM_VERSION)) {
                checksumFromLuceneFile(directory, segmentsFile, builder, logger, maxVersion, true);
            } else {
                throw new IllegalStateException("version must be onOrAfter: " + StoreFileMetaData.FIRST_LUCENE_CHECKSUM_VERSION + " but was: " +  maxVersion);
            }
        } catch (CorruptIndexException | IndexNotFoundException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // we either know the index is corrupted or it's just not there
            throw ex;
        } catch (Exception ex) {
            try {
                // Lucene checks the checksum after it tries to lookup the codec etc.
                // in that case we might get only IAE or similar exceptions while we are really corrupt...
                // TODO we should check the checksum in lucene if we hit an exception
                logger.warn("failed to build store metadata. checking segment info integrity (with commit [{}])",
                        ex, commit == null ? "no" : "yes");
                Lucene.checkSegmentInfoIntegrity(directory);
            } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException cex) {
                cex.addSuppressed(ex);
                throw cex;
            } catch (Exception inner) {
                inner.addSuppressed(ex);
                throw inner;
            }
            throw ex;
        }
        return new LoadedMetadata(unmodifiableMap(builder), unmodifiableMap(commitUserDataBuilder), numDocs);
    }

    private static void checksumFromLuceneFile(Directory directory, String file, Map<String, StoreFileMetaData> builder,
                                               ESLogger logger, Version version, boolean readFileAsHash) throws IOException {
        final String checksum;
        final BytesRefBuilder fileHash = new BytesRefBuilder();
        try (final IndexInput in = directory.openInput(file, IOContext.READONCE)) {
            final long length;
            try {
                length = in.length();
                if (length < CodecUtil.footerLength()) {
                    // truncated files trigger IAE if we seek negative... these files are really corrupted though
                    throw new CorruptIndexException("Can't retrieve checksum from file: " + file + " file length must be >= " + CodecUtil.footerLength() + " but was: " + in.length(), in);
                }
                if (readFileAsHash) {
                    final VerifyingIndexInput verifyingIndexInput = new VerifyingIndexInput(in); // additional safety we checksum the entire file we read the hash for...
                    hashFile(fileHash, new InputStreamIndexInput(verifyingIndexInput, length), length);
                    checksum = digestToString(verifyingIndexInput.verify());
                } else {
                    checksum = digestToString(CodecUtil.retrieveChecksum(in));
                }

            } catch (Exception ex) {
                logger.debug("Can retrieve checksum from file [{}]", ex, file);
                throw ex;
            }
            builder.put(file, new StoreFileMetaData(file, length, checksum, version, fileHash.get()));
        }
    }

    /**
     * Computes a strong hash value for small files. Note that this method should only be used for files &lt; 1MB
     */
    public static void hashFile(BytesRefBuilder fileHash, InputStream in, long size) throws IOException {
        final int len = (int) Math.min(1024 * 1024, size); // for safety we limit this to 1MB
        fileHash.grow(len);
        fileHash.setLength(len);
        final int readBytes = Streams.readFully(in, fileHash.bytes(), 0, len);
        assert readBytes == len : Integer.toString(readBytes) + " != " + Integer.toString(len);
        assert fileHash.length() == len : Integer.toString(fileHash.length()) + " != " + Integer.toString(len);
    }

    @Override
    public Iterator<StoreFileMetaData> iterator() {
        return metadata.values().iterator();
    }

    public StoreFileMetaData get(String name) {
        return metadata.get(name);
    }

    public Map<String, StoreFileMetaData> asMap() {
        return metadata;
    }

    private static final String DEL_FILE_EXTENSION = "del"; // legacy delete file
    private static final String LIV_FILE_EXTENSION = "liv"; // lucene 5 delete file
    private static final String FIELD_INFOS_FILE_EXTENSION = "fnm";
    private static final String SEGMENT_INFO_EXTENSION = "si";

    /**
     * Returns a diff between the two snapshots that can be used for recovery. The given snapshot is treated as the
     * recovery target and this snapshot as the source. The returned diff will hold a list of files that are:
     * <ul>
     * <li>identical: they exist in both snapshots and they can be considered the same ie. they don't need to be recovered</li>
     * <li>different: they exist in both snapshots but their they are not identical</li>
     * <li>missing: files that exist in the source but not in the target</li>
     * </ul>
     * This method groups file into per-segment files and per-commit files. A file is treated as
     * identical if and on if all files in it's group are identical. On a per-segment level files for a segment are treated
     * as identical iff:
     * <ul>
     * <li>all files in this segment have the same checksum</li>
     * <li>all files in this segment have the same length</li>
     * <li>the segments <tt>.si</tt> files hashes are byte-identical Note: This is a using a perfect hash function, The metadata transfers the <tt>.si</tt> file content as it's hash</li>
     * </ul>
     * <p>
     * The <tt>.si</tt> file contains a lot of diagnostics including a timestamp etc. in the future there might be
     * unique segment identifiers in there hardening this method further.
     * <p>
     * The per-commit files handles very similar. A commit is composed of the <tt>segments_N</tt> files as well as generational files like
     * deletes (<tt>_x_y.del</tt>) or field-info (<tt>_x_y.fnm</tt>) files. On a per-commit level files for a commit are treated
     * as identical iff:
     * <ul>
     * <li>all files belonging to this commit have the same checksum</li>
     * <li>all files belonging to this commit have the same length</li>
     * <li>the segments file <tt>segments_N</tt> files hashes are byte-identical Note: This is a using a perfect hash function, The metadata transfers the <tt>segments_N</tt> file content as it's hash</li>
     * </ul>
     * <p>
     * NOTE: this diff will not contain the <tt>segments.gen</tt> file. This file is omitted on recovery.
     */
    public Store.RecoveryDiff recoveryDiff(MetadataSnapshot recoveryTargetSnapshot) {
        final List<StoreFileMetaData> identical = new ArrayList<>();
        final List<StoreFileMetaData> different = new ArrayList<>();
        final List<StoreFileMetaData> missing = new ArrayList<>();
        final Map<String, List<StoreFileMetaData>> perSegment = new HashMap<>();
        final List<StoreFileMetaData> perCommitStoreFiles = new ArrayList<>();

        for (StoreFileMetaData meta : this) {
            if (IndexFileNames.OLD_SEGMENTS_GEN.equals(meta.name())) { // legacy
                continue; // we don't need that file at all
            }
            final String segmentId = IndexFileNames.parseSegmentName(meta.name());
            final String extension = IndexFileNames.getExtension(meta.name());
            assert FIELD_INFOS_FILE_EXTENSION.equals(extension) == false || IndexFileNames.stripExtension(IndexFileNames.stripSegmentName(meta.name())).isEmpty() : "FieldInfos are generational but updateable DV are not supported in elasticsearch";
            if (IndexFileNames.SEGMENTS.equals(segmentId) || DEL_FILE_EXTENSION.equals(extension) || LIV_FILE_EXTENSION.equals(extension)) {
                // only treat del files as per-commit files fnm files are generational but only for upgradable DV
                perCommitStoreFiles.add(meta);
            } else {
                List<StoreFileMetaData> perSegStoreFiles = perSegment.get(segmentId);
                if (perSegStoreFiles == null) {
                    perSegStoreFiles = new ArrayList<>();
                    perSegment.put(segmentId, perSegStoreFiles);
                }
                perSegStoreFiles.add(meta);
            }
        }
        final ArrayList<StoreFileMetaData> identicalFiles = new ArrayList<>();
        for (List<StoreFileMetaData> segmentFiles : Iterables.concat(perSegment.values(), Collections.singleton(perCommitStoreFiles))) {
            identicalFiles.clear();
            boolean consistent = true;
            for (StoreFileMetaData meta : segmentFiles) {
                StoreFileMetaData storeFileMetaData = recoveryTargetSnapshot.get(meta.name());
                if (storeFileMetaData == null) {
                    consistent = false;
                    missing.add(meta);
                } else if (storeFileMetaData.isSame(meta) == false) {
                    consistent = false;
                    different.add(meta);
                } else {
                    identicalFiles.add(meta);
                }
            }
            if (consistent) {
                identical.addAll(identicalFiles);
            } else {
                // make sure all files are added - this can happen if only the deletes are different
                different.addAll(identicalFiles);
            }
        }
        Store.RecoveryDiff recoveryDiff = new Store.RecoveryDiff(Collections.unmodifiableList(identical), Collections.unmodifiableList(different), Collections.unmodifiableList(missing));
        assert recoveryDiff.size() == this.metadata.size() - (metadata.containsKey(IndexFileNames.OLD_SEGMENTS_GEN) ? 1 : 0)
                : "some files are missing recoveryDiff size: [" + recoveryDiff.size() + "] metadata size: [" + this.metadata.size() + "] contains  segments.gen: [" + metadata.containsKey(IndexFileNames.OLD_SEGMENTS_GEN) + "]";
        return recoveryDiff;
    }

    /**
     * Returns the number of files in this snapshot
     */
    public int size() {
        return metadata.size();
    }

    public Map<String, String> getCommitUserData() {
        return commitUserData;
    }

    /**
     * Returns true iff this metadata contains the given file.
     */
    public boolean contains(String existingFile) {
        return metadata.containsKey(existingFile);
    }

    /**
     * Returns the segments file that this metadata snapshot represents or null if the snapshot is empty.
     */
    public StoreFileMetaData getSegmentsFile() {
        for (StoreFileMetaData file : this) {
            if (file.name().startsWith(IndexFileNames.SEGMENTS)) {
                return file;
            }
        }
        assert metadata.isEmpty();
        return null;
    }

    private int numSegmentFiles() { // only for asserts
        int count = 0;
        for (StoreFileMetaData file : this) {
            if (file.name().startsWith(IndexFileNames.SEGMENTS)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Returns the sync id of the commit point that this MetadataSnapshot represents.
     *
     * @return sync id if exists, else null
     */
    public String getSyncId() {
        return commitUserData.get(Engine.SYNC_COMMIT_ID);
    }
}
