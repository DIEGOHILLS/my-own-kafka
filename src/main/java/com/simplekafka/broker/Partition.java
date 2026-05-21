package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Partition {
    private static final long SEGMENT_SIZE = 1024 * 1024;
    private static final int INDEX_ENTRY_SIZE = 16;

    private final int id;
    private int leader;
    private List<Integer> followers;
    private final String baseDir;
    private final AtomicLong nextOffset;
    private final ReadWriteLock lock;
    private RandomAccessFile activeLogFile;
    private FileChannel activeLogChannel;
    private final List<SegmentInfo> segments;

    public Partition(int id, String baseDir, int leader, List<Integer> followers) {
        this.id = id;
        this.baseDir = baseDir;
        this.leader = leader;
        this.followers = followers != null ? followers : new ArrayList<>();
        this.nextOffset = new AtomicLong(0);
        this.lock = new ReentrantReadWriteLock();
        this.segments = new ArrayList<>();
        initialize();
    }

    private void initialize() {
        File dir = new File(baseDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        File[] files = dir.listFiles((d, name) -> name.endsWith(".log"));
        if (files != null && files.length > 0) {
            for (File logFile : files) {
                String baseName = logFile.getName().replace(".log", "");
                long baseOffset = Long.parseLong(baseName);
                File indexFile = new File(dir, baseName + ".index");
                segments.add(new SegmentInfo(baseOffset, logFile, indexFile));
            }
            Collections.sort(segments, (a, b) -> Long.compare(a.baseOffset, b.baseOffset));

            SegmentInfo lastSegment = segments.get(segments.size() - 1);
            long messageCount = calculateMessageCount(lastSegment);
            nextOffset.set(lastSegment.baseOffset + messageCount);
            openSegmentForAppend(lastSegment);
        } else {
            createNewSegment(0);
        }
    }

    private long calculateMessageCount(SegmentInfo segment) {
        long count = 0;
        try (RandomAccessFile raf = new RandomAccessFile(segment.logFile, "r");
             FileChannel channel = raf.getChannel()) {

            long position = 0;
            long fileSize = channel.size();
            ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

            while (position < fileSize) {
                sizeBuffer.clear();
                int read = channel.read(sizeBuffer, position);
                if (read < 4) break;
                sizeBuffer.flip();
                int messageSize = sizeBuffer.getInt();
                position += 4 + messageSize;
                count++;
            }
        } catch (IOException e) {
            // Log error if needed
        }
        return count;
    }

    public long append(byte[] message) {
        lock.writeLock().lock();
        try {
            if (activeLogChannel.position() >= SEGMENT_SIZE) {
                long newBaseOffset = nextOffset.get();
                createNewSegment(newBaseOffset);
            }

            long offset = nextOffset.getAndIncrement();
            long position = activeLogChannel.position();

            ByteBuffer messageBuffer = ByteBuffer.allocate(4 + message.length);
            messageBuffer.putInt(message.length);
            messageBuffer.put(message);
            messageBuffer.flip();

            activeLogChannel.write(messageBuffer);
            activeLogChannel.force(true);

            updateIndex(offset, position);

            return offset;
        } catch (IOException e) {
            throw new RuntimeException("Failed to append message", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<byte[]> readMessages(long offset, int maxBytes) {
        lock.readLock().lock();
        try {
            List<byte[]> messages = new ArrayList<>();
            int bytesRead = 0;

            SegmentInfo segment = findSegmentForOffset(offset);
            if (segment == null) {
                return messages;
            }

            long position = findPositionForOffset(segment, offset);

            try (RandomAccessFile raf = new RandomAccessFile(segment.logFile, "r");
                 FileChannel channel = raf.getChannel()) {

                channel.position(position);
                ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

                while (bytesRead < maxBytes) {
                    sizeBuffer.clear();
                    int read = channel.read(sizeBuffer);
                    if (read < 4) break;
                    sizeBuffer.flip();
                    int messageSize = sizeBuffer.getInt();

                    if (bytesRead + 4 + messageSize > maxBytes) break;

                    ByteBuffer msgBuffer = ByteBuffer.allocate(messageSize);
                    channel.read(msgBuffer);
                    msgBuffer.flip();

                    byte[] message = new byte[messageSize];
                    msgBuffer.get(message);
                    messages.add(message);

                    bytesRead += 4 + messageSize;
                    offset++;
                }
            }

            return messages;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read messages", e);
        } finally {
            lock.readLock().unlock();
        }
    }

    private void createNewSegment(long baseOffset) {
        try {
            if (activeLogChannel != null) {
                activeLogChannel.force(true);
                activeLogChannel.close();
                activeLogFile.close();
            }

            String fileName = String.format("%020d", baseOffset);
            File logFile = new File(baseDir, fileName + ".log");
            File indexFile = new File(baseDir, fileName + ".index");

            logFile.createNewFile();
            indexFile.createNewFile();

            SegmentInfo segment = new SegmentInfo(baseOffset, logFile, indexFile);
            segments.add(segment);
            openSegmentForAppend(segment);

        } catch (IOException e) {
            throw new RuntimeException("Failed to create new segment", e);
        }
    }

    private void openSegmentForAppend(SegmentInfo segment) {
        try {
            if (activeLogChannel != null) {
                activeLogChannel.force(true);
                activeLogChannel.close();
                activeLogFile.close();
            }

            activeLogFile = new RandomAccessFile(segment.logFile, "rw");
            activeLogChannel = activeLogFile.getChannel();
            activeLogChannel.position(activeLogChannel.size());

        } catch (IOException e) {
            throw new RuntimeException("Failed to open segment for append", e);
        }
    }

    private SegmentInfo findSegmentForOffset(long offset) {
        if (segments.isEmpty()) return null;

        int left = 0;
        int right = segments.size() - 1;

        while (left < right) {
            int mid = left + (right - left) / 2;
            if (segments.get(mid).baseOffset <= offset) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        int index = (left > 0 && segments.get(left).baseOffset > offset) ? left - 1 : left;
        if (index < 0 || index >= segments.size()) return null;

        SegmentInfo segment = segments.get(index);
        if (index < segments.size() - 1 && offset >= segments.get(index + 1).baseOffset) {
            return null;
        }

        return segment;
    }

    private void updateIndex(long offset, long position) {
        try {
            SegmentInfo currentSegment = segments.get(segments.size() - 1);

            try (RandomAccessFile indexRaf = new RandomAccessFile(currentSegment.indexFile, "rw");
                 FileChannel indexChannel = indexRaf.getChannel()) {

                indexChannel.position(indexChannel.size());
                ByteBuffer entry = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
                entry.putLong(offset);
                entry.putLong(position);
                entry.flip();
                indexChannel.write(entry);
                indexChannel.force(true);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to update index", e);
        }
    }

    private long findPositionForOffset(SegmentInfo segment, long offset) {
        try (RandomAccessFile indexRaf = new RandomAccessFile(segment.indexFile, "r");
             FileChannel indexChannel = indexRaf.getChannel()) {

            long indexSize = indexChannel.size();
            if (indexSize == 0) {
                return 0;
            }

            long relativeOffset = offset - segment.baseOffset;
            long entryPosition = relativeOffset * INDEX_ENTRY_SIZE;

            if (entryPosition >= indexSize) {
                long numEntries = indexSize / INDEX_ENTRY_SIZE;
                entryPosition = (numEntries - 1) * INDEX_ENTRY_SIZE;
            }

            ByteBuffer entry = ByteBuffer.allocate(INDEX_ENTRY_SIZE);
            indexChannel.read(entry, entryPosition);
            entry.flip();

            long indexedOffset = entry.getLong();
            long indexedPosition = entry.getLong();

            if (indexedOffset == offset) {
                return indexedPosition;
            }

            try (RandomAccessFile logRaf = new RandomAccessFile(segment.logFile, "r");
                 FileChannel logChannel = logRaf.getChannel()) {

                long currentPos = indexedPosition;
                long currentOffset = indexedOffset;
                ByteBuffer sizeBuffer = ByteBuffer.allocate(4);

                while (currentOffset < offset) {
                    sizeBuffer.clear();
                    int read = logChannel.read(sizeBuffer, currentPos);
                    if (read < 4) break;
                    sizeBuffer.flip();
                    int msgSize = sizeBuffer.getInt();
                    currentPos += 4 + msgSize;
                    currentOffset++;
                }

                return currentPos;
            }

        } catch (IOException e) {
            throw new RuntimeException("Failed to find position for offset", e);
        }
    }

    public int getId() {
        return id;
    }

    public int getLeader() {
        return leader;
    }

    public void setLeader(int leader) {
        this.leader = leader;
    }

    public List<Integer> getFollowers() {
        return followers;
    }

    public void setFollowers(List<Integer> followers) {
        this.followers = followers;
    }

    public long getNextOffset() {
        return nextOffset.get();
    }

    public void close() {
        lock.writeLock().lock();
        try {
            if (activeLogChannel != null) {
                activeLogChannel.force(true);
                activeLogChannel.close();
                activeLogFile.close();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to close partition", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static class SegmentInfo {
        final long baseOffset;
        final File logFile;
        final File indexFile;

        SegmentInfo(long baseOffset, File logFile, File indexFile) {
            this.baseOffset = baseOffset;
            this.logFile = logFile;
            this.indexFile = indexFile;
        }
    }
}
