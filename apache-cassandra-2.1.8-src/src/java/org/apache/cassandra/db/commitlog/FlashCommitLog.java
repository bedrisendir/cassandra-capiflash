/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.net.MessagingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import capiblocksim.CapiBlockDevice;
import capiblocksim.Chunk;

import com.google.common.util.concurrent.Futures;

/**
 * @author bsendir
 *
 */
public class FlashCommitLog implements ICommitLog {
	// TUNABLES
	static int BLOCK_SIZE = 4096;
	static long START_OFFSET = DatabaseDescriptor.getFlashCommitLogStartOffset();
	static String[] DEVICES = DatabaseDescriptor.getFlashCommitLogDevices();
	static int flashThreads = DatabaseDescriptor.getFlashCommitLogNumberOfThreads();
	static int bufferSizeinMB = DatabaseDescriptor.getFlashCommitLogThreadBufferSizeinMB();
	static long DATA_OFFSET = START_OFFSET + FlashSegmentManager.MAX_SEGMENTS;
	static final Logger logger = LoggerFactory.getLogger(FlashCommitLog.class);
	public static final FlashCommitLog instance = new FlashCommitLog();
	final CapiBlockDevice dev = CapiBlockDevice.getInstance();
	final BlockingQueue<FlashWorker> queue = new LinkedBlockingQueue<FlashWorker>(flashThreads);
	final ThreadPoolExecutor exec = (ThreadPoolExecutor) Executors.newFixedThreadPool(flashThreads);

	protected volatile FlashSegmentManager fsm;

	protected FlashCommitLog() {
		try {
			Chunk chunk = dev.openChunk(DEVICES[0]);
			fsm = new FlashSegmentManager(chunk);
			for (int i = 0; i < flashThreads; i++) {
				// Pre allocate workers
				Chunk chunkl = dev.openChunk(DEVICES[i % DEVICES.length]);
				queue.add(new FlashWorker(chunkl, bufferSizeinMB));
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	/**
	 * Appends row mutation to CommitLog Called from
	 * org.apache.cassandra.db.Keyspace.java Line 348 adds each mutation to
	 * Commitlog
	 * 
	 * @param
	 */
	public ReplayPosition add(Mutation rm) {
		assert rm != null;
		try {
			FlashWorker r = queue.take();
			r.setMessage(rm);
			long totalSize = Mutation.serializer.serializedSize(rm, MessagingService.current_version) + 28;
			long requiredBlocks = getBlockCount(totalSize);
			if (requiredBlocks > DatabaseDescriptor.getFlashCommitLogSegmentSizeInBlocks()
					|| requiredBlocks > DatabaseDescriptor.getFlashCommitLogThreadBufferSizeinMB() * (256)) {
				throw new IllegalArgumentException(
						String.format("Mutation of %s bytes is too large for the maxiumum size of %s", totalSize,
								DatabaseDescriptor.getFlashCommitLogThreadBufferSizeinMB() * (256)));
			}
			FlashRecordKeeper adder = fsm.allocate(requiredBlocks, rm);
			adder.setSize((int) totalSize);
			r.setOffset(adder);
			queue.add((FlashWorker) exec.submit(r).get());// wait to finish
			long reppos = adder.getStartBlock() + adder.getRequiredBlocks();
			synchronized (queue) {
				if (queue.size() == flashThreads) {
					queue.notifyAll();
				}
			}
			return new ReplayPosition(fsm.active.id, (int) reppos);
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Modifies the per-CF dirty cursors of any commit log segments for the
	 * column family according to the position given. Recycles it if it is
	 * unused. Called from org.apache.cassandra.db.ColumnFamilyStore.java at the
	 * end of flush operation
	 * 
	 * @param cfId
	 *            the column family ID that was flushed
	 * @param context
	 *            the replay position of the flush
	 */
	public void discardCompletedSegments(UUID cfId, final ReplayPosition context) {
		// Keyspace has a static ReEntrantLock. It writeLocks globally.
		// So we only need to wait for writetoFlash to finish
		synchronized (queue) {
			while (queue.size() != flashThreads) {
				try {
					long startTime = System.currentTimeMillis();
					logger.error("deadlock !!!!");
					queue.wait();
					long estimatedTime = System.currentTimeMillis() - startTime;
					logger.error("------------------------>" + " discard release Wait miliseconds " + estimatedTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		// Go thru the active segment files, which are ordered oldest to
		// newest, marking the
		// flushed CF as clean, until we reach the segment file
		// containing the ReplayPosition passed
		// in the arguments. Any segments that become unused after they
		// are marked clean will be
		// recycled or discarded.
		for (Iterator<FlashSegment> iter = fsm.getActiveSegments().iterator(); iter.hasNext();) {
			FlashSegment segment = iter.next();
			segment.markClean(cfId, context);
			// If the segment is no longer needed, and we have another
			// spare segment in the hopper
			// (to keep the last segment from getting discarded), pursue
			// either recycling or deleting
			// this segment file.
			if (iter.hasNext()) {
				if (segment.isUnused()) {
					logger.debug("Commit log segment {} is unused ", segment.physical_block_address);
					fsm.recycleSegment(segment);
				} else {
					logger.debug("Not safe to delete commit log segment {}; dirty is {} ",
							segment.physical_block_address, "  dirty:", segment.dirtyString());
				}
			} else {
				logger.debug("Not deleting active commitlog segment {} " + segment.physical_block_address);
			}
			if (segment.contains(context)) {
				break;
			}
		}

	}

	/**
	 * Recover
	 */
	public int recover() {
		long startTime = System.currentTimeMillis();
		FlashBulkReplayer r = new FlashBulkReplayer();
		try {
			r.recover(fsm);
		} catch (IOException e) {
			e.printStackTrace();
		}
		long count = r.blockForWrites();
		fsm.recycleAfterReplay();
		long estimatedTime = System.currentTimeMillis() - startTime;
		logger.debug("------------------------>" + " Replayed " + count + " records in " + estimatedTime);
		return (int) count;
	}

	/**
	 * Shuts down the threads used by the commit log, blocking until completion.
	 */
	public void shutdownBlocking() {
		exec.shutdown();
		try {
			exec.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
			Iterator<FlashWorker> it = queue.iterator();
			while (it.hasNext()) {
				it.next().closeChunk();
			}
			try {
				fsm.bookkeeper.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 
	 * @return last position in commitlog
	 */
	public ReplayPosition getContext() {
		return fsm.active.getContext();
	}

	/**
	 * Utility function to calculate number of flash blocks needed
	 * 
	 * @param size
	 * @return
	 */
	static long getBlockCount(long size) {
		return (long) (Math.ceil((double) size / (FlashCommitLog.BLOCK_SIZE)));
	}

	public long getPendingTasks() {
		return flashThreads - queue.size();
	}

	// whilst we've flushed all the CFs, which will have recycled all completed
	// segments, we want to ensure
	// there are no segments to replay, so we force the recycling of any
	// remaining (should be at most one)
	public void forceRecycleAllSegments() {
		forceRecycleAllSegments(Collections.<UUID> emptyList());
	}

	public void forceRecycleAllSegments(Iterable<UUID> droppedCfs) {
		fsm.forceRecycleAll(droppedCfs);
	}
}
