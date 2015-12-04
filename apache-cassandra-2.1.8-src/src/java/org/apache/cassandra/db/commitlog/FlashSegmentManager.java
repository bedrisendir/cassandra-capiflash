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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import capiblocksim.Chunk;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author bsendir
 *
 */
public class FlashSegmentManager {
	static final Logger logger = LoggerFactory.getLogger(FlashSegmentManager.class);
	public static int MAX_SEGMENTS = DatabaseDescriptor.getFlashCommitLogNumberOfSegments();
	public static int BLOCKS_IN_SEG = DatabaseDescriptor.getFlashCommitLogSegmentSizeInBlocks();
	public static double EMERGENCY_VALVE = DatabaseDescriptor.getFlashCommitLogEmergencyValve();
	private final BlockingQueue<Integer> freelist = new LinkedBlockingQueue<Integer>(MAX_SEGMENTS);
	private final ConcurrentLinkedQueue<FlashSegment> activeSegments = new ConcurrentLinkedQueue<FlashSegment>();
	ByteBuffer util = ByteBuffer.allocateDirect(1024 * 4);// utility buffer for
															// bookkeping
															// purposes
	HashMap<Integer, Long> unCommitted;
	Chunk bookkeeper = null;
	volatile FlashSegment active;

	FlashSegmentManager(Chunk chunk) {
		bookkeeper = chunk;
		unCommitted = new HashMap<Integer, Long>();
		try {// There is only one instance of FSM
			ByteBuffer recoverMe = ByteBuffer.allocateDirect(1024 * 4 * MAX_SEGMENTS);
			bookkeeper.readBlock(FlashCommitLog.START_OFFSET, MAX_SEGMENTS, recoverMe);
			for (int i = 0; i < MAX_SEGMENTS; i++) {
				recoverMe.position(i * FlashCommitLog.BLOCK_SIZE);
				long segID = recoverMe.getLong();
				if (segID != 0) {// Committed Segments will be 0 unCommitted
									// Segments will contain the unique id
					unCommitted.put(i, segID);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (int i = 0; i < MAX_SEGMENTS; i++) {
			if (!unCommitted.containsKey(i)) {
				freelist.add(i);
			} else {
				logger.debug(i + " will be replayed");
			}
		}
		activateNextSegment();
	}

	private void activateNextSegment() {
		if (freelist.size() < MAX_SEGMENTS * EMERGENCY_VALVE) {
			logger.debug("!!!!!!!----------!!!!!!!!!!--------!!!!!!!\nEmergency valve in action flushing oldest....");
			flushOldestKeyspaces();
		}
		try {
			active = new FlashSegment(freelist.take());
			try {
				ByteBuffer buf = ByteBuffer.allocateDirect(1024 * 4);
				logger.debug("Activating " + active.getID() + " with PB:" + active.getPB() + " --> "
						+ FlashCommitLog.START_OFFSET + active.getPB());
				buf.putLong(active.getID());
				bookkeeper.writeBlock(FlashCommitLog.START_OFFSET + active.getPB(), 1, buf);
			} catch (IOException e) {
				e.printStackTrace();
			}
			activeSegments.add(active);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void flushOldestKeyspaces() {
		{
			FlashSegment oldestSegment = activeSegments.peek();
			if (oldestSegment != null && oldestSegment != this.active) {
				for (UUID dirtyCFId : oldestSegment.getDirtyCFIDs()) {
					Pair<String, String> pair = Schema.instance.getCF(dirtyCFId);
					if (pair == null) {
						// even though we remove the schema entry before a final
						// flush when dropping a CF,
						// it's still possible for a writer to race and finish
						// his append after the flush.
						logger.debug("Marking clean CF {} that doesn't exist anymore", dirtyCFId);
						oldestSegment.markClean(dirtyCFId, oldestSegment.getContext());
					} else {
						String keypace = pair.left;
						final ColumnFamilyStore cfs = Keyspace.open(keypace).getColumnFamilyStore(dirtyCFId);
						cfs.forceFlush();
					}
				}
			}
		}
	}

	synchronized void recycleSegment(final FlashSegment segment) {
		activeSegments.remove(segment);
		try {
			util.putLong(0);
			bookkeeper.writeBlock(FlashCommitLog.START_OFFSET + segment.getPB(), 1, util);
			util.clear();
			freelist.put((int) segment.getPB());
		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
		}
	}

	public Collection<FlashSegment> getActiveSegments() {
		return Collections.unmodifiableCollection(activeSegments);
	}

	synchronized FlashRecordKeeper allocate(long num_blocks, Mutation rm) {
		if (active == null || !active.hasCapacityFor(num_blocks)) {
			logger.error("actiating new segment ->" + active.physical_block_address);
			activateNextSegment();
		}
		active.markDirty(rm, active.getContext());
		return new FlashRecordKeeper(num_blocks, active.getandAddPosition(num_blocks), active.getID());
	}

	/**
	 * Zero all bookkeeping segments
	 */
	public void recycleAfterReplay() {
		for (Integer key : unCommitted.keySet()) {
			try {
				util.clear();
				util.putLong(0);
				bookkeeper.writeBlock(FlashCommitLog.START_OFFSET + key, 1, util);
				freelist.put(key);
				logger.debug("activating key " + key);
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}
		unCommitted.clear();
	}

	
	private Future<?> flushDataFrom(List<FlashSegment> segments, boolean force) {
		if (segments.isEmpty())
			return Futures.immediateFuture(null);
		final ReplayPosition maxReplayPosition = segments.get(segments.size() - 1).getContext();

		// a map of CfId -> forceFlush() to ensure we only queue one flush per
		// cf
		final Map<UUID, ListenableFuture<?>> flushes = new LinkedHashMap<>();

		for (FlashSegment segment : segments) {
			for (UUID dirtyCFId : segment.getDirtyCFIDs()) {
				Pair<String, String> pair = Schema.instance.getCF(dirtyCFId);
				if (pair == null) {
					// even though we remove the schema entry before a final
					// flush when dropping a CF,
					// it's still possible for a writer to race and finish his
					// append after the flush.
					logger.debug("Marking clean CF {} that doesn't exist anymore", dirtyCFId);
					segment.markClean(dirtyCFId, segment.getContext());
				} else if (!flushes.containsKey(dirtyCFId)) {
					String keyspace = pair.left;
					final ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(dirtyCFId);
					// can safely call forceFlush here as we will only ever
					// block (briefly) for other attempts to flush,
					// no deadlock possibility since switchLock removal
					flushes.put(dirtyCFId, force ? cfs.forceFlush() : cfs.forceFlush(maxReplayPosition));
				}
			}
		}

		return Futures.allAsList(flushes.values());
	}

	public void forceRecycleAll(Iterable<UUID> droppedCfs) {
		List<FlashSegment> segmentsToRecycle = new ArrayList<>(activeSegments);
		FlashSegment last = segmentsToRecycle.get(segmentsToRecycle.size() - 1);
		synchronized (this) {
			activateNextSegment();
		}

		synchronized (FlashCommitLog.instance.queue) {
			while (FlashCommitLog.instance.queue.size() != FlashCommitLog.instance.flashThreads) {
				try {
					long startTime = System.currentTimeMillis();
					// logger.error("deadlock !!!!");
					FlashCommitLog.instance.queue.wait();
					long estimatedTime = System.currentTimeMillis() - startTime;
					logger.error("------------------------>" + " release Wait miliseconds " + estimatedTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		// make sure the writes have materialized inside of the memtables by
		// waiting for all outstanding writes
		// on the relevant keyspaces to complete
		Set<Keyspace> keyspaces = new HashSet<>();
		for (UUID cfId : last.getDirtyCFIDs()) {
			ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(cfId);
			if (cfs != null)
				keyspaces.add(cfs.keyspace);
		}
		for (Keyspace keyspace : keyspaces)
			keyspace.writeOrder.awaitNewBarrier();

		// flush and wait for all CFs that are dirty in segments up-to and
		// including 'last'
		Future<?> future = flushDataFrom(segmentsToRecycle, true);
		try {
			future.get();

			for (FlashSegment segment : activeSegments)
				for (UUID cfId : droppedCfs)
					segment.markClean(cfId, segment.getContext());

			// now recycle segments that are unused, as we may not have
			// triggered a discardCompletedSegments()
			// if the previous active segment was the only one to recycle (since
			// an active segment isn't
			// necessarily dirty, and we only call dCS after a flush).
			for (FlashSegment segment : activeSegments)
				if (segment.isUnused())
					recycleSegment(segment);

			FlashSegment first;
			if ((first = activeSegments.peek()) != null && first.id <= last.id)
				logger.error(
						"Failed to force-recycle all segments; at least one segment is still in use with dirty CFs.");
		} catch (Throwable t) {
			// for now just log the error and return false, indicating that we
			// failed
			logger.error("Failed waiting for a forced recycle of in-use commit log segments", t);
		}

	}

}
