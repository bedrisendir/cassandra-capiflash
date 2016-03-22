package org.apache.cassandra.db.commitlog.capi;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.FlashCommitLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.research.capiblock.CapiBlockDevice;
import com.ibm.research.capiblock.Chunk;

public class AsyncProducerConsumerChunkManager implements ChunkManagerInterface {
	static final Logger logger = LoggerFactory.getLogger(FlashCommitLog.class);
	static String[] DEVICES = DatabaseDescriptor.getFlashCommitLogDevices();
	final CapiBlockDevice dev = CapiBlockDevice.getInstance();
	final Chunk chunks[];
	final AtomicInteger nextChunk = new AtomicInteger(0);
	int maxThreads =10;

	BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(
			DatabaseDescriptor.getFlashCommitLogNumberOfThreads());
	final ExecutorService service = new ThreadPoolExecutor(1, maxThreads, 1, TimeUnit.MILLISECONDS,
			new ArrayBlockingQueue<Runnable>(DatabaseDescriptor.getFlashCommitLogNumberOfThreads()));

	public AsyncProducerConsumerChunkManager(int num_async) {
		chunks = new Chunk[DatabaseDescriptor.getFlashCommitLogNumberOfThreads()];// FIXME//TODO
		openChunks(num_async);
		for (int i = 0; i < DatabaseDescriptor.getFlashCommitLogNumberOfThreads(); i++) {
			try {
				queue.put(new Task());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

	public AsyncProducerConsumerChunkManager() {
		this(0);
	}

	@Override
	public void openChunks(int num_async) {
		for (int i = 0; i < chunks.length; i++) {
			try {
				if (num_async == 0) {
					// let the device decide max num of requests
					chunks[i] = dev.openChunk(DEVICES[i % DEVICES.length]);
				} else {
					// user defined max requests per chunk
					chunks[i] = dev.openChunk(DEVICES[i % DEVICES.length], num_async);

				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void write(long l, long m, CheckSummedBuffer buf) {
		Chunk cur = getNextChunk();
		try {
			Task t = (Task) queue.take();
			t.future = cur.writeBlockAsync(l, m, buf.getBuffer());
			service.submit(t).get();
			queue.put(t);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void closeChunks() {
		for (int i = 0; i < chunks.length; i++) {
			try {
				chunks[i].close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	private Chunk getNextChunk() {
		return chunks[Math.abs(nextChunk.getAndIncrement() % chunks.length)];
	}

	private class Task implements Runnable {
		Future<Long> future = null;
		@Override
		public void run() {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
			}
		}
	}
}