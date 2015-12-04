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
import java.util.concurrent.Callable;
import org.apache.cassandra.utils.PureJavaCrc32;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.DataOutputByteBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.PureJavaCrc32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import capiblocksim.Chunk;


/**
 * @author bsendir1
 * Re-Usable Callable used for writing to flash. 
 */
public class FlashWorker implements Callable {
	static final Logger logger = LoggerFactory.getLogger(FlashWorker.class);
	private final ByteBuffer buffer;
	private final DataOutputByteBuffer bufferStream;
	private Chunk ref = null;

	private final PureJavaCrc32 checksum = new PureJavaCrc32();
	private Mutation rm = null;
	private FlashRecordKeeper info;
	
	public FlashWorker(Chunk chunk, int nsegmentSizeinMB) {
		buffer = ByteBuffer.allocateDirect(nsegmentSizeinMB * 1024 * 1024);
		bufferStream = new DataOutputByteBuffer(buffer);
		ref = chunk;
	}

	//TODO Fix CHSUM
	@Override
	public Callable call() {
		try {
			checksum.reset();
			bufferStream.writeLong(info.getSegmentID());
			bufferStream.writeInt(info.getTotalSize());
			//update chsum
			checksum.update(buffer, 0 , 12);
			buffer.putLong(checksum.getValue());
			Mutation.serializer.serialize(rm, bufferStream,
					MessagingService.current_version);
			//update chsum
			checksum.update(buffer,12,buffer.position()-12);
			buffer.putLong(checksum.getValue());
			ref.writeBlock(info.getStartBlock(), info.getRequiredBlocks(),
					buffer);
			buffer.clear();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return this;
	}

	public void setMessage(Mutation rm) {
		this.rm = rm;
	}

	public void setOffset(FlashRecordKeeper adder) {
		info = adder;
	}
	public void closeChunk(){
		try {
			ref.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}