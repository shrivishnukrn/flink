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

package org.apache.flink.core.io;

import org.apache.flink.annotation.Public;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * This interface must be implemented by every class whose objects have to be serialized to their binary representation
 * and vice-versa. In particular, records have to implement this interface in order to specify how their data can be
 * transferred to a binary representation.
 * 
 * <p>When implementing this Interface make sure that the implementing class has a default
 * (zero-argument) constructor!
 */
@Public
public interface IOReadableWritable {

	/**
	 * Writes the object's internal data to the given data output view.
	 * 
	 * @param out
	 *        the output view to receive the data.
	 * @throws IOException
	 *         thrown if any error occurs while writing to the output stream
	 */
	void write(DataOutputView out) throws IOException;

	/**
	 * Reads the object's internal data from the given data input view.
	 * 
	 * @param in
	 *        the input view to read the data from
	 * @throws IOException
	 *         thrown if any error occurs while reading from the input stream
	 */
	void read(DataInputView in) throws IOException;

	class TracePojo {
		public static final int TRACE_POJO_HEADER = Integer.MAX_VALUE;

		private final int recordWriterId;
		private final int serializerId;
		private final long threadId;
		private final long currentTimeMillis;
		private final long counter;

		public TracePojo(
				int recordWriterId,
				int serializerId,
				long threadId,
				long currentTimeMillis,
				long counter) {
			this.recordWriterId = recordWriterId;
			this.serializerId = serializerId;
			this.threadId = threadId;
			this.currentTimeMillis = currentTimeMillis;
			this.counter = counter;
		}

		@Override
		public String toString() {
			return "TraceRecord{" +
				"recordWriterId=" + recordWriterId +
				", serializerId=" + serializerId +
				", threadId=" + threadId +
				", currentTimeMillis=" + currentTimeMillis +
				", counter=" + counter + '}';
		}


		public void write(DataOutputView out) throws IOException {
			out.writeInt(recordWriterId);
			out.writeInt(serializerId);
			out.writeLong(threadId);
			out.writeLong(currentTimeMillis);
			out.writeLong(counter);
		}

		public static TracePojo readTracePojo(DataInputView in) throws IOException {
			int recordWriterId = in.readInt();
			int serializerId = in.readInt();
			long threadId = in.readLong();
			long currentTimeMillis = in.readLong();
			long counter = in.readLong();

			return new TracePojo(recordWriterId, serializerId, threadId, currentTimeMillis, counter);
		}
		public static int getSizeInBytes() {
			// 5 bytes for variable length encoded TRACE_POJO_HEADER
			return 5 + Integer.BYTES * 2 + Long.BYTES * 3;
		}
	}
}
