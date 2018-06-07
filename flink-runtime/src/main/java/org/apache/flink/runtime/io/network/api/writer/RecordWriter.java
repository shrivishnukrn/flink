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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordSerializer;
import org.apache.flink.runtime.io.network.api.serialization.SpanningRecordSerializer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.operators.RecordBatch;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.util.XORShiftRandom;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.Random;

import static org.apache.flink.runtime.io.network.api.serialization.RecordSerializer.SerializationResult;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A record-oriented runtime result writer.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of
 * serializing records into buffers.
 *
 * <p><strong>Important</strong>: it is necessary to call {@link #flushAll()} after
 * all records have been written with {@link #emit(IOReadableWritable)}. This
 * ensures that all produced records are written to the output stream (incl.
 * partially filled ones).
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public class RecordWriter<T extends IOReadableWritable> {

	protected final ResultPartitionWriter targetPartition;

	private final ChannelSelector<T> channelSelector;

	private final Optional<SimpleChannelSelector<T>> simpleChannelSelector;

	private final int numChannels;

	/**
	 * {@link RecordSerializer} per outgoing channel.
	 */
	private final RecordSerializer<T>[] serializers;

	private final Optional<BufferBuilder>[] bufferBuilders;

	private final Random rng = new XORShiftRandom();

	private final boolean flushAlways;

	private Counter numBytesOut = new SimpleCounter();

	private static class BatchIndexToChannel {
		public int batchIndex, channel;

		private BatchIndexToChannel(int batchIndex, int channel) {
			this.batchIndex = batchIndex;
			this.channel = channel;
		}

		public void set(int batchIndex, int channel) {
			this.batchIndex = batchIndex;
			this.channel = channel;
		}
	}

	private final BatchIndexToChannel[] indexes = new BatchIndexToChannel[RuntimeContext.MAX_BATCH];

	public RecordWriter(ResultPartitionWriter writer) {
		this(writer, new RoundRobinChannelSelector<T>());
	}

	@SuppressWarnings("unchecked")
	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector) {
		this(writer, channelSelector, false);
	}

	public RecordWriter(ResultPartitionWriter writer, ChannelSelector<T> channelSelector, boolean flushAlways) {
		this.flushAlways = flushAlways;
		this.targetPartition = writer;
		this.channelSelector = channelSelector;
		this.simpleChannelSelector = channelSelector instanceof SimpleChannelSelector ? Optional.of((SimpleChannelSelector<T>) channelSelector) : Optional.empty();

		this.numChannels = writer.getNumberOfSubpartitions();

		/*
		 * The runtime exposes a channel abstraction for the produced results
		 * (see {@link ChannelSelector}). Every channel has an independent
		 * serializer.
		 */
		this.serializers = new SpanningRecordSerializer[numChannels];
		this.bufferBuilders = new Optional[numChannels];
		for (int i = 0; i < numChannels; i++) {
			serializers[i] = new SpanningRecordSerializer<T>();
			bufferBuilders[i] = Optional.empty();
		}

		for (int i = 0; i < indexes.length; i++) {
			indexes[i] = new BatchIndexToChannel(-1, -1);
		}
	}

	public <X> void emit(RecordBatch<X> batch, SerializationDelegate serializationDelegate) throws IOException, InterruptedException {
		if (!simpleChannelSelector.isPresent()) {
			naitveBatchEmit(batch, serializationDelegate);
		}
		optimizedBatchEmit(batch, serializationDelegate);
	}

	private <X> void naitveBatchEmit(RecordBatch<X> batch, SerializationDelegate serializationDelegate) throws IOException, InterruptedException {
		for (int i = 0; i < batch.getNumberOfElements(); i++) {
			X record = batch.get(i);
			serializationDelegate.setInstance(record);
			emit((T) serializationDelegate);
		}
		finishBatch();
	}

	private <X> void optimizedBatchEmit(RecordBatch<X> batch, SerializationDelegate serializationDelegate) throws IOException, InterruptedException {
		int limit = batch.getNumberOfElements();
		for (int i = 0; i < batch.getNumberOfElements(); i++) {
			serializationDelegate.setInstance(batch.get(i));
			indexes[i].set(i, simpleChannelSelector.get().selectChannel((T) serializationDelegate, numChannels));
		}

		Arrays.sort(indexes, 0, limit, BatchIndexToChannelComparator.INSTANCE);
		sendToTarget(batch, indexes, serializationDelegate);
	}

	public void emit(T record) throws IOException, InterruptedException {
		for (int targetChannel : channelSelector.selectChannels(record, numChannels)) {
			sendToTarget(record, targetChannel);
		}
	}

	/**
	 * This is used to broadcast Streaming Watermarks in-band with records. This ignores
	 * the {@link ChannelSelector}.
	 */
	public void broadcastEmit(T record) throws IOException, InterruptedException {
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			sendToTarget(record, targetChannel);
		}
	}

	/**
	 * This is used to send LatencyMarks to a random target channel.
	 */
	public void randomEmit(T record) throws IOException, InterruptedException {
		sendToTarget(record, rng.nextInt(numChannels));
	}

	private <X> void sendToTarget(RecordBatch<X> batch, BatchIndexToChannel[] indexes, SerializationDelegate serializationDelegate) throws IOException, InterruptedException {
		int lastIndex = 0;
		int lastChannel = indexes[0].channel;

		for (int i = 0; i < batch.getNumberOfElements(); i++) {
			int newChannel = indexes[i].channel;
			if (lastChannel != newChannel) {
				sendToTarget(batch, indexes, lastIndex, i, serializationDelegate);
				lastChannel = newChannel;
				lastIndex = i;
			}
		}
		sendToTarget(batch, indexes, lastIndex, batch.getNumberOfElements(), serializationDelegate);
	}

	private <X> void sendToTarget(RecordBatch<X> batch, BatchIndexToChannel[] indexes, int startIndex, int endIndex, SerializationDelegate serializationDelegate) throws IOException, InterruptedException {
		RecordSerializer<T> serializer = serializers[indexes[startIndex].channel];
		int targetChannel = indexes[startIndex].channel;

		for (int i = startIndex; i < endIndex; i++) {
			serializationDelegate.setInstance(batch.get(indexes[i].batchIndex));

			SerializationResult result = serializer.addRecord((T) serializationDelegate);

			while (result.isFullBuffer()) {
				if (tryFinishCurrentBufferBuilder(targetChannel, serializer)) {
					// If this was a full record, we are done. Not breaking
					// out of the loop at this point will lead to another
					// buffer request before breaking out (that would not be
					// a problem per se, but it can lead to stalls in the
					// pipeline).
					if (result.isFullRecord()) {
						break;
					}
				}
				BufferBuilder bufferBuilder = requestNewBufferBuilder(targetChannel);

				result = serializer.continueWritingWithNextBufferBuilder(bufferBuilder);
			}
			checkState(!serializer.hasSerializedData(), "All data should be written at once");


		}
		serializer.finishBatch();
	}

	private void sendToTarget(T record, int targetChannel) throws IOException, InterruptedException {
		RecordSerializer<T> serializer = serializers[targetChannel];

		SerializationResult result = serializer.addRecord(record);

		while (result.isFullBuffer()) {
			if (tryFinishCurrentBufferBuilder(targetChannel, serializer)) {
				// If this was a full record, we are done. Not breaking
				// out of the loop at this point will lead to another
				// buffer request before breaking out (that would not be
				// a problem per se, but it can lead to stalls in the
				// pipeline).
				if (result.isFullRecord()) {
					break;
				}
			}
			BufferBuilder bufferBuilder = requestNewBufferBuilder(targetChannel);

			result = serializer.continueWritingWithNextBufferBuilder(bufferBuilder);
		}
		checkState(!serializer.hasSerializedData(), "All data should be written at once");

		if (flushAlways) {
			targetPartition.flush(targetChannel);
		}
	}

	public void finishBatch() {
		for (RecordSerializer<T> serializer : serializers) {
			serializer.finishBatch();
		}
	}

	public void broadcastEvent(AbstractEvent event) throws IOException {
		try (BufferConsumer eventBufferConsumer = EventSerializer.toBufferConsumer(event)) {
			for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
				RecordSerializer<T> serializer = serializers[targetChannel];

				tryFinishCurrentBufferBuilder(targetChannel, serializer);

				// retain the buffer so that it can be recycled by each channel of targetPartition
				targetPartition.addBufferConsumer(eventBufferConsumer.copy(), targetChannel);
			}

			if (flushAlways) {
				flushAll();
			}
		}
	}

	public void flushAll() {
		targetPartition.flushAll();
	}

	public void clearBuffers() {
		for (int targetChannel = 0; targetChannel < numChannels; targetChannel++) {
			RecordSerializer<?> serializer = serializers[targetChannel];
			closeBufferBuilder(targetChannel);
			serializer.clear();
		}
	}

	/**
	 * Sets the metric group for this RecordWriter.
     */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		numBytesOut = metrics.getNumBytesOutCounter();
	}

	/**
	 * Marks the current {@link BufferBuilder} as finished and clears the state for next one.
	 *
	 * @return true if some data were written
	 */
	private boolean tryFinishCurrentBufferBuilder(int targetChannel, RecordSerializer<T> serializer) {

		if (!bufferBuilders[targetChannel].isPresent()) {
			return false;
		}
		BufferBuilder bufferBuilder = bufferBuilders[targetChannel].get();
		bufferBuilders[targetChannel] = Optional.empty();

		numBytesOut.inc(bufferBuilder.finish());
		serializer.clear();
		return true;
	}

	private BufferBuilder requestNewBufferBuilder(int targetChannel) throws IOException, InterruptedException {
		checkState(!bufferBuilders[targetChannel].isPresent());
		BufferBuilder bufferBuilder = targetPartition.getBufferProvider().requestBufferBuilderBlocking();
		bufferBuilders[targetChannel] = Optional.of(bufferBuilder);
		targetPartition.addBufferConsumer(bufferBuilder.createBufferConsumer(), targetChannel);
		return bufferBuilder;
	}

	private void closeBufferBuilder(int targetChannel) {
		if (bufferBuilders[targetChannel].isPresent()) {
			bufferBuilders[targetChannel].get().finish();
			bufferBuilders[targetChannel] = Optional.empty();
		}
	}

	private static class BatchIndexToChannelComparator implements Comparator<BatchIndexToChannel> {
		public static final BatchIndexToChannelComparator INSTANCE = new BatchIndexToChannelComparator();

		@Override
		public int compare(BatchIndexToChannel o1, BatchIndexToChannel o2) {
			return Integer.compare(o1.channel, o2.channel);
		}
	}
}
