/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.misc;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

/**
 * Created by pnowojski on 06/06/2018.
 */
public class TemporaryBenchmarkITCase  extends TestLogger {

	private static final int NUM_TM = 2;
	private static final int SLOTS_PER_TM = 7;
	private static final int PARALLELISM = NUM_TM * SLOTS_PER_TM;
	private static final int RECORDS_PER_INVOCATION = 7_000_000;

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE = new MiniClusterResource(
		new MiniClusterResource.MiniClusterResourceConfiguration(
			new Configuration(),
			NUM_TM,
			SLOTS_PER_TM));

	@Test
	public void testBatching() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);
		DataStreamSource<Long> source = env.addSource(new LongSource(RECORDS_PER_INVOCATION));

		source
			.map(new MultiplyByTwo())
			.windowAll(GlobalWindows.create())
			.reduce(new SumReduce())
			.print();

		env.execute();
	}

	public static class LongSource extends RichParallelSourceFunction<Long> {

		private volatile boolean running = true;
		private long maxValue;

		public LongSource(long maxValue) {
			this.maxValue = maxValue;
		}

		@Override
		public void run(SourceContext<Long> ctx) throws Exception {
			long counter = 0;

			while (running) {
				ctx.collectBatch(counter);
				counter++;
				if (counter >= maxValue) {
					cancel();
				}
			}
			ctx.finishBatch();
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static class MultiplyByTwo implements MapFunction<Long, Long> {
		@Override
		public Long map(Long value) throws Exception {
			return value * 2;
		}
	}

	public static class SumReduce implements ReduceFunction<Long> {
		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return value1 + value2;
		}
	}
}
