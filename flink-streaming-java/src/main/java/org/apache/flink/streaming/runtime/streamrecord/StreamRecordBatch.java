/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamrecord;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.operators.RecordBatch;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * ???
 */
@Internal
public final class StreamRecordBatch<T> extends RecordBatch<StreamRecord<T>> {
	public StreamRecordBatch(int maxBatchSize) {
		super(new StreamRecord[maxBatchSize]);
		checkState(maxBatchSize > 0);
		for (int i = 0; i < batch.length; i++) {
			batch[i] = new StreamRecord<T>(null);
		}
	}

	public void addStreamRecord(T element) {
		batch[index++].replace(element);
	}

	public void clear() {
		for (int i = 0; i < index; i++) {
			batch[i].replace(null);
		}
		index = 0;
	}
}
