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

package org.apache.flink.table.examples.java;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.Arrays;

/**
 * Simple example that shows how the Batch SQL API is used in Java.
 *
 * <p>This example shows how to:
 *  - Convert DataSets to Tables
 *  - Register a Table under a name
 *  - Run a SQL query on the registered Table
 */
public class WordCountSQL {

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Throwable {

//		int x = (int) new Object();
//
//		int i = (int) getter$methodHandle$test.invokeExact(new WC("hello", 222L, new int[]{12, 23, 444}, null));

//		System.out.println(i);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(new WC("hello", 222L, new int[]{12, 23, 444}, null)).rebalance().map(new MapFunction<WC, WC>() {
			@Override
			public WC map(WC value) {
				return value;
			}
		}).print();

		env.execute();
	}

	// *************************************************************************
	//     USER DATA TYPES
	// *************************************************************************

	/**
	 * Simple POJO containing a word and its respective count.
	 */
	public static class WC {
		public String word;
		public long frequency;
		public int[] ello;
		public Object nullable;
		private int test;

		// public constructor to make it a Flink POJO
		public WC() {}

		public WC(String word, long frequency, int[] ello, Object nullable) {
			this.word = word;
			this.frequency = frequency;
			this.ello = ello;
			this.nullable = nullable;
		}

		@Override
		public String toString() {
			return "WC{" + "word='" + word + '\'' + ", frequency=" + frequency + ", ello=" + Arrays.toString(ello) + ", nullable=" + nullable + ", test=" + test + '}';
		}

		public int getTest() {
			return test;
		}

		public void setTest(int test) {
			this.test = test;
		}
	}

//	private static final java.lang.invoke.MethodHandle getter$methodHandle$test = init$getter$methodHandle$test();
//  private static java.lang.invoke.MethodHandle init$getter$methodHandle$test() {
//    try {
//      final java.lang.invoke.MethodHandles.Lookup lookup = java.lang.invoke.MethodHandles.lookup();
//      final java.lang.reflect.Field f = org.apache.flink.table.examples.java.WordCountSQL.WC.class
//        .getDeclaredField("test");
//      f.setAccessible(true);
//      return lookup
//        .unreflectGetter(f)
//        .asType(java.lang.invoke.MethodType.methodType(
//          int.class,
//          org.apache.flink.table.examples.java.WordCountSQL.WC.class));
//    } catch (Throwable t) {
//      throw new RuntimeException("Could not access field 'test'" +
//        "using a method handle.", t);
//    }
//  }
}
