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

package org.apache.flink.api.java.typeutils.runtime.codegen;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import org.codehaus.janino.SimpleCompiler;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicInteger;

public class PojoSerializerGenerator<T> {

	private static final String DEFAULT_CLASS_PREFIX = "GeneratedPojoSerializer";

	private static AtomicInteger uniqueId = new AtomicInteger();

	private Class<T> clazz;

	private Field[] fields;

	private TypeSerializer<?>[] fieldSerializers;

	public PojoSerializerGenerator(Class<T> clazz, Field[] fields, TypeSerializer<?>[] fieldSerializers) {
		this.fields = fields;
		this.fieldSerializers = fieldSerializers;
		this.clazz = clazz;
	}

	@SuppressWarnings("unchecked")
	public TypeSerializer<T> generate(ClassLoader cl) throws Exception {
		// generate name
		final String name = DEFAULT_CLASS_PREFIX + "$" + clazz.getSimpleName() + "$" + uniqueId.getAndIncrement();

		// generate code
		final String code = createClassCode(name);

		// compile
		final SimpleCompiler compiler = new SimpleCompiler();
		compiler.setParentClassLoader(cl);
		try {
			compiler.cook(code);
		} catch (Throwable t) {
			throw new FlinkRuntimeException("Generated PojoSerializer cannot be compiled. " +
				"This is a bug. Please file an issue.", t);
		}
		final Class<TypeSerializer<T>> generatedClazz = (Class<TypeSerializer<T>>) compiler.getClassLoader().loadClass(name);

		final Constructor<TypeSerializer<T>> constructor = generatedClazz.getConstructor();
		return constructor.newInstance();
	}

	public String createClassCode(String name) {
		final LinkedHashSet<String> headerMembers = new LinkedHashSet<>();
		final LinkedHashSet<String> headerMembersInit = new LinkedHashSet<>();
		final LinkedHashSet<String> bodyMembers = new LinkedHashSet<>();

		bodyMembers.add(createDuplicate());
		bodyMembers.add(createCreateInstance());
		bodyMembers.add(createCopyWithReuse());
		bodyMembers.add(createSerialize());
		bodyMembers.add(createDeserialize());
		bodyMembers.add(createDeserializeWithReuse());

		return
			"public final class " + name + " extends " + TypeSerializer.class.getName() + "{\n" +
			"\n" +
			String.join("\n\n", headerMembers) + "\n" +
			"\n" +
			"public " + name + "() {\n" +
			String.join("\n", headerMembersInit) +
			"}\n" +
			"\n" +
			String.join("\n\n", bodyMembers) + "\n" +
			"}";
	}

	public String createDuplicate() {
		return ""; // TODO
	}

	public String createCreateInstance() {
		return ""; // TODO
	}

	public String createCopy() {
		return ""; // TODO
	}

	public String createCopyWithReuse() {
		return ""; // TODO
	}

	public String createSerialize() {
		return ""; // TODO
	}

	public String createDeserialize() {
		return ""; // TODO
	}

	public String createDeserializeWithReuse() {
		return ""; // TODO
	}

}
