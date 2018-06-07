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
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.FlinkRuntimeException;

import org.codehaus.janino.SimpleCompiler;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PojoSerializerGenerator<T> {

	// Flags for the header
	private static byte IS_NULL = 1;
	@Deprecated
	private static byte NO_SUBCLASS = 2;
	private static byte IS_SUBCLASS = 4;
	private static byte IS_TAGGED_SUBCLASS = 8;
	private static byte GENERATED = 16;

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
	public Class<TypeSerializer<T>> generate(ClassLoader cl) throws Exception {
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
		return (Class<TypeSerializer<T>>) compiler.getClassLoader().loadClass(name);
	}

	private String createClassCode(String name) {
		final LinkedHashSet<String> headerMembers = new LinkedHashSet<>();
		final LinkedHashSet<String> headerMembersInit = new LinkedHashSet<>();
		final LinkedHashSet<String> bodyMembers = new LinkedHashSet<>();

		bodyMembers.add(createCreateInstance());
		bodyMembers.add(createCopyWithReuse());
		bodyMembers.add(createSerialize(headerMembers));
		bodyMembers.add(createDeserialize());
		bodyMembers.add(createDeserializeWithReuse());

		return
			"public final class " + name + " extends " + TypeSerializer.class.getName() + "{\n" +
			"\n" +
			"private final Class clazz;" +
			indent(headerMembers, 2) + "\n" +
			"\n" +
			"public " + name + "(Class clazz) {\n" +
			"  this.clazz = clazz;\n" +
			indent(headerMembersInit, 4) +
			"}\n" +
			"\n" +
			indent(bodyMembers, 2) + "\n" +
			"}";
	}

	private String createCreateInstance() {
		return ""; // TODO
	}

	private String createCopy() {
		return ""; // TODO
	}

	private String createCopyWithReuse() {
		return ""; // TODO
	}

	private String createSerialize(LinkedHashSet<String> headerMembers) {
		// create field code for non-subclasses
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < fields.length; i++) {
			if (fields[i] != null) {
				addSerializerCode(headerMembers, sb, fields[i], fieldSerializers[i]);
			}
		}

		return
			"public void serialize(Object value, DataOutputView target) throws IOException {\n" +
			// handle null values (but only for top-level serializer)
			"  if (value == null) {\n" +
			"    target.writeByte(" + IS_NULL + ");\n" +
			"    return;\n" +
			"  }\n" +
			// check for subclass
			"  final Class<?> actualClass = value.getClass();\n" +
			"  if (clazz == actualClass) {\n" +
			"    target.writeByte(" + GENERATED + ");\n" +
			"    final " + clazz.getName() + " pojo = (" + clazz.getName() + ") value;\n" +
			indent(Collections.singleton(sb.toString()), 4) +
			"  } else {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }" +
			"}"; // TODO
	}

	private String createDeserialize() {
		return ""; // TODO
	}

	private String createDeserializeWithReuse() {
		return ""; // TODO
	}

	private static String indent(Collection<String> parts, int spaces) {
		final char[] chars = new char[spaces];
		Arrays.fill(chars, ' ');
		final String space = new String(chars);
		return String.join(space + "\n",
				parts
					.stream()
					.map(s -> s.replace("\n", "\n" + space))
					.collect(Collectors.toList()));
	}

	private static void addSerializerCode(LinkedHashSet<String> headerMembers, StringBuilder sb, Field field, TypeSerializer<?> fieldSerializer) {
		final boolean isPrimitive = field.getType().isPrimitive();
		if (fieldSerializer.getClass() == IntSerializer.class) {
			addSerializerIntCode(sb, isPrimitive);
		} else if (fieldSerializer.getClass() == LongSerializer.class) {
			addSerializerLongCode(sb, isPrimitive);
		} else if (fieldSerializer.getClass() == ByteSerializer.class) {
			addSerializerByteCode(sb, isPrimitive);
		}
		final Tuple2<String, String> fieldAccess = createFieldReadAccess(headerMembers, field);
	}

	private static void addSerializerBooleanCode(StringBuilder sb, boolean isPrimitive) {

	}

	private static void addSerializerByteCode(StringBuilder sb, boolean isPrimitive) {

	}

	private static void addSerializerShortCode(StringBuilder sb, boolean isPrimitive) {

	}

	private static void addSerializerIntCode(StringBuilder sb, boolean isPrimitive) {

	}

	private static void addSerializerLongCode(StringBuilder sb, boolean isPrimitive) {

	}

	private static void addSerializerFloatCode(StringBuilder sb, boolean isPrimitive) {

	}

	private static void addSerializerDoubleCode(StringBuilder sb, boolean isPrimitive) {

	}

	/**
	 * Returns the field's type term and the expression to access the field.
	 */
	private static Tuple2<String, String> createFieldReadAccess(LinkedHashSet<String> headerMembers, Field field) {
		final String fieldTypeTerm = createTypeTerm(field.getType());
		final String fieldAccessExpr;
		if (Modifier.isPublic(field.getModifiers())) {
			fieldAccessExpr = "pojo." + field.getName();
		} else {
			final String pojoTypeTerm = createTypeTerm(field.getDeclaringClass());
			final String methodHandleName = "methodHandle$" + field.getName();
			final String methodHandleCode =
				"private static final MethodHandle " + methodHandleName + " = access$" + methodHandleName + "();\n" +
				"private static MethodHandle access$" + methodHandleName + "() {\n" +
				"  try {\n" +
				"    final MethodHandles.Lookup lookup = MethodHandles.lookup();\n" +
				"    final Field f = " + pojoTypeTerm + ".class" +
				"      .getDeclaredField(\"" + field.getName() + "\");\n" +
				"    f.setAccessible(true);\n" +
				"    return lookup\n" +
				"      .unreflectGetter(f)\n" +
				"      .asType(MethodType.methodType(\n" +
				"        " + fieldTypeTerm + ".class,\n" +
				"        " + pojoTypeTerm + ".class));\n" +
				"  } catch (Throwable t) {\n" +
				"    throw new RuntimeException(\"Could not access field '" + field.getName() + "'\" +\n" +
				"      \"using a method handle.\", t);\n" +
				"  }\n" +
				"}";
			headerMembers.add(methodHandleCode);
			fieldAccessExpr = "(" + fieldTypeTerm + ") " + methodHandleName + ".invokeExact(pojo);";
		}
		return Tuple2.of(fieldTypeTerm, fieldAccessExpr);
	}

	private static String createTypeTerm(Class<?> t) {
		// TODO this does not work for arrays
		return t.getName();
	}
}
