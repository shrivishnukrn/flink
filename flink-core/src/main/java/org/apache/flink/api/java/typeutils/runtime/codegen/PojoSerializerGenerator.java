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

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.array.BooleanPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.DoublePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.FloatPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.IntPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.ShortPrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.StringValue;
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

import static org.apache.flink.util.Preconditions.checkNotNull;

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

	private boolean[] fieldsPrimitive;

	private TypeSerializer<?>[] fieldSerializers;

	private PojoSerializerGenerator(Class<T> clazz, Field[] fields, boolean[] fieldsPrimitive, TypeSerializer<?>[] fieldSerializers) {
		this.fields = checkNotNull(fields);
		this.fieldsPrimitive = checkNotNull(fieldsPrimitive);
		this.fieldSerializers = checkNotNull(fieldSerializers);
		this.clazz = checkNotNull(clazz);
	}

	@SuppressWarnings("unchecked")
	public static <T> TypeSerializer<T> generate(Class<T> clazz, Field[] fields, boolean[] fieldsPrimitive,
			TypeSerializer<?>[] fieldSerializers) throws Exception {

		PojoSerializerGenerator<T> generator = new PojoSerializerGenerator<>(clazz, fields, fieldsPrimitive, fieldSerializers);

		// generate name
		final String name = DEFAULT_CLASS_PREFIX + "$" + clazz.getSimpleName() + "$" + uniqueId.getAndIncrement();

		// generate code
		final String code = generator.createClassCode(name);

		// compile
		final SimpleCompiler compiler = new SimpleCompiler();
		compiler.setParentClassLoader(Thread.currentThread().getContextClassLoader());
		try {
			compiler.cook(code);
		} catch (Throwable t) {
			throw new FlinkRuntimeException("Generated PojoSerializer cannot be compiled. " +
				"This is a bug. Please file an issue.", t);
		}
		Class<TypeSerializer<T>> serializer = (Class<TypeSerializer<T>>) compiler.getClassLoader().loadClass(name);

		return serializer
				.getConstructor(Class.class, TypeSerializer[].class, Field[].class, boolean[].class)
				.newInstance(clazz, fieldSerializers, fields, fieldsPrimitive);
	}

	private String createClassCode(String name) {
		final LinkedHashSet<String> headerMembers = new LinkedHashSet<>();
		final LinkedHashSet<String> headerMembersInit = new LinkedHashSet<>();
		final LinkedHashSet<String> bodyMembers = new LinkedHashSet<>();

		bodyMembers.add(createCreateInstance());
		bodyMembers.add(createCopy());
		bodyMembers.add(createCopyWithReuse());
		bodyMembers.add(createDirectCopy());
		bodyMembers.add(createSerialize(headerMembers));
		bodyMembers.add(createDeserialize(headerMembers));
		bodyMembers.add(createDeserializeWithReuse());
		bodyMembers.add(createDuplicate());

		final String typeSerializerTerm = createTypeTerm(TypeSerializer.class);
		return
			"public final class " + name + " extends " + typeSerializerTerm + " {\n" +
			"\n" +
			"  private final Class clazz;\n" +
			"  private final " + createTypeTerm(TypeSerializer[].class) + " fieldSerializers;\n" +
			"  private final java.lang.reflect.Field[] fields;\n" +
			"  private final boolean[] fieldsPrimitive;\n" +
			"  " + indent(headerMembers, 2) + "\n" +
			"\n" +
			"  public " + name + "(Class clazz, " + createTypeTerm(TypeSerializer[].class) + " fieldSerializers, \n" +
			"      java.lang.reflect.Field[] fields, boolean[] fieldsPrimitive) {\n" +
			"    this.clazz = clazz;\n" +
			"    this.fieldSerializers = fieldSerializers;\n" +
			"    this.fields = fields;\n" +
			"    this.fieldsPrimitive = fieldsPrimitive;\n" +
			"    " + indent(headerMembersInit, 4) + "\n" +
			"  }\n" +
			"\n" +
			"  " + indent(bodyMembers, 2) + "\n" +
			"\n" +
			"  public int getLength() {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n" +
			"  public boolean isImmutableType() {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n" +
			"  public boolean canEqual(Object o) {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n" +
			"  public boolean equals(Object o) {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n" +
			"  public " + createTypeTerm(TypeSerializerConfigSnapshot.class) + " snapshotConfiguration() {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n" +
			"  public " + createTypeTerm(CompatibilityResult.class) + " ensureCompatibility(" + createTypeTerm(TypeSerializerConfigSnapshot.class) + " configSnapshot) {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n" +
			"\n" +
			"  public int hashCode() {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n" +
			"}";
	}

	private String createCreateInstance() {
		return
			"  public " + createTypeTerm(TypeSerializer.class) + " createInstance() {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n";
	}

	private String createCopy() {
		return
			"  public Object copy(Object from) {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n";
	}

	private String createDirectCopy() {
		return
			"  public void copy(" + createTypeTerm(DataInputView.class) + " source, " + createTypeTerm(DataOutputView.class) + " target) throws java.io.IOException {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n";
	}

	private String createCopyWithReuse() {
		return
			"  public Object copy(Object from, Object reuse) {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n";
	}

	private String createDuplicate() {
		return
			"  public " + createTypeTerm(TypeSerializer.class) + " duplicate() {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n";
	}

	private String createSerialize(LinkedHashSet<String> headerMembers) {
		// create field code for non-subclasses
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < fields.length; i++) {
			if (fields[i] != null) {
				addSerializerCode(headerMembers, sb, i);
			}
		}

		final String classTerm = createTypeTerm(clazz);
		return
			"public void serialize(Object value, " + createTypeTerm(DataOutputView.class) + " target) throws java.io.IOException {\n" +
			// handle null values (but only for top-level serializer)
			"  if (value == null) {\n" +
			"    target.writeByte(" + IS_NULL + ");\n" +
			"    return;\n" +
			"  }\n" +
			// check for subclass
			"  final Class actualClass = value.getClass();\n" +
			"  if (clazz == actualClass) {\n" +
			"    final " + classTerm + " pojo = (" + classTerm + ") value;\n" +
			"    target.writeByte(" + GENERATED + ");\n" +
			"    try {\n" +
			"      " + indent(Collections.singleton(sb.toString()), 6) + "\n" +
			"    } catch (Throwable t) {\n" +
			"      throw new RuntimeException(t);\n" +
			"    }\n" +
			"  } else {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }\n" +
			"}\n"; // TODO
	}

	private String createDeserialize(LinkedHashSet<String> headerMembers) {
		// create field code for non-subclasses
		final StringBuilder sb = new StringBuilder();
		for (int i = 0; i < fields.length; i++) {
			if (fields[i] != null) {
				addDeserializerCode(headerMembers, sb, i);
			}
		}

		final String classTerm = createTypeTerm(clazz);
		return
			"public " + classTerm + " deserialize(" + createTypeTerm(DataInputView.class) + " source) throws java.io.IOException {\n" +
			"  final int flags = source.readByte();\n" +
			"  if ((flags & " + IS_NULL + ") == " + IS_NULL + ") {\n" +
			"    return null;\n" +
			"  }\n" +
			"\n" +
			"  final " + classTerm + " pojo;\n" +
			"  if ((flags & " + GENERATED + ") == " + GENERATED + ") {\n" +
			"    try {\n" +
			"      pojo = new " + classTerm + "();\n" +
			"      " + indent(Collections.singleton(sb.toString()), 6) + "\n" +
			"    } catch (Throwable t) {\n" +
			"      throw new RuntimeException(t);\n" +
			"    }\n" +
			"  }\n" +
			"  else if ((flags & " + IS_SUBCLASS + ") == " + IS_SUBCLASS + ") {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  } else if ((flags & " + IS_TAGGED_SUBCLASS + ") == " + IS_TAGGED_SUBCLASS + ") {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  } else {\n" +
			"    throw new UnsupportedOperationException();\n" + // legacy path for primitives
			"  }\n" +
			"\n" +
			"  return pojo;\n" +
			"}\n";
	}

	private String createDeserializeWithReuse() {
		final String classTerm = createTypeTerm(clazz);
		return
			"public " + classTerm + " deserialize(Object reuse, " + createTypeTerm(DataInputView.class) + " source) throws java.io.IOException {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"}\n";
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

	private void addSerializerCode(LinkedHashSet<String> headerMembers, StringBuilder sb, int fieldIdx) {
		final Field field = fields[fieldIdx];
		final TypeSerializer<?> fieldSerializer = fieldSerializers[fieldIdx];
		final boolean isPrimitive = field.getType().isPrimitive();
		final Tuple2<String, String> fieldAccess = createFieldReadAccess(headerMembers, field);
		if (fieldSerializer.getClass() == BooleanSerializer.class) {
			addSerializerBasicCode(sb, fieldAccess, isPrimitive, "writeBoolean");
		} else if (fieldSerializer.getClass() == ByteSerializer.class) {
			addSerializerBasicCode(sb, fieldAccess, isPrimitive, "writeByte");
		} else if (fieldSerializer.getClass() == ShortSerializer.class) {
			addSerializerBasicCode(sb, fieldAccess, isPrimitive, "writeShort");
		} else if (fieldSerializer.getClass() == IntSerializer.class) {
			addSerializerBasicCode(sb, fieldAccess, isPrimitive, "writeInt");
		} else if (fieldSerializer.getClass() == LongSerializer.class) {
			addSerializerBasicCode(sb, fieldAccess, isPrimitive, "writeLong");
		} else if (fieldSerializer.getClass() == FloatSerializer.class) {
			addSerializerBasicCode(sb, fieldAccess, isPrimitive, "writeFloat");
		} else if (fieldSerializer.getClass() == DoubleSerializer.class) {
			addSerializerBasicCode(sb, fieldAccess, isPrimitive, "writeDouble");
		} else if (fieldSerializer.getClass() == StringSerializer.class) {
			addSerializerStringCode(sb, fieldAccess);
		} else if (fieldSerializer.getClass() == BooleanPrimitiveArraySerializer.class) {
			addSerializerBasicArrayCode(sb, fieldAccess, field.getType().getComponentType(), "writeBoolean");
		} else if (fieldSerializer.getClass() == BytePrimitiveArraySerializer.class) {
			addSerializerBasicArrayCode(sb, fieldAccess, field.getType().getComponentType(), "writeByte");
		} else if (fieldSerializer.getClass() == ShortPrimitiveArraySerializer.class) {
			addSerializerBasicArrayCode(sb, fieldAccess, field.getType().getComponentType(), "writeShort");
		} else if (fieldSerializer.getClass() == IntPrimitiveArraySerializer.class) {
			addSerializerBasicArrayCode(sb, fieldAccess, field.getType().getComponentType(), "writeInt");
		} else if (fieldSerializer.getClass() == LongPrimitiveArraySerializer.class) {
			addSerializerBasicArrayCode(sb, fieldAccess, field.getType().getComponentType(), "writeLong");
		} else if (fieldSerializer.getClass() == FloatPrimitiveArraySerializer.class) {
			addSerializerBasicArrayCode(sb, fieldAccess, field.getType().getComponentType(), "writeFloat");
		} else if (fieldSerializer.getClass() == DoublePrimitiveArraySerializer.class) {
			addSerializerBasicArrayCode(sb, fieldAccess, field.getType().getComponentType(), "writeDouble");
		} else {
			addSerializerCallingCode(sb, fieldAccess, fieldIdx);
		}
	}

	private static void addSerializerCallingCode(StringBuilder sb, Tuple2<String, String> fieldAccess, int fieldIdx) {
		final String fieldValueName = newName("fieldValue");
		addSerializerNullableCode(sb, fieldAccess, fieldValueName,
			"fieldSerializers[" + fieldIdx + "].serialize(" + fieldValueName + ", target);");
	}

	private static void addSerializerBasicCode(StringBuilder sb, Tuple2<String, String> fieldAccess,
			boolean isPrimitive, String method) {
		if (isPrimitive) {
			final String serializationCode = "target." + method + "(" + fieldAccess.f1 + ");\n";
			sb.append(serializationCode);
		} else {
			final String fieldValueName = newName("fieldValue");
			final String serializationCode = "target." + method + "(" + fieldValueName + ");\n";
			addSerializerNullableCode(sb, fieldAccess, fieldValueName, serializationCode);
		}
	}

	private static void addSerializerBasicArrayCode(StringBuilder sb, Tuple2<String, String> fieldAccess,
			Class<?> component, String method) {
		final String componentTerm = createTypeTerm(component);
		final String fieldValueName = newName("fieldValue");
		final String serializationCode =
			"target.writeInt("+ fieldValueName + ".length);\n" +
			"for (final " + componentTerm + " c : " + fieldValueName + ") {\n" +
			"  target." + method + "(c);\n" +
			"}\n";
		addSerializerNullableCode(sb, fieldAccess, fieldValueName, serializationCode);
	}

	private static void addSerializerStringCode(StringBuilder sb, Tuple2<String, String> fieldAccess) {
		// the string serializer considers null values
		final String serializationCode = createTypeTerm(StringValue.class) + ".writeString(" + fieldAccess.f1 + ", target);";
		sb.append(serializationCode);
	}

	private static void addSerializerNullableCode(StringBuilder sb, Tuple2<String, String> fieldAccess,
			String fieldValueName, String serializationCode) {
		final String code =
			"final " + fieldAccess.f0 + " " + fieldValueName + " = " + fieldAccess.f1 + ";\n" +
			"if (" + fieldValueName + " == null) {\n" +
			"  target.writeBoolean(true);\n" +
			"} else {\n" +
			"  target.writeBoolean(false);\n" +
			"  " + indent(Collections.singleton(serializationCode), 2) + "\n" +
			"  \n" +
			"}\n";
		sb.append(code);
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
//			final String pojoTypeTerm = createTypeTerm(field.getDeclaringClass());
//			final String methodHandleName = "getter$methodHandle$" + field.getName();
//			final String methodHandleCode =
//				"private static final java.lang.invoke.MethodHandle " + methodHandleName + " = init$" + methodHandleName + "();\n" +
//				"private static java.lang.invoke.MethodHandle init$" + methodHandleName + "() {\n" +
//				"  try {\n" +
//				"    final java.lang.invoke.MethodHandles.Lookup lookup = java.lang.invoke.MethodHandles.lookup();\n" +
//				"    final java.lang.reflect.Field f = " + pojoTypeTerm + ".class\n" +
//				"      .getDeclaredField(\"" + field.getName() + "\");\n" +
//				"    f.setAccessible(true);\n" +
//				"    return lookup\n" +
//				"      .unreflectGetter(f)\n" +
//				"      .asType(java.lang.invoke.MethodType.methodType(\n" +
//				"        " + fieldTypeTerm + ".class,\n" +
//				"        " + pojoTypeTerm + ".class));\n" +
//				"  } catch (Throwable t) {\n" +
//				"    throw new RuntimeException(\"Could not access field '" + field.getName() + "'\" +\n" +
//				"      \"using a method handle.\", t);\n" +
//				"  }\n" +
//				"}";
//			headerMembers.add(methodHandleCode);
//			fieldAccessExpr = "(" + fieldTypeTerm + ") " + methodHandleName + ".invokeExact(pojo)";
			final char first = Character.toUpperCase(field.getName().charAt(0));
			fieldAccessExpr = "pojo.get" + first + field.getName().substring(1) + "()";
		}
		return Tuple2.of(fieldTypeTerm, fieldAccessExpr);
	}

	private void addDeserializerCode(LinkedHashSet<String> headerMembers, StringBuilder sb, int fieldIdx) {
		final Field field = fields[fieldIdx];
		final TypeSerializer<?> fieldSerializer = fieldSerializers[fieldIdx];
		final boolean isNullable = fieldsPrimitive[fieldIdx];
		if (fieldSerializer.getClass() == BooleanSerializer.class) {
			addDeserializerBasicCode(headerMembers, sb, field, isNullable, "readBoolean");
		} else if (fieldSerializer.getClass() == ByteSerializer.class) {
			addDeserializerBasicCode(headerMembers, sb, field, isNullable, "readByte");
		} else if (fieldSerializer.getClass() == ShortSerializer.class) {
			addDeserializerBasicCode(headerMembers, sb, field, isNullable, "readShort");
		} else if (fieldSerializer.getClass() == IntSerializer.class) {
			addDeserializerBasicCode(headerMembers, sb, field, isNullable, "readInt");
		} else if (fieldSerializer.getClass() == LongSerializer.class) {
			addDeserializerBasicCode(headerMembers, sb, field, isNullable, "readLong");
		} else if (fieldSerializer.getClass() == FloatSerializer.class) {
			addDeserializerBasicCode(headerMembers, sb, field, isNullable, "readFloat");
		} else if (fieldSerializer.getClass() == DoubleSerializer.class) {
			addDeserializerBasicCode(headerMembers, sb, field, isNullable, "readDouble");
		} else if (fieldSerializer.getClass() == StringSerializer.class) {
			addDeserializerStringCode(headerMembers, sb, field);
		} else if (fieldSerializer.getClass() == BooleanPrimitiveArraySerializer.class) {
			addDeserializerBasicArrayCode(headerMembers, sb, field, "readBoolean");
		} else if (fieldSerializer.getClass() == BytePrimitiveArraySerializer.class) {
			addDeserializerBasicArrayCode(headerMembers, sb, field, "readByte");
		} else if (fieldSerializer.getClass() == ShortPrimitiveArraySerializer.class) {
			addDeserializerBasicArrayCode(headerMembers, sb, field, "readShort");
		} else if (fieldSerializer.getClass() == IntPrimitiveArraySerializer.class) {
			addDeserializerBasicArrayCode(headerMembers, sb, field, "readInt");
		} else if (fieldSerializer.getClass() == LongPrimitiveArraySerializer.class) {
			addDeserializerBasicArrayCode(headerMembers, sb, field, "readLong");
		} else if (fieldSerializer.getClass() == FloatPrimitiveArraySerializer.class) {
			addDeserializerBasicArrayCode(headerMembers, sb, field, "readFloat");
		} else if (fieldSerializer.getClass() == DoublePrimitiveArraySerializer.class) {
			addDeserializerBasicArrayCode(headerMembers, sb, field, "readDouble");
		} else {
			addDeserializerCallingCode(headerMembers, sb, field, fieldIdx);
		}
	}

	private void addDeserializerBasicCode(LinkedHashSet<String> headerMembers, StringBuilder sb,
			Field field, boolean isNullable, String method) {
		if (isNullable) {
			final Tuple2<String, String> fieldAccess = createFieldWriteAccess(headerMembers, field, "source." + method + "()");
			sb.append(fieldAccess.f1);
		} else {
			final String fieldValueName = newName("fieldValue");
			final Tuple2<String, String> fieldAccess = createFieldWriteAccess(headerMembers, field, fieldValueName);
			final String deserializationCode = "source." + method + "()";
			addDeserializerNullableCode(sb, fieldAccess, fieldValueName, deserializationCode);
		}
	}

	private void addDeserializerNullableCode(StringBuilder sb, Tuple2<String, String> fieldAccess, String fieldValueName, String deserializationCode) {
		final String code =
			"final " + fieldAccess.f0 + " " + fieldValueName + ";\n" +
			"if (source.readBoolean()) {\n" +
			"  " + fieldValueName + " = null;\n" +
			"} else {\n" +
			"  " + indent(Collections.singleton(deserializationCode), 2) + "\n" +
			"}\n" +
			fieldAccess.f1 + "\n";
		sb.append(code);
	}

	private void addDeserializerBasicArrayCode(LinkedHashSet<String> headerMembers, StringBuilder sb, Field field, String method) {
		final String fieldValueName = newName("fieldValue");
		final String lengthName = newName("length");
		final Tuple2<String, String> fieldAccess = createFieldWriteAccess(headerMembers, field, fieldValueName);
		final String deserializationCode =
			"final int " + lengthName + " = source.readInt();\n" +
			fieldValueName + " = new " + fieldAccess.f0.replace("[]", "") + "[" + lengthName + "];\n" +
			"for (int i = 0; i < " + lengthName + "; i++) {\n" +
			"  " + fieldValueName + "[i] = source." + method + "();\n" +
			"}\n";
		addDeserializerNullableCode(sb, fieldAccess, fieldValueName, deserializationCode);
	}

	private void addDeserializerStringCode(LinkedHashSet<String> headerMembers, StringBuilder sb, Field field) {
		// the string serializer considers null values
		final String deserializationCode = createTypeTerm(StringValue.class) + ".readString(source)";
		final String code = createFieldWriteAccess(headerMembers, field, deserializationCode).f1;
		sb.append(code);
	}

	private void addDeserializerCallingCode(LinkedHashSet<String> headerMembers, StringBuilder sb, Field field, int fieldIdx) {
		final String fieldValueName = newName("fieldValue");
		final Tuple2<String, String> fieldAccess = createFieldWriteAccess(headerMembers, field, fieldValueName);
		final String deserializationCode = fieldValueName + " = (" + fieldAccess.f0 + ") fieldSerializers[" + fieldIdx + "].deserialize(source);\n";
		addDeserializerNullableCode(sb, fieldAccess, fieldValueName, deserializationCode);
	}

	/**
	 * Returns the field's type term and the expression to gain write access to the field.
	 */
	private static Tuple2<String, String> createFieldWriteAccess(LinkedHashSet<String> headerMembers, Field field, String valueToSet) {
		final String fieldTypeTerm = createTypeTerm(field.getType());
		final String fieldAccessExpr;
		if (Modifier.isPublic(field.getModifiers())) {
			fieldAccessExpr = "pojo." + field.getName() + "=" + valueToSet + ";\n";
		} else {
//			final String pojoTypeTerm = createTypeTerm(field.getDeclaringClass());
//			final String methodHandleName = "setter$methodHandle$" + field.getName();
//			final String methodHandleCode =
//				"private static final java.lang.invoke.MethodHandle " + methodHandleName + " = init$" + methodHandleName + "();\n" +
//				"private static java.lang.invoke.MethodHandle init$" + methodHandleName + "() {\n" +
//				"  try {\n" +
//				"    final java.lang.invoke.MethodHandles.Lookup lookup = java.lang.invoke.MethodHandles.lookup();\n" +
//				"    final java.lang.reflect.Field f = " + pojoTypeTerm + ".class\n" +
//				"      .getDeclaredField(\"" + field.getName() + "\");\n" +
//				"    f.setAccessible(true);\n" +
//				"    return lookup\n" +
//				"      .unreflectSetter(f)\n" +
//				"      .asType(java.lang.invoke.MethodType.methodType(\n" +
//				"        void.class,\n" +
//				"        " + pojoTypeTerm + ".class,\n" +
//				"        " + fieldTypeTerm + ".class));\n" +
//				"  } catch (Throwable t) {\n" +
//				"    throw new RuntimeException(\"Could not access field '" + field.getName() + "'\" +\n" +
//				"      \"using a method handle.\", t);\n" +
//				"  }\n" +
//				"}";
//			headerMembers.add(methodHandleCode);
//			fieldAccessExpr = methodHandleName + ".invokeExact(pojo, " + valueToSet + ");\n";
			final char first = Character.toUpperCase(field.getName().charAt(0));
			fieldAccessExpr = "pojo.set" + first + field.getName().substring(1) + "(" + valueToSet + ");\n";
		}
		return Tuple2.of(fieldTypeTerm, fieldAccessExpr);
	}

	private static String createTypeTerm(Class<?> t) {
		// TODO this does not work for arrays
		return t.getCanonicalName();
	}

	private static Class<?> toBoxed(Class<?> t) {
		if (t == int.class) {
			return Integer.class;
		} else if (t == long.class) {
			return Long.class;
		} else if (t == byte.class) {
			return Byte.class;
		} else if (t == short.class) {
			return Short.class;
		} else if (t == boolean.class) {
			return Boolean.class;
		} else if (t == double.class) {
			return Double.class;
		} else if (t == float.class) {
			return Float.class;
		} else {
			return t;
		}
	}

	private static String newName(String name) {
		return name + "$" + uniqueId.getAndIncrement();
	}
}
