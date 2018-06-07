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
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
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

	public PojoSerializerGenerator(Class<T> clazz, Field[] fields, boolean[] fieldsPrimitive, TypeSerializer<?>[] fieldSerializers) {
		this.fields = checkNotNull(fields);
		this.fieldsPrimitive = checkNotNull(fieldsPrimitive);
		this.fieldSerializers = checkNotNull(fieldSerializers);
		this.clazz = checkNotNull(clazz);
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
		bodyMembers.add(createDeserialize(headerMembers));
		bodyMembers.add(createDeserializeWithReuse());

		final String typeSerializerTerm = createTypeTerm(TypeSerializer.class);
		return
			"public final class " + name + " extends " + typeSerializerTerm + "{\n" +
			"\n" +
			"  private final Class clazz;\n" +
			"  private final " + typeSerializerTerm + " fieldSerializers;\n" +
			"  private final java.lang.reflect.Field[] fields;\n" +
			"  private final boolean[] fieldsPrimitive;\n" +
			"  " + indent(headerMembers, 2) + "\n" +
			"\n" +
			"  public " + name + "(Class clazz, " + typeSerializerTerm + "[] fieldSerializers, java.lang.reflect.Field[] fields, boolean[] fieldsPrimitive) {\n" +
			"    this.clazz = clazz;\n" +
			"    this.fieldSerializers = fieldSerializers;\n" +
			"    this.fields = fields;\n" +
			"    this.fieldsPrimitive = fieldsPrimitive;\n" +
			"    " + indent(headerMembersInit, 4) +
			"  }\n" +
			"\n" +
			"  " + indent(bodyMembers, 2) + "\n" +
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
				addSerializerCode(headerMembers, sb, i);
			}
		}

		final String classTerm = createTypeTerm(clazz);
		return
			"public void serialize(" + classTerm + " pojo, DataOutputView target) throws IOException {\n" +
			// handle null values (but only for top-level serializer)
			"  if (pojo == null) {\n" +
			"    target.writeByte(" + IS_NULL + ");\n" +
			"    return;\n" +
			"  }\n" +
			// check for subclass
			"  final Class actualClass = pojo.getClass();\n" +
			"  if (clazz == actualClass) {\n" +
			"    target.writeByte(" + GENERATED + ");\n" +
			"    " + indent(Collections.singleton(sb.toString()), 4) +
			"  } else {\n" +
			"    throw new UnsupportedOperationException();\n" +
			"  }" +
			"}"; // TODO
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
			"public " + classTerm + " deserialize(DataInputView source) throws IOException {\n" +
				"  int flags = source.readByte();\n" +
				"  if ((flags & " + IS_NULL + ") != 0) {\n" +
				"    return null;\n" +
				"  }\n" +
				"\n" +
				"  " + classTerm + " target;\n" +
				"\n" +
				"  if ((flags & " + IS_SUBCLASS + ") != 0) {\n" +
				"    throw new UnsupportedOperationException();\n" +
				"  } else if ((flags & " + IS_TAGGED_SUBCLASS + ") != 0) {\n" +
				"    throw new UnsupportedOperationException();\n" +
				"  } else {\n" +
				"    target = createInstance();\n" +
				"    try {\n" +
				"    " + indent(Collections.singleton(sb.toString()), 6) +
				"    } catch (IllegalAccessException e) {\n" +
				"      throw new IOException(\"Error during POJO copy, this should not happen since we check the fields before.\", e);\n" +
				"    }\n" +
				"  }\n" +
				"\n" +
				"  return target;\n" +
				"  }"; // TODO
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
		}
//		else if (fieldSerializer.getClass() == BooleanPrimitiveArraySerializer.class) {
//			addSerializerBasicArrayCode(sb, fieldAccess, true, "writeBoolean");
//		}
		else {
			addSerializerCallingCode(sb, fieldAccess, fieldIdx);
		}
	}

	private static void addSerializerCallingCode(StringBuilder sb, Tuple2<String, String> fieldAccess, int fieldIdx) {
		final String fieldValueName = newName("fieldValue");
		addSerializerNullableCode(sb, fieldAccess, fieldValueName,
			"fieldSerializers[" + fieldIdx + "].serialize(" + fieldValueName + ", target)");
	}

	private static void addSerializerBasicCode(StringBuilder sb, Tuple2<String, String> fieldAccess,
			boolean isPrimitive, String method) {
		if (isPrimitive) {
			final String serializationCode = "target." + method + "(" + fieldAccess.f1 + ");";
			sb.append(serializationCode);
		} else {
			final String fieldValueName = newName("fieldValue");
			addSerializerNullableCode(sb, fieldAccess, fieldValueName, "target." + method + "(" + fieldValueName + ");");
		}
	}

//	private static void addSerializerBasicArrayCode(StringBuilder sb, Tuple2<String, String> fieldAccess,
//			boolean isPrimitive, String method) {
//		final String fieldValueName = newName("fieldValue");
//		final String serializationCode =
//			"";
//		addSerializerNullableCode(sb, fieldAccess, fieldValueName, "target." + method + "(" + fieldValueName + ");");
//	}

	private static void addSerializerStringCode(StringBuilder sb, Tuple2<String, String> fieldAccess) {
		final String fieldValueName = newName("fieldValue");
		final String serializationCode = createTypeTerm(StringValue.class) + ".writeString(" + fieldValueName + ");";
		addSerializerNullableCode(sb, fieldAccess, fieldValueName, serializationCode);
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
			"}";
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

	private void addDeserializerCode(LinkedHashSet<String> headerMembers, StringBuilder sb, int fieldIdx) {
		final Field field = fields[fieldIdx];
		final TypeSerializer<?> fieldSerializer = fieldSerializers[fieldIdx];
		final boolean isPrimitive = fieldsPrimitive[fieldIdx];
		final String fieldValueName = newName("fieldValue");
		final Tuple2<String, String> fieldAccess = createFieldWriteAccess(headerMembers, field, fieldValueName);
		if (fieldSerializer.getClass() == BooleanSerializer.class) {
			addDeserializerBasicCode(sb, fieldIdx, fieldAccess, fieldValueName, isPrimitive, "readBoolean");
		} else if (fieldSerializer.getClass() == ByteSerializer.class) {
			addDeserializerBasicCode(sb, fieldIdx, fieldAccess, fieldValueName, isPrimitive, "readByte");
		} else if (fieldSerializer.getClass() == ShortSerializer.class) {
			addDeserializerBasicCode(sb, fieldIdx, fieldAccess, fieldValueName, isPrimitive, "readBoolean");
		} else if (fieldSerializer.getClass() == IntSerializer.class) {
			addDeserializerBasicCode(sb, fieldIdx, fieldAccess, fieldValueName, isPrimitive, "readInt");
		} else if (fieldSerializer.getClass() == LongSerializer.class) {
			addDeserializerBasicCode(sb, fieldIdx, fieldAccess, fieldValueName, isPrimitive, "readLong");
		} else if (fieldSerializer.getClass() == FloatSerializer.class) {
			addDeserializerBasicCode(sb, fieldIdx, fieldAccess, fieldValueName, isPrimitive, "readFloat");
		} else if (fieldSerializer.getClass() == DoubleSerializer.class) {
			addDeserializerBasicCode(sb, fieldIdx, fieldAccess, fieldValueName, isPrimitive, "readDouble");
		} else if (fieldSerializer.getClass() == StringSerializer.class) {
			addDeserializerStringCode(sb, fieldIdx, fieldAccess, fieldValueName, isPrimitive);
		} else {
			addDeserializerCallingCode(sb, fieldIdx, fieldValueName);
		}
	}

	private void addDeserializerCallingCode(
			StringBuilder sb,
			int fieldIdx,
			String fieldValueName) {
		// non-generated code does not assume non-null for primitive types
		Tuple2<String, String> fieldAccess = Tuple2.of("Object", "fields[" + fieldIdx + "].set(target, " + fieldValueName + ");");
		addDeserializerNullableCode(sb, fieldIdx, fieldAccess, fieldValueName, false, "fieldSerializers[" + fieldIdx + "].deserialize(source)");
	}

	private void addDeserializerBasicCode(
			StringBuilder sb,
			int fieldIdx,
			Tuple2<String, String> fieldAccess,
			String fieldValueName,
			boolean isPrimitive,
			String method) {
		addDeserializerNullableCode(sb, fieldIdx, fieldAccess, fieldValueName, isPrimitive, "target." + method);
	}

	private void addDeserializerStringCode(
			StringBuilder sb,
			int fieldIdx,
			Tuple2<String, String> fieldAccess,
			String fieldValueName,
			boolean isPrimitive) {
		final String deserializationCode = createTypeTerm(StringValue.class) + ".readString(source)";
		addDeserializerNullableCode(sb, fieldIdx, fieldAccess, fieldValueName, isPrimitive, deserializationCode);
	}

	private void addDeserializerNullableCode(
			StringBuilder sb,
			int fieldIdx,
			Tuple2<String, String> fieldAccess,
			String fieldValueName,
			boolean isPrimitive,
			String readFromSourceCode) {
		final String isNullCode =
				"final boolean isNull;\n" +
				"if ((flags & " + GENERATED + ") != 0 && "  + isPrimitive + ") {\n" +
				"  isNull = false;\n" +
				"} else {\n" +
				"  isNull = source.readBoolean();\n" +
				"}\n";
		sb.append(isNullCode);
		sb.append("\n");

		if (fields[fieldIdx] != null) {
			final String deserializeCode1 =
				"final " + fieldAccess.f0 + " " + fieldValueName + ";\n" +
				"if (isNull) {\n" +
				"  " + fieldValueName + " = null;\n" +
				"} else {\n" +
				"  " + fieldValueName + " = ";

			sb.append(deserializeCode1).append(readFromSourceCode).append(";\n")
				.append("}\n")
				.append(fieldAccess.f1).append(";");
		} else {
			// read and drop a pre-existing field value
			sb.append(readFromSourceCode).append(";\n");
		}
	}

	/**
	 * Returns the field's type term and the expression to gain write access to the field.
	 */
	private static Tuple2<String, String> createFieldWriteAccess(LinkedHashSet<String> headerMembers, Field field, String valueToSet) {
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
					"      .unreflectSetter(f)\n" +
					"      .asType(MethodType.methodType(\n" +
					"        void.class,\n" +
					"        " + pojoTypeTerm + ".class," +
					"        " + fieldTypeTerm + "));\n" +
					"  } catch (Throwable t) {\n" +
					"    throw new RuntimeException(\"Could not access field '" + field.getName() + "'\" +\n" +
					"      \"using a method handle.\", t);\n" +
					"  }\n" +
					"}";
			headerMembers.add(methodHandleCode);
			fieldAccessExpr = "(" + fieldTypeTerm + ") " + methodHandleName + ".invokeExact(pojo, " + valueToSet + ");";
		}
		return Tuple2.of(fieldTypeTerm, fieldAccessExpr);
	}

	private static String createTypeTerm(Class<?> t) {
		// TODO this does not work for arrays
		return t.getName();
	}

	private static String newName(String name) {
		return name + "$" + uniqueId.getAndIncrement();
	}
}
