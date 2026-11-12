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

package org.apache.flink.connector.cassandra.mapper;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.cassandra.CassandraMapperOptions;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.GettableById;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.mapper.annotations.Computed;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.datastax.oss.driver.api.mapper.annotations.TransientProperties;
import com.datastax.oss.driver.api.mapper.entity.naming.NameConverter;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;

import javax.annotation.Nullable;

import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Maps POJOs annotated with {@link Entity} to and from Cassandra at runtime.
 *
 * <p>The mapper of the Cassandra driver generates its code with an annotation processor at compile
 * time, so it cannot map a class that is only known at runtime. This class supports the subset of
 * the driver mapper annotations that the connector needs: {@link Entity}, {@link CqlName}, {@link
 * NamingStrategy}, {@link Transient}, {@link TransientProperties} and {@link Computed}. A property
 * whose type is itself annotated with {@link Entity} is mapped to a user defined type.
 *
 * @param <T> the type of the mapped POJO
 */
@Internal
public final class PojoMapper<T> {

    private final Class<T> pojoClass;
    @Nullable private final CqlIdentifier keyspace;
    private final CqlIdentifier table;
    private final List<Property> properties;
    private final List<Property> writableProperties;

    private PojoMapper(
            Class<T> pojoClass,
            @Nullable CqlIdentifier keyspace,
            CqlIdentifier table,
            List<Property> properties) {
        this.pojoClass = pojoClass;
        this.keyspace = keyspace;
        this.table = table;
        this.properties = properties;
        this.writableProperties =
                properties.stream().filter(p -> !p.computed).collect(Collectors.toList());
    }

    /**
     * Creates the mapper of a class annotated with {@link Entity}.
     *
     * @param pojoClass the class to map
     * @param <T> the type of the mapped POJO
     * @return the mapper
     */
    public static <T> PojoMapper<T> create(Class<T> pojoClass) {
        return create(pojoClass, new HashSet<>());
    }

    private static <T> PojoMapper<T> create(Class<T> pojoClass, Set<Class<?>> visiting) {
        Preconditions.checkNotNull(pojoClass, "The POJO class must not be null.");
        final Entity entity = pojoClass.getAnnotation(Entity.class);
        Preconditions.checkArgument(
                entity != null,
                "%s must be annotated with %s.",
                pojoClass.getName(),
                Entity.class.getName());
        Preconditions.checkArgument(
                visiting.add(pojoClass),
                "%s references itself, recursive entities are not supported.",
                pojoClass.getName());

        final NameConverter nameConverter = createNameConverter(pojoClass);
        final CqlName tableName = pojoClass.getAnnotation(CqlName.class);
        final CqlIdentifier table =
                CqlIdentifier.fromCql(
                        tableName != null
                                ? tableName.value()
                                : nameConverter.toCassandraName(
                                        Introspector.decapitalize(pojoClass.getSimpleName())));
        final CqlIdentifier keyspace =
                entity.defaultKeyspace().isEmpty()
                        ? null
                        : CqlIdentifier.fromCql(entity.defaultKeyspace());

        final TransientProperties transientProperties =
                pojoClass.getAnnotation(TransientProperties.class);
        final Set<String> transientNames =
                transientProperties == null
                        ? new HashSet<>()
                        : new HashSet<>(Arrays.asList(transientProperties.value()));

        final List<Property> properties = new ArrayList<>();
        for (PropertyDescriptor descriptor : getPropertyDescriptors(pojoClass)) {
            final Method getter = descriptor.getReadMethod();
            final Method setter = descriptor.getWriteMethod();
            if (getter == null || getter.getDeclaringClass() == Object.class) {
                continue;
            }
            final String name = descriptor.getName();
            final Field field = findField(pojoClass, name);
            if (transientNames.contains(name)
                    || isAnnotated(Transient.class, getter, field)
                    || (field != null
                            && java.lang.reflect.Modifier.isTransient(field.getModifiers()))) {
                continue;
            }
            final Computed computed = getAnnotation(Computed.class, getter, field);
            if (setter == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Property %s of %s has no setter. Add a setter or annotate the property with %s.",
                                name, pojoClass.getName(), Transient.class.getName()));
            }
            getter.setAccessible(true);
            setter.setAccessible(true);
            final CqlName cqlName = getAnnotation(CqlName.class, getter, field);
            final CqlIdentifier column =
                    CqlIdentifier.fromCql(
                            cqlName != null
                                    ? cqlName.value()
                                    : nameConverter.toCassandraName(name));
            final Type type = getter.getGenericReturnType();
            final Class<?> rawType = getter.getReturnType();
            final PojoMapper<?> udtMapper;
            if (rawType.isAnnotationPresent(Entity.class)) {
                udtMapper = create(rawType, visiting);
            } else {
                checkNoNestedEntity(pojoClass, name, type);
                udtMapper = null;
            }
            properties.add(
                    new Property(
                            name,
                            column,
                            getter,
                            setter,
                            GenericType.of(type).wrap(),
                            rawType.isPrimitive(),
                            computed != null,
                            udtMapper));
        }
        Preconditions.checkArgument(
                !properties.isEmpty(), "%s has no mapped property.", pojoClass.getName());
        visiting.remove(pojoClass);
        return new PojoMapper<>(pojoClass, keyspace, table, properties);
    }

    /**
     * Builds the statement inserting an instance of the mapped class. The statement contains one
     * positional bind marker per mapped property, to bind with {@link #bind(PreparedStatement,
     * Object, CassandraMapperOptions)}.
     *
     * @param defaultKeyspace the keyspace to use instead of the one of the {@link Entity}
     *     annotation, when null the keyspace of the annotation or of the session is used
     * @param options the mapper options
     * @return the insert statement
     */
    public SimpleStatement buildInsert(
            @Nullable String defaultKeyspace, CassandraMapperOptions options) {
        Preconditions.checkState(
                !writableProperties.isEmpty(), "%s has no property to save.", pojoClass.getName());
        final CqlIdentifier insertKeyspace =
                defaultKeyspace == null || defaultKeyspace.isEmpty()
                        ? keyspace
                        : CqlIdentifier.fromCql(defaultKeyspace);
        final InsertInto insertInto =
                insertKeyspace == null
                        ? QueryBuilder.insertInto(table)
                        : QueryBuilder.insertInto(insertKeyspace, table);
        RegularInsert regularInsert = null;
        for (Property property : writableProperties) {
            regularInsert =
                    (regularInsert == null ? insertInto : regularInsert)
                            .value(property.column, QueryBuilder.bindMarker());
        }
        Insert insert = regularInsert;
        if (options.isIfNotExists()) {
            insert = insert.ifNotExists();
        }
        if (options.getTimestamp() != null) {
            insert = insert.usingTimestamp(options.getTimestamp());
        }
        if (options.getTtl() != null) {
            insert = insert.usingTtl(options.getTtl());
        }
        return insert.build();
    }

    /**
     * Binds an instance of the mapped class to a statement prepared from {@link
     * #buildInsert(String, CassandraMapperOptions)}.
     *
     * @param preparedStatement the prepared insert statement
     * @param pojo the instance to save
     * @param options the mapper options
     * @return the bound statement
     */
    public BoundStatement bind(
            PreparedStatement preparedStatement, T pojo, CassandraMapperOptions options) {
        BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();
        for (int i = 0; i < writableProperties.size(); i++) {
            final Property property = writableProperties.get(i);
            final Object value = property.get(pojo);
            if (value == null) {
                if (options.isSaveNullFields()) {
                    builder = builder.setToNull(i);
                }
            } else if (property.udtMapper != null) {
                builder =
                        builder.setUdtValue(
                                i,
                                property.udtMapper.toUdtValue(
                                        value,
                                        preparedStatement
                                                .getVariableDefinitions()
                                                .get(i)
                                                .getType()));
            } else {
                builder = builder.set(i, value, property.type);
            }
        }
        if (options.getConsistencyLevel() != null) {
            builder = builder.setConsistencyLevel(options.getConsistencyLevel());
        }
        if (options.isTracing()) {
            builder = builder.setTracing(true);
        }
        return builder.build();
    }

    /**
     * Creates an instance of the mapped class from a row. The columns of the row that do not match
     * a property are ignored and the properties that do not match a column are left unset.
     *
     * @param row the row to map
     * @return the mapped instance
     */
    public T map(Row row) {
        return map(row, column -> row.getColumnDefinitions().contains(column));
    }

    private T map(GettableById source, Predicate<CqlIdentifier> contains) {
        final T pojo;
        try {
            pojo = pojoClass.getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Cannot instantiate %s, it must have a public no-argument constructor.",
                            pojoClass.getName()),
                    e);
        }
        for (Property property : properties) {
            if (!contains.test(property.column)) {
                continue;
            }
            if (source.isNull(property.column)) {
                if (!property.primitive) {
                    property.set(pojo, null);
                }
            } else if (property.udtMapper != null) {
                property.set(
                        pojo, property.udtMapper.fromUdtValue(source.getUdtValue(property.column)));
            } else {
                property.set(pojo, source.get(property.column, property.type));
            }
        }
        return pojo;
    }

    private T fromUdtValue(UdtValue udtValue) {
        return map(udtValue, field -> udtValue.getType().contains(field));
    }

    private UdtValue toUdtValue(Object pojo, DataType dataType) {
        Preconditions.checkArgument(
                dataType instanceof UserDefinedType,
                "%s is mapped as a user defined type but the type of the column is %s.",
                pojoClass.getName(),
                dataType);
        final UserDefinedType userDefinedType = (UserDefinedType) dataType;
        UdtValue udtValue = userDefinedType.newValue();
        for (Property property : writableProperties) {
            final int index = userDefinedType.firstIndexOf(property.column);
            Preconditions.checkArgument(
                    index >= 0,
                    "%s is not a field of the user defined type %s.",
                    property.column,
                    userDefinedType.getName());
            final Object value = property.get(pojo);
            if (value == null) {
                udtValue = udtValue.setToNull(index);
            } else if (property.udtMapper != null) {
                udtValue =
                        udtValue.setUdtValue(
                                index,
                                property.udtMapper.toUdtValue(
                                        value, userDefinedType.getFieldTypes().get(index)));
            } else {
                udtValue = udtValue.set(index, value, property.type);
            }
        }
        return udtValue;
    }

    private static NameConverter createNameConverter(Class<?> pojoClass) {
        final NamingStrategy namingStrategy = pojoClass.getAnnotation(NamingStrategy.class);
        if (namingStrategy == null) {
            return name -> toCassandraName(name, NamingConvention.SNAKE_CASE_INSENSITIVE);
        }
        final NamingConvention[] conventions = namingStrategy.convention();
        final Class<? extends NameConverter>[] converters = namingStrategy.customConverterClass();
        Preconditions.checkArgument(
                conventions.length + converters.length == 1,
                "%s on %s must have exactly one convention or one custom converter class.",
                NamingStrategy.class.getSimpleName(),
                pojoClass.getName());
        if (conventions.length == 1) {
            return name -> toCassandraName(name, conventions[0]);
        }
        try {
            return converters[0].getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot instantiate %s, it must have a public no-argument constructor.",
                            converters[0].getName()),
                    e);
        }
    }

    /** Converts a Java name to a CQL name the same way the driver mapper does. */
    static String toCassandraName(String javaName, NamingConvention convention) {
        switch (convention) {
            case CASE_INSENSITIVE:
                return javaName;
            case EXACT_CASE:
                return doubleQuote(javaName);
            case LOWER_CAMEL_CASE:
                return doubleQuote(
                        Character.toLowerCase(javaName.charAt(0)) + javaName.substring(1));
            case UPPER_CAMEL_CASE:
                return doubleQuote(
                        Character.toUpperCase(javaName.charAt(0)) + javaName.substring(1));
            case SNAKE_CASE_INSENSITIVE:
                return toSnakeCase(javaName).toLowerCase();
            case UPPER_SNAKE_CASE:
                return doubleQuote(toSnakeCase(javaName).toUpperCase());
            case UPPER_CASE:
                return doubleQuote(javaName.toUpperCase());
            default:
                throw new IllegalArgumentException("Unsupported convention: " + convention);
        }
    }

    private static String toSnakeCase(String javaName) {
        final StringBuilder snakeCase = new StringBuilder();
        for (int i = 0; i < javaName.length(); i++) {
            final char c = javaName.charAt(i);
            if (i > 0 && c >= 'A' && c <= 'Z') {
                snakeCase.append('_');
            }
            snakeCase.append(c);
        }
        return snakeCase.toString();
    }

    private static String doubleQuote(String name) {
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    private static PropertyDescriptor[] getPropertyDescriptors(Class<?> pojoClass) {
        try {
            return Introspector.getBeanInfo(pojoClass).getPropertyDescriptors();
        } catch (IntrospectionException e) {
            throw new IllegalArgumentException(
                    String.format("Cannot introspect %s.", pojoClass.getName()), e);
        }
    }

    @Nullable
    private static Field findField(Class<?> pojoClass, String name) {
        for (Class<?> clazz = pojoClass; clazz != null; clazz = clazz.getSuperclass()) {
            try {
                return clazz.getDeclaredField(name);
            } catch (NoSuchFieldException e) {
                // look in the parent class
            }
        }
        return null;
    }

    private static boolean isAnnotated(
            Class<? extends Annotation> annotation, Method getter, @Nullable Field field) {
        return getAnnotation(annotation, getter, field) != null;
    }

    @Nullable
    private static <A extends Annotation> A getAnnotation(
            Class<A> annotation, Method getter, @Nullable Field field) {
        final A getterAnnotation = getter.getAnnotation(annotation);
        if (getterAnnotation != null || field == null) {
            return getterAnnotation;
        }
        return field.getAnnotation(annotation);
    }

    private static void checkNoNestedEntity(Class<?> pojoClass, String property, Type type) {
        if (type instanceof ParameterizedType) {
            for (Type argument : ((ParameterizedType) type).getActualTypeArguments()) {
                if (argument instanceof Class
                        && ((Class<?>) argument).isAnnotationPresent(Entity.class)) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Property %s of %s is a collection of %s, collections of entities are not supported."
                                            + " Use %s as the element type instead.",
                                    property,
                                    pojoClass.getName(),
                                    ((Class<?>) argument).getName(),
                                    UdtValue.class.getName()));
                }
                checkNoNestedEntity(pojoClass, property, argument);
            }
        }
    }

    /** A mapped property of the POJO. */
    private static final class Property {
        private final String name;
        private final CqlIdentifier column;
        private final Method getter;
        private final Method setter;
        private final GenericType<Object> type;
        private final boolean primitive;
        private final boolean computed;
        @Nullable private final PojoMapper<Object> udtMapper;

        @SuppressWarnings("unchecked")
        private Property(
                String name,
                CqlIdentifier column,
                Method getter,
                Method setter,
                GenericType<?> type,
                boolean primitive,
                boolean computed,
                @Nullable PojoMapper<?> udtMapper) {
            this.name = name;
            this.column = column;
            this.getter = getter;
            this.setter = setter;
            this.type = (GenericType<Object>) type;
            this.primitive = primitive;
            this.computed = computed;
            this.udtMapper = (PojoMapper<Object>) udtMapper;
        }

        private Object get(Object pojo) {
            try {
                return getter.invoke(pojo);
            } catch (ReflectiveOperationException e) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Cannot read property %s of %s.", name, pojo.getClass().getName()),
                        e);
            }
        }

        private void set(Object pojo, @Nullable Object value) {
            try {
                setter.invoke(pojo, value);
            } catch (ReflectiveOperationException e) {
                throw new FlinkRuntimeException(
                        String.format(
                                "Cannot write property %s of %s.", name, pojo.getClass().getName()),
                        e);
            }
        }
    }
}
