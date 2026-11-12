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

import org.apache.flink.streaming.connectors.cassandra.CassandraMapperOptions;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.mapper.annotations.Computed;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Transient;
import com.datastax.oss.driver.api.mapper.entity.naming.NameConverter;
import com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link PojoMapper}. */
class PojoMapperTest {

    private static final GenericType<List<String>> LIST_OF_STRINGS =
            GenericType.listOf(String.class);

    @Test
    void testNamingConventions() {
        assertThat(PojoMapper.toCassandraName("productId", NamingConvention.CASE_INSENSITIVE))
                .isEqualTo("productId");
        assertThat(PojoMapper.toCassandraName("productId", NamingConvention.EXACT_CASE))
                .isEqualTo("\"productId\"");
        assertThat(PojoMapper.toCassandraName("ProductId", NamingConvention.LOWER_CAMEL_CASE))
                .isEqualTo("\"productId\"");
        assertThat(PojoMapper.toCassandraName("productId", NamingConvention.UPPER_CAMEL_CASE))
                .isEqualTo("\"ProductId\"");
        assertThat(PojoMapper.toCassandraName("productId", NamingConvention.SNAKE_CASE_INSENSITIVE))
                .isEqualTo("product_id");
        assertThat(PojoMapper.toCassandraName("productId", NamingConvention.UPPER_SNAKE_CASE))
                .isEqualTo("\"PRODUCT_ID\"");
        assertThat(PojoMapper.toCassandraName("productId", NamingConvention.UPPER_CASE))
                .isEqualTo("\"PRODUCTID\"");
    }

    @Test
    void testInsertStatement() {
        final PojoMapper<Product> mapper = PojoMapper.create(Product.class);

        // the transient and the computed properties are not saved
        assertThat(mapper.buildInsert(null, new CassandraMapperOptions()).getQuery())
                .isEqualTo(
                        "INSERT INTO shop.products (description,product_id,tags,unit_price) VALUES (?,?,?,?)");
    }

    @Test
    void testInsertStatementWithOptions() {
        final PojoMapper<Product> mapper = PojoMapper.create(Product.class);

        final String query =
                mapper.buildInsert(
                                "other_keyspace",
                                new CassandraMapperOptions()
                                        .ifNotExists(true)
                                        .timestamp(5L)
                                        .ttl(10))
                        .getQuery();

        assertThat(query)
                .startsWith("INSERT INTO other_keyspace.products (")
                .contains(" IF NOT EXISTS")
                .contains("USING TIMESTAMP 5")
                .contains("TTL 10");
    }

    @Test
    void testInsertStatementWithoutKeyspace() {
        final PojoMapper<DefaultNames> mapper = PojoMapper.create(DefaultNames.class);

        // the table is named after the class and the keyspace of the session is used
        assertThat(mapper.buildInsert(null, new CassandraMapperOptions()).getQuery())
                .isEqualTo("INSERT INTO default_names (batch_id) VALUES (?)");
    }

    @Test
    void testNamingStrategies() {
        assertThat(
                        PojoMapper.create(UpperSnakeCase.class)
                                .buildInsert(null, new CassandraMapperOptions())
                                .getQuery())
                .isEqualTo("INSERT INTO \"UPPER_SNAKE_CASE\" (\"ITEM_COUNT\") VALUES (?)");
        assertThat(
                        PojoMapper.create(CustomConverter.class)
                                .buildInsert(null, new CassandraMapperOptions())
                                .getQuery())
                .isEqualTo("INSERT INTO t_customconverter (t_itemcount) VALUES (?)");
    }

    @Test
    void testBind() {
        final PojoMapper<Product> mapper = PojoMapper.create(Product.class);
        final BoundStatementBuilder builder = mockBoundStatementBuilder();
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(preparedStatement.boundStatementBuilder()).thenReturn(builder);

        final Product product = new Product();
        product.setProductId(7);
        product.setTags(Arrays.asList("a", "b"));
        product.setUnitPrice(12.5d);

        mapper.bind(
                preparedStatement,
                product,
                new CassandraMapperOptions()
                        .consistencyLevel(DefaultConsistencyLevel.QUORUM)
                        .tracing(true));

        // the null description is saved by default
        verify(builder).setToNull(0);
        verify(builder).set(1, 7, GenericType.INTEGER);
        verify(builder).set(2, Arrays.asList("a", "b"), LIST_OF_STRINGS);
        verify(builder).set(3, 12.5d, GenericType.DOUBLE);
        verify(builder).setConsistencyLevel(DefaultConsistencyLevel.QUORUM);
        verify(builder).setTracing(true);
    }

    @Test
    void testBindWithoutSavingNullFields() {
        final PojoMapper<Product> mapper = PojoMapper.create(Product.class);
        final BoundStatementBuilder builder = mockBoundStatementBuilder();
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(preparedStatement.boundStatementBuilder()).thenReturn(builder);

        mapper.bind(
                preparedStatement,
                new Product(),
                new CassandraMapperOptions().saveNullFields(false));

        // the null properties are left unset
        verify(builder, never()).setToNull(anyInt());
        verify(builder).set(1, 0, GenericType.INTEGER);
        verify(builder, never()).setConsistencyLevel(any());
        verify(builder, never()).setTracing(true);
    }

    @Test
    void testMap() {
        final PojoMapper<Product> mapper = PojoMapper.create(Product.class);
        final Row row = mock(Row.class);
        final ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
        when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);
        // the transient property is not read even if a column has its name
        when(row.isNull(CqlIdentifier.fromCql("description"))).thenReturn(true);
        when(row.get(CqlIdentifier.fromCql("product_id"), GenericType.INTEGER)).thenReturn(7);
        when(row.get(CqlIdentifier.fromCql("tags"), LIST_OF_STRINGS))
                .thenReturn(Arrays.asList("a", "b"));
        when(row.get(CqlIdentifier.fromCql("unit_price"), GenericType.DOUBLE)).thenReturn(12.5d);
        when(row.get(CqlIdentifier.fromCql("write_time"), GenericType.LONG)).thenReturn(99L);

        final Product product = mapper.map(row);

        assertThat(product.getProductId()).isEqualTo(7);
        assertThat(product.getDescription()).isNull();
        assertThat(product.getTags()).containsExactly("a", "b");
        assertThat(product.getUnitPrice()).isEqualTo(12.5d);
        assertThat(product.getWriteTime()).isEqualTo(99L);
        assertThat(product.getCache()).isNull();
    }

    @Test
    void testNestedEntityIsMappedToUserDefinedType() {
        final PojoMapper<Customer> mapper = PojoMapper.create(Customer.class);

        // write
        final UserDefinedType addressType = mock(UserDefinedType.class);
        final UdtValue newUdtValue = mock(UdtValue.class, Answers.RETURNS_SELF);
        when(addressType.newValue()).thenReturn(newUdtValue);
        when(addressType.firstIndexOf(CqlIdentifier.fromCql("city"))).thenReturn(0);
        final ColumnDefinition addressColumn = mock(ColumnDefinition.class);
        when(addressColumn.getType()).thenReturn(addressType);
        final ColumnDefinitions variableDefinitions = mock(ColumnDefinitions.class);
        when(variableDefinitions.get(0)).thenReturn(addressColumn);
        final BoundStatementBuilder builder = mockBoundStatementBuilder();
        final PreparedStatement preparedStatement = mock(PreparedStatement.class);
        when(preparedStatement.boundStatementBuilder()).thenReturn(builder);
        when(preparedStatement.getVariableDefinitions()).thenReturn(variableDefinitions);

        final Address address = new Address();
        address.setCity("Paris");
        final Customer customer = new Customer();
        customer.setAddress(address);
        customer.setName("name1");

        mapper.bind(preparedStatement, customer, new CassandraMapperOptions());

        verify(newUdtValue).set(0, "Paris", GenericType.STRING);
        verify(builder).setUdtValue(0, newUdtValue);
        verify(builder).set(1, "name1", GenericType.STRING);

        // read
        final UdtValue udtValue = mock(UdtValue.class);
        when(udtValue.getType()).thenReturn(addressType);
        when(addressType.contains(CqlIdentifier.fromCql("city"))).thenReturn(true);
        when(udtValue.get(CqlIdentifier.fromCql("city"), GenericType.STRING)).thenReturn("Rome");
        final Row row = mock(Row.class);
        final ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
        when(columnDefinitions.contains(any(CqlIdentifier.class))).thenReturn(true);
        when(row.getUdtValue(CqlIdentifier.fromCql("address"))).thenReturn(udtValue);
        when(row.get(CqlIdentifier.fromCql("name"), GenericType.STRING)).thenReturn("name2");

        final Customer readCustomer = mapper.map(row);

        assertThat(readCustomer.getName()).isEqualTo("name2");
        assertThat(readCustomer.getAddress().getCity()).isEqualTo("Rome");
    }

    @Test
    void testInvalidClassesAreRejected() {
        assertThatThrownBy(() -> PojoMapper.create(String.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be annotated with");

        assertThatThrownBy(() -> PojoMapper.create(CollectionOfEntities.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("collections of entities are not supported");

        assertThatThrownBy(() -> PojoMapper.create(NoSetter.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("has no setter");
    }

    private static BoundStatementBuilder mockBoundStatementBuilder() {
        final BoundStatementBuilder builder =
                mock(BoundStatementBuilder.class, Answers.RETURNS_SELF);
        when(builder.build()).thenReturn(mock(BoundStatement.class));
        return builder;
    }

    /** Entity with explicit and default names, a transient and a computed property. */
    @Entity(defaultKeyspace = "shop")
    @CqlName("products")
    public static class Product {
        @PartitionKey private int productId;
        private String description;
        private List<String> tags;
        private double unitPrice;

        @Computed("writetime(description)")
        @CqlName("write_time")
        private long writeTime;

        @Transient private String cache;

        public int getProductId() {
            return productId;
        }

        public void setProductId(int productId) {
            this.productId = productId;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }

        public double getUnitPrice() {
            return unitPrice;
        }

        public void setUnitPrice(double unitPrice) {
            this.unitPrice = unitPrice;
        }

        public long getWriteTime() {
            return writeTime;
        }

        public void setWriteTime(long writeTime) {
            this.writeTime = writeTime;
        }

        public String getCache() {
            return cache;
        }

        public void setCache(String cache) {
            this.cache = cache;
        }
    }

    /** Entity without any name annotation. */
    @Entity
    public static class DefaultNames {
        private int batchId;

        public int getBatchId() {
            return batchId;
        }

        public void setBatchId(int batchId) {
            this.batchId = batchId;
        }
    }

    /** Entity with a naming convention. */
    @Entity
    @NamingStrategy(convention = NamingConvention.UPPER_SNAKE_CASE)
    public static class UpperSnakeCase {
        private int itemCount;

        public int getItemCount() {
            return itemCount;
        }

        public void setItemCount(int itemCount) {
            this.itemCount = itemCount;
        }
    }

    /** Entity with a custom name converter. */
    @Entity
    @NamingStrategy(customConverterClass = PrefixConverter.class)
    public static class CustomConverter {
        private int itemCount;

        public int getItemCount() {
            return itemCount;
        }

        public void setItemCount(int itemCount) {
            this.itemCount = itemCount;
        }
    }

    /** Converter prefixing the lower-cased Java name. */
    public static class PrefixConverter implements NameConverter {
        @Override
        public String toCassandraName(String javaName) {
            return "t_" + javaName.toLowerCase();
        }
    }

    /** Entity with a property mapped to a user defined type. */
    @Entity
    public static class Customer {
        private Address address;
        private String name;

        public Address getAddress() {
            return address;
        }

        public void setAddress(Address address) {
            this.address = address;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    /** Entity mapped to a user defined type. */
    @Entity
    public static class Address {
        private String city;

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }
    }

    /** Entity with an unsupported collection of entities. */
    @Entity
    public static class CollectionOfEntities {
        private List<Address> addresses;

        public List<Address> getAddresses() {
            return addresses;
        }

        public void setAddresses(List<Address> addresses) {
            this.addresses = addresses;
        }
    }

    /** Entity with a read-only property. */
    @Entity
    public static class NoSetter {
        public int getId() {
            return 0;
        }
    }
}
