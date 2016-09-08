/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.ReverseNested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.count;
import static org.elasticsearch.search.aggregations.AggregationBuilders.filter;
import static org.elasticsearch.search.aggregations.AggregationBuilders.nested;
import static org.elasticsearch.search.aggregations.AggregationBuilders.reverseNested;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.IsNull.notNullValue;

@ESIntegTestCase.SuiteScopeTestCase
public class ReverseNestedIT extends ESIntegTestCase {

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(prepareCreate("idx")
                .addMapping(
                        "type1",
                        jsonBuilder().startObject().startObject("properties")
                                .startObject("field1").field("type", "keyword").endObject()
                                .startObject("nested1").field("type", "nested").startObject("properties")
                                    .startObject("field2").field("type", "keyword").endObject()
                                .endObject().endObject()
                                .endObject().endObject()
                )
                .addMapping(
                        "type2",
                        jsonBuilder().startObject().startObject("properties")
                                .startObject("nested1").field("type", "nested").startObject("properties")
                                    .startObject("field1").field("type", "keyword").endObject()
                                        .startObject("nested2").field("type", "nested").startObject("properties")
                                            .startObject("field2").field("type", "keyword").endObject()
                                        .endObject().endObject()
                                    .endObject().endObject()
                                .endObject().endObject()
                )
        );

        insertType1(Arrays.asList("a", "b", "c"), Arrays.asList("1", "2", "3", "4"));
        insertType1(Arrays.asList("b", "c", "d"), Arrays.asList("4", "5", "6", "7"));
        insertType1(Arrays.asList("c", "d", "e"), Arrays.asList("7", "8", "9", "1"));
        refresh();
        insertType1(Arrays.asList("a", "e"), Arrays.asList("7", "4", "1", "1"));
        insertType1(Arrays.asList("a", "c"), Arrays.asList("2", "1"));
        insertType1(Arrays.asList("a"), Arrays.asList("3", "4"));
        refresh();
        insertType1(Arrays.asList("x", "c"), Arrays.asList("1", "8"));
        insertType1(Arrays.asList("y", "c"), Arrays.asList("6"));
        insertType1(Arrays.asList("z"), Arrays.asList("5", "9"));
        refresh();

        insertType2(new String[][]{new String[]{"a", "0", "0", "1", "2"}, new String[]{"b", "0", "1", "1", "2"}, new String[]{"a", "0"}});
        insertType2(new String[][]{new String[]{"c", "1", "1", "2", "2"}, new String[]{"d", "3", "4"}});
        refresh();

        insertType2(new String[][]{new String[]{"a", "0", "0", "0", "0"}, new String[]{"b", "0", "0", "0", "0"}});
        insertType2(new String[][]{new String[]{"e", "1", "2"}, new String[]{"f", "3", "4"}});
        refresh();

        ensureSearchable();
    }

    private void insertType1(List<String> values1, List<String> values2) throws Exception {
        XContentBuilder source = jsonBuilder()
                .startObject()
                .array("field1", values1.toArray())
                .startArray("nested1");
        for (String value1 : values2) {
            source.startObject().field("field2", value1).endObject();
        }
        source.endArray().endObject();
        indexRandom(false, client().prepareIndex("idx", "type1").setRouting("1").setSource(source));
    }

    private void insertType2(String[][] values) throws Exception {
        XContentBuilder source = jsonBuilder()
                .startObject()
                .startArray("nested1");
        for (String[] value : values) {
            source.startObject().field("field1", value[0]).startArray("nested2");
            for (int i = 1; i < value.length; i++) {
                source.startObject().field("field2", value[i]).endObject();
            }
            source.endArray().endObject();
        }
        source.endArray().endObject();
        indexRandom(false, client().prepareIndex("idx", "type2").setRouting("1").setSource(source));
    }

    public void testSimpleReverseNestedToRoot() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type1")
                .addAggregation(nested("nested1", "nested1")
                        .subAggregation(
                                terms("field2").field("nested1.field2")
                                        .subAggregation(
                                                reverseNested("nested1_to_field1")
                                                        .subAggregation(
                                                                terms("field1").field("field1")
                                                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                                        )
                                        )
                        )
                ).get();

        assertSearchResponse(response);

        Nested nested = response.getAggregations().get("nested1");
        assertThat(nested, notNullValue());
        assertThat(nested.getName(), equalTo("nested1"));
        assertThat(nested.getDocCount(), equalTo(25L));
        assertThat(nested.getAggregations().asList().isEmpty(), is(false));

        Terms usernames = nested.getAggregations().get("field2");
        assertThat(usernames, notNullValue());
        assertThat(usernames.getBuckets().size(), equalTo(9));
        List<Terms.Bucket> usernameBuckets = new ArrayList<>(usernames.getBuckets());

        // nested.field2: 1
        Terms.Bucket bucket = usernameBuckets.get(0);
        assertThat(bucket.getKeyAsString(), equalTo("1"));
        assertThat(bucket.getDocCount(), equalTo(6L));
        ReverseNested reverseNested = bucket.getAggregations().get("nested1_to_field1");
        assertThat(reverseNested.getProperty("_count"), equalTo(5L));
        Terms tags = reverseNested.getAggregations().get("field1");
        assertThat(reverseNested.getProperty("field1"), sameInstance(tags));
        List<Terms.Bucket> tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(6));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("c"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(4L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("a"));
        assertThat(tagsBuckets.get(1).getDocCount(), equalTo(3L));
        assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("e"));
        assertThat(tagsBuckets.get(2).getDocCount(), equalTo(2L));
        assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("b"));
        assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(4).getKeyAsString(), equalTo("d"));
        assertThat(tagsBuckets.get(4).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(5).getKeyAsString(), equalTo("x"));
        assertThat(tagsBuckets.get(5).getDocCount(), equalTo(1L));

        // nested.field2: 4
        bucket = usernameBuckets.get(1);
        assertThat(bucket.getKeyAsString(), equalTo("4"));
        assertThat(bucket.getDocCount(), equalTo(4L));
        reverseNested = bucket.getAggregations().get("nested1_to_field1");
        tags = reverseNested.getAggregations().get("field1");
        tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(5));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("a"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(3L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("b"));
        assertThat(tagsBuckets.get(1).getDocCount(), equalTo(2L));
        assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("c"));
        assertThat(tagsBuckets.get(2).getDocCount(), equalTo(2L));
        assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("d"));
        assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(4).getKeyAsString(), equalTo("e"));
        assertThat(tagsBuckets.get(4).getDocCount(), equalTo(1L));

        // nested.field2: 7
        bucket = usernameBuckets.get(2);
        assertThat(bucket.getKeyAsString(), equalTo("7"));
        assertThat(bucket.getDocCount(), equalTo(3L));
        reverseNested = bucket.getAggregations().get("nested1_to_field1");
        tags = reverseNested.getAggregations().get("field1");
        tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(5));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("c"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(2L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("d"));
        assertThat(tagsBuckets.get(1).getDocCount(), equalTo(2L));
        assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("e"));
        assertThat(tagsBuckets.get(2).getDocCount(), equalTo(2L));
        assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("a"));
        assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(4).getKeyAsString(), equalTo("b"));
        assertThat(tagsBuckets.get(4).getDocCount(), equalTo(1L));

        // nested.field2: 2
        bucket = usernameBuckets.get(3);
        assertThat(bucket.getKeyAsString(), equalTo("2"));
        assertThat(bucket.getDocCount(), equalTo(2L));
        reverseNested = bucket.getAggregations().get("nested1_to_field1");
        tags = reverseNested.getAggregations().get("field1");
        tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(3));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("a"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(2L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("c"));
        assertThat(tagsBuckets.get(1).getDocCount(), equalTo(2L));
        assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("b"));
        assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));

        // nested.field2: 3
        bucket = usernameBuckets.get(4);
        assertThat(bucket.getKeyAsString(), equalTo("3"));
        assertThat(bucket.getDocCount(), equalTo(2L));
        reverseNested = bucket.getAggregations().get("nested1_to_field1");
        tags = reverseNested.getAggregations().get("field1");
        tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(3));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("a"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(2L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("b"));
        assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("c"));
        assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));

        // nested.field2: 5
        bucket = usernameBuckets.get(5);
        assertThat(bucket.getKeyAsString(), equalTo("5"));
        assertThat(bucket.getDocCount(), equalTo(2L));
        reverseNested = bucket.getAggregations().get("nested1_to_field1");
        tags = reverseNested.getAggregations().get("field1");
        tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(4));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("b"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("c"));
        assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("d"));
        assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("z"));
        assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));

        // nested.field2: 6
        bucket = usernameBuckets.get(6);
        assertThat(bucket.getKeyAsString(), equalTo("6"));
        assertThat(bucket.getDocCount(), equalTo(2L));
        reverseNested = bucket.getAggregations().get("nested1_to_field1");
        tags = reverseNested.getAggregations().get("field1");
        tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(4));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("c"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(2L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("b"));
        assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("d"));
        assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("y"));
        assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));

        // nested.field2: 8
        bucket = usernameBuckets.get(7);
        assertThat(bucket.getKeyAsString(), equalTo("8"));
        assertThat(bucket.getDocCount(), equalTo(2L));
        reverseNested = bucket.getAggregations().get("nested1_to_field1");
        tags = reverseNested.getAggregations().get("field1");
        tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(4));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("c"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(2L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("d"));
        assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("e"));
        assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("x"));
        assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));

        // nested.field2: 9
        bucket = usernameBuckets.get(8);
        assertThat(bucket.getKeyAsString(), equalTo("9"));
        assertThat(bucket.getDocCount(), equalTo(2L));
        reverseNested = bucket.getAggregations().get("nested1_to_field1");
        tags = reverseNested.getAggregations().get("field1");
        tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(4));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("c"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("d"));
        assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("e"));
        assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("z"));
        assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));
    }

    public void testSimpleNested1ToRootToNested2() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type2")
                .addAggregation(nested("nested1", "nested1")
                                .subAggregation(
                                        reverseNested("nested1_to_root")
                                                .subAggregation(nested("root_to_nested2", "nested1.nested2"))
                                        )
                                )
                .get();

        assertSearchResponse(response);
        Nested nested = response.getAggregations().get("nested1");
        assertThat(nested.getName(), equalTo("nested1"));
        assertThat(nested.getDocCount(), equalTo(9L));
        ReverseNested reverseNested = nested.getAggregations().get("nested1_to_root");
        assertThat(reverseNested.getName(), equalTo("nested1_to_root"));
        assertThat(reverseNested.getDocCount(), equalTo(4L));
        nested = reverseNested.getAggregations().get("root_to_nested2");
        assertThat(nested.getName(), equalTo("root_to_nested2"));
        assertThat(nested.getDocCount(), equalTo(27L));
    }

    public void testSimpleReverseNestedToNested1() throws Exception {
        SearchResponse response = client().prepareSearch("idx").setTypes("type2")
                .addAggregation(nested("nested1", "nested1.nested2")
                                .subAggregation(
                                        terms("field2").field("nested1.nested2.field2").order(Terms.Order.term(true))
                                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                                .size(10000)
                                                .subAggregation(
                                                        reverseNested("nested1_to_field1").path("nested1")
                                                                .subAggregation(
                                                                        terms("field1").field("nested1.field1").order(Terms.Order.term(true))
                                                                                .collectMode(randomFrom(SubAggCollectionMode.values()))
                                                                )
                                                )
                                )
                ).get();

        assertSearchResponse(response);

        Nested nested = response.getAggregations().get("nested1");
        assertThat(nested, notNullValue());
        assertThat(nested.getName(), equalTo("nested1"));
        assertThat(nested.getDocCount(), equalTo(27L));
        assertThat(nested.getAggregations().asList().isEmpty(), is(false));

        Terms usernames = nested.getAggregations().get("field2");
        assertThat(usernames, notNullValue());
        assertThat(usernames.getBuckets().size(), equalTo(5));
        List<Terms.Bucket> usernameBuckets = new ArrayList<>(usernames.getBuckets());

        Terms.Bucket bucket = usernameBuckets.get(0);
        assertThat(bucket.getKeyAsString(), equalTo("0"));
        assertThat(bucket.getDocCount(), equalTo(12L));
        ReverseNested reverseNested = bucket.getAggregations().get("nested1_to_field1");
        assertThat(reverseNested.getDocCount(), equalTo(5L));
        Terms tags = reverseNested.getAggregations().get("field1");
        List<Terms.Bucket> tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(2));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("a"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(3L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("b"));
        assertThat(tagsBuckets.get(1).getDocCount(), equalTo(2L));

        bucket = usernameBuckets.get(1);
        assertThat(bucket.getKeyAsString(), equalTo("1"));
        assertThat(bucket.getDocCount(), equalTo(6L));
        reverseNested = bucket.getAggregations().get("nested1_to_field1");
        assertThat(reverseNested.getDocCount(), equalTo(4L));
        tags = reverseNested.getAggregations().get("field1");
        tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(4));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("a"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("b"));
        assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("c"));
        assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("e"));
        assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));

        bucket = usernameBuckets.get(2);
        assertThat(bucket.getKeyAsString(), equalTo("2"));
        assertThat(bucket.getDocCount(), equalTo(5L));
        reverseNested = bucket.getAggregations().get("nested1_to_field1");
        assertThat(reverseNested.getDocCount(), equalTo(4L));
        tags = reverseNested.getAggregations().get("field1");
        tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(4));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("a"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("b"));
        assertThat(tagsBuckets.get(1).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(2).getKeyAsString(), equalTo("c"));
        assertThat(tagsBuckets.get(2).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(3).getKeyAsString(), equalTo("e"));
        assertThat(tagsBuckets.get(3).getDocCount(), equalTo(1L));

        bucket = usernameBuckets.get(3);
        assertThat(bucket.getKeyAsString(), equalTo("3"));
        assertThat(bucket.getDocCount(), equalTo(2L));
        reverseNested = bucket.getAggregations().get("nested1_to_field1");
        assertThat(reverseNested.getDocCount(), equalTo(2L));
        tags = reverseNested.getAggregations().get("field1");
        tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(2));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("d"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("f"));

        bucket = usernameBuckets.get(4);
        assertThat(bucket.getKeyAsString(), equalTo("4"));
        assertThat(bucket.getDocCount(), equalTo(2L));
        reverseNested = bucket.getAggregations().get("nested1_to_field1");
        assertThat(reverseNested.getDocCount(), equalTo(2L));
        tags = reverseNested.getAggregations().get("field1");
        tagsBuckets = new ArrayList<>(tags.getBuckets());
        assertThat(tagsBuckets.size(), equalTo(2));
        assertThat(tagsBuckets.get(0).getKeyAsString(), equalTo("d"));
        assertThat(tagsBuckets.get(0).getDocCount(), equalTo(1L));
        assertThat(tagsBuckets.get(1).getKeyAsString(), equalTo("f"));
    }

    public void testReverseNestedAggWithoutNestedAgg() {
        try {
            client().prepareSearch("idx")
                    .addAggregation(terms("field2").field("nested1.nested2.field2")
                            .collectMode(randomFrom(SubAggCollectionMode.values()))
                                    .subAggregation(
                                            reverseNested("nested1_to_field1")
                                                    .subAggregation(
                                                            terms("field1").field("nested1.field1")
                                                            .collectMode(randomFrom(SubAggCollectionMode.values()))
                                                    )
                                    )
                    ).get();
            fail("Expected SearchPhaseExecutionException");
        } catch (SearchPhaseExecutionException e) {
            assertThat(e.getMessage(), is("all shards failed"));
        }
    }

    public void testNonExistingNestedField() throws Exception {
        SearchResponse searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(nested("nested2", "nested1.nested2").subAggregation(reverseNested("incorrect").path("nested3")))
                .get();

        Nested nested = searchResponse.getAggregations().get("nested2");
        assertThat(nested, notNullValue());
        assertThat(nested.getName(), equalTo("nested2"));

        ReverseNested reverseNested = nested.getAggregations().get("incorrect");
        assertThat(reverseNested.getDocCount(), is(0L));

        // Test that parsing the reverse_nested agg doesn't fail, because the parent nested agg is unmapped:
        searchResponse = client().prepareSearch("idx")
                .setQuery(matchAllQuery())
                .addAggregation(nested("incorrect1", "incorrect1").subAggregation(reverseNested("incorrect2").path("incorrect2")))
                .get();

        nested = searchResponse.getAggregations().get("incorrect1");
        assertThat(nested, notNullValue());
        assertThat(nested.getName(), equalTo("incorrect1"));
        assertThat(nested.getDocCount(), is(0L));
    }

    public void testSameParentDocHavingMultipleBuckets() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("product").field("dynamic", "strict").startObject("properties")
                .startObject("id").field("type", "long").endObject()
                .startObject("category")
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("name").field("type", "keyword").endObject()
                    .endObject()
                .endObject()
                .startObject("sku")
                    .field("type", "nested")
                    .startObject("properties")
                        .startObject("sku_type").field("type", "keyword").endObject()
                            .startObject("colors")
                                .field("type", "nested")
                                .startObject("properties")
                                    .startObject("name").field("type", "keyword").endObject()
                                .endObject()
                            .endObject()
                    .endObject()
                .endObject()
                .endObject().endObject().endObject();
        assertAcked(
                prepareCreate("idx3")
                        .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1).put(SETTING_NUMBER_OF_REPLICAS, 0))
                        .addMapping("product", mapping)
        );

        client().prepareIndex("idx3", "product", "1").setRefreshPolicy(IMMEDIATE).setSource(
                jsonBuilder().startObject()
                        .startArray("sku")
                            .startObject()
                                .field("sku_type", "bar1")
                                .startArray("colors")
                                    .startObject().field("name", "red").endObject()
                                    .startObject().field("name", "green").endObject()
                                    .startObject().field("name", "yellow").endObject()
                                .endArray()
                            .endObject()
                            .startObject()
                                .field("sku_type", "bar1")
                                .startArray("colors")
                                .startObject().field("name", "red").endObject()
                                .startObject().field("name", "blue").endObject()
                                .startObject().field("name", "white").endObject()
                                .endArray()
                            .endObject()
                            .startObject()
                                .field("sku_type", "bar1")
                                .startArray("colors")
                                .startObject().field("name", "black").endObject()
                                .startObject().field("name", "blue").endObject()
                                .endArray()
                            .endObject()
                            .startObject()
                                .field("sku_type", "bar2")
                                .startArray("colors")
                                .startObject().field("name", "orange").endObject()
                                .endArray()
                            .endObject()
                            .startObject()
                                .field("sku_type", "bar2")
                                .startArray("colors")
                                .startObject().field("name", "pink").endObject()
                                .endArray()
                            .endObject()
                        .endArray()
                        .startArray("category")
                            .startObject().field("name", "abc").endObject()
                            .startObject().field("name", "klm").endObject()
                            .startObject().field("name", "xyz").endObject()
                        .endArray()
                        .endObject()
        ).get();

        SearchResponse response = client().prepareSearch("idx3")
                .addAggregation(
                        nested("nested_0", "category").subAggregation(
                                terms("group_by_category").field("category.name").subAggregation(
                                        reverseNested("to_root").subAggregation(
                                                nested("nested_1", "sku").subAggregation(
                                                        filter("filter_by_sku", termQuery("sku.sku_type", "bar1")).subAggregation(
                                                                count("sku_count").field("sku.sku_type")
                                                        )
                                                )
                                        )
                                )
                        )
                ).get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        Nested nested0 = response.getAggregations().get("nested_0");
        assertThat(nested0.getDocCount(), equalTo(3L));
        Terms terms = nested0.getAggregations().get("group_by_category");
        assertThat(terms.getBuckets().size(), equalTo(3));
        for (String bucketName : new String[]{"abc", "klm", "xyz"}) {
            logger.info("Checking results for bucket {}", bucketName);
            Terms.Bucket bucket = terms.getBucketByKey(bucketName);
            assertThat(bucket.getDocCount(), equalTo(1L));
            ReverseNested toRoot = bucket.getAggregations().get("to_root");
            assertThat(toRoot.getDocCount(), equalTo(1L));
            Nested nested1 = toRoot.getAggregations().get("nested_1");
            assertThat(nested1.getDocCount(), equalTo(5L));
            Filter filterByBar = nested1.getAggregations().get("filter_by_sku");
            assertThat(filterByBar.getDocCount(), equalTo(3L));
            ValueCount barCount = filterByBar.getAggregations().get("sku_count");
            assertThat(barCount.getValue(), equalTo(3L));
        }

        response = client().prepareSearch("idx3")
                .addAggregation(
                        nested("nested_0", "category").subAggregation(
                                terms("group_by_category").field("category.name").subAggregation(
                                        reverseNested("to_root").subAggregation(
                                                nested("nested_1", "sku").subAggregation(
                                                        filter("filter_by_sku", termQuery("sku.sku_type", "bar1")).subAggregation(
                                                                nested("nested_2", "sku.colors").subAggregation(
                                                                        filter("filter_sku_color", termQuery("sku.colors.name", "red")).subAggregation(
                                                                                reverseNested("reverse_to_sku").path("sku").subAggregation(
                                                                                        count("sku_count").field("sku.sku_type")
                                                                                )
                                                                        )
                                                                )
                                                        )
                                                )
                                        )
                                )
                        )
                ).get();
        assertNoFailures(response);
        assertHitCount(response, 1);

        nested0 = response.getAggregations().get("nested_0");
        assertThat(nested0.getDocCount(), equalTo(3L));
        terms = nested0.getAggregations().get("group_by_category");
        assertThat(terms.getBuckets().size(), equalTo(3));
        for (String bucketName : new String[]{"abc", "klm", "xyz"}) {
            logger.info("Checking results for bucket {}", bucketName);
            Terms.Bucket bucket = terms.getBucketByKey(bucketName);
            assertThat(bucket.getDocCount(), equalTo(1L));
            ReverseNested toRoot = bucket.getAggregations().get("to_root");
            assertThat(toRoot.getDocCount(), equalTo(1L));
            Nested nested1 = toRoot.getAggregations().get("nested_1");
            assertThat(nested1.getDocCount(), equalTo(5L));
            Filter filterByBar = nested1.getAggregations().get("filter_by_sku");
            assertThat(filterByBar.getDocCount(), equalTo(3L));
            Nested nested2 = filterByBar.getAggregations().get("nested_2");
            assertThat(nested2.getDocCount(), equalTo(8L));
            Filter filterBarColor = nested2.getAggregations().get("filter_sku_color");
            assertThat(filterBarColor.getDocCount(), equalTo(2L));
            ReverseNested reverseToBar = filterBarColor.getAggregations().get("reverse_to_sku");
            assertThat(reverseToBar.getDocCount(), equalTo(2L));
            ValueCount barCount = reverseToBar.getAggregations().get("sku_count");
            assertThat(barCount.getValue(), equalTo(2L));
        }
    }
}
