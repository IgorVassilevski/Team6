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

package org.elasticsearch.search.aggregations.metrics.geoheatmap;

import com.vividsolutions.jts.geom.Coordinate;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.metrics.geoheatmap.GeoHeatmap;
import org.elasticsearch.search.aggregations.metrics.geoheatmap.GeoHeatmapAggregationBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;

import java.io.IOException;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoIntersectionQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

public class GeoHeatmapIT extends ESIntegTestCase {

    /**
     * Indexes a random shape, builds a random heatmap with that geometry, and
     * makes sure there are '1' entries in the heatmap counts
     */
    public void testShapeFilterWithRandomGeoCollection() throws IOException {
        String name = randomAsciiOfLengthBetween(3, 20);
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());
        logger.info("Created Random GeometryCollection containing {} shapes", gcb.numShapes());

        client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape,tree=quadtree").execute()
                .actionGet();

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("location"), null).endObject();
        client().prepareIndex("test", "type", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder filterShape = (gcb.getShapeAt(randomIntBetween(0, gcb.numShapes() - 1)));

        GeoShapeQueryBuilder geom;
        try {
            geom = QueryBuilders.geoShapeQuery("location", filterShape);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        geom.relation(ShapeRelation.INTERSECTS);

        GeoHeatmapAggregationBuilder factory = new GeoHeatmapAggregationBuilder(name);
        if (randomBoolean()) {
            factory.geom(geom);
        }
        if (randomBoolean()) {
            int gridLevel = randomIntBetween(1, 12);
            factory.gridLevel(gridLevel);
        }
        if (randomBoolean()) {
            factory.maxCells(randomIntBetween(1, Integer.MAX_VALUE));
        }
        factory.field("location");

        SearchResponse result = client().prepareSearch("test").setTypes("type").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(geom)
                .get();
        assertSearchResponse(result);
        assertHitCount(result, 1);

        result = client().prepareSearch("test").setTypes("type").setQuery(QueryBuilders.matchAllQuery()).addAggregation(factory).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);

        GeoHeatmap heatmap = result.getAggregations().get(name);
        assertThat(heatmap, notNullValue());

        int maxHeatmapValue = 0;
        for (int i = 0; i < heatmap.getCounts().length; i++) {
            maxHeatmapValue = Math.max(maxHeatmapValue, heatmap.getCounts()[i]);
        }
        assertEquals(1, maxHeatmapValue);
    }

    // Run similar queries to Lucene's SpatialHeatmapFacetsTest
    public void testSpecificShapes() throws IOException {
        client().admin().indices().prepareCreate("test").addMapping("type1", "location", "type=geo_shape,tree=quadtree").execute()
                .actionGet();

        // on right side
        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject().field("name", "Document 1").startObject("location").field("type", "envelope")
                        .startArray("coordinates").startArray().value(100).value(80).endArray().startArray().value(120).value(40).endArray()
                        .endArray().endObject().endObject())
                .setRefreshPolicy(IMMEDIATE).get();

        // on left side (outside heatmap)
        client().prepareIndex("test", "type1", "2")
                .setSource(jsonBuilder().startObject().field("name", "Document 2").startObject("location").field("type", "envelope")
                        .startArray("coordinates").startArray().value(-120).value(80).endArray().startArray().value(-110).value(20)
                        .endArray().endArray().endObject().endObject())
                .setRefreshPolicy(IMMEDIATE).get();

        // just left of BOX 0
        client().prepareIndex("test", "type1", "3")
                .setSource(jsonBuilder().startObject().field("name", "Document 3").startObject("location").field("type", "point")
                        .startArray("coordinates").value(70).value(-60).endArray().endObject().endObject())
                .setRefreshPolicy(IMMEDIATE).get();

        // just outside box 0 (above it) near pole,
        client().prepareIndex("test", "type1", "4")
                .setSource(jsonBuilder().startObject().field("name", "Document 4").startObject("location").field("type", "point")
                        .startArray("coordinates").value(91).value(89).endArray().endObject().endObject())
                .setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder query = ShapeBuilders.newEnvelope(new Coordinate(180, 90), new Coordinate(50, 20));

        SearchResponse searchResponse = client().prepareSearch("test").setTypes("type1").setQuery(geoIntersectionQuery("location", query))
                .execute().actionGet();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
    }

}
