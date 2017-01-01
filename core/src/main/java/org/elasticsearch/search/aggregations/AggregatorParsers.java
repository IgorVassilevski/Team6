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
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ParseFieldRegistry;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A registry for all the aggregator parser, also servers as the main parser for the aggregations module
 */
public class AggregatorParsers {
    public static final Pattern VALID_AGG_NAME = Pattern.compile("[^\\[\\]>]+");

    private final ParseFieldRegistry<Aggregator.Parser> aggregationParserRegistry;
    private final ParseFieldRegistry<PipelineAggregator.Parser> pipelineAggregationParserRegistry;

    public AggregatorParsers(ParseFieldRegistry<Aggregator.Parser> aggregationParserRegistry,
            ParseFieldRegistry<PipelineAggregator.Parser> pipelineAggregationParserRegistry) {
        this.aggregationParserRegistry = aggregationParserRegistry;
        this.pipelineAggregationParserRegistry = pipelineAggregationParserRegistry;
    }

    /**
     * Returns the parser that is registered under the given aggregation type.
     *
     * @param type The aggregation type
     * @param parseFieldMatcher used for making error messages.
     * @return The parser associated with the given aggregation type or null if it wasn't found.
     */
    public Aggregator.Parser parser(String type, ParseFieldMatcher parseFieldMatcher) {
        return aggregationParserRegistry.lookupReturningNullIfNotFound(type, parseFieldMatcher);
    }

    /**
     * Returns the parser that is registered under the given pipeline aggregator type.
     *
     * @param type The pipeline aggregator type
     * @param parseFieldMatcher used for making error messages.
     * @return The parser associated with the given pipeline aggregator type or null if it wasn't found.
     */
    public PipelineAggregator.Parser pipelineParser(String type, ParseFieldMatcher parseFieldMatcher) {
        return pipelineAggregationParserRegistry.lookupReturningNullIfNotFound(type, parseFieldMatcher);
    }

    /**
     * Parses the aggregation request recursively generating aggregator factories in turn.
     *
     * @param parseContext   The parse context.
     *
     * @return          The parsed aggregator factories.
     *
     * @throws IOException When parsing fails for unknown reasons.
     */
    public AggregatorFactories.Builder parseAggregators(QueryParseContext parseContext) throws IOException {
        return parseAggregators(parseContext, 0);
    }

    private AggregatorFactories.Builder parseAggregators(QueryParseContext parseContext, int level) throws IOException {
        Matcher validAggMatcher = VALID_AGG_NAME.matcher("");
        AggregatorFactories.Builder factories = new AggregatorFactories.Builder();

        XContentParser.Token token = null;
        XContentParser parser = parseContext.parser();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            final String aggregationName = getString(validAggMatcher, token, parser);

            AggregationBuilder aggFactory = null;
            PipelineAggregationBuilder pipelineAggregatorFactory = null;
            AggregatorFactories.Builder subFactories = null;

            Map<String, Object> metaData = null;

            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token != XContentParser.Token.FIELD_NAME) {
                    throw new ParsingException(
                            parser.getTokenLocation(), "Expected [" + XContentParser.Token.FIELD_NAME + "] under a ["
                            + XContentParser.Token.START_OBJECT + "], but got a [" + token + "] in [" + aggregationName + "]",
                            parser.getTokenLocation());
                }
                final String fieldName = parser.currentName();

                token = parser.nextToken();
                if (token == XContentParser.Token.START_OBJECT) {
                    switch (fieldName) {
                    case "meta":
                        metaData = parser.map();
                        break;
                    case "aggregations":
                    case "aggs":
                        if (subFactories != null) {
                            getBuilder(parser.getTokenLocation(), "Found two sub aggregation definitions under [" + aggregationName + "]");
                        }
                        subFactories = parseAggregators(parseContext, level + 1);
                        break;
                    default:
                        throwExpection(parser, aggregationName, aggFactory, pipelineAggregatorFactory, fieldName);

                        Aggregator.Parser aggregatorParser = parser(fieldName, parseContext.getParseFieldMatcher());
                        if (aggregatorParser == null) {
                                PipelineAggregator.Parser pipelineAggregatorParser = pipelineParser(fieldName,
                                        parseContext.getParseFieldMatcher());
                                if (pipelineAggregatorParser == null) {
                                    return getBuilder(parser.getTokenLocation(), "Could not find aggregator type [" + fieldName + "] in [" + aggregationName + "]");
                                } else {
                                    pipelineAggregatorFactory = pipelineAggregatorParser.parse(aggregationName, parseContext);
                                }
                        } else {
                            aggFactory = aggregatorParser.parse(aggregationName, parseContext);
                        }
                    }
                } else {
                    getBuilder(parser.getTokenLocation(), "Expected [" + XContentParser.Token.START_OBJECT + "] under ["
                            + fieldName + "], but got a [" + token + "] in [" + aggregationName + "]");
                }
            }

            noAggFactoryOrPipline(factories, parser, aggregationName, aggFactory, pipelineAggregatorFactory, subFactories, metaData);
        }

        return factories;
    }

    private void noAggFactoryOrPipline(AggregatorFactories.Builder factories, XContentParser parser, String aggregationName, AggregationBuilder aggFactory, PipelineAggregationBuilder pipelineAggregatorFactory, AggregatorFactories.Builder subFactories, Map<String, Object> metaData) {
        if (aggFactory == null && pipelineAggregatorFactory == null) {
            throw new ParsingException(parser.getTokenLocation(), "Missing definition for aggregation [" + aggregationName + "]",
                    parser.getTokenLocation());
        } else if (aggFactory != null) {
            assert pipelineAggregatorFactory == null;
        if (metaData != null) {
                aggFactory.setMetaData(metaData);
        }

        if (subFactories != null) {
                aggFactory.subAggregations(subFactories);
        }

            factories.addAggregator(aggFactory);
        } else {
            assert pipelineAggregatorFactory != null;
            if (subFactories != null) {
                throw new ParsingException(parser.getTokenLocation(),
                        "Aggregation [" + aggregationName + "] cannot define sub-aggregations",
                        parser.getTokenLocation());
            }
            if (metaData != null) {
                pipelineAggregatorFactory.setMetaData(metaData);
            }
            factories.addPipelineAggregator(pipelineAggregatorFactory);
        }
    }

    private String getString(Matcher validAggMatcher, XContentParser.Token token, XContentParser parser) throws IOException {
        if (token != XContentParser.Token.FIELD_NAME) {
            getBuilder(parser.getTokenLocation(), "Unexpected token " + token + " in [aggs]: aggregations definitions must start with the name of the aggregation.");
        }
        final String aggregationName = parser.currentName();
        if (!validAggMatcher.reset(aggregationName).matches()) {
            getBuilder(parser.getTokenLocation(), "Invalid aggregation name [" + aggregationName
                    + "]. Aggregation names must be alpha-numeric and can only contain '_' and '-'");
        }

        token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            getBuilder(parser.getTokenLocation(), "Aggregation definition for [" + aggregationName + " starts with a ["
                    + token + "], expected a [" + XContentParser.Token.START_OBJECT + "].");
        }
        return aggregationName;
    }

    private AggregatorFactories.Builder getBuilder(XContentLocation tokenLocation, String msg) {
        throw new ParsingException(tokenLocation,
                msg);
    }

    private void throwExpection(XContentParser parser, String aggregationName, AggregationBuilder aggFactory, PipelineAggregationBuilder pipelineAggregatorFactory, String fieldName) {
        if (aggFactory != null) {
            getBuilder(parser.getTokenLocation(), "Found two aggregation type definitions in ["
                    + aggregationName + "]: [" + aggFactory.type + "] and [" + fieldName + "]");
        }
        if (pipelineAggregatorFactory != null) {
            getBuilder(parser.getTokenLocation(), "Found two aggregation type definitions in ["
                    + aggregationName + "]: [" + pipelineAggregatorFactory + "] and [" + fieldName + "]");
        }
    }

}
