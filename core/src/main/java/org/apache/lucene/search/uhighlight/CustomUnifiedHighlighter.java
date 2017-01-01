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

package org.apache.lucene.search.uhighlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.Snippet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;

import java.io.IOException;
import java.text.BreakIterator;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Subclass of the {@link UnifiedHighlighter} that works for a single field in a single document.
 * Uses a custom {@link org.apache.lucene.search.uhighlight.PassageFormatter}. Accepts field content as a constructor
 * argument, given that loadingis custom and can be done reading from _source field.
 * Supports using different {@link BreakIterator} to break the text into fragments. Considers every distinct field
 * value as a discrete passage for highlighting (unless the whole content needs to be highlighted).
 * Supports both returning empty snippets and non highlighted snippets when no highlighting can be performed.
 */
public class CustomUnifiedHighlighter extends UnifiedHighlighter {
    private static final Snippet[] EMPTY_SNIPPET = new Snippet[0];

    private final String fieldValue;
    private final PassageFormatter passageFormatter;
    private final BreakIterator breakIterator;
    private final OffsetSource offsetSource;
    private final boolean requireFieldMatch;
    private final boolean returnNonHighlightedSnippets;

    /**
     * Creates a new instance of {@link CustomUnifiedHighlighter}
     *
     * @param analyzer the analyzer used for the field at index time, used for multi term queries internally
     * @param passageFormatter our own {@link org.apache.lucene.search.uhighlight.PassageFormatter}
     *                         which generates snippets in forms of {@link Snippet} objects
     * @param fieldValue the original field values as constructor argument, loaded from te _source field or
     *                   the relevant stored field.
     * @param forceOffsetSource force the mode for this highlighter
     * @param returnNonHighlightedSnippets whether non highlighted snippets should be
     *                                     returned rather than empty snippets when no highlighting can be performed
     * @param requireFieldMatch highlights queries that match the provided field when set to true, all queries otherwise
     */
    public CustomUnifiedHighlighter(IndexSearcher searcher, Analyzer analyzer, PassageFormatter passageFormatter,
                                    BreakIterator breakIterator, String fieldValue, OffsetSource forceOffsetSource,
                                    boolean returnNonHighlightedSnippets, boolean requireFieldMatch) {
        super(searcher, analyzer);
        this.breakIterator = breakIterator;
        this.passageFormatter = passageFormatter;
        this.fieldValue = fieldValue;
        this.offsetSource = forceOffsetSource;
        this.returnNonHighlightedSnippets = returnNonHighlightedSnippets;
        this.requireFieldMatch = requireFieldMatch;
    }

    /**
     * Highlights terms extracted from the provided query within the content of the provided field name
     */
    public Snippet[] highlightField(String field, Query query, int docId, int maxPassages) throws IOException {
        Map<String, Object[]> fieldsAsObjects = super.highlightFieldsAsObjects(new String[]{field}, query,
                new int[]{docId}, new int[]{maxPassages});
        Object[] snippetObjects = fieldsAsObjects.get(field);
        if (snippetObjects != null) {
            //one single document at a time
            assert snippetObjects.length == 1;
            Object snippetObject = snippetObjects[0];
            if (snippetObject != null && snippetObject instanceof Snippet[]) {
                return (Snippet[]) snippetObject;
            }
        }
        return EMPTY_SNIPPET;
    }

    @Override
    protected List<CharSequence[]> loadFieldValues(String[] fields,
                                                   DocIdSetIterator docIter,
                                                   int cacheCharsThreshold) throws IOException {
        //we only highlight one field, one document at a time
        return Collections.singletonList(new String[]{fieldValue});
    }

    @Override
    protected BreakIterator getBreakIterator(String field) {
        if (breakIterator != null) {
            return breakIterator;
        }
        return super.getBreakIterator(field);
    }

    @Override
    protected PassageFormatter getFormatter(String field) {
        return passageFormatter;
    }

    @Override
    protected OffsetSource getOptimizedOffsetSource(String field,
                                                    BytesRef[] terms,
                                                    PhraseHelper phraseHelper,
                                                    CharacterRunAutomaton[] automata) {
        if (offsetSource != null) {
            return offsetSource;
        }
        return super.getOptimizedOffsetSource(field, terms, phraseHelper, automata);
    }

    @Override
    protected int getMaxNoHighlightPassages(String field) {
        if (returnNonHighlightedSnippets) {
            return 1;
        }
        return 0;
    }


}

