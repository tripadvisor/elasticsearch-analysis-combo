/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.apache.lucene.analysis;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.analysis.NamedAnalyzer;

import java.util.ArrayList;
import java.util.function.Function;

/**
 * ElasticSearch ComboAnalyzerWrapper wrapper over Lucene ComboAnalyzerWrapper.
 */
public final class ComboAnalyzerWrapper extends Analyzer {

    public static final String NAME = "combo";

    private final Logger logger;

    private final Settings settings;
    private final String name;
    private final Function<String, NamedAnalyzer> analyzerResolver;

    private org.apache.lucene.analysis.ComboAnalyzer analyzer;

    public ComboAnalyzerWrapper(String name, Settings settings, Function<String, NamedAnalyzer> analyzerResolver) {
        logger = ESLoggerFactory.getLogger(NAME+">"+name);

        this.name = name;

        // Store parameters for lazy usage
        // See ComboAnalyzerProvider comments to learn why
        this.settings = settings;

        this.analyzer = null; // must be lazy initialized to get free of the cyclic dependency on AnalysisService
        this.analyzerResolver = analyzerResolver;
    }

    /**
     * Read settings and load the appropriate sub-analyzers.
     */
    synchronized
    protected void init() {
        if (analyzer != null) return;

        String[] sub = settings.getAsArray("sub_analyzers");
        ArrayList<Analyzer> subAnalyzers = new ArrayList<Analyzer>();
        if (sub == null) {
            throw new IllegalArgumentException("Analyzer ["+name+"] analyzer of type ["+NAME+"], must have a \"sub_analyzers\" list property");
        }

        for (String subname : sub) {
            NamedAnalyzer analyzer = analyzerResolver.apply(subname);
            if (analyzer == null) {
                logger.debug("Sub-analyzer \""+subname+"\" not found!");
            } else {
                subAnalyzers.add(analyzer);
            }
        }

        this.analyzer = new org.apache.lucene.analysis.ComboAnalyzer(subAnalyzers.toArray(new Analyzer[subAnalyzers.size()]));

        Boolean tokenstreamCaching = settings.getAsBoolean("tokenstream_caching", null);
        if (tokenstreamCaching != null)
            this.analyzer.setTokenStreamCachingEnabled(tokenstreamCaching);

        Boolean deduplication = settings.getAsBoolean("deduplication", null);
        if (deduplication != null)
            this.analyzer.setDeduplicationEnabled(deduplication);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        if (analyzer == null) init();
        return this.analyzer.createComponents(fieldName);
    }

    @Override public void close() {
        if (analyzer != null) this.analyzer.close();
        super.close();
    }

}
