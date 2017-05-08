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

package org.elasticsearch.index.analysis;

import org.apache.lucene.analysis.ComboAnalyzerWrapper;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.plugin.analysis.combo.AnalysisComboPlugin;
import org.elasticsearch.plugins.AnalysisPlugin;
import org.elasticsearch.plugins.PluginsService;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ComboAnalyzerProvider extends AbstractIndexAnalyzerProvider<ComboAnalyzerWrapper> {

    private final Environment environment;
    private final Settings analyzerSettings;
    private final String name;

    public ComboAnalyzerProvider(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        super(indexSettings, name, settings);
        // Store parameters for delegated usage inside the ComboAnalyzerWrapper itself
        // Sub-analyzer resolution must use the AnalysisService,
        // but as we're a dependency of it (and it's not a Proxy-able interface)
        // we cannot in turn rely on it. Therefore the dependency has to be used lazily.
        this.environment = environment;
        this.analyzerSettings = settings;
        this.name = name;
    }

    @Override
    public ComboAnalyzerWrapper get() {
        // This function is also called during the AnalysisService initialization,
        // hence the following constructor also needs to to perform lazy loading by itself.
        try {
            IndexAnalyzers indexAnalyzers = getIndexAnalyzers(environment);
            return new ComboAnalyzerWrapper(name, analyzerSettings, indexAnalyzers::get);
        } catch (Exception e) {
            e.printStackTrace();
            throw new ElasticsearchException("Exception while getting ComboAnalyzerWrapper from ComboAnalyzerProvider for analyzer with name: " + name + " and with settings: " + analyzerSettings, e);
        }
    }

    private IndexAnalyzers getIndexAnalyzers(Environment environment) throws IOException {

        final PluginsService pluginsService = new PluginsService(
            environment.settings(),
            environment.modulesFile(),
            environment.pluginsFile(),
            Collections.singleton(AnalysisComboPlugin.class) // otherwise it will crash when parsing settings of our analysis combo plugin
        );

        final AnalysisModule analysisModule = new AnalysisModule(
            environment,
            pluginsService.filterPlugins(AnalysisPlugin.class)
        );

        final AnalysisRegistry analysisRegistry = analysisModule.getAnalysisRegistry();

        final Map<String, AnalyzerProvider<?>> analyzerProviderMap = analysisRegistry.buildAnalyzerFactories(indexSettings);

        // we have to filter ComboAnalyzerProvider or we will have infinite recursion
        Map<String, AnalyzerProvider<?>> analyzerFactories = filterByValue(
            analyzerProviderMap,
            e -> !ComboAnalyzerProvider.class.isAssignableFrom(e.getClass())
        );

        return analysisRegistry.build(indexSettings, analyzerFactories, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    }

    private static <K, V> Map<K, V> filterByValue(Map<K, V> map, Predicate<V> predicate) {
        return map.entrySet()
            .stream()
            .filter(entry -> predicate.test(entry.getValue()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
