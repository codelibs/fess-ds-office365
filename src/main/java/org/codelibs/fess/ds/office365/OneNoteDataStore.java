/*
 * Copyright 2012-2023 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.office365;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.office365.client.Office365Client;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.Notebook;
import com.microsoft.graph.models.Site;
import com.microsoft.graph.requests.GraphServiceClient;
import com.microsoft.graph.requests.NotebookCollectionPage;
import com.microsoft.graph.requests.OnenoteRequestBuilder;

import okhttp3.Request;

public class OneNoteDataStore extends Office365DataStore {

    private static final Logger logger = LoggerFactory.getLogger(OneNoteDataStore.class);

    // scripts
    protected static final String NOTEBOOK = "notebook";
    protected static final String NOTEBOOK_NAME = "name";
    protected static final String NOTEBOOK_CONTENTS = "contents";
    protected static final String NOTEBOOK_SIZE = "size";
    protected static final String NOTEBOOK_CREATED = "created";
    protected static final String NOTEBOOK_LAST_MODIFIED = "last_modified";
    protected static final String NOTEBOOK_WEB_URL = "web_url";
    protected static final String NOTEBOOK_ROLES = "roles";
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    protected static final String SITE_NOTE_CRAWLER = "site_note_crawler";
    protected static final String USER_NOTE_CRAWLER = "user_note_crawler";
    protected static final String GROUP_NOTE_CRAWLER = "group_note_crawler";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Office365Client client = createClient(paramMap)) {
            if (isSiteNoteCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("crawling site notes.");
                }
                storeSiteNotes(dataConfig, callback, paramMap, scriptMap, defaultDataMap, executorService, client);
            }
            if (isUserNoteCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("crawling user notes.");
                }
                storeUsersNotes(dataConfig, callback, paramMap, scriptMap, defaultDataMap, executorService, client);
            }
            if (isGroupNoteCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("crawling group notes.");
                }
                storeGroupsNotes(dataConfig, callback, paramMap, scriptMap, defaultDataMap, executorService, client);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Shutting down thread executor.");
            }
            executorService.shutdown();
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
            throw new InterruptedRuntimeException(e);
        } finally {
            executorService.shutdownNow();
        }
    }

    protected Office365Client createClient(final DataStoreParams params) {
        return new Office365Client(params);
    }

    protected boolean isGroupNoteCrawler(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(GROUP_NOTE_CRAWLER, Constants.TRUE));
    }

    protected boolean isUserNoteCrawler(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(USER_NOTE_CRAWLER, Constants.TRUE));
    }

    protected boolean isSiteNoteCrawler(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(SITE_NOTE_CRAWLER, Constants.TRUE));
    }

    protected void storeSiteNotes(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final ExecutorService executorService,
            final Office365Client client) {
        final Site root = client.getSite("root");
        final List<String> roles = Collections.emptyList();
        getNotebooks(client, c -> c.sites(root.id).onenote(), notebook -> executorService.execute(() -> processNotebook(dataConfig,
                callback, paramMap, scriptMap, defaultDataMap, client, c -> c.sites(root.id).onenote(), notebook, roles)));
    }

    protected void storeUsersNotes(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final ExecutorService executorService,
            final Office365Client client) {
        getLicensedUsers(client, user -> {
            final List<String> roles = getUserRoles(user);
            try {
                getNotebooks(client, c -> c.users(user.id).onenote(), notebook -> executorService.execute(() -> processNotebook(dataConfig,
                        callback, paramMap, scriptMap, defaultDataMap, client, c -> c.users(user.id).onenote(), notebook, roles)));
            } catch (final GraphServiceException e) {
                logger.warn("Failed to store {}'s Notebooks.", user.displayName, e);
            }
        });
    }

    protected void storeGroupsNotes(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final ExecutorService executorService,
            final Office365Client client) {
        getOffice365Groups(client, group -> {
            final List<String> roles = getGroupRoles(group);
            getNotebooks(client, c -> c.groups(group.id).onenote(), notebook -> executorService.execute(() -> processNotebook(dataConfig,
                    callback, paramMap, scriptMap, defaultDataMap, client, c -> c.groups(group.id).onenote(), notebook, roles)));
        });
    }

    protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Office365Client client,
            final Function<GraphServiceClient<Request>, OnenoteRequestBuilder> builder, final Notebook notebook, final List<String> roles) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
        final Map<String, Object> notebooksMap = new HashMap<>();
        final StatsKeyObject statsKey = new StatsKeyObject(notebook.id);
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        try {
            crawlerStatsHelper.begin(statsKey);
            final String url = notebook.links.oneNoteWebUrl.href;
            logger.info("Crawling URL: {}", url);

            final String contents = client.getNotebookContent(builder, notebook.id);
            final long size = contents != null ? contents.length() : 0L;
            notebooksMap.put(NOTEBOOK_NAME, notebook.displayName);
            notebooksMap.put(NOTEBOOK_CONTENTS, contents);
            notebooksMap.put(NOTEBOOK_SIZE, size);
            notebooksMap.put(NOTEBOOK_CREATED, notebook.createdDateTime);
            notebooksMap.put(NOTEBOOK_LAST_MODIFIED, notebook.lastModifiedDateTime);
            notebooksMap.put(NOTEBOOK_WEB_URL, url);
            notebooksMap.put(NOTEBOOK_ROLES, roles);

            resultMap.put(NOTEBOOK, notebooksMap);

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("notebooksMap: {}", notebooksMap);
            }

            final String scriptType = getScriptType(paramMap);
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }

            crawlerStatsHelper.record(statsKey, StatsAction.EVALUATED);

            if (logger.isDebugEnabled()) {
                logger.debug("dataMap: {}", dataMap);
            }

            if (dataMap.get("url") instanceof final String statsUrl) {
                statsKey.setUrl(statsUrl);
            }

            callback.store(paramMap, dataMap);
            crawlerStatsHelper.record(statsKey, StatsAction.FINISHED);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : {}", dataMap, e);

            Throwable target = e;
            if (target instanceof final MultipleCrawlingAccessException ex) {
                final Throwable[] causes = ex.getCauses();
                if (causes.length > 0) {
                    target = causes[causes.length - 1];
                }
            }

            String errorName;
            final Throwable cause = target.getCause();
            if (cause != null) {
                errorName = cause.getClass().getCanonicalName();
            } else {
                errorName = target.getClass().getCanonicalName();
            }

            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, errorName, notebook.displayName, target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), notebook.displayName, t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    protected void getNotebooks(final Office365Client client, final Function<GraphServiceClient<Request>, OnenoteRequestBuilder> builder,
            final Consumer<Notebook> consumer) {
        try {
            NotebookCollectionPage page = client.getNotebookPage(builder);
            page.getCurrentPage().forEach(consumer);
            while (page.getNextPage() != null) {
                page = page.getNextPage().buildRequest().get();
                page.getCurrentPage().forEach(consumer);
            }
        } catch (final GraphServiceException e) {
            if (e.getResponseCode() == 404) {
                logger.debug("Notebook is not found.", e);
            } else {
                logger.warn("Failed to access a notebook.", e);
            }
        } catch (final ClientException e) {
            logger.warn("Failed to access a notebook.", e);
        }
    }

}
