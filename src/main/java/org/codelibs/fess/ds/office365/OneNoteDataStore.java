/*
 * Copyright 2012-2018 CodeLibs Project and the Others.
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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.Notebook;
import com.microsoft.graph.models.extensions.OnenotePage;
import com.microsoft.graph.models.extensions.OnenoteSection;
import com.microsoft.graph.models.extensions.Site;
import com.microsoft.graph.requests.extensions.INotebookCollectionPage;
import com.microsoft.graph.requests.extensions.INotebookRequestBuilder;
import com.microsoft.graph.requests.extensions.IOnenotePageCollectionPage;
import com.microsoft.graph.requests.extensions.IOnenoteRequestBuilder;
import com.microsoft.graph.requests.extensions.IOnenoteSectionCollectionPage;
import com.microsoft.graph.requests.extensions.IOnenoteSectionRequestBuilder;

public class OneNoteDataStore extends Office365DataStore {

    // scripts
    protected static final String NOTEBOOKS = "notebooks";
    protected static final String NOTEBOOKS_NAME = "name";
    protected static final String NOTEBOOKS_CONTENTS = "contents";
    protected static final String NOTEBOOKS_SIZE = "size";
    protected static final String NOTEBOOKS_CREATED = "created";
    protected static final String NOTEBOOKS_LAST_MODIFIED = "last_modified";
    protected static final String NOTEBOOKS_WEB_URL = "web_url";
    protected static final String NOTEBOOKS_ROLES = "roles";

    private static final Logger logger = LoggerFactory.getLogger(OneNoteDataStore.class);

    @Override
    protected String getName() {
        return "OneNote";
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final String tenant = getTenant(paramMap);
        final String clientId = getClientId(paramMap);
        final String clientSecret = getClientSecret(paramMap);

        if (tenant.isEmpty() || clientId.isEmpty() || clientSecret.isEmpty()) {
            throw new DataStoreException("parameter '" + //
                    TENANT_PARAM + "', '" + //
                    CLIENT_ID_PARAM + "', '" + //
                    CLIENT_SECRET_PARAM + "' is required");
        }

        final String accessToken = getAccessToken(tenant, clientId, clientSecret);

        final IGraphServiceClient client = getClient(accessToken);
        try {
            storeSiteNotes(dataConfig, callback, paramMap, scriptMap, defaultDataMap, client);
            storeUsersNotes(dataConfig, callback, paramMap, scriptMap, defaultDataMap, client);
            storeGroupsNotes(dataConfig, callback, paramMap, scriptMap, defaultDataMap, client);
        } finally {
            client.shutdown();
        }
    }

    protected void storeSiteNotes(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        try {
            final Site root = client.sites("root").buildRequest().get();
            getNotebooks(client.sites(root.id).onenote(), notebook -> {
                processNotebook(dataConfig, callback, paramMap, scriptMap, defaultDataMap, client.sites(root.id).onenote(), notebook, null);
            });
        } catch (ClientException e) {
            logger.warn("Failed to process site notes.", e);
        }
    }

    protected void storeUsersNotes(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        try {
            getLicensedUsers(client, user -> {
                final List<String> roles = getUserRoles(user);
                try {
                    getNotebooks(client.users(user.id).onenote(), notebook -> {
                        processNotebook(dataConfig, callback, paramMap, scriptMap, defaultDataMap, client.users(user.id).onenote(),
                                notebook, roles);
                    });
                } catch (final GraphServiceException e) {
                    logger.warn("Failed to store " + user.displayName + "'s Notebooks.", e);
                }
            });
        } catch (Exception e) {
            logger.warn("Failed to process user notes.", e);
        }
    }

    protected void storeGroupsNotes(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        try {
            getOffice365Groups(client, group -> {
                final List<String> roles = getGroupRoles(group);
                getNotebooks(client.groups(group.id).onenote(), notebook -> {
                    processNotebook(dataConfig, callback, paramMap, scriptMap, defaultDataMap, client.groups(group.id).onenote(), notebook,
                            roles);
                });
            });
        } catch (Exception e) {
            logger.warn("Failed to process group notes.", e);
        }
    }

    protected void processNotebook(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IOnenoteRequestBuilder builder,
            final Notebook notebook, final List<String> roles) {
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
        final Map<String, Object> notebooksMap = new HashMap<>();

        try {
            final String url = notebook.links.oneNoteWebUrl.href;
            logger.info("Crawling URL: " + url);

            final String contents = getNotebookContents(builder, notebook);
            final long size = contents != null ? contents.length() : 0L;
            notebooksMap.put(NOTEBOOKS_NAME, notebook.displayName);
            notebooksMap.put(NOTEBOOKS_CONTENTS, contents);
            notebooksMap.put(NOTEBOOKS_SIZE, size);
            notebooksMap.put(NOTEBOOKS_CREATED, notebook.createdDateTime.getTime());
            notebooksMap.put(NOTEBOOKS_LAST_MODIFIED, notebook.lastModifiedDateTime.getTime());
            notebooksMap.put(NOTEBOOKS_WEB_URL, url);
            notebooksMap.put(NOTEBOOKS_ROLES, roles);
            resultMap.put(NOTEBOOKS, notebooksMap);
            if (logger.isDebugEnabled()) {
                logger.debug("notebooksMap: {}", notebooksMap);
            }

            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("dataMap: {}", dataMap);
            }
            callback.store(paramMap, dataMap);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : " + dataMap, e);

            Throwable target = e;
            if (target instanceof MultipleCrawlingAccessException) {
                final Throwable[] causes = ((MultipleCrawlingAccessException) target).getCauses();
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
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : " + dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), notebook.displayName, t);
        }
    }

    protected void getNotebooks(final IOnenoteRequestBuilder builder, final Consumer<Notebook> consumer) {
        try {
            INotebookCollectionPage page = builder.notebooks().buildRequest().get();
            while (page.getNextPage() != null) {
                try {
                    page = page.getNextPage().buildRequest().get();
                    page.getCurrentPage().forEach(note -> consumer.accept(note));
                } catch (ClientException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to process a next page.", e);
                    }
                }
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

    protected List<OnenoteSection> getSections(final INotebookRequestBuilder builder) {
        IOnenoteSectionCollectionPage page = builder.sections().buildRequest().get();
        final List<OnenoteSection> sections = new ArrayList<>(page.getCurrentPage());
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            sections.addAll(page.getCurrentPage());
        }
        return sections;
    }

    protected List<OnenotePage> getPages(final IOnenoteSectionRequestBuilder builder) {
        IOnenotePageCollectionPage page = builder.pages().buildRequest().get();
        final List<OnenotePage> pages = new ArrayList<>(page.getCurrentPage());
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            pages.addAll(page.getCurrentPage());
        }
        return pages;
    }

    protected String getNotebookContents(final IOnenoteRequestBuilder builder, final Notebook notebook) {
        final List<OnenoteSection> sections = getSections(builder.notebooks(notebook.id));
        Collections.reverse(sections);
        return sections.stream().map(section -> getSectionContents(builder, section)).collect(Collectors.joining("\n"));
    }

    protected String getSectionContents(final IOnenoteRequestBuilder builder, final OnenoteSection section) {
        final StringBuilder sb = new StringBuilder();
        sb.append(section.displayName).append('\n');
        final List<OnenotePage> pages = getPages(builder.sections(section.id));
        Collections.reverse(pages);
        sb.append(pages.stream().map(page -> getPageContents(builder, page)).collect(Collectors.joining("\n")));
        return sb.toString();
    }

    protected String getPageContents(final IOnenoteRequestBuilder builder, final OnenotePage page) {
        final StringBuilder sb = new StringBuilder();
        sb.append(page.title).append('\n');
        try (final InputStream in = builder.pages(page.id).content().buildRequest().get()) {
            final TikaExtractor extractor = ComponentUtil.getComponent("tikaExtractor");
            sb.append(extractor.getText(in, null).getContent());
        } catch (final IOException e) {
            logger.warn("Failed to get contents of Page: " + page.title, e);
        }
        return sb.toString();
    }

}
