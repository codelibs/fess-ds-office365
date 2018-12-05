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

import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.extensions.*;
import com.microsoft.graph.requests.extensions.*;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.*;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.codelibs.fess.ds.office365.Office365Helper.*;

public class OneNoteDataStore extends AbstractDataStore {

    // scripts
    private static final String NOTEBOOKS = "notebooks";
    private static final String NOTEBOOKS_NAME = "name";
    private static final String NOTEBOOKS_CONTENTS = "contents";
    private static final String NOTEBOOKS_CREATED = "created";
    private static final String NOTEBOOKS_LAST_MODIFIED = "last_modified";
    private static final String NOTEBOOKS_WEB_URL = "web_url";
    private static final String NOTEBOOKS_ROLES = "roles";

    private static final Logger logger = LoggerFactory.getLogger(OneNoteDataStore.class);

    protected String getName() {
        return "OneNote";
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final String tenant = paramMap.getOrDefault(TENANT_PARAM, "");
        final String clientId = paramMap.getOrDefault(CLIENT_ID_PARAM, "");
        final String clientSecret = paramMap.getOrDefault(CLIENT_SECRET_PARAM, "");

        if (tenant.isEmpty() || clientId.isEmpty() || clientSecret.isEmpty()) {
            logger.warn("parameter '" + //
                    TENANT_PARAM + "', '" + //
                    CLIENT_ID_PARAM + "', '" + //
                    CLIENT_SECRET_PARAM + "' is required");
            return;
        }

        final String accessToken;
        try {
            accessToken = getAccessToken(tenant, clientId, clientSecret);
        } catch (final MalformedURLException | ExecutionException | InterruptedException e) {
            logger.warn("failed to get access token.", e);
            return;
        }

        final IGraphServiceClient client = getClient(accessToken);
        storeSiteNotes(callback, paramMap, scriptMap, defaultDataMap, client);
        storeUsersNotes(callback, paramMap, scriptMap, defaultDataMap, client);
        storeGroupsNotes(callback, paramMap, scriptMap, defaultDataMap, client);

    }

    protected void storeSiteNotes(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        final Site root = client.sites("root").buildRequest().get();
        getNotebooks(client.sites(root.id).onenote()).forEach(notebook -> {
            processNotebook(callback, paramMap, scriptMap, defaultDataMap, client.sites(root.id).onenote(), notebook, null);
        });
    }

    protected void storeUsersNotes(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        getLicensedUsers(client).forEach(user -> {
            final List<String> roles = getUserRoles(user);
            try {
                getNotebooks(client.users(user.id).onenote()).forEach(notebook -> {
                    processNotebook(callback, paramMap, scriptMap, defaultDataMap, client.users(user.id).onenote(), notebook, roles);
                });
            } catch (final GraphServiceException e) {
                logger.warn("Failed to store " + user.displayName + "'s Notebooks: " + e.getMessage());
                logger.debug("Details:", e);
            }
        });
    }

    protected void storeGroupsNotes(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        getOffice365Groups(client).forEach(group -> {
            final List<String> roles = getGroupRoles(group);
            getNotebooks(client.groups(group.id).onenote()).forEach(notebook -> {
                processNotebook(callback, paramMap, scriptMap, defaultDataMap, client.groups(group.id).onenote(), notebook, roles);
            });
        });
    }

    protected void processNotebook(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IOnenoteRequestBuilder builder,
            final Notebook notebook, final List<String> roles) {
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
        final Map<String, Object> notebooksMap = new HashMap<>();

        notebooksMap.put(NOTEBOOKS_NAME, notebook.displayName);
        notebooksMap.put(NOTEBOOKS_CONTENTS, getNotebookContents(builder, notebook));
        notebooksMap.put(NOTEBOOKS_CREATED, notebook.createdDateTime.getTime());
        notebooksMap.put(NOTEBOOKS_LAST_MODIFIED, notebook.lastModifiedDateTime.getTime());
        notebooksMap.put(NOTEBOOKS_WEB_URL, notebook.links.oneNoteWebUrl.href);
        notebooksMap.put(NOTEBOOKS_ROLES, roles);
        resultMap.put(NOTEBOOKS, notebooksMap);

        try {
            for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                final Object convertValue = convertValue(entry.getValue(), resultMap);
                if (convertValue != null) {
                    dataMap.put(entry.getKey(), convertValue);
                }
            }
            callback.store(paramMap, dataMap);
        } catch (final CrawlingAccessException e) {
            logger.warn("Crawling Access Exception at : " + dataMap, e);
        }
    }

    protected static List<Notebook> getNotebooks(final IOnenoteRequestBuilder builder) {
        INotebookCollectionPage page = builder.notebooks().buildRequest().get();
        final List<Notebook> notebooks = new ArrayList<>(page.getCurrentPage());
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            notebooks.addAll(page.getCurrentPage());
        }
        return notebooks;
    }

    protected static List<OnenoteSection> getSections(final INotebookRequestBuilder builder) {
        IOnenoteSectionCollectionPage page = builder.sections().buildRequest().get();
        final List<OnenoteSection> sections = new ArrayList<>(page.getCurrentPage());
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            sections.addAll(page.getCurrentPage());
        }
        return sections;
    }

    protected static List<OnenotePage> getPages(final IOnenoteSectionRequestBuilder builder) {
        IOnenotePageCollectionPage page = builder.pages().buildRequest().get();
        final List<OnenotePage> pages = new ArrayList<>(page.getCurrentPage());
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            pages.addAll(page.getCurrentPage());
        }
        return pages;
    }

    protected static String getNotebookContents(final IOnenoteRequestBuilder builder, final Notebook notebook) {
        final List<OnenoteSection> sections = getSections(builder.notebooks(notebook.id));
        Collections.reverse(sections);
        return sections.stream().map(section -> getSectionContents(builder, section)).collect(Collectors.joining("\n"));
    }

    protected static String getSectionContents(final IOnenoteRequestBuilder builder, final OnenoteSection section) {
        final StringBuilder sb = new StringBuilder();
        sb.append(section.displayName).append("\n");
        final List<OnenotePage> pages = getPages(builder.sections(section.id));
        Collections.reverse(pages);
        sb.append(pages.stream().map(page -> getPageContents(builder, page)).collect(Collectors.joining("\n")));
        return sb.toString();
    }

    protected static String getPageContents(final IOnenoteRequestBuilder builder, final OnenotePage page) {
        final StringBuilder sb = new StringBuilder();
        sb.append(page.title).append("\n");
        try (final InputStream in = builder.pages(page.id).content().buildRequest().get()) {
            final TikaExtractor extractor = ComponentUtil.getComponent("tikaExtractor");
            sb.append(extractor.getText(in, null).getContent());
        } catch (final IOException e) {
            logger.warn("Failed to get contents of Page: " + page.title, e);
        }
        return sb.toString();
    }

}
