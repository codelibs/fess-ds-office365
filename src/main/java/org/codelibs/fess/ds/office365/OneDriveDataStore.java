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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.IDriveItemCollectionPage;
import com.microsoft.graph.requests.extensions.IDriveRequestBuilder;

public class OneDriveDataStore extends Office365DataStore {

    protected static final String IGNORE_FOLDER = "ignore_folder";

    // scripts
    protected static final String FILES = "files";
    protected static final String FILES_NAME = "name";
    protected static final String FILES_DESCRIPTION = "description";
    protected static final String FILES_CONTENTS = "contents";
    protected static final String FILES_MIMETYPE = "mimetype";
    protected static final String FILES_FILETYPE = "filetype";
    protected static final String FILES_CREATED = "created";
    protected static final String FILES_LAST_MODIFIED = "last_modified";
    protected static final String FILES_SIZE = "size";
    protected static final String FILES_WEB_URL = "web_url";
    protected static final String FILES_ROLES = "roles";

    private static final Logger logger = LoggerFactory.getLogger(OneDriveDataStore.class);

    protected String[] supportedMimeTypes = new String[] { "application/vnd\\.openxmlformats-officedocument\\.(.*)", "text/.*" };

    protected String extractorName = "tikaExtractor";

    @Override
    protected String getName() {
        return "OneDrive";
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
            storeSharedDocumentsDrive(callback, paramMap, scriptMap, defaultDataMap, client);
            storeUsersDrive(callback, paramMap, scriptMap, defaultDataMap, client);
            storeGroupsDrive(callback, paramMap, scriptMap, defaultDataMap, client);
        } finally {
            client.shutdown();
        }
    }

    protected boolean isIgnoreFolder(final Map<String, String> paramMap) {
        return paramMap.getOrDefault(IGNORE_FOLDER, Constants.TRUE).equalsIgnoreCase(Constants.TRUE);
    }

    protected void storeSharedDocumentsDrive(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        getDriveItemsInDrive(client.drive()).forEach(item -> {
            processDriveItem(callback, paramMap, scriptMap, defaultDataMap, client.drive(), item, null);
        });
    }

    protected void storeUsersDrive(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        getLicensedUsers(client).forEach(user -> {
            final List<String> roles = getUserRoles(user);
            try {
                getDriveItemsInDrive(client.users(user.id).drive()).forEach(item -> {
                    processDriveItem(callback, paramMap, scriptMap, defaultDataMap, client.users(user.id).drive(), item, roles);
                });
            } catch (final GraphServiceException e) {
                logger.warn("Failed to store " + user.displayName + "'s Drive, ", e);
            }
        });
    }

    protected void storeGroupsDrive(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        getOffice365Groups(client).forEach(group -> {
            final List<String> roles = getGroupRoles(group);
            getDriveItemsInDrive(client.groups(group.id).drive()).forEach(item -> {
                processDriveItem(callback, paramMap, scriptMap, defaultDataMap, client.groups(group.id).drive(), item, roles);
            });
        });
    }

    protected void processDriveItem(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IDriveRequestBuilder builder,
            final DriveItem item, final List<String> roles) {
        final String mimetype = item.file != null ? item.file.mimeType : null;
        if (isIgnoreFolder(paramMap) && mimetype == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Ignore item: {}", item.webUrl);
            }
            return;
        }

        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
        final Map<String, Object> filesMap = new HashMap<>();

        final String filetype = ComponentUtil.getFileTypeHelper().get(mimetype);
        filesMap.put(FILES_NAME, item.name);
        filesMap.put(FILES_DESCRIPTION, item.description != null ? item.description : StringUtil.EMPTY);
        filesMap.put(FILES_CONTENTS, getDriveItemContents(builder, item));
        filesMap.put(FILES_MIMETYPE, mimetype);
        filesMap.put(FILES_FILETYPE, filetype);
        filesMap.put(FILES_CREATED, item.createdDateTime.getTime());
        filesMap.put(FILES_LAST_MODIFIED, item.lastModifiedDateTime.getTime());
        filesMap.put(FILES_SIZE, item.size);
        filesMap.put(FILES_WEB_URL, item.webUrl);
        filesMap.put(FILES_ROLES, roles);
        resultMap.put(FILES, filesMap);
        if (logger.isDebugEnabled()) {
            logger.debug("filesMap: {}", filesMap);
        }

        try {
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
        }
    }

    protected String getDriveItemContents(final IDriveRequestBuilder builder, final DriveItem item) {
        if (item.file != null) {
            if (isSupportedMimeType(item.file.mimeType)) {
                try (final InputStream in = builder.items(item.id).content().buildRequest().get()) {
                    final TikaExtractor extractor = ComponentUtil.getComponent(extractorName);
                    return extractor.getText(in, null).getContent();
                } catch (final IOException e) {
                    logger.warn("Failed to get contents of DriveItem: " + item.name, e);
                }
            }
        }
        return StringUtil.EMPTY;
    }

    protected boolean isSupportedMimeType(final String mimeType) {
        return Stream.of(supportedMimeTypes).anyMatch(s -> mimeType.matches(s));
    }

    protected List<DriveItem> getDriveItemsInDrive(final IDriveRequestBuilder builder) {
        return getDriveItemChildren(builder, null);
    }

    protected List<DriveItem> getDriveItemChildren(final IDriveRequestBuilder builder, final DriveItem root) {
        final List<DriveItem> items = new ArrayList<>();
        IDriveItemCollectionPage page;
        if (root == null) {
            page = builder.root().children().buildRequest().get();
        } else {
            items.add(root);
            if (root.folder == null) {
                return items;
            }
            page = builder.items(root.id).children().buildRequest().get();
        }
        page.getCurrentPage().forEach(i -> items.addAll(getDriveItemChildren(builder, i)));
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(i -> items.addAll(getDriveItemChildren(builder, i)));
        }
        return items;
    }

    public void setSupportedMimeTypes(String[] supportedMimeTypes) {
        this.supportedMimeTypes = supportedMimeTypes;
    }

    public void setExtractorName(String extractorName) {
        this.extractorName = extractorName;
    }

}
