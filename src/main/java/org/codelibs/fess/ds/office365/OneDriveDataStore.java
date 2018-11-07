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

import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.IDriveItemCollectionPage;
import com.microsoft.graph.requests.extensions.IDriveRequestBuilder;
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
import java.util.concurrent.ExecutionException;

import static org.codelibs.fess.ds.office365.Office365Helper.*;

public class OneDriveDataStore extends AbstractDataStore {

    // scripts
    private static final String FILES = "files";
    private static final String FILES_NAME = "name";
    private static final String FILES_DESCRIPTION = "description";
    private static final String FILES_CONTENTS = "contents";
    private static final String FILES_MIMETYPE = "mimetype";
    private static final String FILES_CREATED = "created";
    private static final String FILES_LAST_MODIFIED = "last_modified";
    private static final String FILES_SIZE = "size";
    private static final String FILES_WEB_URL = "web_url";

    private static final Logger logger = LoggerFactory.getLogger(OneDriveDataStore.class);

    protected String getName() {
        return "OneDrive";
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

        final IGraphServiceClient client = getClient(accessToken, logger);
        storeSharedDocumentsDrive(callback, paramMap, scriptMap, defaultDataMap, client);
        storeUsersDrive(callback, paramMap, scriptMap, defaultDataMap, client);
        storeGroupsDrive(callback, paramMap, scriptMap, defaultDataMap, client);

    }

    protected void storeSharedDocumentsDrive(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        getDriveItemsInDrive(client.drive()).forEach(item -> {
            processDriveItem(callback, paramMap, scriptMap, defaultDataMap, client.drive(), item);
        });
    }

    protected void storeUsersDrive(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        getLicensedUsers(client).forEach(user -> {
            getDriveItemsInDrive(client.users(user.id).drive()).forEach(item -> {
                processDriveItem(callback, paramMap, scriptMap, defaultDataMap, client.users(user.id).drive(), item);
            });
        });
    }

    protected void storeGroupsDrive(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        getOffice365Groups(client).forEach(group -> {
            getDriveItemsInDrive(client.groups(group.id).drive()).forEach(item -> {
                processDriveItem(callback, paramMap, scriptMap, defaultDataMap, client.groups(group.id).drive(), item);
            });
        });
    }

    protected void processDriveItem(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IDriveRequestBuilder builder,
            final DriveItem item) {
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
        final Map<String, Object> filesMap = new HashMap<>();

        filesMap.put(FILES_NAME, item.name);
        filesMap.put(FILES_DESCRIPTION, item.description != null ? item.description : "");
        filesMap.put(FILES_CONTENTS, getDriveItemContents(builder, item));
        filesMap.put(FILES_MIMETYPE, item.file != null ? item.file.mimeType : null);
        filesMap.put(FILES_CREATED, item.createdDateTime.getTime());
        filesMap.put(FILES_LAST_MODIFIED, item.lastModifiedDateTime.getTime());
        filesMap.put(FILES_SIZE, item.size);
        filesMap.put(FILES_WEB_URL, item.webUrl);
        resultMap.put(FILES, filesMap);

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

    protected static String getDriveItemContents(final IDriveRequestBuilder builder, final DriveItem item) {
        if (item.file != null) {
            final String mimeType = item.file.mimeType;
            if (mimeType.matches("application/vnd\\.openxmlformats-officedocument\\.(.*)") || mimeType.matches("text/.*")) {
                try (final InputStream in = builder.items(item.id).content().buildRequest().get()) {
                    final TikaExtractor extractor = ComponentUtil.getComponent("tikaExtractor");
                    return extractor.getText(in, null).getContent();
                } catch (final IOException e) {
                    logger.warn("Failed to get contents of DriveItem: " + item.name, e);
                }
            }
        }
        return "";
    }

    protected static List<DriveItem> getDriveItemsInDrive(final IDriveRequestBuilder builder) {
        return getDriveItemChildren(builder, null);
    }

    private static List<DriveItem> getDriveItemChildren(final IDriveRequestBuilder builder, final DriveItem root) {
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

}
