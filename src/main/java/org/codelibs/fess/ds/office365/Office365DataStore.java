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

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.graph.logger.DefaultLogger;
import com.microsoft.graph.models.extensions.Drive;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.User;
import com.microsoft.graph.options.QueryOption;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import com.microsoft.graph.requests.extensions.IDriveItemCollectionPage;
import com.microsoft.graph.requests.extensions.IGroupCollectionPage;
import com.microsoft.graph.requests.extensions.IUserCollectionPage;
import org.apache.commons.io.IOUtils;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class Office365DataStore extends AbstractDataStore {

    // parameters
    private static final String TENANT_PARAM = "tenant";
    private static final String CLIENT_ID_PARAM = "client_id";
    private static final String CLIENT_SECRET_PARAM = "client_secret";

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

    private static final Logger logger = LoggerFactory.getLogger(Office365DataStore.class);

    protected String getName() {
        return "Office365";
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
        storeSharedDocumentsDrive(callback, paramMap, scriptMap, defaultDataMap, client);
        storeUsersDrive(callback, paramMap, scriptMap, defaultDataMap, client);
        storeGroupsDrive(callback, paramMap, scriptMap, defaultDataMap, client);

    }

    protected void storeSharedDocumentsDrive(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        final Drive drive = client.drive().buildRequest().get();
        logger.debug("Start to store " + drive.name + "'s Drive");
        getDriveItemsInDrive(client, drive.id).forEach(item -> {
            processDriveItem(callback, paramMap, scriptMap, defaultDataMap, client, drive.id, item);
        });
        logger.debug("----------");
    }

    protected void storeUsersDrive(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        IUserCollectionPage page = client.users().buildRequest().get();
        while (true) {
            page.getCurrentPage().forEach(u -> {
                final User user = client.users(u.id).buildRequest(Collections.singletonList(new QueryOption("$select", "mySite"))).get();
                if (user.mySite != null) {
                    final Drive drive = client.users(u.id).drive().buildRequest().get();
                    logger.debug("Start to store " + u.displayName + "'s Drive");
                    getDriveItemsInDrive(client, drive.id).forEach(item -> {
                        processDriveItem(callback, paramMap, scriptMap, defaultDataMap, client, drive.id, item);
                    });
                    logger.debug("----------");
                }
            });
            if (page.getNextPage() == null) {
                break;
            }
            page = page.getNextPage().buildRequest().get();
        }
    }

    protected void storeGroupsDrive(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client) {
        IGroupCollectionPage page =
                client.groups().buildRequest(Collections.singletonList(new QueryOption("$filter", "groupTypes/any(c:c eq 'Unified')")))
                        .get();
        while (true) {
            page.getCurrentPage().forEach(g -> {
                final Drive drive = client.groups(g.id).drive().buildRequest().get();
                logger.debug("Start to store " + g.displayName + "'s Drive");
                getDriveItemsInDrive(client, drive.id).forEach(item -> {
                    processDriveItem(callback, paramMap, scriptMap, defaultDataMap, client, drive.id, item);
                });
                logger.debug("----------");
            });
            if (page.getNextPage() == null) {
                break;
            }
            page = page.getNextPage().buildRequest().get();
        }
    }

    protected void processDriveItem(final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final IGraphServiceClient client,
            final String driveId, final DriveItem item) {
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
        final Map<String, Object> filesMap = new HashMap<>();

        filesMap.put(FILES_NAME, item.name);
        filesMap.put(FILES_DESCRIPTION, item.description != null ? item.description : "");
        filesMap.put(FILES_CONTENTS, getDriveItemContents(client, driveId, item));
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

    protected static String getDriveItemContents(final IGraphServiceClient client, final String driveId, final DriveItem item) {
        if (item.file != null) {
            if (item.file.mimeType.matches("text/.*")) {
                try {
                    return IOUtils.toString(client.drives(driveId).items(item.id).content().buildRequest().get(), StandardCharsets.UTF_8);
                } catch (final Exception e) {
                    logger.warn("Failed to get contents of " + item.name, e);
                }
            }
        }
        return "";
    }

    protected static List<DriveItem> getDriveItemsInDrive(final IGraphServiceClient client, final String driveId) {
        return getDriveItemsChildren(client, driveId, null);
    }

    private static List<DriveItem> getDriveItemsChildren(final IGraphServiceClient client, final String driveId, final DriveItem root) {
        final List<DriveItem> items = new ArrayList<>();
        IDriveItemCollectionPage page;
        if (root == null) {
            page = client.drives(driveId).root().children().buildRequest().get();
        } else {
            items.add(root);
            if (root.folder == null) {
                return items;
            }
            page = client.drives(driveId).items(root.id).children().buildRequest().get();
        }
        while (true) {
            page.getCurrentPage().forEach(i -> {
                items.addAll(getDriveItemsChildren(client, driveId, i));
            });
            if (page.getNextPage() == null) {
                break;
            }
            page = page.getNextPage().buildRequest().get();
        }
        return items;
    }

    protected static String getAccessToken(final String tenant, final String clientId, final String clientSecret)
            throws MalformedURLException, ExecutionException, InterruptedException {
        final AuthenticationContext context =
                new AuthenticationContext("https://login.microsoftonline.com/" + tenant + "/", false, Executors.newFixedThreadPool(1));
        final AuthenticationResult result =
                context.acquireToken("https://graph.microsoft.com", new ClientCredential(clientId, clientSecret), null).get();
        return result.getAccessToken();
    }

    protected static IGraphServiceClient getClient(final String accessToken) {
        return GraphServiceClient.builder() //
                .authenticationProvider(request -> request.addHeader("Authorization", "Bearer " + accessToken)) //
                .logger(new DefaultLogger() {
                    @Override
                    public void logDebug(String message) {
                        switch (getLoggingLevel()) {
                        case DEBUG:
                            logger.debug(message);
                        case ERROR:
                        }
                    }

                    @Override
                    public void logError(String message, Throwable throwable) {
                        switch (getLoggingLevel()) {
                        case DEBUG:
                        case ERROR:
                            logger.error(message, throwable);
                        }
                    }
                }).buildClient();
    }

}
