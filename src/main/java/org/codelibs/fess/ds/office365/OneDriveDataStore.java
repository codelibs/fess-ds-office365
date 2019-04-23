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

import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.Hashes;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.requests.extensions.IDriveItemCollectionPage;
import com.microsoft.graph.requests.extensions.IDriveRequestBuilder;

public class OneDriveDataStore extends Office365DataStore {

    private static final Logger logger = LoggerFactory.getLogger(OneDriveDataStore.class);

    protected static final long DEFAULT_MAX_SIZE = 10000000L; // 10m

    protected static final String CURRENT_CRAWLER = "current_crawler";
    protected static final String CRAWLER_TYPE_GROUP = "group";
    protected static final String CRAWLER_TYPE_USER = "user";
    protected static final String CRAWLER_TYPE_SHARED = "shared";

    // parameters
    protected static final String MAX_SIZE = "max_size";
    protected static final String IGNORE_FOLDER = "ignore_folder";
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    protected static final String INCLUDE_PATTERN = "include_pattern";
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    protected static final String URL_FILTER = "url_filter";

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
    protected static final String FILES_URL = "url";
    protected static final String FILES_ROLES = "roles";
    protected static final String FILES_CTAG = "ctag";
    protected static final String FILES_ETAG = "etag";
    protected static final String FILES_ID = "id";
    protected static final String FILES_WEBDAV_URL = "webdav_url";
    protected static final String FILES_LOCATION = "location";
    protected static final String FILES_CREATEDBY_APPLICATION = "createdby_application";
    protected static final String FILES_CREATEDBY_DEVICE = "createdby_device";
    protected static final String FILES_CREATEDBY_USER = "createdby_user";
    protected static final String FILES_DELETED = "deleted";
    protected static final String FILES_HASHES = "hashes";
    protected static final String FILES_LAST_MODIFIEDBY_APPLICATION = "last_modifiedby_application";
    protected static final String FILES_LAST_MODIFIEDBY_DEVICE = "last_modifiedby_device";
    protected static final String FILES_LAST_MODIFIEDBY_USER = "last_modifiedby_user";
    protected static final String FILES_IMAGE = "image";
    protected static final String FILES_PARENT = "parent";
    protected static final String FILES_PARENT_ID = "parent_id";
    protected static final String FILES_PARENT_NAME = "parent_name";
    protected static final String FILES_PARENT_PATH = "parent_path";
    protected static final String FILES_PHOTO = "photo";
    protected static final String FILES_PUBLICATION = "publication";
    protected static final String FILES_SEARCH_RESULT = "search_result";
    protected static final String FILES_SPECIAL_FOLDER = "special_folder";
    protected static final String FILES_VIDEO = "video";

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

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(MAX_SIZE, getMaxSize(paramMap));
        configMap.put(IGNORE_FOLDER, isIgnoreFolder(paramMap));
        configMap.put(SUPPORTED_MIMETYPES, getSupportedMimeTypes(paramMap));
        configMap.put(URL_FILTER, getUrlFilter(paramMap));
        if (logger.isDebugEnabled()) {
            logger.debug("configMap: {}", configMap);
        }

        try (final Office365Client client = new Office365Client(tenant, clientId, clientSecret, getAccessTimeout(paramMap))) {
            if (isSharedDocumentsDriveCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("crawling shared documents drive.");
                }
                configMap.put(CURRENT_CRAWLER, CRAWLER_TYPE_SHARED);
                storeSharedDocumentsDrive(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client);
            }
            if (isUserDriveCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("crawling user drive.");
                }
                configMap.put(CURRENT_CRAWLER, CRAWLER_TYPE_USER);
                storeUsersDrive(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client);
            }
            if (isGroupDriveCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("crawling group drive.");
                }
                configMap.put(CURRENT_CRAWLER, CRAWLER_TYPE_GROUP);
                storeGroupsDrive(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client);
            }
        }
    }

    protected UrlFilter getUrlFilter(final Map<String, String> paramMap) {
        final UrlFilter urlFilter = ComponentUtil.getComponent(UrlFilter.class);
        final String include = paramMap.get(INCLUDE_PATTERN);
        if (StringUtil.isNotBlank(include)) {
            urlFilter.addInclude(include);
        }
        final String exclude = paramMap.get(EXCLUDE_PATTERN);
        if (StringUtil.isNotBlank(exclude)) {
            urlFilter.addExclude(exclude);
        }
        urlFilter.init(paramMap.get(Constants.CRAWLING_INFO_ID));
        if (logger.isDebugEnabled()) {
            logger.debug("urlFilter: " + urlFilter);
        }
        return urlFilter;
    }

    protected boolean isSharedDocumentsDriveCrawler(final Map<String, String> paramMap) {
        return paramMap.getOrDefault("shared_documents_drive_crawler", Constants.TRUE).equalsIgnoreCase(Constants.TRUE);
    }

    protected boolean isUserDriveCrawler(final Map<String, String> paramMap) {
        return paramMap.getOrDefault("user_drive_crawler", Constants.TRUE).equalsIgnoreCase(Constants.TRUE);
    }

    protected boolean isGroupDriveCrawler(final Map<String, String> paramMap) {
        return paramMap.getOrDefault("group_drive_crawler", Constants.TRUE).equalsIgnoreCase(Constants.TRUE);
    }

    protected boolean isIgnoreFolder(final Map<String, String> paramMap) {
        return paramMap.getOrDefault(IGNORE_FOLDER, Constants.TRUE).equalsIgnoreCase(Constants.TRUE);
    }

    protected long getMaxSize(final Map<String, String> paramMap) {
        final String value = paramMap.get(MAX_SIZE);
        try {
            return StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_MAX_SIZE;
        } catch (final NumberFormatException e) {
            return DEFAULT_MAX_SIZE;
        }
    }

    protected String[] getSupportedMimeTypes(final Map<String, String> paramMap) {
        return StreamUtil.split(paramMap.getOrDefault(SUPPORTED_MIMETYPES, ".*"), ",")
                .get(stream -> stream.map(s -> s.trim()).toArray(n -> new String[n]));
    }

    protected void storeSharedDocumentsDrive(final DataConfig dataConfig, final IndexUpdateCallback callback,
            final Map<String, Object> configMap, final Map<String, String> paramMap, final Map<String, String> scriptMap,
            final Map<String, Object> defaultDataMap, final Office365Client client) {
        try {
            getDriveItemsInDrive(client, c -> c.drive(), item -> processDriveItem(dataConfig, callback, configMap, paramMap, scriptMap,
                    defaultDataMap, client, c -> c.drive(), item, null));
        } catch (final Exception e) {
            logger.warn("Failed to process shared documents drive.", e);
        }
    }

    protected void storeUsersDrive(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final Map<String, String> paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Office365Client client) {
        try {
            getLicensedUsers(client, user -> {
                final List<String> roles = getUserRoles(user);
                try {
                    getDriveItemsInDrive(client, c -> c.users(user.id).drive(), item -> processDriveItem(dataConfig, callback, configMap,
                            paramMap, scriptMap, defaultDataMap, client, c -> c.users(user.id).drive(), item, roles));
                } catch (final GraphServiceException e) {
                    logger.warn("Failed to store " + user.displayName + "'s Drive, ", e);
                }
            });
        } catch (final Exception e) {
            logger.warn("Failed to process user drive.", e);
        }
    }

    protected void storeGroupsDrive(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final Map<String, String> paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Office365Client client) {
        try {
            getOffice365Groups(client, group -> {
                final List<String> roles = getGroupRoles(group);
                getDriveItemsInDrive(client, c -> c.groups(group.id).drive(), //
                        item -> processDriveItem(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, client,
                                c -> c.groups(group.id).drive(), item, roles));
            });
        } catch (final Exception e) {
            logger.warn("Failed to process group drive.", e);
        }
    }

    protected void processDriveItem(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final Map<String, String> paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Office365Client client, final Function<IGraphServiceClient, IDriveRequestBuilder> builder, final DriveItem item,
            final List<String> roles) {
        final String mimetype;
        final Hashes hashes;
        if (item.file != null) {
            mimetype = item.file.mimeType;
            hashes = item.file.hashes;
        } else {
            mimetype = null;
            hashes = null;
        }
        if (((Boolean) configMap.get(IGNORE_FOLDER)).booleanValue() && mimetype == null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Ignore item: {}", item.webUrl);
            }
            return;
        }

        final String url = getUrl(configMap, paramMap, item);
        final UrlFilter urlFilter = (UrlFilter) configMap.get(URL_FILTER);
        if (urlFilter != null && !urlFilter.match(url)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Not matched: " + url);
            }
            return;
        }

        logger.info("Crawling URL: " + url);

        final String[] supportedMimeTypes = (String[]) configMap.get(SUPPORTED_MIMETYPES);

        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
        final Map<String, Object> filesMap = new HashMap<>();

        try {
            if (item.size.longValue() > ((Long) configMap.get(MAX_SIZE)).longValue()) {
                throw new MaxLengthExceededException("The content length (" + item.size + " byte) is over " + configMap.get(MAX_SIZE)
                        + " byte. The url is " + item.webUrl);
            }

            final String filetype = ComponentUtil.getFileTypeHelper().get(mimetype);
            filesMap.put(FILES_NAME, item.name);
            filesMap.put(FILES_DESCRIPTION, item.description != null ? item.description : StringUtil.EMPTY);
            filesMap.put(FILES_CONTENTS, getDriveItemContents(client, builder, item, supportedMimeTypes));
            filesMap.put(FILES_MIMETYPE, mimetype);
            filesMap.put(FILES_FILETYPE, filetype);
            filesMap.put(FILES_CREATED, item.createdDateTime.getTime());
            filesMap.put(FILES_LAST_MODIFIED, item.lastModifiedDateTime.getTime());
            filesMap.put(FILES_SIZE, item.size);
            filesMap.put(FILES_WEB_URL, item.webUrl);
            filesMap.put(FILES_URL, url);
            filesMap.put(FILES_ROLES, roles);
            filesMap.put(FILES_CTAG, item.cTag);
            filesMap.put(FILES_ETAG, item.eTag);
            filesMap.put(FILES_ID, item.id);
            filesMap.put(FILES_WEBDAV_URL, item.webDavUrl);
            filesMap.put(FILES_LOCATION, item.location);
            filesMap.put(FILES_CREATEDBY_APPLICATION, item.createdBy != null ? item.createdBy.application : null);
            filesMap.put(FILES_CREATEDBY_DEVICE, item.createdBy != null ? item.createdBy.device : null);
            filesMap.put(FILES_CREATEDBY_USER, item.createdBy != null ? item.createdBy.user : null);
            filesMap.put(FILES_DELETED, item.deleted);
            filesMap.put(FILES_HASHES, hashes);
            filesMap.put(FILES_LAST_MODIFIEDBY_APPLICATION, item.lastModifiedBy != null ? item.lastModifiedBy.application : null);
            filesMap.put(FILES_LAST_MODIFIEDBY_DEVICE, item.lastModifiedBy != null ? item.lastModifiedBy.device : null);
            filesMap.put(FILES_LAST_MODIFIEDBY_USER, item.lastModifiedBy != null ? item.lastModifiedBy.user : null);
            filesMap.put(FILES_IMAGE, item.image);
            filesMap.put(FILES_PARENT, item.parentReference);
            filesMap.put(FILES_PARENT_ID, item.parentReference != null ? item.parentReference.id : null);
            filesMap.put(FILES_PARENT_NAME, item.parentReference != null ? item.parentReference.name : null);
            filesMap.put(FILES_PARENT_PATH, item.parentReference != null ? item.parentReference.path : null);
            filesMap.put(FILES_PHOTO, item.photo);
            filesMap.put(FILES_PUBLICATION, item.publication);
            filesMap.put(FILES_SEARCH_RESULT, item.searchResult);
            filesMap.put(FILES_SPECIAL_FOLDER, item.specialFolder != null ? item.specialFolder.name : null);
            filesMap.put(FILES_VIDEO, item.video);

            resultMap.put(FILES, filesMap);
            if (logger.isDebugEnabled()) {
                logger.debug("filesMap: {}", filesMap);
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
            failureUrlService.store(dataConfig, errorName, item.webUrl, target);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : " + dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), item.webUrl, t);
        }
    }

    protected String getUrl(final Map<String, Object> configMap, final Map<String, String> paramMap, final DriveItem item) {
        if (item.webUrl == null) {
            return null;
        }
        if (!item.webUrl.contains("/_layouts/")) {
            return item.webUrl;
        }

        final String baseUrl = item.webUrl.substring(0, item.webUrl.indexOf("/_layouts/"));
        final String parentPath;
        if (item.parentReference != null && item.parentReference.path != null) {
            final String[] values = item.parentReference.path.split(":", 2);
            parentPath = values.length > 1 ? values[1] : "/";
        } else {
            parentPath = "/";
        }
        if (CRAWLER_TYPE_SHARED.equals(configMap.get(CURRENT_CRAWLER)) || CRAWLER_TYPE_GROUP.equals(configMap.get(CURRENT_CRAWLER))) {
            return baseUrl + "/Shared%20Documents" + parentPath + "/" + item.name;
        } else {
            return baseUrl + "/Documents" + parentPath + "/" + item.name;
        }
    }

    protected String getDriveItemContents(final Office365Client client, final Function<IGraphServiceClient, IDriveRequestBuilder> builder,
            final DriveItem item, final String[] supportedMimeTypes) {
        if (item.file != null) {
            final String mimeType = item.file.mimeType;
            if (Stream.of(supportedMimeTypes).anyMatch(s -> mimeType.matches(s))) {
                try (final InputStream in = client.getContent(builder, item.id)) {
                    final TikaExtractor extractor = ComponentUtil.getComponent(extractorName);
                    return extractor.getText(in, null).getContent();
                } catch (final Exception e) {
                    throw new DataStoreCrawlingException(item.webUrl, "Failed to get contents of DriveItem: " + item.name, e);
                }
            }
        }
        return StringUtil.EMPTY;
    }

    protected void getDriveItemsInDrive(final Office365Client client, final Function<IGraphServiceClient, IDriveRequestBuilder> builder,
            final Consumer<DriveItem> consumer) {
        getDriveItemChildren(client, builder, consumer, null);
    }

    protected void getDriveItemChildren(final Office365Client client, final Function<IGraphServiceClient, IDriveRequestBuilder> builder,
            final Consumer<DriveItem> consumer, final DriveItem root) {
        IDriveItemCollectionPage page;
        try {
            if (root != null) {
                consumer.accept(root);
                if (root.folder == null) {
                    return;
                }
            }
            page = client.getItemPage(builder, root != null ? root.id : null);
            if (logger.isDebugEnabled()) {
                logger.debug("crawling item: {}", page.getRawObject());
            }
            page.getCurrentPage().forEach(i -> getDriveItemChildren(client, builder, consumer, i));
            while (page.getNextPage() != null) {
                try {
                    page = client.getNextItemPage(page);
                    page.getCurrentPage().forEach(i -> getDriveItemChildren(client, builder, consumer, i));
                } catch (final Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Failed to process a next page.", e);
                    }
                }
            }
        } catch (final GraphServiceException e) {
            if (e.getResponseCode() == 404) {
                logger.debug("Drive item is not found.", e);
            } else {
                logger.warn("Failed to access a drive item.", e);
            }
        } catch (final ClientException e) {
            logger.warn("Failed to access a drive item.", e);
        }
    }

    public void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }

}
