/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
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
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.crawler.helper.ContentLengthHelper;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.office365.client.Office365Client;
import org.codelibs.fess.ds.office365.client.Office365Client.UserType;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.mylasta.direction.FessConfig;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.Drive;
import com.microsoft.graph.models.DriveItem;
import com.microsoft.graph.models.Hashes;
import com.microsoft.graph.models.Permission;
import com.microsoft.graph.requests.DriveItemCollectionPage;
import com.microsoft.graph.requests.DriveRequestBuilder;
import com.microsoft.graph.requests.GraphServiceClient;
import com.microsoft.graph.requests.PermissionCollectionPage;

import okhttp3.Request;

/**
 * This class is a data store for crawling and indexing files in Microsoft OneDrive.
 * It supports crawling user drives, group drives, and shared document libraries.
 * It also handles file metadata, permissions, and content extraction.
 */
public class OneDriveDataStore extends Office365DataStore {

    /**
     * Default constructor.
     */
    public OneDriveDataStore() {
        super();
    }

    private static final Logger logger = LogManager.getLogger(OneDriveDataStore.class);

    /** Default maximum size of a file to be crawled. */
    protected static final long DEFAULT_MAX_SIZE = -1L;

    /** Key for the current crawler type in the configuration map. */
    protected static final String CURRENT_CRAWLER = "current_crawler";
    /** Crawler type for group drives. */
    protected static final String CRAWLER_TYPE_GROUP = "group";
    /** Crawler type for user drives. */
    protected static final String CRAWLER_TYPE_USER = "user";
    /** Crawler type for shared drives. */
    protected static final String CRAWLER_TYPE_SHARED = "shared";
    /** Crawler type for a specific drive. */
    protected static final String CRAWLER_TYPE_DRIVE = "drive";
    /** Key for drive information in the configuration map. */
    protected static final String DRIVE_INFO = "drive_info";

    // parameters
    /** Parameter name for the maximum content length. */
    protected static final String MAX_CONTENT_LENGTH = "max_content_length";
    /** Parameter name for ignoring folders. */
    protected static final String IGNORE_FOLDER = "ignore_folder";
    /** Parameter name for ignoring errors. */
    protected static final String IGNORE_ERROR = "ignore_error";
    /** Parameter name for supported MIME types. */
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    /** Parameter name for the include pattern for URLs. */
    protected static final String INCLUDE_PATTERN = "include_pattern";
    /** Parameter name for the exclude pattern for URLs. */
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    /** Parameter name for the URL filter. */
    protected static final String URL_FILTER = "url_filter";
    /** Parameter name for the drive ID. */
    protected static final String DRIVE_ID = "drive_id";
    /** Parameter name for default permissions. */
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    /** Parameter name for the number of threads. */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    /** Parameter name for enabling the shared documents drive crawler. */
    protected static final String SHARED_DOCUMENTS_DRIVE_CRAWLER = "shared_documents_drive_crawler";
    /** Parameter name for enabling the user drive crawler. */
    protected static final String USER_DRIVE_CRAWLER = "user_drive_crawler";
    /** Parameter name for enabling the group drive crawler. */
    protected static final String GROUP_DRIVE_CRAWLER = "group_drive_crawler";

    // scripts
    /** Key for the file object in the script map. */
    protected static final String FILE = "file";
    /** Key for the file name in the script map. */
    protected static final String FILE_NAME = "name";
    /** Key for the file description in the script map. */
    protected static final String FILE_DESCRIPTION = "description";
    /** Key for the file contents in the script map. */
    protected static final String FILE_CONTENTS = "contents";
    /** Key for the file MIME type in the script map. */
    protected static final String FILE_MIMETYPE = "mimetype";
    /** Key for the file type in the script map. */
    protected static final String FILE_FILETYPE = "filetype";
    /** Key for the file creation date in the script map. */
    protected static final String FILE_CREATED = "created";
    /** Key for the file last modified date in the script map. */
    protected static final String FILE_LAST_MODIFIED = "last_modified";
    /** Key for the file size in the script map. */
    protected static final String FILE_SIZE = "size";
    /** Key for the file web URL in the script map. */
    protected static final String FILE_WEB_URL = "web_url";
    /** Key for the file URL in the script map. */
    protected static final String FILE_URL = "url";
    /** Key for the file roles in the script map. */
    protected static final String FILE_ROLES = "roles";
    /** Key for the file cTag in the script map. */
    protected static final String FILE_CTAG = "ctag";
    /** Key for the file eTag in the script map. */
    protected static final String FILE_ETAG = "etag";
    /** Key for the file ID in the script map. */
    protected static final String FILE_ID = "id";
    /** Key for the file WebDAV URL in the script map. */
    protected static final String FILE_WEBDAV_URL = "webdav_url";
    /** Key for the file location in the script map. */
    protected static final String FILE_LOCATION = "location";
    /** Key for the application that created the file in the script map. */
    protected static final String FILE_CREATEDBY_APPLICATION = "createdby_application";
    /** Key for the device that created the file in the script map. */
    protected static final String FILE_CREATEDBY_DEVICE = "createdby_device";
    /** Key for the user who created the file in the script map. */
    protected static final String FILE_CREATEDBY_USER = "createdby_user";
    /** Key for the deleted status of the file in the script map. */
    protected static final String FILE_DELETED = "deleted";
    /** Key for the file hashes in the script map. */
    protected static final String FILE_HASHES = "hashes";
    /** Key for the application that last modified the file in the script map. */
    protected static final String FILE_LAST_MODIFIEDBY_APPLICATION = "last_modifiedby_application";
    /** Key for the device that last modified the file in the script map. */
    protected static final String FILE_LAST_MODIFIEDBY_DEVICE = "last_modifiedby_device";
    /** Key for the user who last modified the file in the script map. */
    protected static final String FILE_LAST_MODIFIEDBY_USER = "last_modifiedby_user";
    /** Key for the file image in the script map. */
    protected static final String FILE_IMAGE = "image";
    /** Key for the file parent in the script map. */
    protected static final String FILE_PARENT = "parent";
    /** Key for the file parent ID in the script map. */
    protected static final String FILE_PARENT_ID = "parent_id";
    /** Key for the file parent name in the script map. */
    protected static final String FILE_PARENT_NAME = "parent_name";
    /** Key for the file parent path in the script map. */
    protected static final String FILE_PARENT_PATH = "parent_path";
    /** Key for the file photo in the script map. */
    protected static final String FILE_PHOTO = "photo";
    /** Key for the file publication in the script map. */
    protected static final String FILE_PUBLICATION = "publication";
    /** Key for the file search result in the script map. */
    protected static final String FILE_SEARCH_RESULT = "search_result";
    /** Key for the file special folder in the script map. */
    protected static final String FILE_SPECIAL_FOLDER = "special_folder";
    /** Key for the file video in the script map. */
    protected static final String FILE_VIDEO = "video";

    /** The name of the extractor to use for file content. */
    protected String extractorName = "tikaExtractor";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(MAX_CONTENT_LENGTH, getMaxSize(paramMap));
        configMap.put(IGNORE_FOLDER, isIgnoreFolder(paramMap));
        configMap.put(IGNORE_ERROR, isIgnoreError(paramMap));
        configMap.put(SUPPORTED_MIMETYPES, getSupportedMimeTypes(paramMap));
        configMap.put(URL_FILTER, getUrlFilter(paramMap));
        if (logger.isDebugEnabled()) {
            logger.debug("configMap: {}", configMap);
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Office365Client client = createClient(paramMap)) {
            if (isSharedDocumentsDriveCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("crawling shared documents drive.");
                }
                configMap.put(CURRENT_CRAWLER, CRAWLER_TYPE_SHARED);
                storeSharedDocumentsDrive(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client,
                        null);
            }

            if (isUserDriveCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("crawling user drive.");
                }
                configMap.put(CURRENT_CRAWLER, CRAWLER_TYPE_USER);
                storeUsersDrive(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client);
            }

            if (isGroupDriveCrawler(paramMap)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("crawling group drive.");
                }
                configMap.put(CURRENT_CRAWLER, CRAWLER_TYPE_GROUP);
                storeGroupsDrive(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client);
            }

            final String driveId = paramMap.getAsString(DRIVE_ID);
            if (StringUtil.isNotBlank(driveId)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("crawling doclument library drive: {}", driveId);
                }
                configMap.put(CURRENT_CRAWLER, CRAWLER_TYPE_DRIVE);
                configMap.put(DRIVE_INFO, client.getDrive(driveId));
                storeSharedDocumentsDrive(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, executorService, client,
                        driveId);
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

    /**
     * Creates a new Office365Client.
     *
     * @param params The data store parameters.
     * @return A new Office365Client.
     */
    protected Office365Client createClient(final DataStoreParams params) {
        return new Office365Client(params);
    }

    /**
     * Gets the URL filter from the data store parameters.
     *
     * @param paramMap The data store parameters.
     * @return The URL filter.
     */
    protected UrlFilter getUrlFilter(final DataStoreParams paramMap) {
        final UrlFilter urlFilter = ComponentUtil.getComponent(UrlFilter.class);
        final String include = paramMap.getAsString(INCLUDE_PATTERN);
        if (StringUtil.isNotBlank(include)) {
            urlFilter.addInclude(include);
        }
        final String exclude = paramMap.getAsString(EXCLUDE_PATTERN);
        if (StringUtil.isNotBlank(exclude)) {
            urlFilter.addExclude(exclude);
        }
        urlFilter.init(paramMap.getAsString(Constants.CRAWLING_INFO_ID));
        if (logger.isDebugEnabled()) {
            logger.debug("urlFilter: {}", urlFilter);
        }
        return urlFilter;
    }

    /**
     * Checks if the shared documents drive crawler is enabled.
     *
     * @param paramMap The data store parameters.
     * @return true if the shared documents drive crawler is enabled, false otherwise.
     */
    protected boolean isSharedDocumentsDriveCrawler(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(SHARED_DOCUMENTS_DRIVE_CRAWLER, Constants.TRUE));
    }

    /**
     * Checks if the user drive crawler is enabled.
     *
     * @param paramMap The data store parameters.
     * @return true if the user drive crawler is enabled, false otherwise.
     */
    protected boolean isUserDriveCrawler(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(USER_DRIVE_CRAWLER, Constants.TRUE));
    }

    /**
     * Checks if the group drive crawler is enabled.
     *
     * @param paramMap The data store parameters.
     * @return true if the group drive crawler is enabled, false otherwise.
     */
    protected boolean isGroupDriveCrawler(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(GROUP_DRIVE_CRAWLER, Constants.TRUE));
    }

    /**
     * Checks if folders should be ignored.
     *
     * @param paramMap The data store parameters.
     * @return true if folders should be ignored, false otherwise.
     */
    protected boolean isIgnoreFolder(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_FOLDER, Constants.TRUE));
    }

    /**
     * Checks if errors should be ignored.
     *
     * @param paramMap The data store parameters.
     * @return true if errors should be ignored, false otherwise.
     */
    protected boolean isIgnoreError(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_ERROR, Constants.TRUE));
    }

    /**
     * Gets the maximum content length from the data store parameters.
     *
     * @param paramMap The data store parameters.
     * @return The maximum content length.
     */
    protected long getMaxSize(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(MAX_CONTENT_LENGTH);
        try {
            return StringUtil.isNotBlank(value) ? Long.parseLong(value) : DEFAULT_MAX_SIZE;
        } catch (final NumberFormatException e) {
            return DEFAULT_MAX_SIZE;
        }
    }

    /**
     * Gets the supported MIME types from the data store parameters.
     *
     * @param paramMap The data store parameters.
     * @return An array of supported MIME types.
     */
    protected String[] getSupportedMimeTypes(final DataStoreParams paramMap) {
        return StreamUtil.split(paramMap.getAsString(SUPPORTED_MIMETYPES, ".*"), ",")
                .get(stream -> stream.map(String::trim).toArray(n -> new String[n]));
    }

    /**
     * Stores the shared documents drive.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param configMap The configuration map.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param executorService The executor service.
     * @param client The Office365Client.
     * @param driveId The drive ID.
     */
    protected void storeSharedDocumentsDrive(final DataConfig dataConfig, final IndexUpdateCallback callback,
            final Map<String, Object> configMap, final DataStoreParams paramMap, final Map<String, String> scriptMap,
            final Map<String, Object> defaultDataMap, final ExecutorService executorService, final Office365Client client,
            final String driveId) {
        getDriveItemsInDrive(client, c -> driveId != null ? c.drives(driveId) : c.drive(),
                item -> executorService.execute(() -> processDriveItem(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                        client, c -> driveId != null ? c.drives(driveId) : c.drive(), item, Collections.emptyList())));
    }

    /**
     * Stores the users' drives.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param configMap The configuration map.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param executorService The executor service.
     * @param client The Office365Client.
     */
    protected void storeUsersDrive(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final ExecutorService executorService, final Office365Client client) {
        getLicensedUsers(client, user -> {
            try {
                getDriveItemsInDrive(client, c -> c.users(user.id).drive(),
                        item -> executorService.execute(() -> processDriveItem(dataConfig, callback, configMap, paramMap, scriptMap,
                                defaultDataMap, client, c -> c.users(user.id).drive(), item, getUserRoles(user))));
            } catch (final GraphServiceException e) {
                logger.warn("Failed to store {}'s Drive, ", user.displayName, e);
            }
        });
    }

    /**
     * Checks if the current thread is interrupted.
     *
     * @param e The exception to check.
     */
    protected void isInterrupted(final Exception e) {
        if (e instanceof InterruptedException) {
            throw new InterruptedRuntimeException((InterruptedException) e);
        }
    }

    /**
     * Stores the groups' drives.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param configMap The configuration map.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param executorService The executor service.
     * @param client The Office365Client.
     */
    protected void storeGroupsDrive(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final ExecutorService executorService, final Office365Client client) {
        getOffice365Groups(client, group -> {
            getDriveItemsInDrive(client, c -> c.groups(group.id).drive(), //
                    item -> executorService.execute(() -> processDriveItem(dataConfig, callback, configMap, paramMap, scriptMap,
                            defaultDataMap, client, c -> c.groups(group.id).drive(), item, getGroupRoles(group))));
        });
    }

    /**
     * Processes a drive item.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param configMap The configuration map.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param client The Office365Client.
     * @param builder The drive request builder.
     * @param item The drive item.
     * @param roles The roles.
     */
    protected void processDriveItem(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final DataStoreParams paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Office365Client client, final Function<GraphServiceClient<Request>, DriveRequestBuilder> builder, final DriveItem item,
            final List<String> roles) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final FessConfig fessConfig = ComponentUtil.getFessConfig();
        final String mimetype;
        final Hashes hashes;
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final StatsKeyObject statsKey = new StatsKeyObject(item.webUrl);
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        try {
            crawlerStatsHelper.begin(statsKey);
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
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            final String[] supportedMimeTypes = (String[]) configMap.get(SUPPORTED_MIMETYPES);
            if (!Stream.of(supportedMimeTypes).anyMatch(mimetype::matches)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} is not an indexing target.", mimetype);
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            final String url = getUrl(configMap, paramMap, item);
            final UrlFilter urlFilter = (UrlFilter) configMap.get(URL_FILTER);
            if (urlFilter != null && !urlFilter.match(url)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not matched: {}", url);
                }
                crawlerStatsHelper.discard(statsKey);
                return;
            }

            logger.info("Crawling URL: {}", url);

            final boolean ignoreError = ((Boolean) configMap.get(IGNORE_ERROR));

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
            final Map<String, Object> filesMap = new HashMap<>();

            long maxContentLength = ((Long) configMap.get(MAX_CONTENT_LENGTH)).longValue();
            if (maxContentLength < 0) {
                try {
                    final ContentLengthHelper contentLengthHelper = ComponentUtil.getComponent("contentLengthHelper");
                    maxContentLength = contentLengthHelper.getMaxLength(mimetype);
                } catch (Exception e) {
                    logger.warn("Failed to get maxContentLength.", e);
                }
            }
            if (maxContentLength >= 0 && item.size.longValue() > maxContentLength) {
                throw new MaxLengthExceededException(
                        "The content length (" + item.size + " byte) is over " + maxContentLength + " byte. The url is " + item.webUrl);
            }

            final String filetype = ComponentUtil.getFileTypeHelper().get(mimetype);
            filesMap.put(FILE_NAME, item.name);
            filesMap.put(FILE_DESCRIPTION, item.description != null ? item.description : StringUtil.EMPTY);
            filesMap.put(FILE_CONTENTS, getDriveItemContents(client, builder, item, maxContentLength, ignoreError));
            filesMap.put(FILE_MIMETYPE, mimetype);
            filesMap.put(FILE_FILETYPE, filetype);
            filesMap.put(FILE_CREATED, item.createdDateTime);
            filesMap.put(FILE_LAST_MODIFIED, item.lastModifiedDateTime);
            filesMap.put(FILE_SIZE, item.size);
            filesMap.put(FILE_WEB_URL, item.webUrl);
            filesMap.put(FILE_URL, url);
            filesMap.put(FILE_CTAG, item.cTag);
            filesMap.put(FILE_ETAG, item.eTag);
            filesMap.put(FILE_ID, item.id);
            filesMap.put(FILE_WEBDAV_URL, item.webDavUrl);
            filesMap.put(FILE_LOCATION, item.location);
            filesMap.put(FILE_CREATEDBY_APPLICATION, item.createdBy != null ? item.createdBy.application : null);
            filesMap.put(FILE_CREATEDBY_DEVICE, item.createdBy != null ? item.createdBy.device : null);
            filesMap.put(FILE_CREATEDBY_USER, item.createdBy != null ? item.createdBy.user : null);
            filesMap.put(FILE_DELETED, item.deleted);
            filesMap.put(FILE_HASHES, hashes);
            filesMap.put(FILE_LAST_MODIFIEDBY_APPLICATION, item.lastModifiedBy != null ? item.lastModifiedBy.application : null);
            filesMap.put(FILE_LAST_MODIFIEDBY_DEVICE, item.lastModifiedBy != null ? item.lastModifiedBy.device : null);
            filesMap.put(FILE_LAST_MODIFIEDBY_USER, item.lastModifiedBy != null ? item.lastModifiedBy.user : null);
            filesMap.put(FILE_IMAGE, item.image);
            filesMap.put(FILE_PARENT, item.parentReference);
            filesMap.put(FILE_PARENT_ID, item.parentReference != null ? item.parentReference.id : null);
            filesMap.put(FILE_PARENT_NAME, item.parentReference != null ? item.parentReference.name : null);
            filesMap.put(FILE_PARENT_PATH, item.parentReference != null ? item.parentReference.path : null);
            filesMap.put(FILE_PHOTO, item.photo);
            filesMap.put(FILE_PUBLICATION, item.publication);
            filesMap.put(FILE_SEARCH_RESULT, item.searchResult);
            filesMap.put(FILE_SPECIAL_FOLDER, item.specialFolder != null ? item.specialFolder.name : null);
            filesMap.put(FILE_VIDEO, item.video);

            final List<String> permissions = getDriveItemPermissions(client, builder, item);
            roles.forEach(permissions::add);
            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(permissions::add));
            if (defaultDataMap.get(fessConfig.getIndexFieldRole()) instanceof List<?> roleTypeList) {
                roleTypeList.stream().map(s -> (String) s).forEach(permissions::add);
            }
            filesMap.put(FILE_ROLES, permissions.stream().distinct().collect(Collectors.toList()));

            resultMap.put(FILE, filesMap);

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("filesMap: {}", filesMap);
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
            failureUrlService.store(dataConfig, errorName, item.webUrl, target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), item.webUrl, t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }
    }

    /**
     * Gets the permissions for a drive item.
     *
     * @param client The Office365Client.
     * @param builder The drive request builder.
     * @param item The drive item.
     * @return A list of permissions.
     */
    protected List<String> getDriveItemPermissions(final Office365Client client,
            final Function<GraphServiceClient<Request>, DriveRequestBuilder> builder, final DriveItem item) {
        final List<String> permissions = new ArrayList<>();
        PermissionCollectionPage page = client.getDrivePermissions(builder, item.id);
        final Consumer<Permission> consumer = p -> {
            if (p.grantedToV2 != null && p.grantedToV2.user != null) {
                assignPermission(client, permissions, p);
            }
        };
        page.getCurrentPage().forEach(consumer);
        while (page.getNextPage() != null) {
            page = page.getNextPage().buildRequest().get();
            page.getCurrentPage().forEach(consumer);
        }
        return permissions;
    }

    /**
     * Assigns a permission to a user or group.
     *
     * @param client The Office365Client.
     * @param permissions The list of permissions.
     * @param permission The permission to assign.
     */
    protected void assignPermission(final Office365Client client, final List<String> permissions, final Permission permission) {
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        final String id = permission.grantedToV2.user.id;
        final String email = getUserEmail(permission);
        if (StringUtil.isNotBlank(email)) {
            final List<String> idList = new ArrayList<>();
            if (StringUtil.isBlank(id)) {
                Collections.addAll(idList, client.getGroupIdsByEmail(email));
            } else {
                idList.add(id);
            }
            if (idList.isEmpty()) {
                permissions.add(systemHelper.getSearchRoleByUser(email));
                permissions.add(systemHelper.getSearchRoleByGroup(email));
            } else {
                idList.stream().forEach(i -> {
                    final UserType userType = client.getUserType(i);
                    switch (userType) {
                    case USER:
                        permissions.add(systemHelper.getSearchRoleByUser(email));
                        permissions.add(systemHelper.getSearchRoleByUser(i));
                        break;
                    case GROUP:
                        permissions.add(systemHelper.getSearchRoleByGroup(email));
                        permissions.add(systemHelper.getSearchRoleByGroup(i));
                        break;
                    default:
                        permissions.add(systemHelper.getSearchRoleByUser(email));
                        permissions.add(systemHelper.getSearchRoleByGroup(email));
                        permissions.add(systemHelper.getSearchRoleByUser(i));
                        permissions.add(systemHelper.getSearchRoleByGroup(i));
                        break;
                    }
                });
            }
        } else if (StringUtil.isNotBlank(id)) {
            final UserType userType = client.getUserType(id);
            switch (userType) {
            case USER:
                permissions.add(systemHelper.getSearchRoleByUser(id));
                break;
            case GROUP:
                permissions.add(systemHelper.getSearchRoleByGroup(id));
                break;
            default:
                permissions.add(systemHelper.getSearchRoleByUser(id));
                permissions.add(systemHelper.getSearchRoleByGroup(id));
                break;
            }
        } else if (logger.isDebugEnabled()) {
            logger.debug("No identity for permission.");
        }
    }

    /**
     * Gets the user email from a permission.
     *
     * @param permission The permission.
     * @return The user email.
     */
    protected String getUserEmail(final Permission permission) {
        if (permission.grantedToV2 != null && permission.grantedToV2.user != null && permission.grantedToV2.user.displayName != null) {
            // TODO email?
            return permission.grantedToV2.user.displayName;
        }
        return null;
    }

    /**
     * Gets the URL for a drive item.
     *
     * @param configMap The configuration map.
     * @param paramMap The data store parameters.
     * @param item The drive item.
     * @return The URL.
     */
    protected String getUrl(final Map<String, Object> configMap, final DataStoreParams paramMap, final DriveItem item) {
        if (item.webUrl == null) {
            return null;
        }
        if (!item.webUrl.contains("/_layouts/")) {
            return item.webUrl;
        }

        final String baseUrl = item.webUrl.substring(0, item.webUrl.indexOf("/_layouts/"));
        final List<String> pathList = new ArrayList<>();
        if (item.parentReference != null && item.parentReference.path != null) {
            final String[] values = item.parentReference.path.split(":", 2);
            if (values.length == 2) {
                for (final String s : values[1].split("/")) {
                    pathList.add(encodeUrl(s));
                }
            }
        }
        pathList.add(encodeUrl(item.name));
        final String path = pathList.stream().filter(StringUtil::isNotBlank).collect(Collectors.joining("/"));
        if (CRAWLER_TYPE_SHARED.equals(configMap.get(CURRENT_CRAWLER)) || CRAWLER_TYPE_GROUP.equals(configMap.get(CURRENT_CRAWLER))) {
            return baseUrl + "/Shared%20Documents/" + path;
        }
        if (CRAWLER_TYPE_DRIVE.equals(configMap.get(CURRENT_CRAWLER))) {
            final Drive drive = (Drive) configMap.get(DRIVE_INFO);
            return baseUrl + "/" + drive.name + "/" + path;
        }
        return baseUrl + "/Documents/" + path;
    }

    /**
     * Encodes a URL string.
     *
     * @param s The string to encode.
     * @return The encoded string.
     */
    protected String encodeUrl(final String s) {
        if (StringUtil.isEmpty(s)) {
            return s;
        }
        try {
            return URLEncoder.encode(s, Constants.UTF_8).replace("+", "%20");
        } catch (final UnsupportedEncodingException e) {
            // ignore
            return s;
        }
    }

    /**
     * Gets the contents of a drive item.
     *
     * @param client The Office365Client.
     * @param builder The drive request builder.
     * @param item The drive item.
     * @param maxContentLength The maximum content length.
     * @param ignoreError true to ignore errors.
     * @return The contents of the drive item.
     */
    protected String getDriveItemContents(final Office365Client client,
            final Function<GraphServiceClient<Request>, DriveRequestBuilder> builder, final DriveItem item, final long maxContentLength,
            final boolean ignoreError) {
        if (item.file != null) {
            try (final InputStream in = client.getDriveContent(builder, item.id)) {
                return ComponentUtil.getExtractorFactory().builder(in, Collections.emptyMap()).filename(item.name)
                        .maxContentLength(maxContentLength).extractorName(extractorName).extract().getContent();
            } catch (final Exception e) {
                if (!ignoreError && !ComponentUtil.getFessConfig().isCrawlerIgnoreContentException()) {
                    throw new DataStoreCrawlingException(item.webUrl, "Failed to get contents: " + item.name, e);
                }
                if (logger.isDebugEnabled()) {
                    logger.warn("Failed to get contents: {}", item.name, e);
                } else {
                    logger.warn("Failed to get contents: {}. {}", item.name, e.getMessage());
                }
                return StringUtil.EMPTY;
            }
        }
        return StringUtil.EMPTY;
    }

    /**
     * Gets the drive items in a drive.
     *
     * @param client The Office365Client.
     * @param builder The drive request builder.
     * @param consumer The consumer to process each drive item.
     */
    protected void getDriveItemsInDrive(final Office365Client client,
            final Function<GraphServiceClient<Request>, DriveRequestBuilder> builder, final Consumer<DriveItem> consumer) {
        getDriveItemChildren(client, builder, consumer, null);
    }

    /**
     * Gets the children of a drive item.
     *
     * @param client The Office365Client.
     * @param builder The drive request builder.
     * @param consumer The consumer to process each drive item.
     * @param item The drive item.
     */
    protected void getDriveItemChildren(final Office365Client client,
            final Function<GraphServiceClient<Request>, DriveRequestBuilder> builder, final Consumer<DriveItem> consumer,
            final DriveItem item) {
        if (logger.isDebugEnabled()) {
            logger.debug("Current item: {}", item != null ? item.name + " -> " + item.webUrl : "root");
        }
        DriveItemCollectionPage page;
        try {
            if (item != null) {
                consumer.accept(item);
                if (item.folder == null) {
                    return;
                }
            }
            page = client.getDriveItemPage(builder, item != null ? item.id : null);
            page.getCurrentPage().forEach(child -> getDriveItemChildren(client, builder, consumer, child));
            while (page.getNextPage() != null) {
                try {
                    page = page.getNextPage().buildRequest().get();
                    page.getCurrentPage().forEach(child -> getDriveItemChildren(client, builder, consumer, child));
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

    /**
     * Sets the name of the extractor to use for file content.
     *
     * @param extractorName The name of the extractor.
     */
    public void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }

}
