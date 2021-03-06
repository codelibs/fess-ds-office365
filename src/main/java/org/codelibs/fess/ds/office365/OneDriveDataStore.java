/*
 * Copyright 2012-2021 CodeLibs Project and the Others.
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

import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MaxLengthExceededException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.crawler.extractor.Extractor;
import org.codelibs.fess.crawler.filter.UrlFilter;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.office365.Office365Client.UserType;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.microsoft.graph.core.ClientException;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.models.extensions.Drive;
import com.microsoft.graph.models.extensions.DriveItem;
import com.microsoft.graph.models.extensions.Hashes;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.Permission;
import com.microsoft.graph.requests.extensions.IDriveItemCollectionPage;
import com.microsoft.graph.requests.extensions.IDriveRequestBuilder;
import com.microsoft.graph.requests.extensions.IPermissionCollectionPage;

public class OneDriveDataStore extends Office365DataStore {

    private static final Logger logger = LoggerFactory.getLogger(OneDriveDataStore.class);

    protected static final long DEFAULT_MAX_SIZE = 10000000L; // 10m

    protected static final String CURRENT_CRAWLER = "current_crawler";
    protected static final String CRAWLER_TYPE_GROUP = "group";
    protected static final String CRAWLER_TYPE_USER = "user";
    protected static final String CRAWLER_TYPE_SHARED = "shared";
    protected static final String CRAWLER_TYPE_DRIVE = "drive";
    protected static final String DRIVE_INFO = "drive_info";

    // parameters
    protected static final String MAX_SIZE = "max_size";
    protected static final String IGNORE_FOLDER = "ignore_folder";
    protected static final String IGNORE_ERROR = "ignore_error";
    protected static final String SUPPORTED_MIMETYPES = "supported_mimetypes";
    protected static final String INCLUDE_PATTERN = "include_pattern";
    protected static final String EXCLUDE_PATTERN = "exclude_pattern";
    protected static final String URL_FILTER = "url_filter";
    protected static final String DRIVE_ID = "drive_id";
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    protected static final String SHARED_DOCUMENTS_DRIVE_CRAWLER = "shared_documents_drive_crawler";
    protected static final String USER_DRIVE_CRAWLER = "user_drive_crawler";
    protected static final String GROUP_DRIVE_CRAWLER = "group_drive_crawler";

    // scripts
    protected static final String FILE = "file";
    protected static final String FILE_NAME = "name";
    protected static final String FILE_DESCRIPTION = "description";
    protected static final String FILE_CONTENTS = "contents";
    protected static final String FILE_MIMETYPE = "mimetype";
    protected static final String FILE_FILETYPE = "filetype";
    protected static final String FILE_CREATED = "created";
    protected static final String FILE_LAST_MODIFIED = "last_modified";
    protected static final String FILE_SIZE = "size";
    protected static final String FILE_WEB_URL = "web_url";
    protected static final String FILE_URL = "url";
    protected static final String FILE_ROLES = "roles";
    protected static final String FILE_CTAG = "ctag";
    protected static final String FILE_ETAG = "etag";
    protected static final String FILE_ID = "id";
    protected static final String FILE_WEBDAV_URL = "webdav_url";
    protected static final String FILE_LOCATION = "location";
    protected static final String FILE_CREATEDBY_APPLICATION = "createdby_application";
    protected static final String FILE_CREATEDBY_DEVICE = "createdby_device";
    protected static final String FILE_CREATEDBY_USER = "createdby_user";
    protected static final String FILE_DELETED = "deleted";
    protected static final String FILE_HASHES = "hashes";
    protected static final String FILE_LAST_MODIFIEDBY_APPLICATION = "last_modifiedby_application";
    protected static final String FILE_LAST_MODIFIEDBY_DEVICE = "last_modifiedby_device";
    protected static final String FILE_LAST_MODIFIEDBY_USER = "last_modifiedby_user";
    protected static final String FILE_IMAGE = "image";
    protected static final String FILE_PARENT = "parent";
    protected static final String FILE_PARENT_ID = "parent_id";
    protected static final String FILE_PARENT_NAME = "parent_name";
    protected static final String FILE_PARENT_PATH = "parent_path";
    protected static final String FILE_PHOTO = "photo";
    protected static final String FILE_PUBLICATION = "publication";
    protected static final String FILE_SEARCH_RESULT = "search_result";
    protected static final String FILE_SPECIAL_FOLDER = "special_folder";
    protected static final String FILE_VIDEO = "video";

    protected String extractorName = "tikaExtractor";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, String> paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(MAX_SIZE, getMaxSize(paramMap));
        configMap.put(IGNORE_FOLDER, isIgnoreFolder(paramMap));
        configMap.put(IGNORE_ERROR, isIgnoreError(paramMap));
        configMap.put(SUPPORTED_MIMETYPES, getSupportedMimeTypes(paramMap));
        configMap.put(URL_FILTER, getUrlFilter(paramMap));
        if (logger.isDebugEnabled()) {
            logger.debug("configMap: {}", configMap);
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getOrDefault(NUMBER_OF_THREADS, "1")));
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

            final String driveId = paramMap.get(DRIVE_ID);
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
            throw new DataStoreException("Interrupted.", e);
        } finally {
            executorService.shutdownNow();
        }
    }

    protected Office365Client createClient(final Map<String, String> params) {
        return new Office365Client(params);
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
            logger.debug("urlFilter: {}", urlFilter);
        }
        return urlFilter;
    }

    protected boolean isSharedDocumentsDriveCrawler(final Map<String, String> paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getOrDefault(SHARED_DOCUMENTS_DRIVE_CRAWLER, Constants.TRUE));
    }

    protected boolean isUserDriveCrawler(final Map<String, String> paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getOrDefault(USER_DRIVE_CRAWLER, Constants.TRUE));
    }

    protected boolean isGroupDriveCrawler(final Map<String, String> paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getOrDefault(GROUP_DRIVE_CRAWLER, Constants.TRUE));
    }

    protected boolean isIgnoreFolder(final Map<String, String> paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getOrDefault(IGNORE_FOLDER, Constants.TRUE));
    }

    protected boolean isIgnoreError(final Map<String, String> paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getOrDefault(IGNORE_ERROR, Constants.TRUE));
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
                .get(stream -> stream.map(String::trim).toArray(n -> new String[n]));
    }

    protected void storeSharedDocumentsDrive(final DataConfig dataConfig, final IndexUpdateCallback callback,
            final Map<String, Object> configMap, final Map<String, String> paramMap, final Map<String, String> scriptMap,
            final Map<String, Object> defaultDataMap, final ExecutorService executorService, final Office365Client client,
            final String driveId) {
        getDriveItemsInDrive(client, c -> driveId != null ? c.drives(driveId) : c.drive(),
                item -> executorService.execute(() -> processDriveItem(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                        client, c -> driveId != null ? c.drives(driveId) : c.drive(), item, Collections.emptyList())));
    }

    protected void storeUsersDrive(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final Map<String, String> paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final ExecutorService executorService, final Office365Client client) {
        getLicensedUsers(client, user -> {
            try {
                getDriveItemsInDrive(client, c -> c.users(user.id).drive(),
                        item -> executorService.execute(() -> processDriveItem(dataConfig, callback, configMap, paramMap, scriptMap,
                                defaultDataMap, client, c -> c.users(user.id).drive(), item, getUserRoles(user))));
            } catch (final GraphServiceException e) {
                logger.warn("Failed to store " + user.displayName + "'s Drive, ", e);
            }
        });
    }

    protected void isInterrupted(final Exception e) throws InterruptedException {
        if (e instanceof InterruptedException) {
            throw (InterruptedException) e;
        }
    }

    protected void storeGroupsDrive(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final Map<String, String> paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final ExecutorService executorService, final Office365Client client) {
        getOffice365Groups(client, group -> {
            getDriveItemsInDrive(client, c -> c.groups(group.id).drive(), //
                    item -> executorService.execute(() -> processDriveItem(dataConfig, callback, configMap, paramMap, scriptMap,
                            defaultDataMap, client, c -> c.groups(group.id).drive(), item, getGroupRoles(group))));
        });
    }

    protected void processDriveItem(final DataConfig dataConfig, final IndexUpdateCallback callback, final Map<String, Object> configMap,
            final Map<String, String> paramMap, final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap,
            final Office365Client client, final Function<IGraphServiceClient, IDriveRequestBuilder> builder, final DriveItem item,
            final List<String> roles) {
        final String mimetype;
        final Hashes hashes;
        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        try {
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

            final String[] supportedMimeTypes = (String[]) configMap.get(SUPPORTED_MIMETYPES);
            if (!Stream.of(supportedMimeTypes).anyMatch(mimetype::matches)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} is not an indexing target.", mimetype);
                }
                return;
            }

            final String url = getUrl(configMap, paramMap, item);
            final UrlFilter urlFilter = (UrlFilter) configMap.get(URL_FILTER);
            if (urlFilter != null && !urlFilter.match(url)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not matched: {}", url);
                }
                return;
            }

            logger.info("Crawling URL: {}", url);

            final boolean ignoreError = ((Boolean) configMap.get(IGNORE_ERROR));

            final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap);
            final Map<String, Object> filesMap = new HashMap<>();

            if (item.size.longValue() > ((Long) configMap.get(MAX_SIZE)).longValue()) {
                throw new MaxLengthExceededException("The content length (" + item.size + " byte) is over " + configMap.get(MAX_SIZE)
                        + " byte. The url is " + item.webUrl);
            }

            final String filetype = ComponentUtil.getFileTypeHelper().get(mimetype);
            filesMap.put(FILE_NAME, item.name);
            filesMap.put(FILE_DESCRIPTION, item.description != null ? item.description : StringUtil.EMPTY);
            filesMap.put(FILE_CONTENTS, getDriveItemContents(client, builder, item, ignoreError));
            filesMap.put(FILE_MIMETYPE, mimetype);
            filesMap.put(FILE_FILETYPE, filetype);
            filesMap.put(FILE_CREATED, item.createdDateTime.getTime());
            filesMap.put(FILE_LAST_MODIFIED, item.lastModifiedDateTime.getTime());
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
            StreamUtil.split(paramMap.get(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(permissions::add));
            filesMap.put(FILE_ROLES, permissions.stream().distinct().collect(Collectors.toList()));

            resultMap.put("files", filesMap); // TODO deprecated
            resultMap.put(FILE, filesMap);
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

    protected List<String> getDriveItemPermissions(final Office365Client client,
            final Function<IGraphServiceClient, IDriveRequestBuilder> builder, final DriveItem item) {
        final List<String> permissions = new ArrayList<>();
        IPermissionCollectionPage page = client.getDrivePermissions(builder, item.id);
        while (page != null) {
            page.getCurrentPage().forEach(p -> {
                if (p.grantedTo != null && p.grantedTo.user != null) {
                    assignPermission(client, permissions, p);
                }
            });
            page = client.getNextPermissionPage(page);
        }
        return permissions;
    }

    protected void assignPermission(final Office365Client client, final List<String> permissions, final Permission permission) {
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        final String id = permission.grantedTo.user.id;
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
            logger.debug("No identity for permission: {}", permission.getRawObject());
        }
    }

    protected String getUserEmail(final Permission permission) {
        JsonObject rawObject = permission.getRawObject();
        if (rawObject != null) {
            rawObject = rawObject.getAsJsonObject("grantedTo");
            if (rawObject != null) {
                rawObject = rawObject.getAsJsonObject("user");
                if (rawObject != null) {
                    final JsonElement jsonElement = rawObject.get("email");
                    if (jsonElement != null) {
                        return jsonElement.getAsString();
                    }
                }
            }
        }
        return null;
    }

    protected String getUrl(final Map<String, Object> configMap, final Map<String, String> paramMap, final DriveItem item) {
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
        } else {
            return baseUrl + "/Documents/" + path;
        }
    }

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

    protected String getDriveItemContents(final Office365Client client, final Function<IGraphServiceClient, IDriveRequestBuilder> builder,
            final DriveItem item, final boolean ignoreError) {
        if (item.file != null) {
            final String mimeType = item.file.mimeType;
            try (final InputStream in = client.getDriveContent(builder, item.id)) {
                Extractor extractor = ComponentUtil.getExtractorFactory().getExtractor(mimeType);
                if (extractor == null) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("use a defautl extractor as {} by {}", extractorName, mimeType);
                    }
                    extractor = ComponentUtil.getComponent(extractorName);
                }
                return extractor.getText(in, null).getContent();
            } catch (final Exception e) {
                if (ignoreError) {
                    logger.warn("Failed to get contents: " + item.name, e);
                    return StringUtil.EMPTY;
                }
                throw new DataStoreCrawlingException(item.webUrl, "Failed to get contents: " + item.name, e);
            }
        }
        return StringUtil.EMPTY;
    }

    protected void getDriveItemsInDrive(final Office365Client client, final Function<IGraphServiceClient, IDriveRequestBuilder> builder,
            final Consumer<DriveItem> consumer) {
        getDriveItemChildren(client, builder, consumer, null);
    }

    protected void getDriveItemChildren(final Office365Client client, final Function<IGraphServiceClient, IDriveRequestBuilder> builder,
            final Consumer<DriveItem> consumer, final DriveItem item) {
        if (logger.isDebugEnabled()) {
            logger.debug("Current item: {}", item != null ? item.name + " -> " + item.webUrl : "root");
        }
        IDriveItemCollectionPage page;
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
                    page = client.getNextItemPage(page);
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

    public void setExtractorName(final String extractorName) {
        this.extractorName = extractorName;
    }

}
