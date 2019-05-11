/*
 * Copyright 2012-2019 CodeLibs Project and the Others.
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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.timer.TimeoutManager;
import org.codelibs.core.timer.TimeoutTarget;
import org.codelibs.core.timer.TimeoutTask;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.graph.authentication.IAuthenticationProvider;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.http.IHttpRequest;
import com.microsoft.graph.logger.DefaultLogger;
import com.microsoft.graph.logger.LoggerLevel;
import com.microsoft.graph.models.extensions.Drive;
import com.microsoft.graph.models.extensions.Group;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.OnenotePage;
import com.microsoft.graph.models.extensions.OnenoteSection;
import com.microsoft.graph.models.extensions.Site;
import com.microsoft.graph.models.extensions.User;
import com.microsoft.graph.options.Option;
import com.microsoft.graph.options.QueryOption;
import com.microsoft.graph.requests.extensions.GraphServiceClient;
import com.microsoft.graph.requests.extensions.IDriveCollectionPage;
import com.microsoft.graph.requests.extensions.IDriveItemCollectionPage;
import com.microsoft.graph.requests.extensions.IDriveRequestBuilder;
import com.microsoft.graph.requests.extensions.IGroupCollectionPage;
import com.microsoft.graph.requests.extensions.INotebookCollectionPage;
import com.microsoft.graph.requests.extensions.INotebookRequestBuilder;
import com.microsoft.graph.requests.extensions.IOnenotePageCollectionPage;
import com.microsoft.graph.requests.extensions.IOnenoteRequestBuilder;
import com.microsoft.graph.requests.extensions.IOnenoteSectionCollectionPage;
import com.microsoft.graph.requests.extensions.IOnenoteSectionRequestBuilder;
import com.microsoft.graph.requests.extensions.IPermissionCollectionPage;
import com.microsoft.graph.requests.extensions.ISiteCollectionPage;
import com.microsoft.graph.requests.extensions.IUserCollectionPage;

public class Office365Client implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(Office365Client.class);

    protected static final String TENANT_PARAM = "tenant";
    protected static final String CLIENT_ID_PARAM = "client_id";
    protected static final String CLIENT_SECRET_PARAM = "client_secret";
    protected static final String ACCESS_TIMEOUT = "access_timeout";
    protected static final String REFRESH_TOKEN_INTERVAL = "refresh_token_interval";
    protected static final String USER_TYPE_CACHE_SIZE = "user_type_cache_size";
    protected static final String GROUP_ID_CACHE_SIZE = "group_id_cache_size";

    protected static final String INVALID_AUTHENTICATION_TOKEN = "InvalidAuthenticationToken";

    protected IGraphServiceClient client;
    protected Map<String, String> params;
    protected TimeoutTask refreshTokenTask;
    protected LoadingCache<String, UserType> userTypeCache;
    protected LoadingCache<String, String[]> groupIdCache;

    public Office365Client(final Map<String, String> params) {
        this.params = params;

        final AuthenticationProvider authenticationProvider = new AuthenticationProvider(params);
        refreshTokenTask = TimeoutManager.getInstance().addTimeoutTarget(authenticationProvider,
                Integer.parseInt(params.getOrDefault(REFRESH_TOKEN_INTERVAL, "3540")), true);

        try {
            client = GraphServiceClient.builder() //
                    .authenticationProvider(authenticationProvider) //
                    .logger(new DefaultLogger() {
                        @Override
                        public void logDebug(final String message) {
                            if (LoggerLevel.DEBUG == getLoggingLevel()) {
                                logger.debug(message);
                            }
                        }

                        @Override
                        public void logError(final String message, final Throwable t) {
                            if (t instanceof GraphServiceException) {
                                final GraphServiceException e = (GraphServiceException) t;
                                if (expired(e) || e.getResponseCode() == 404) {
                                    logger.debug("[Office365Client] " + message, t);
                                } else {
                                    logger.warn("[Office365Client] " + message, t);
                                }
                            } else {
                                logger.error("[Office365Client] " + message, t);
                            }
                        }
                    }).buildClient();
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create a client.", e);
        }

        userTypeCache = CacheBuilder.newBuilder().maximumSize(Integer.parseInt(params.getOrDefault(USER_TYPE_CACHE_SIZE, "10000")))
                .build(new CacheLoader<String, UserType>() {
                    @Override
                    public UserType load(final String key) {
                        try {
                            getUser(key, Collections.emptyList());
                            return UserType.USER;
                        } catch (final GraphServiceException e) {
                            if (e.getResponseCode() == 404) {
                                return UserType.GROUP;
                            }
                            logger.warn("Failed to detect an user type.", e);
                        } catch (final Exception e) {
                            logger.warn("Failed to get an user.", e);
                        }
                        return UserType.UNKNOWN;
                    }
                });

        groupIdCache = CacheBuilder.newBuilder().maximumSize(Integer.parseInt(params.getOrDefault(GROUP_ID_CACHE_SIZE, "10000")))
                .build(new CacheLoader<String, String[]>() {
                    @Override
                    public String[] load(final String email) {
                        final List<String> idList = new ArrayList<>();
                        getGroups(Collections.singletonList(new QueryOption("$filter", "mail eq '" + email + "'")), g -> idList.add(g.id));
                        return idList.toArray(new String[idList.size()]);
                    }
                });
    }

    @Override
    public void close() {
        if (refreshTokenTask != null) {
            refreshTokenTask.cancel();
        }
        if (client != null) {
            client.shutdown();
        }
    }

    public enum UserType {
        USER, GROUP, UNKNOWN;
    }

    public UserType getUserType(final String id) {
        if (StringUtil.isBlank(id)) {
            return UserType.UNKNOWN;
        }
        try {
            return userTypeCache.get(id);
        } catch (final ExecutionException e) {
            logger.warn("Failed to get an user type.", e);
            return UserType.UNKNOWN;
        }
    }

    public InputStream getDriveContent(final Function<IGraphServiceClient, IDriveRequestBuilder> builder, final String id) {
        return builder.apply(client).items(id).content().buildRequest().get();
    }

    public IPermissionCollectionPage getDrivePermissions(final Function<IGraphServiceClient, IDriveRequestBuilder> builder,
            final String id) {
        final IPermissionCollectionPage value = builder.apply(client).items(id).permissions().buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    public IDriveItemCollectionPage getDriveItemPage(final Function<IGraphServiceClient, IDriveRequestBuilder> builder, final String id) {
        final IDriveItemCollectionPage value;
        if (id == null) {
            value = builder.apply(client).root().children().buildRequest().get();
        } else {
            value = builder.apply(client).items(id).children().buildRequest().get();
        }
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    public User getUser(final String userId, final List<? extends Option> options) {
        final User value = client.users(userId).buildRequest(options).get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    public void getUsers(final List<QueryOption> options, final Consumer<User> consumer) {
        IUserCollectionPage page = getUserPage(options);
        page.getCurrentPage().stream().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = getNextUserPage(page);
            page.getCurrentPage().stream().forEach(consumer::accept);
        }
    }

    protected IUserCollectionPage getUserPage(final List<? extends Option> options) {
        final IUserCollectionPage value = client.users().buildRequest(options).get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    protected IUserCollectionPage getNextUserPage(final IUserCollectionPage page) {
        if (page.getNextPage() == null) {
            return null;
        }
        final IUserCollectionPage value = page.getNextPage().buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    public String[] getGroupIdsByEmail(final String email) {
        try {
            return groupIdCache.get(email);
        } catch (ExecutionException e) {
            logger.warn("Failed to get group ids.", e);
            return StringUtil.EMPTY_STRINGS;
        }
    }

    public void getGroups(final List<QueryOption> options, final Consumer<Group> consumer) {
        IGroupCollectionPage page = getGroupPage(options);
        page.getCurrentPage().stream().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = getNextGroupPage(page);
            page.getCurrentPage().forEach(consumer::accept);
        }
    }

    protected IGroupCollectionPage getGroupPage(final List<? extends Option> options) {
        final IGroupCollectionPage value = client.groups().buildRequest(options).get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    protected IGroupCollectionPage getNextGroupPage(final IGroupCollectionPage page) {
        if (page.getNextPage() == null) {
            return null;
        }
        final IGroupCollectionPage value = page.getNextPage().buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    public IDriveItemCollectionPage getNextItemPage(final IDriveItemCollectionPage page) {
        if (page.getNextPage() == null) {
            return null;
        }
        final IDriveItemCollectionPage value = page.getNextPage().buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    public INotebookCollectionPage getNotebookPage(final Function<IGraphServiceClient, IOnenoteRequestBuilder> builder) {
        final INotebookCollectionPage value = builder.apply(client).notebooks().buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
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

    public String getNotebookContent(final Function<IGraphServiceClient, IOnenoteRequestBuilder> builder, final String id) {
        final List<OnenoteSection> sections = getSections(builder.apply(client).notebooks(id));
        Collections.reverse(sections);
        return sections.stream().map(section -> getSectionContents(builder.apply(client), section)).collect(Collectors.joining("\n"));
    }

    public INotebookCollectionPage getNextNotebookPage(final INotebookCollectionPage page) {
        if (page.getNextPage() == null) {
            return null;
        }
        final INotebookCollectionPage value = page.getNextPage().buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    public Site getSite(final String id) {
        final Site value = client.sites(StringUtil.isNotBlank(id) ? id : "root").buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    protected boolean expired(final GraphServiceException e) {
        if (logger.isDebugEnabled()) {
            logger.debug("Failed to process a request.", e);
        }
        return INVALID_AUTHENTICATION_TOKEN.equals(e.getServiceError().code);
    }

    public ISiteCollectionPage getSites() {
        final ISiteCollectionPage value = client.sites().buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    public ISiteCollectionPage getNextSitePage(final ISiteCollectionPage page) {
        if (page.getNextPage() == null) {
            return null;
        }
        final ISiteCollectionPage value = page.getNextPage().buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    public Drive getDrive(final String driveId) {
        final Drive value = client.drives(driveId).buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    public void getDrives(final Consumer<Drive> consumer) {
        IDriveCollectionPage page = getDrives();
        page.getCurrentPage().stream().forEach(consumer::accept);
        while (page.getNextPage() != null) {
            page = getNextDrivePage(page);
            page.getCurrentPage().stream().forEach(consumer::accept);
        }
    }

    protected IDriveCollectionPage getDrives() {
        final IDriveCollectionPage value = client.drives().buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    protected IDriveCollectionPage getNextDrivePage(final IDriveCollectionPage page) {
        if (page.getNextPage() == null) {
            return null;
        }
        final IDriveCollectionPage value = page.getNextPage().buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    public IPermissionCollectionPage getNextPermissionPage(final IPermissionCollectionPage page) {
        if (page.getNextPage() == null) {
            return null;
        }
        final IPermissionCollectionPage value = page.getNextPage().buildRequest().get();
        if (logger.isDebugEnabled()) {
            logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
        }
        return value;
    }

    protected static class AuthenticationProvider implements IAuthenticationProvider, TimeoutTarget {

        protected final String tenant;
        protected final String clientId;
        protected final String clientSecret;
        protected final long accessTimeout;
        protected String accessToken;

        protected AuthenticationProvider(final Map<String, String> params) {
            tenant = params.getOrDefault(TENANT_PARAM, StringUtil.EMPTY);
            clientId = params.getOrDefault(CLIENT_ID_PARAM, StringUtil.EMPTY);
            clientSecret = params.getOrDefault(CLIENT_SECRET_PARAM, StringUtil.EMPTY);
            if (tenant.isEmpty() || clientId.isEmpty() || clientSecret.isEmpty()) {
                throw new DataStoreException("parameter '" + //
                        TENANT_PARAM + "', '" + //
                        CLIENT_ID_PARAM + "', '" + //
                        CLIENT_SECRET_PARAM + "' is required");
            }

            this.accessTimeout = Long.parseLong(params.getOrDefault(ACCESS_TIMEOUT, "30000"));

            refreshToken();
        }

        protected void refreshToken() {
            if (logger.isDebugEnabled()) {
                logger.debug("Refreshing access token.");
            }
            final ExecutorService executorService = Executors.newFixedThreadPool(1);
            try {
                final AuthenticationContext context =
                        new AuthenticationContext("https://login.microsoftonline.com/" + tenant + "/", false, executorService);
                final AuthenticationResult result =
                        context.acquireToken("https://graph.microsoft.com", new ClientCredential(clientId, clientSecret), null)
                                .get(accessTimeout, TimeUnit.MILLISECONDS);
                if (logger.isDebugEnabled()) {
                    logger.debug("Access Token: {}", result.getAccessToken());
                }
                accessToken = result.getAccessToken();
            } catch (final Exception e) {
                throw new DataStoreException("Failed to get an access token.", e);
            } finally {
                executorService.shutdown();
            }
        }

        @Override
        public void expired() {
            try {
                refreshToken();
            } catch (final Exception e) {
                logger.warn("Failed to refresh an access token.", e);
            }
        }

        @Override
        public void authenticateRequest(final IHttpRequest request) {
            request.addHeader("Authorization", "Bearer " + accessToken);
        }

    }

}
