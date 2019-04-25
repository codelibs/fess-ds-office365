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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.crawler.extractor.impl.TikaExtractor;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.graph.http.GraphServiceException;
import com.microsoft.graph.logger.DefaultLogger;
import com.microsoft.graph.logger.LoggerLevel;
import com.microsoft.graph.models.extensions.Drive;
import com.microsoft.graph.models.extensions.IGraphServiceClient;
import com.microsoft.graph.models.extensions.OnenotePage;
import com.microsoft.graph.models.extensions.OnenoteSection;
import com.microsoft.graph.models.extensions.Site;
import com.microsoft.graph.models.extensions.User;
import com.microsoft.graph.options.Option;
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
import com.microsoft.graph.requests.extensions.ISiteCollectionPage;
import com.microsoft.graph.requests.extensions.IUserCollectionPage;

public class Office365Client implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Office365Client.class);

    protected static final String INVALID_AUTHENTICATION_TOKEN = "InvalidAuthenticationToken";

    protected volatile IGraphServiceClient client;

    protected final String tenant;

    protected final String clientId;

    protected final String clientSecret;

    protected final long accessTimeout;

    public Office365Client(final String tenant, final String clientId, final String clientSecret, final long accessTimeout) {
        this.tenant = tenant;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.accessTimeout = accessTimeout;
        connect(getAccessToken());
    }

    protected void reconnect() {
        if (logger.isDebugEnabled()) {
            logger.debug("Recreating a client.");
        }
        final IGraphServiceClient oldClient = client;
        try {
            connect(getAccessToken());
        } finally {
            if (oldClient != null) {
                try {
                    oldClient.shutdown();
                } catch (final Exception e) {
                    // ignore
                }
            }
        }
    }

    protected String getAccessToken() {
        final ExecutorService executorService = Executors.newFixedThreadPool(1);
        try {
            final AuthenticationContext context =
                    new AuthenticationContext("https://login.microsoftonline.com/" + tenant + "/", false, executorService);
            final AuthenticationResult result =
                    context.acquireToken("https://graph.microsoft.com", new ClientCredential(clientId, clientSecret), null)
                            .get(accessTimeout, TimeUnit.MILLISECONDS);
            if (logger.isDebugEnabled()) {
                logger.debug("Access Token: " + result.getAccessToken());
            }
            return result.getAccessToken();
        } catch (final Exception e) {
            throw new DataStoreException("Failed to get an access token.", e);
        } finally {
            executorService.shutdown();
        }
    }

    public void connect(final String accessToken) {
        if (logger.isDebugEnabled()) {
            logger.debug("Connecting with " + accessToken);
        }
        try {
            client = GraphServiceClient.builder() //
                    .authenticationProvider(request -> request.addHeader("Authorization", "Bearer " + accessToken)) //
                    .logger(new DefaultLogger() {
                        @Override
                        public void logDebug(final String message) {
                            if (LoggerLevel.DEBUG == getLoggingLevel()) {
                                logger.debug(message);
                            }
                        }

                        @Override
                        public void logError(final String message, final Throwable t) {
                            if (t instanceof GraphServiceException && expired((GraphServiceException) t)) {
                                logger.debug(message, t);
                            } else {
                                logger.error(message, t);
                            }
                        }
                    }).buildClient();
        } catch (final Exception e) {
            throw new DataStoreException("Failed to create a client.", e);
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.shutdown();
        }
    }

    public InputStream getContent(final Function<IGraphServiceClient, IDriveRequestBuilder> builder, final String id) {
        final Supplier<InputStream> supplier = () -> builder.apply(client).items(id).content().buildRequest().get();
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public IDriveItemCollectionPage getItemPage(final Function<IGraphServiceClient, IDriveRequestBuilder> builder, final String id) {
        final Supplier<IDriveItemCollectionPage> supplier = () -> {
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
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }

    }

    public User getUser(final String userId, final List<? extends Option> options) {
        final Supplier<User> supplier = () -> {
            final User value = client.users(userId).buildRequest(options).get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public IUserCollectionPage getUserPage(final List<? extends Option> options) {
        final Supplier<IUserCollectionPage> supplier = () -> {
            final IUserCollectionPage value = client.users().buildRequest(options).get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public IGroupCollectionPage getGroupPage(final List<? extends Option> options) {
        final Supplier<IGroupCollectionPage> supplier = () -> {
            final IGroupCollectionPage value = client.groups().buildRequest(options).get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public IDriveItemCollectionPage getNextItemPage(final IDriveItemCollectionPage page) {
        final Supplier<IDriveItemCollectionPage> supplier = () -> {
            final IDriveItemCollectionPage value = page.getNextPage().buildRequest().get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public IUserCollectionPage getNextUserPage(final IUserCollectionPage page) {
        final Supplier<IUserCollectionPage> supplier = () -> {
            final IUserCollectionPage value = page.getNextPage().buildRequest().get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public IGroupCollectionPage getNextGroupPage(final IGroupCollectionPage page) {
        final Supplier<IGroupCollectionPage> supplier = () -> {
            final IGroupCollectionPage value = page.getNextPage().buildRequest().get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public INotebookCollectionPage getNotebookPage(final Function<IGraphServiceClient, IOnenoteRequestBuilder> builder) {
        final Supplier<INotebookCollectionPage> supplier = () -> {
            final INotebookCollectionPage value = builder.apply(client).notebooks().buildRequest().get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
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
        final Supplier<String> supplier = () -> {
            final List<OnenoteSection> sections = getSections(builder.apply(client).notebooks(id));
            Collections.reverse(sections);
            return sections.stream().map(section -> getSectionContents(builder.apply(client), section)).collect(Collectors.joining("\n"));
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public INotebookCollectionPage getNextNotebookPage(final INotebookCollectionPage page) {
        final Supplier<INotebookCollectionPage> supplier = () -> {
            final INotebookCollectionPage value = page.getNextPage().buildRequest().get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public Site getSite(final String id) {
        final Supplier<Site> supplier = () -> {
            final Site value = client.sites(StringUtil.isNotBlank(id) ? id : "root").buildRequest().get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    protected boolean expired(final GraphServiceException e) {
        if (logger.isDebugEnabled()) {
            logger.debug("Failed to process a request.", e);
        }
        return INVALID_AUTHENTICATION_TOKEN.equals(e.getServiceError().code);
    }

    public IDriveCollectionPage getDrives() {
        final Supplier<IDriveCollectionPage> supplier = () -> {
            final IDriveCollectionPage value = client.drives().buildRequest().get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public IDriveCollectionPage getNextDrivePage(final IDriveCollectionPage page) {
        final Supplier<IDriveCollectionPage> supplier = () -> {
            final IDriveCollectionPage value = page.getNextPage().buildRequest().get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public ISiteCollectionPage getSites() {
        final Supplier<ISiteCollectionPage> supplier = () -> {
            final ISiteCollectionPage value = client.sites().buildRequest().get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public ISiteCollectionPage getNextSitePage(final ISiteCollectionPage page) {
        final Supplier<ISiteCollectionPage> supplier = () -> {
            final ISiteCollectionPage value = page.getNextPage().buildRequest().get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }

    public Drive getDrive(final String driveId) {
        final Supplier<Drive> supplier = () -> {
            final Drive value = client.drives(driveId).buildRequest().get();
            if (logger.isDebugEnabled()) {
                logger.debug("raw: {}", value != null ? value.getRawObject() : "null");
            }
            return value;
        };
        try {
            return supplier.get();
        } catch (final GraphServiceException e) {
            if (expired(e)) {
                reconnect();
                return supplier.get();
            }
            throw e;
        }
    }
}
