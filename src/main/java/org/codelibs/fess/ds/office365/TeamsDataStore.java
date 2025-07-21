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

import java.io.IOException;
import java.io.StringReader;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.charfilter.HTMLStripCharFilter;
import org.codelibs.core.exception.InterruptedRuntimeException;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.core.stream.StreamUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.ds.office365.client.Office365Client;
import org.codelibs.fess.ds.office365.client.Office365Client.UserType;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.exception.FessSystemException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.helper.PermissionHelper;
import org.codelibs.fess.helper.SystemHelper;
import org.codelibs.fess.opensearch.config.exentity.DataConfig;
import org.codelibs.fess.util.ComponentUtil;

import com.microsoft.graph.models.AadUserConversationMember;
import com.microsoft.graph.models.BodyType;
import com.microsoft.graph.models.Channel;
import com.microsoft.graph.models.ChatMessage;
import com.microsoft.graph.models.ChatMessageFromIdentitySet;
import com.microsoft.graph.models.ConversationMember;
import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.ItemBody;

/**
 * This class is a data store for crawling and indexing content from Microsoft Teams.
 * It supports crawling messages from teams, channels, and chats.
 * It extracts message content, metadata, attachments, and permissions for indexing.
 */
public class TeamsDataStore extends Office365DataStore {

    /**
     * Default constructor.
     */
    public TeamsDataStore() {
        super();
    }

    /** Key for the message title. */
    private static final String MESSAGE_TITLE = "title";

    /** Key for the message content. */
    private static final String MESSAGE_CONTENT = "content";

    private static final Logger logger = LogManager.getLogger(TeamsDataStore.class);

    // parameters
    /** Parameter name for the team ID. */
    private static final String TEAM_ID = "team_id";
    /** Parameter name for the exclude team IDs. */
    private static final String EXCLUDE_TEAM_ID = "exclude_team_ids";
    /** Parameter name for the include visibility. */
    private static final String INCLUDE_VISIBILITY = "include_visibility";
    /** Parameter name for the channel ID. */
    private static final String CHANNEL_ID = "channel_id";
    /** Parameter name for the chat ID. */
    private static final String CHAT_ID = "chat_id";
    /** Parameter name for the number of threads. */
    protected static final String NUMBER_OF_THREADS = "number_of_threads";
    /** Parameter name for default permissions. */
    protected static final String DEFAULT_PERMISSIONS = "default_permissions";
    /** Parameter name for ignoring replies. */
    private static final String IGNORE_REPLIES = "ignore_replies";
    /** Parameter name for appending attachments. */
    private static final String APPEND_ATTACHMENT = "append_attachment";
    /** Parameter name for ignoring system events. */
    private static final String IGNORE_SYSTEM_EVENTS = "ignore_system_events";
    /** Parameter name for the title date format. */
    private static final String TITLE_DATEFORMAT = "title_dateformat";
    /** Parameter name for the title timezone offset. */
    private static final String TITLE_TIMEZONE = "title_timezone_offset";

    // scripts
    /** Key for the message object in the script map. */
    private static final String MESSAGE = "message";
    /** Key for the message attachments in the script map (internal use only). */
    private static final String MESSAGE_ATTACHMENTS = "attachments"; // internal user only
    /** Key for the message body in the script map. */
    private static final String MESSAGE_BODY = "body";
    /** Key for the message channel identity in the script map. */
    private static final String MESSAGE_CHANNEL_IDENTITY = "channel_identity";
    /** Key for the message chat ID in the script map. */
    private static final String MESSAGE_CHAT_ID = "chat_id";
    /** Key for the message created date time in the script map. */
    private static final String MESSAGE_CREATED_DATE_TIME = "created_date_time";
    /** Key for the message deleted date time in the script map. */
    private static final String MESSAGE_DELETED_DATE_TIME = "deleted_date_time";
    /** Key for the message eTag in the script map. */
    private static final String MESSAGE_ETAG = "etag";
    /** Key for the message from in the script map. */
    private static final String MESSAGE_FROM = "from";
    /** Key for the message hosted contents in the script map (internal use only). */
    private static final String MESSAGE_HOSTED_CONTENTS = "hosted_contents"; // internal user only
    /** Key for the message ID in the script map. */
    private static final String MESSAGE_ID = "id";
    /** Key for the message importance in the script map. */
    private static final String MESSAGE_IMPORTANCE = "importance";
    /** Key for the message last edited date time in the script map. */
    private static final String MESSAGE_LAST_EDITED_DATE_TIME = "last_edited_date_time";
    /** Key for the message last modified date time in the script map. */
    private static final String MESSAGE_LAST_MODIFIED_DATE_TIME = "last_modified_date_time";
    /** Key for the message locale in the script map. */
    private static final String MESSAGE_LOCALE = "locale";
    /** Key for the message mentions in the script map. */
    private static final String MESSAGE_MENTIONS = "mentions";
    /** Key for the message replies in the script map (internal use only). */
    private static final String MESSAGE_REPLIES = "replies"; // internal user only
    /** Key for the message reply to ID in the script map. */
    private static final String MESSAGE_REPLY_TO_ID = "reply_to_id";
    /** Key for the message subject in the script map. */
    private static final String MESSAGE_SUBJECT = "subject";
    /** Key for the message summary in the script map. */
    private static final String MESSAGE_SUMMARY = "summary";
    /** Key for the message web URL in the script map. */
    private static final String MESSAGE_WEB_URL = "web_url";
    /** Key for the message roles in the script map. */
    private static final String MESSAGE_ROLES = "roles";
    /** Key for the parent object in the script map. */
    private static final String PARENT = "parent";
    /** Key for the team object in the script map. */
    private static final String TEAM = "team";
    /** Key for the channel object in the script map. */
    private static final String CHANNEL = "channel";

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(TEAM_ID, getTeamId(paramMap));
        configMap.put(EXCLUDE_TEAM_ID, getExcludeTeamIds(paramMap));
        configMap.put(INCLUDE_VISIBILITY, getIncludeVisibilities(paramMap));
        configMap.put(CHANNEL_ID, getChannelId(paramMap));
        configMap.put(CHAT_ID, getChatId(paramMap));
        configMap.put(IGNORE_REPLIES, isIgnoreReplies(paramMap));
        configMap.put(APPEND_ATTACHMENT, isAppendAttachment(paramMap));
        configMap.put(TITLE_DATEFORMAT, getTitleDateformat(paramMap));
        configMap.put(TITLE_TIMEZONE, getTitleTimezone(paramMap));
        configMap.put(IGNORE_SYSTEM_EVENTS, isIgnoreSystemEvents(paramMap));

        if (logger.isDebugEnabled()) {
            logger.debug("configMap: {}", configMap);
        }

        final ExecutorService executorService = newFixedThreadPool(Integer.parseInt(paramMap.getAsString(NUMBER_OF_THREADS, "1")));
        try (final Office365Client client = createClient(paramMap)) {
            processTeamMessages(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap, client);
            processChatMessages(dataConfig, callback, paramMap, scriptMap, defaultDataMap, configMap, client);

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
     * Processes chat messages.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param configMap The configuration map.
     * @param client The Office365Client.
     */
    protected void processChatMessages(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap,
            final Office365Client client) {
        final String chatId = (String) configMap.get(CHAT_ID);
        if (StringUtil.isNotBlank(chatId)) {
            final List<ChatMessage> msgList = new ArrayList<>();
            client.getChatMessages(Collections.emptyList(), m -> msgList.add(m), chatId);
            if (!msgList.isEmpty()) {
                final ChatMessage m = createChatMessage(msgList, client);
                processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap, getGroupRoles(client, chatId), m,
                        map -> map.put("messages", msgList), client);
            }
        }
    }

    /**
     * Creates a chat message from a list of messages.
     *
     * @param msgList The list of chat messages.
     * @param client The Office365Client.
     * @return A new chat message.
     */
    protected ChatMessage createChatMessage(final List<ChatMessage> msgList, final Office365Client client) {
        final ChatMessage msg = new ChatMessage();
        final ChatMessage defaultMsg = msgList.get(0);
        msg.attachments = new ArrayList<>();
        msgList.stream().forEach(m -> msg.attachments.addAll(m.attachments));
        msg.body = new ItemBody();
        msg.body.contentType = BodyType.TEXT;
        final StringBuilder bodyBuf = new StringBuilder(1000);
        final Map<String, Object> configMap = new HashMap<>();
        configMap.put(APPEND_ATTACHMENT, false);
        msgList.stream().forEach(m -> bodyBuf.append(getConent(configMap, m, client)));
        msg.body.content = bodyBuf.toString();
        msg.channelIdentity = defaultMsg.channelIdentity;
        msg.createdDateTime = defaultMsg.createdDateTime;
        msg.deletedDateTime = defaultMsg.deletedDateTime;
        msg.etag = defaultMsg.etag;
        msg.from = defaultMsg.from;
        msg.importance = defaultMsg.importance;
        msg.lastEditedDateTime = defaultMsg.lastEditedDateTime;
        msg.lastModifiedDateTime = defaultMsg.lastModifiedDateTime;
        msg.locale = defaultMsg.locale;
        msg.mentions = new ArrayList<>();
        msgList.stream().forEach(m -> msg.mentions.addAll(m.mentions));
        msg.messageType = defaultMsg.messageType;
        msg.policyViolation = defaultMsg.policyViolation;
        msg.reactions = new ArrayList<>();
        msgList.stream().forEach(m -> msg.reactions.addAll(m.reactions));
        msg.replyToId = defaultMsg.replyToId;
        msg.subject = defaultMsg.subject;
        msg.summary = defaultMsg.summary;
        msg.webUrl = "https://teams.microsoft.com/_#/conversations/" + defaultMsg.chatId + "?ctx=chat";
        msg.hostedContents = defaultMsg.hostedContents;
        msg.replies = defaultMsg.replies;
        return msg;
    }

    /**
     * Processes team messages.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map.
     * @param defaultDataMap The default data map.
     * @param configMap The configuration map.
     * @param client The Office365Client.
     */
    protected void processTeamMessages(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final Map<String, Object> configMap,
            final Office365Client client) {
        final String teamId = (String) configMap.get(TEAM_ID);
        if (StringUtil.isNotBlank(teamId)) {
            final Group g = client.getGroupById(teamId);
            if (g == null) {
                throw new DataStoreException("Could not find a team: " + teamId);
            }
            final String channelId = (String) configMap.get(CHANNEL_ID);
            if (StringUtil.isNotBlank(channelId)) {
                final Channel c = client.getChannelById(teamId, channelId);
                if (c == null) {
                    throw new DataStoreException("Could not find a channel: " + channelId);
                }
                client.getTeamMessages(Collections.emptyList(), m -> {
                    final Map<String, Object> message = processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap,
                            defaultDataMap, getGroupRoles(client, g.id, c.id), m, map -> {
                                map.put(TEAM, g);
                                map.put(CHANNEL, c);
                            }, client);
                    if (message != null && !((Boolean) configMap.get(IGNORE_REPLIES)).booleanValue()) {
                        client.getTeamReplyMessages(Collections.emptyList(), r -> {
                            processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                                    getGroupRoles(client, g.id, c.id), r, map -> {
                                        map.put(TEAM, g);
                                        map.put(CHANNEL, c);
                                        map.put(PARENT, message);
                                    }, client);
                        }, teamId, channelId, (String) message.get(MESSAGE_ID));
                    }
                }, teamId, channelId);
            } else {
                client.getChannels(Collections.emptyList(), c -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Channel: {} : {}", c.id, ToStringBuilder.reflectionToString(c));
                    } else {
                        logger.info("Channel: {} : {}", c.id, c.displayName);
                    }
                    client.getTeamMessages(Collections.emptyList(), m -> {
                        final Map<String, Object> message = processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap,
                                defaultDataMap, getGroupRoles(client, g.id, c.id), m, map -> {
                                    map.put(TEAM, g);
                                    map.put(CHANNEL, c);
                                }, client);
                        if (message != null && !((Boolean) configMap.get(IGNORE_REPLIES)).booleanValue()) {
                            client.getTeamReplyMessages(Collections.emptyList(), r -> {
                                processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                                        getGroupRoles(client, g.id, c.id), r, map -> {
                                            map.put(TEAM, g);
                                            map.put(CHANNEL, c);
                                            map.put(PARENT, message);
                                        }, client);
                            }, teamId, c.id, (String) message.get(MESSAGE_ID));
                        }
                    }, teamId, c.id);
                }, teamId);
            }
        } else if (teamId == null) {
            final Set<String> excludeGroupIdSet = getExcludeGroupIdSet(configMap, client);
            if (logger.isDebugEnabled()) {
                logger.debug("Exclude Group IDs: {}", excludeGroupIdSet);
            }
            client.geTeams(Collections.emptyList(), g -> {
                if (logger.isDebugEnabled()) {
                    logger.debug("Team: {} : {}", g.id, ToStringBuilder.reflectionToString(g));
                } else {
                    logger.info("Team: {} : {}", g.id, g.displayName);
                }
                if (excludeGroupIdSet.contains(g.id)) {
                    logger.info("Skpped Team: {} : {}", g.id, g.displayName);
                    return;
                }
                if (!isTargetVisibility(configMap, g.visibility)) {
                    logger.info("Skpped Team: {} : {} : {}", g.id, g.displayName, g.visibility);
                    return;
                }
                client.getChannels(Collections.emptyList(), c -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Channel: {} : {}", c.id, ToStringBuilder.reflectionToString(c));
                    } else {
                        logger.info("Channel: {} : {}", c.id, c.displayName);
                    }
                    client.getTeamMessages(Collections.emptyList(), m -> {
                        final Map<String, Object> message = processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap,
                                defaultDataMap, getGroupRoles(client, g.id, c.id), m, map -> {
                                    map.put(TEAM, g);
                                    map.put(CHANNEL, c);
                                }, client);
                        if (message != null && !((Boolean) configMap.get(IGNORE_REPLIES)).booleanValue()) {
                            client.getTeamReplyMessages(Collections.emptyList(), r -> {
                                processChatMessage(dataConfig, callback, configMap, paramMap, scriptMap, defaultDataMap,
                                        getGroupRoles(client, g.id, c.id), r, map -> {
                                            map.put(TEAM, g);
                                            map.put(CHANNEL, c);
                                            map.put(PARENT, message);
                                        }, client);
                            }, g.id, c.id, (String) message.get(MESSAGE_ID));
                        }
                    }, g.id, c.id);
                }, g.id);
            });
        }
    }

    /**
     * Gets the set of excluded group IDs based on configured exclude team IDs.
     *
     * @param configMap The configuration map containing exclude team ID settings.
     * @param client The Office365Client for group lookups.
     * @return A set of group IDs to exclude from processing.
     */
    protected Set<String> getExcludeGroupIdSet(final Map<String, Object> configMap, final Office365Client client) {
        final String[] teamIds = (String[]) configMap.get(EXCLUDE_TEAM_ID);
        return StreamUtil.stream(teamIds).get(stream -> stream.map(teamId -> {
            final Group g = client.getGroupById(teamId);
            if (g == null) {
                throw new DataStoreException("Could not find a team: " + teamId);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Team -> Group: {} -> {}", teamId, g.id);
            }
            return g.id;
        }).collect(Collectors.toSet()));
    }

    /**
     * Determines if a team visibility level is included in the target visibility settings.
     *
     * @param configMap The configuration map containing visibility settings.
     * @param visibility The visibility level to check.
     * @return true if the visibility should be processed, false otherwise.
     */
    protected boolean isTargetVisibility(final Map<String, Object> configMap, final String visibility) {
        final String[] visibilities = (String[]) configMap.get(INCLUDE_VISIBILITY);
        if (visibilities.length == 0) {
            return true;
        }
        for (final String value : visibilities) {
            if (value.equalsIgnoreCase(visibility)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Gets the date formatter for message titles.
     *
     * @param paramMap The data store parameters containing date format settings.
     * @return The configured DateTimeFormatter for titles.
     */
    protected DateTimeFormatter getTitleDateformat(final DataStoreParams paramMap) {
        return DateTimeFormatter.ofPattern(paramMap.getAsString(TITLE_DATEFORMAT, "yyyy/MM/dd'T'HH:mm:ss"));
    }

    /**
     * Gets the timezone offset for message titles.
     *
     * @param paramMap The data store parameters containing timezone settings.
     * @return The configured ZoneOffset for titles.
     */
    protected ZoneOffset getTitleTimezone(final DataStoreParams paramMap) {
        return ZoneOffset.of(paramMap.getAsString(TITLE_TIMEZONE, "Z"));
    }

    /**
     * Determines if system events should be ignored during processing.
     *
     * @param paramMap The data store parameters containing system event settings.
     * @return true if system events should be ignored, false otherwise.
     */
    protected Object isIgnoreSystemEvents(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_SYSTEM_EVENTS, Constants.TRUE));
    }

    /**
     * Determines if attachments should be appended to message content.
     *
     * @param paramMap The data store parameters containing attachment settings.
     * @return true if attachments should be appended, false otherwise.
     */
    protected Object isAppendAttachment(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(APPEND_ATTACHMENT, Constants.TRUE));
    }

    /**
     * Determines if reply messages should be ignored during processing.
     *
     * @param paramMap The data store parameters containing reply settings.
     * @return true if replies should be ignored, false otherwise.
     */
    protected boolean isIgnoreReplies(final DataStoreParams paramMap) {
        return Constants.TRUE.equalsIgnoreCase(paramMap.getAsString(IGNORE_REPLIES, Constants.FALSE));
    }

    /**
     * Gets the configured team ID for processing a specific team.
     *
     * @param paramMap The data store parameters containing team ID setting.
     * @return The team ID to process, or null if not specified.
     */
    protected String getTeamId(final DataStoreParams paramMap) {
        return paramMap.getAsString(TEAM_ID);
    }

    /**
     * Gets the array of team IDs to exclude from processing.
     *
     * @param paramMap The data store parameters containing exclude team ID settings.
     * @return An array of team IDs to exclude.
     */
    protected String[] getExcludeTeamIds(final DataStoreParams paramMap) {
        final String idStr = paramMap.getAsString(EXCLUDE_TEAM_ID);
        if (StringUtil.isBlank(idStr)) {
            return new String[0];
        }
        return StreamUtil.split(idStr, ",")
                .get(stream -> stream.map(s -> s.trim()).filter(StringUtil::isNotBlank).toArray(n -> new String[n]));
    }

    /**
     * Gets the array of team visibility levels to include in processing.
     *
     * @param paramMap The data store parameters containing visibility settings.
     * @return An array of visibility levels to include.
     */
    protected String[] getIncludeVisibilities(final DataStoreParams paramMap) {
        final String idStr = paramMap.getAsString(INCLUDE_VISIBILITY);
        if (StringUtil.isBlank(idStr)) {
            return new String[0];
        }
        return StreamUtil.split(idStr, ",")
                .get(stream -> stream.map(s -> s.trim()).filter(StringUtil::isNotBlank).toArray(n -> new String[n]));
    }

    /**
     * Gets the configured channel ID for processing a specific channel.
     *
     * @param paramMap The data store parameters containing channel ID setting.
     * @return The channel ID to process, or null if not specified.
     */
    protected String getChannelId(final DataStoreParams paramMap) {
        return paramMap.getAsString(CHANNEL_ID);
    }

    /**
     * Gets the configured chat ID for processing a specific chat.
     *
     * @param paramMap The data store parameters containing chat ID setting.
     * @return The chat ID to process, or null if not specified.
     */
    protected String getChatId(final DataStoreParams paramMap) {
        return paramMap.getAsString(CHAT_ID);
    }

    /**
     * Creates a new Office365Client instance for API communication.
     *
     * @param params The data store parameters containing authentication settings.
     * @return A new Office365Client instance.
     */
    protected Office365Client createClient(final DataStoreParams params) {
        return new Office365Client(params);
    }

    /**
     * Gets the group roles for members of a specific team channel.
     *
     * @param client The Office365Client for API communication.
     * @param teamId The team ID.
     * @param channelId The channel ID.
     * @return A list of group role permissions.
     */
    protected List<String> getGroupRoles(final Office365Client client, final String teamId, final String channelId) {
        final List<String> permissions = new ArrayList<>();
        client.getChannelMembers(Collections.emptyList(), m -> getGroupRoles(client, permissions, m), teamId, channelId);
        return permissions;
    }

    /**
     * Gets the group roles for members of a specific chat.
     *
     * @param client The Office365Client for API communication.
     * @param chatId The chat ID.
     * @return A list of group role permissions.
     */
    protected List<String> getGroupRoles(final Office365Client client, final String chatId) {
        final List<String> permissions = new ArrayList<>();
        client.getChatMembers(Collections.emptyList(), m -> getGroupRoles(client, permissions, m), chatId);
        return permissions;
    }

    /**
     * Extracts and adds group roles from a conversation member to the permissions list.
     *
     * @param client The Office365Client for API communication.
     * @param permissions The list to add permissions to.
     * @param m The conversation member to process.
     */
    protected void getGroupRoles(final Office365Client client, final List<String> permissions, final ConversationMember m) {
        final SystemHelper systemHelper = ComponentUtil.getSystemHelper();
        if (logger.isDebugEnabled()) {
            logger.debug("Member: {} : {}", m.id, ToStringBuilder.reflectionToString(m));
        } else {
            logger.info("Member: {} : {}", m.id, m.displayName);
        }
        if (m instanceof AadUserConversationMember member) {
            final String id = member.userId;
            final String email = member.email;
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
    }

    /**
     * Determines if a chat message is a system event that should be filtered.
     *
     * @param configMap The configuration map containing system event settings.
     * @param message The chat message to check.
     * @return true if the message is a system event and should be ignored, false otherwise.
     */
    protected boolean isSystemEvent(final Map<String, Object> configMap, final ChatMessage message) {
        if (((Boolean) configMap.get(IGNORE_SYSTEM_EVENTS)).booleanValue()) {
            if (message.body != null && "<systemEventMessage/>".equals(message.body.content)) {
                return true;
            }

            return false;
        }
        return false;
    }

    /**
     * Processes a chat message for indexing, extracting content and metadata.
     *
     * @param dataConfig The data configuration.
     * @param callback The index update callback.
     * @param configMap The configuration map.
     * @param paramMap The data store parameters.
     * @param scriptMap The script map for field mappings.
     * @param defaultDataMap The default data map.
     * @param permissions The list of permissions for the message.
     * @param message The chat message to process.
     * @param resultAppender Consumer to append additional result data.
     * @param client The Office365Client for API communication.
     * @return A map containing the processed message data, or null if the message was filtered.
     */
    protected Map<String, Object> processChatMessage(final DataConfig dataConfig, final IndexUpdateCallback callback,
            final Map<String, Object> configMap, final DataStoreParams paramMap, final Map<String, String> scriptMap,
            final Map<String, Object> defaultDataMap, final List<String> permissions, final ChatMessage message,
            final Consumer<Map<String, Object>> resultAppender, final Office365Client client) {
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        if (logger.isDebugEnabled()) {
            logger.debug("Message: {} : {}", message.id, ToStringBuilder.reflectionToString(message));
        } else {
            logger.info("Message: {} : {}", message.id, message.webUrl);
        }

        if (isSystemEvent(configMap, message)) {
            logger.info("Message {} is a system event.", message.id);
            return null;
        }

        final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
        final Map<String, Object> resultMap = new LinkedHashMap<>(paramMap.asMap());
        final Map<String, Object> messageMap = new HashMap<>();
        final StatsKeyObject statsKey = new StatsKeyObject(message.webUrl);
        paramMap.put(Constants.CRAWLER_STATS_KEY, statsKey);
        try {
            crawlerStatsHelper.begin(statsKey);

            messageMap.put(MESSAGE_CONTENT, getConent(configMap, message, client));
            messageMap.put(MESSAGE_TITLE, getTitle(configMap, message));

            messageMap.put(MESSAGE_ATTACHMENTS, message.attachments);
            messageMap.put(MESSAGE_BODY, message.body);
            messageMap.put(MESSAGE_CHANNEL_IDENTITY, message.channelIdentity);
            messageMap.put(MESSAGE_CHAT_ID, message.chatId);
            messageMap.put(MESSAGE_CREATED_DATE_TIME, message.createdDateTime);
            messageMap.put(MESSAGE_DELETED_DATE_TIME, message.deletedDateTime);
            messageMap.put(MESSAGE_ETAG, message.etag);
            messageMap.put(MESSAGE_FROM, message.from);
            messageMap.put(MESSAGE_HOSTED_CONTENTS, message.hostedContents);
            messageMap.put(MESSAGE_ID, message.id);
            messageMap.put(MESSAGE_IMPORTANCE, message.importance);
            messageMap.put(MESSAGE_LAST_EDITED_DATE_TIME, message.lastEditedDateTime);
            messageMap.put(MESSAGE_LAST_MODIFIED_DATE_TIME, message.lastModifiedDateTime);
            messageMap.put(MESSAGE_LOCALE, message.locale);
            messageMap.put(MESSAGE_MENTIONS, message.mentions);
            messageMap.put(MESSAGE_REPLIES, message.replies);
            messageMap.put(MESSAGE_REPLY_TO_ID, message.replyToId);
            messageMap.put(MESSAGE_SUBJECT, message.subject);
            messageMap.put(MESSAGE_SUMMARY, message.summary);
            messageMap.put(MESSAGE_WEB_URL, message.webUrl);

            resultMap.put(MESSAGE, messageMap);
            resultAppender.accept(resultMap);

            final PermissionHelper permissionHelper = ComponentUtil.getPermissionHelper();
            StreamUtil.split(paramMap.getAsString(DEFAULT_PERMISSIONS), ",")
                    .of(stream -> stream.filter(StringUtil::isNotBlank).map(permissionHelper::encode).forEach(permissions::add));
            messageMap.put(MESSAGE_ROLES, permissions.stream().distinct().collect(Collectors.toList()));

            crawlerStatsHelper.record(statsKey, StatsAction.PREPARED);

            if (logger.isDebugEnabled()) {
                logger.debug("messageMap: {}", messageMap);
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
            failureUrlService.store(dataConfig, errorName, message.webUrl, target);
            crawlerStatsHelper.record(statsKey, StatsAction.ACCESS_EXCEPTION);
        } catch (final Throwable t) {
            logger.warn("Crawling Access Exception at : {}", dataMap, t);
            final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
            failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), message.webUrl, t);
            crawlerStatsHelper.record(statsKey, StatsAction.EXCEPTION);
        } finally {
            crawlerStatsHelper.done(statsKey);
        }

        return messageMap;
    }

    /**
     * Generates a title for the chat message based on sender and timestamp.
     *
     * @param configMap The configuration map containing title formatting settings.
     * @param message The chat message.
     * @return The generated title string.
     */
    protected String getTitle(final Map<String, Object> configMap, final ChatMessage message) {
        final StringBuilder titleBuf = new StringBuilder(100);
        if (message.from != null) {
            final ChatMessageFromIdentitySet identity = message.from;
            if (identity.user != null) {
                titleBuf.append(identity.user.displayName);
            } else if (identity.application != null) {
                titleBuf.append(identity.application.displayName);
            } else if (identity.device != null) {
                titleBuf.append(identity.device.displayName);
            }
        } else {
            titleBuf.append("unknown");
        }

        if (message.createdDateTime != null) {
            titleBuf.append(' ');
            final DateTimeFormatter fmt = (DateTimeFormatter) configMap.get(TITLE_DATEFORMAT);
            final ZoneOffset zone = (ZoneOffset) configMap.get(TITLE_TIMEZONE);
            titleBuf.append(fmt.format(message.createdDateTime.withOffsetSameInstant(zone)));
        }

        return titleBuf.toString();
    }

    /**
     * Extracts and formats the content from a chat message, including attachments if configured.
     *
     * @param configMap The configuration map containing content extraction settings.
     * @param message The chat message.
     * @param client The Office365Client for API communication.
     * @return The formatted message content.
     */
    protected String getConent(final Map<String, Object> configMap, final ChatMessage message, final Office365Client client) {
        final StringBuilder bodyBuf = new StringBuilder(1000);
        if (message.body != null) {
            switch (message.body.contentType) {
            case HTML:
                bodyBuf.append(stripHtmlTags(message.body.content));
                break;
            case TEXT:
                bodyBuf.append(normalizeTextContent(message.body.content));
                break;
            default:
                bodyBuf.append(message.body.content);
                break;
            }
        }
        if (((Boolean) configMap.get(APPEND_ATTACHMENT)).booleanValue() && message.attachments != null) {
            message.attachments.forEach(a -> {
                if (StringUtil.isNotBlank(a.name)) {
                    bodyBuf.append('\n').append(a.name);
                }
                if (a.content != null) {
                    bodyBuf.append('\n').append(a.content);
                } else {
                    bodyBuf.append('\n').append(client.getAttachmentContent(a));
                }
            });
        }
        return bodyBuf.toString();
    }

    /**
     * Normalizes text content by removing attachment tags and extra whitespace.
     *
     * @param content The raw text content.
     * @return The normalized text content.
     */
    protected String normalizeTextContent(final String content) {
        if (StringUtil.isBlank(content)) {
            return StringUtil.EMPTY;
        }
        return content.replaceAll("<attachment[^>]*></attachment>", StringUtil.EMPTY).trim();
    }

    /**
     * Strips HTML tags from the given value using Lucene's HTML strip filter.
     *
     * @param value The HTML content to strip tags from.
     * @return The text content with HTML tags removed.
     */
    protected String stripHtmlTags(final String value) {
        if (value == null) {
            return "";
        }

        if (!value.contains("<") || !value.contains(">")) {
            return value;
        }

        final StringBuilder builder = new StringBuilder();
        try (HTMLStripCharFilter filter = new HTMLStripCharFilter(new StringReader(value))) {
            int ch;
            while ((ch = filter.read()) != -1) {
                builder.append((char) ch);
            }
        } catch (final IOException e) {
            throw new FessSystemException(e);
        }

        return builder.toString();
    }
}
