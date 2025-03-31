const { Client, GatewayIntentBits, Partials, ChannelType } = require('discord.js');
const fs = require('fs');
const path = require('path');
const { createWriteStream } = require('fs');
require('dotenv').config();

// Create a new client instance
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
  ],
  partials: [Partials.Channel],
});

// Set up command prefix
const PREFIX = '!';
// Number of channels to process in parallel
const PARALLEL_CHANNELS = 5;
// Directory to store exports
const EXPORT_DIR = './discord_exports';

client.once('ready', () => {
  console.log(`Logged in as ${client.user.tag}`);
  
  // Create export directory if it doesn't exist
  if (!fs.existsSync(EXPORT_DIR)) {
    fs.mkdirSync(EXPORT_DIR, { recursive: true });
  }
});

client.on('messageCreate', async (message) => {
  // Ignore messages from bots and messages without our prefix
  if (message.author.bot || !message.content.startsWith(PREFIX)) return;

  const args = message.content.slice(PREFIX.length).trim().split(/ +/);
  const command = args.shift().toLowerCase();

  if (command === 'exportguild') {
    await message.reply('Starting full guild message export (including threads and forums)... This may take a long time depending on the number of channels and messages.');
    
    try {
      const guildId = args[0] || message.guild.id;
      const guild = client.guilds.cache.get(guildId);
      
      if (!guild) {
        return message.reply('Guild not found. Please specify a valid guild ID.');
      }
      
      await exportGuildMessages(guild, message);
    } catch (error) {
      console.error('Error during export:', error);
      message.reply('An error occurred during export. Check console for details.');
    }
  }
});

// Helper function to handle rate limits
async function fetchWithRateLimit(func) {
  try {
    return await func();
  } catch (error) {
    if (error.code === 429) { // Rate limit error code
      const retryAfter = error.retry_after || 5000;
      console.log(`Rate limited! Waiting ${retryAfter}ms before retrying...`);
      await new Promise(resolve => setTimeout(resolve, retryAfter));
      return fetchWithRateLimit(func); // Retry after waiting
    }
    throw error; // Re-throw if it's not a rate limit error
  }
}

// Helper function to clean objects by removing null, undefined, or empty values
function cleanObject(obj) {
  const result = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value === null || value === undefined) continue;
    if (Array.isArray(value) && value.length === 0) continue;
    if (typeof value === 'object' && !Array.isArray(value) && Object.keys(value).length === 0) continue;
    
    result[key] = value;
  }
  return result;
}

async function fetchMessagesFromChannel(channel) {
  const messages = [];
  let lastMessageId = null;
  
  // Loop to handle pagination
  while (true) {
    const options = { limit: 100 };
    if (lastMessageId) {
      options.before = lastMessageId;
    }
    
    let fetchedMessages;
    try {
      fetchedMessages = await fetchWithRateLimit(() => channel.messages.fetch(options));
    } catch (error) {
      console.error(`Error fetching messages from ${channel.name}:`, error);
      break;
    }
    
    if (fetchedMessages.size === 0) {
      break;
    }
    
    fetchedMessages.forEach(msg => {
      // Extract all relevant metadata with cleaned objects
      messages.push(cleanObject({
        id: msg.id,
        channelId: msg.channelId,
        author: cleanObject({
          id: msg.author.id,
          username: msg.author.username,
          discriminator: msg.author.discriminator,
          bot: msg.author.bot
        }),
        content: msg.content,
        timestamp: msg.createdTimestamp,
        editedTimestamp: msg.editedTimestamp,
        attachments: [...msg.attachments.values()].map(attachment => cleanObject({
          id: attachment.id,
          url: attachment.url,
          name: attachment.name,
          size: attachment.size,
          contentType: attachment.contentType
        })),
        embeds: msg.embeds.map(embed => cleanObject({
          title: embed.title,
          description: embed.description,
          url: embed.url,
          timestamp: embed.timestamp,
          color: embed.color,
          fields: embed.fields,
          footer: embed.footer,
          image: embed.image,
          thumbnail: embed.thumbnail,
          author: embed.author,
          video: embed.video
        })),
        reactions: [...msg.reactions.cache.values()].map(reaction => cleanObject({
          emoji: reaction.emoji.name,
          count: reaction.count,
          me: reaction.me
        })),
        mentions: cleanObject({
          users: [...msg.mentions.users.values()].map(user => cleanObject({
            id: user.id,
            username: user.username
          })),
          roles: [...msg.mentions.roles.values()].map(role => cleanObject({
            id: role.id,
            name: role.name
          })),
          channels: [...msg.mentions.channels.values()].map(channel => cleanObject({
            id: channel.id,
            name: channel.name
          }))
        }),
        pinned: msg.pinned,
        type: msg.type,
        flags: msg.flags.toArray(),
        components: msg.components.map(component => cleanObject({
          type: component.type,
          components: component.components
        })),
        reference: msg.reference ? cleanObject({
          messageId: msg.reference.messageId,
          channelId: msg.reference.channelId,
          guildId: msg.reference.guildId
        }) : null
      }));
    });
    
    console.log(`Channel ${channel.name}: Fetched ${messages.length} messages so far...`);
    
    // Update lastMessageId for pagination
    lastMessageId = fetchedMessages.last().id;
    
    // If we got less than 100 messages, we've reached the end
    if (fetchedMessages.size < 100) {
      break;
    }
  }
  
  return messages;
}

// Stream-based approach to save messages to file
function saveMessagesToFile(messages, filePath) {
  return new Promise((resolve, reject) => {
    const stream = createWriteStream(filePath);
    
    stream.write('[\n');
    
    messages.forEach((message, index) => {
      const content = JSON.stringify(message, null, 2);
      stream.write(content + (index < messages.length - 1 ? ',\n' : '\n'));
    });
    
    stream.write(']\n');
    
    stream.on('finish', () => {
      resolve();
    });
    
    stream.on('error', (err) => {
      reject(err);
    });
    
    stream.end();
  });
}

async function processThreads(channel, channelData) {
  // Get all active threads
  let activeThreads;
  try {
    activeThreads = await fetchWithRateLimit(() => channel.threads.fetchActive());
  } catch (error) {
    console.error(`Error fetching active threads for ${channel.name}:`, error);
    return [];
  }
  
  const threadDataList = [];
  
  // Process active threads
  const activeThreadPromises = Array.from(activeThreads.threads.values()).map(async (thread) => {
    try {
      const messages = await fetchMessagesFromChannel(thread);
      
      if (messages.length > 0) {
        // Sort messages chronologically
        messages.sort((a, b) => a.timestamp - b.timestamp);
        
        // Save thread messages to separate file
        const threadFileName = path.join(EXPORT_DIR, `thread_${thread.id}_export.json`);
        await saveMessagesToFile(messages, threadFileName);
        
        return cleanObject({
          id: thread.id,
          name: thread.name,
          ownerId: thread.ownerId,
          parentId: thread.parentId,
          messagesCount: messages.length,
          firstMessageTimestamp: messages.length > 0 ? messages[0].timestamp : null,
          messagesFile: threadFileName
        });
      }
    } catch (threadError) {
      console.error(`Error processing thread ${thread.name}:`, threadError);
    }
    return null;
  });
  
  const activeThreadResults = await Promise.all(activeThreadPromises);
  threadDataList.push(...activeThreadResults.filter(Boolean));
  
  // Get archived threads
  let lastThreadId = null;
  let hasMoreArchivedThreads = true;
  
  while (hasMoreArchivedThreads) {
    try {
      const options = { limit: 100 };
      if (lastThreadId) {
        options.before = lastThreadId;
      }
      
      const archivedThreads = await fetchWithRateLimit(() => channel.threads.fetchArchived(options));
      
      if (archivedThreads.threads.size === 0) {
        hasMoreArchivedThreads = false;
        break;
      }
      
      const archivedThreadPromises = Array.from(archivedThreads.threads.values()).map(async (thread) => {
        try {
          const messages = await fetchMessagesFromChannel(thread);
          
          if (messages.length > 0) {
            // Sort messages chronologically
            messages.sort((a, b) => a.timestamp - b.timestamp);
            
            // Save thread messages to separate file
            const threadFileName = path.join(EXPORT_DIR, `thread_${thread.id}_export.json`);
            await saveMessagesToFile(messages, threadFileName);
            
            return cleanObject({
              id: thread.id,
              name: thread.name,
              ownerId: thread.ownerId,
              parentId: thread.parentId,
              archived: true,
              messagesCount: messages.length,
              firstMessageTimestamp: messages.length > 0 ? messages[0].timestamp : null,
              messagesFile: threadFileName
            });
          }
        } catch (threadError) {
          console.error(`Error processing archived thread ${thread.name}:`, threadError);
        }
        return null;
      });
      
      const archivedThreadResults = await Promise.all(archivedThreadPromises);
      threadDataList.push(...archivedThreadResults.filter(Boolean));
      
      // Update for pagination
      if (archivedThreads.threads.size > 0) {
        const threads = Array.from(archivedThreads.threads.values());
        lastThreadId = threads[threads.length - 1].id;
      } else {
        hasMoreArchivedThreads = false;
      }
      
    } catch (archiveError) {
      console.error(`Error fetching archived threads:`, archiveError);
      hasMoreArchivedThreads = false;
    }
  }
  
  // Sort threads chronologically by their first message timestamp
  threadDataList.sort((a, b) => {
    if (!a.firstMessageTimestamp) return 1;
    if (!b.firstMessageTimestamp) return -1;
    return a.firstMessageTimestamp - b.firstMessageTimestamp;
  });
  
  return threadDataList;
}

async function processChannel(channel, guildId) {
  try {
    console.log(`Processing channel: ${channel.name} (${channel.id})`);
    
    const channelData = cleanObject({
      id: channel.id,
      name: channel.name,
      type: channel.type
    });
    
    // If it's a forum channel
    if (channel.type === ChannelType.GuildForum) {
      // Get forum messages (usually just metadata)
      const messages = await fetchMessagesFromChannel(channel);
      
      if (messages.length > 0) {
        // Sort messages chronologically
        messages.sort((a, b) => a.timestamp - b.timestamp);
        
        // Save forum messages to file
        const messagesFileName = path.join(EXPORT_DIR, `channel_${channel.id}_messages_export.json`);
        await saveMessagesToFile(messages, messagesFileName);
        
        channelData.messagesCount = messages.length;
        channelData.messagesFile = messagesFileName;
      }
      
      // Get all threads in the forum
      const threads = await processThreads(channel, channelData);
      
      if (threads.length > 0) {
        // Save threads data to file
        const threadsFileName = path.join(EXPORT_DIR, `channel_${channel.id}_threads_export.json`);
        await saveMessagesToFile(threads, threadsFileName);
        
        channelData.threadsCount = threads.length;
        channelData.threadsFile = threadsFileName;
      }
    } 
    // If it's a regular text channel
    else if (channel.type === ChannelType.GuildText) {
      // Get messages from the channel
      const messages = await fetchMessagesFromChannel(channel);
      
      if (messages.length > 0) {
        // Sort messages chronologically
        messages.sort((a, b) => a.timestamp - b.timestamp);
        
        // Save channel messages to file
        const messagesFileName = path.join(EXPORT_DIR, `channel_${channel.id}_messages_export.json`);
        await saveMessagesToFile(messages, messagesFileName);
        
        channelData.messagesCount = messages.length;
        channelData.firstMessageTimestamp = messages.length > 0 ? messages[0].timestamp : null;
        channelData.messagesFile = messagesFileName;
      }
      
      // Get all threads in the text channel
      const threads = await processThreads(channel, channelData);
      
      if (threads.length > 0) {
        // Save threads data to file
        const threadsFileName = path.join(EXPORT_DIR, `channel_${channel.id}_threads_export.json`);
        await saveMessagesToFile(threads, threadsFileName);
        
        channelData.threadsCount = threads.length;
        channelData.threadsFile = threadsFileName;
      }
    }
    // If it's already a thread
    else if (
      channel.type === ChannelType.PublicThread || 
      channel.type === ChannelType.PrivateThread || 
      channel.type === ChannelType.AnnouncementThread
    ) {
      const messages = await fetchMessagesFromChannel(channel);
      
      if (messages.length > 0) {
        // Sort messages chronologically
        messages.sort((a, b) => a.timestamp - b.timestamp);
        
        // Save thread messages to file
        const messagesFileName = path.join(EXPORT_DIR, `thread_${channel.id}_messages_export.json`);
        await saveMessagesToFile(messages, messagesFileName);
        
        channelData.messagesCount = messages.length;
        channelData.firstMessageTimestamp = messages.length > 0 ? messages[0].timestamp : null;
        channelData.messagesFile = messagesFileName;
        channelData.parentId = channel.parentId;
        channelData.ownerId = channel.ownerId;
      }
    }
    
    // Save individual channel metadata
    const channelMetaFile = path.join(EXPORT_DIR, `channel_${channel.id}_meta.json`);
    fs.writeFileSync(channelMetaFile, JSON.stringify(channelData, null, 2));
    
    return channelData;
  } catch (error) {
    console.error(`Error processing channel ${channel.name}:`, error);
    return null;
  }
}

async function exportGuildMessages(guild, originalMessage) {
  // Create guild-specific directory
  const guildDir = path.join(EXPORT_DIR, `guild_${guild.id}`);
  if (!fs.existsSync(guildDir)) {
    fs.mkdirSync(guildDir, { recursive: true });
  }
  
  // Initialize guild data
  const guildData = {
    id: guild.id,
    name: guild.name,
    exportStartedAt: new Date().toISOString(),
    channels: []
  };
  
  // Check for processed channels from previous runs
  const processedChannelIdsFile = path.join(guildDir, 'processed_channel_ids.json');
  const processedChannelIds = new Set();
  
  if (fs.existsSync(processedChannelIdsFile)) {
    const processed = JSON.parse(fs.readFileSync(processedChannelIdsFile));
    processed.forEach(id => processedChannelIds.add(id));
    console.log(`Resuming export. ${processedChannelIds.size} channels already processed.`);
    originalMessage.channel.send(`Resuming previous export. ${processedChannelIds.size} channels already processed.`);
  }
  
  // Get all channels we want to export from
  const channels = Array.from(guild.channels.cache.filter(channel => 
    (channel.type === ChannelType.GuildText || 
     channel.type === ChannelType.GuildForum || 
     channel.type === ChannelType.PublicThread || 
     channel.type === ChannelType.PrivateThread || 
     channel.type === ChannelType.AnnouncementThread) &&
    !processedChannelIds.has(channel.id)
  ).values());
  
  originalMessage.channel.send(`Found ${channels.length} channels to process (including text channels, forums, and threads).`);
  
  // Process channels in parallel batches
  for (let i = 0; i < channels.length; i += PARALLEL_CHANNELS) {
    const channelBatch = channels.slice(i, i + PARALLEL_CHANNELS);
    
    originalMessage.channel.send(`Processing batch ${Math.floor(i/PARALLEL_CHANNELS) + 1}: Channels ${i+1} to ${Math.min(i+PARALLEL_CHANNELS, channels.length)} of ${channels.length}`);
    
    // Process this batch of channels in parallel
    const batchResults = await Promise.all(channelBatch.map(async (channel) => {
      return processChannel(channel, guild.id);
    }));
    
    // Filter out nulls (failed channels) and add to guild data
    const successfulChannels = batchResults.filter(Boolean);
    guildData.channels.push(...successfulChannels);
    
    // Mark these channels as processed
    channelBatch.forEach(channel => {
      processedChannelIds.add(channel.id);
    });
    
    // Update the processed channels file
    fs.writeFileSync(processedChannelIdsFile, JSON.stringify(Array.from(processedChannelIds)));
    
    // Save guild progress after each batch
    const guildDataFile = path.join(guildDir, `guild_${guild.id}_export_progress.json`);
    fs.writeFileSync(guildDataFile, JSON.stringify(guildData, null, 2));
    
    originalMessage.channel.send(`Progress: ${processedChannelIds.size}/${processedChannelIds.size + channels.length - (i + channelBatch.length)} channels processed`);
  }
  
  // Sort channels chronologically
  guildData.channels.sort((a, b) => {
    if (!a.firstMessageTimestamp) return 1;
    if (!b.firstMessageTimestamp) return -1;
    return a.firstMessageTimestamp - b.firstMessageTimestamp;
  });
  
  guildData.exportCompletedAt = new Date().toISOString();
  
  // Write final result to file
  const finalFileName = path.join(guildDir, `guild_${guild.id}_export_${Date.now()}.json`);
  fs.writeFileSync(finalFileName, JSON.stringify(guildData, null, 2));
  
  originalMessage.reply(`Export complete! Exported data from ${guildData.channels.length} channels (including threads and forums) to ${finalFileName}`);
}

// Login to Discord
client.login(process.env.DISCORD_TOKEN);