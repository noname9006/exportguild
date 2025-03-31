const { Client, GatewayIntentBits, Partials, ChannelType } = require('discord.js');
const fs = require('fs');
const path = require('path');
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
// Number of channels to process in parallel - reduced for stability
const PARALLEL_CHANNELS = 3;
// Directory to store exports - now set to root folder
const EXPORT_DIR = './';

// Track total messages processed and start time
let totalMessagesProcessed = 0;
let lastStatusUpdateCount = 0;
let exportStartTime;

// Track rate limit hits to adjust backoff
let rateLimitHits = 0;

// Single guild export object that will contain everything
let fullGuildExport = {};

// Store reference to status update message
let statusMessage = null;

client.once('ready', () => {
  console.log(`Logged in as ${client.user.tag}`);
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
      
      // Reset counters and set start time
      totalMessagesProcessed = 0;
      lastStatusUpdateCount = 0;
      rateLimitHits = 0;
      exportStartTime = new Date();
      statusMessage = null; // Reset status message reference
      
      // Initialize the full guild export object
      fullGuildExport = {
        id: guild.id,
        name: guild.name,
        exportStartedAt: new Date().toISOString(),
        timestamp: Date.now(),
        channels: []
      };
      
      await exportGuildMessages(guild, message);
    } catch (error) {
      console.error('Error during export:', error);
      message.reply('An error occurred during export. Check console for details.');
    }
  }
});

// Enhanced rate limit handler with progressive backoff
async function fetchWithRateLimit(func, retryCount = 0) {
  try {
    return await func();
  } catch (error) {
    if (error.code === 429) { // Rate limit error code
      rateLimitHits++;
      const baseDelay = error.retry_after || 1000;
      
      // Apply progressive backoff based on multiple factors
      const messageBackoffFactor = 1 + Math.floor(totalMessagesProcessed / 10000) * 0.2;
      const rateLimitFactor = 1 + Math.min(rateLimitHits / 10, 2);
      const retryFactor = 1 + Math.min(retryCount * 0.5, 3);
      const retryAfter = baseDelay * messageBackoffFactor * rateLimitFactor * retryFactor;
      
      console.log(`Rate limited! Waiting ${Math.round(retryAfter)}ms before retrying... (Attempt ${retryCount + 1}, Total rate limits: ${rateLimitHits})`);
      await new Promise(resolve => setTimeout(resolve, retryAfter));
      return fetchWithRateLimit(func, retryCount + 1); // Retry with increased count
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

// Calculate time elapsed in a human-readable format
function getTimeElapsed() {
  const now = new Date();
  const elapsed = now - exportStartTime;
  
  const seconds = Math.floor(elapsed / 1000) % 60;
  const minutes = Math.floor(elapsed / (1000 * 60)) % 60;
  const hours = Math.floor(elapsed / (1000 * 60 * 60));
  
  return `${hours}h ${minutes}m ${seconds}s`;
}

// Calculate processing speed
function getProcessingSpeed() {
  const now = new Date();
  const elapsedSeconds = (now - exportStartTime) / 1000;
  if (elapsedSeconds === 0) return "0";
  
  const messagesPerSecond = totalMessagesProcessed / elapsedSeconds;
  return messagesPerSecond.toFixed(2);
}

// Send status update if we've processed enough new messages
async function checkAndSendStatusUpdate(channel, guild) {
  if (totalMessagesProcessed - lastStatusUpdateCount >= 1000) {
    const timeElapsed = getTimeElapsed();
    const speed = getProcessingSpeed();
    
    const statusContent = 
      `Status update: Processed ${totalMessagesProcessed.toLocaleString()} messages from ${guild.name}\n` +
      `Time elapsed: ${timeElapsed}\n` +
      `Processing speed: ${speed} messages/second\n` +
      `Rate limit hits: ${rateLimitHits}`;
    
    try {
      // If we haven't sent a status message yet, create one and store the reference
      if (!statusMessage) {
        statusMessage = await channel.send(statusContent);
      } else {
        // Otherwise, edit the existing message
        await statusMessage.edit(statusContent);
      }
      
      lastStatusUpdateCount = totalMessagesProcessed;
      
      // Try to trigger garbage collection if available
      if (global.gc) {
        try {
          global.gc();
        } catch (e) {
          // Continue if gc is not available
        }
      }
    } catch (error) {
      console.error('Error updating status message:', error);
      // If editing fails (e.g., message was deleted), create a new status message
      statusMessage = await channel.send(statusContent + '\n(Created new status message due to error updating previous one)');
    }
  }
}

async function fetchMessagesFromChannel(channel, originalChannel, guild) {
  const messages = [];
  let lastMessageId = null;
  let batchCount = 0;
  
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
    
    batchCount++;
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
    
    // Update the total message count
    totalMessagesProcessed += fetchedMessages.size;
    
    // Check if we should send a status update
    await checkAndSendStatusUpdate(originalChannel, guild);
    
    console.log(`Channel ${channel.name}: Fetched ${messages.length} messages so far... (Total: ${totalMessagesProcessed})`);
    
    // Update lastMessageId for pagination
    lastMessageId = fetchedMessages.last().id;
    
    // If we got less than 100 messages, we've reached the end
    if (fetchedMessages.size < 100) {
      break;
    }
    
    // Add a small delay between batches to avoid rate limiting
    // The delay increases as we process more batches from the same channel
    if (batchCount > 5) {
      const delay = Math.min(200 + batchCount * 20, 1000);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  return messages;
}

// Process threads from a channel
async function processThreads(channel, channelData, originalChannel, guild) {
  // Get all active threads with backoff retry
  let activeThreads;
  try {
    activeThreads = await fetchWithRateLimit(() => channel.threads.fetchActive());
  } catch (error) {
    console.error(`Error fetching active threads for ${channel.name}:`, error);
    return [];
  }
  
  const threadDataList = [];
  let processedCount = 0;
  
  // Process active threads in smaller batches to avoid memory pressure
  const activeThreadsArray = Array.from(activeThreads.threads.values());
  const batchSize = 3; // Smaller batch size for more stable processing
  
  for (let i = 0; i < activeThreadsArray.length; i += batchSize) {
    const threadBatch = activeThreadsArray.slice(i, i + batchSize);
    const batchPromises = threadBatch.map(async (thread) => {
      try {
        const messages = await fetchMessagesFromChannel(thread, originalChannel, guild);
        
        if (messages.length > 0) {
          // Sort messages chronologically
          messages.sort((a, b) => a.timestamp - b.timestamp);
          
          processedCount++;
          if (processedCount % 5 === 0) {
            // Provide interim updates for thread processing
            const timeElapsed = getTimeElapsed();
            console.log(`Processed ${processedCount} threads in channel ${channel.name} (${timeElapsed})`);
          }
          
          return cleanObject({
            id: thread.id,
            name: thread.name,
            ownerId: thread.ownerId,
            parentId: thread.parentId,
            messagesCount: messages.length,
            firstMessageTimestamp: messages.length > 0 ? messages[0].timestamp : null,
            messages: messages // Include the messages directly in the thread object
          });
        }
      } catch (threadError) {
        console.error(`Error processing thread ${thread.name}:`, threadError);
      }
      return null;
    });
    
    // Wait for each batch to complete before starting the next
    const batchResults = await Promise.all(batchPromises);
    threadDataList.push(...batchResults.filter(Boolean));
    
    // Force garbage collection if available (requires --expose-gc flag)
    if (global.gc && i % 3 === 0) {
      try {
        global.gc();
      } catch (e) {
        // Continue if gc is not available
      }
    }
    
    // Brief pause between batches to allow API recovery
    if (i + batchSize < activeThreadsArray.length) {
      await new Promise(resolve => setTimeout(resolve, 200));
    }
  }
  
  // Process archived threads with similar optimizations
  let lastThreadId = null;
  let hasMoreArchivedThreads = true;
  
  while (hasMoreArchivedThreads) {
    try {
      const options = { limit: 50 }; // Reduced from 100 to avoid rate limits
      if (lastThreadId) {
        options.before = lastThreadId;
      }
      
      const archivedThreads = await fetchWithRateLimit(() => channel.threads.fetchArchived(options));
      
      if (archivedThreads.threads.size === 0) {
        hasMoreArchivedThreads = false;
        break;
      }
      
      // Process archived threads in smaller batches too
      const archivedThreadsArray = Array.from(archivedThreads.threads.values());
      
      for (let i = 0; i < archivedThreadsArray.length; i += batchSize) {
        const threadBatch = archivedThreadsArray.slice(i, i + batchSize);
        const batchPromises = threadBatch.map(async (thread) => {
          try {
            const messages = await fetchMessagesFromChannel(thread, originalChannel, guild);
            
            if (messages.length > 0) {
              // Sort messages chronologically
              messages.sort((a, b) => a.timestamp - b.timestamp);
              
              processedCount++;
              if (processedCount % 5 === 0) {
                // Provide interim updates for thread processing
                const timeElapsed = getTimeElapsed();
                console.log(`Processed ${processedCount} threads in channel ${channel.name} (${timeElapsed})`);
              }
              
              return cleanObject({
                id: thread.id,
                name: thread.name,
                ownerId: thread.ownerId,
                parentId: thread.parentId,
                archived: true,
                messagesCount: messages.length,
                firstMessageTimestamp: messages.length > 0 ? messages[0].timestamp : null,
                messages: messages // Include the messages directly in the thread object
              });
            }
          } catch (threadError) {
            console.error(`Error processing archived thread ${thread.name}:`, threadError);
          }
          return null;
        });
        
        const batchResults = await Promise.all(batchPromises);
        threadDataList.push(...batchResults.filter(Boolean));
        
        // Allow brief recovery period
        if (i + batchSize < archivedThreadsArray.length) {
          await new Promise(resolve => setTimeout(resolve, 300));
        }
      }
      
      // Update for pagination
      if (archivedThreads.threads.size > 0) {
        const threads = Array.from(archivedThreads.threads.values());
        lastThreadId = threads[threads.length - 1].id;
      } else {
        hasMoreArchivedThreads = false;
      }
      
      // Add a pause between archived thread fetches to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 1000));
      
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

async function processChannel(channel, guildId, originalChannel, guild) {
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
      const messages = await fetchMessagesFromChannel(channel, originalChannel, guild);
      
      if (messages.length > 0) {
        // Sort messages chronologically
        messages.sort((a, b) => a.timestamp - b.timestamp);
        channelData.messagesCount = messages.length;
        channelData.messages = messages; // Store messages directly in the channel object
      }
      
      // Get all threads in the forum
      const threads = await processThreads(channel, channelData, originalChannel, guild);
      
      if (threads.length > 0) {
        channelData.threadsCount = threads.length;
        channelData.threads = threads; // Store threads directly in the channel object
      }
    } 
    // If it's a regular text channel
    else if (channel.type === ChannelType.GuildText) {
      // Get messages from the channel
      const messages = await fetchMessagesFromChannel(channel, originalChannel, guild);
      
      if (messages.length > 0) {
        // Sort messages chronologically
        messages.sort((a, b) => a.timestamp - b.timestamp);
        
        channelData.messagesCount = messages.length;
        channelData.firstMessageTimestamp = messages.length > 0 ? messages[0].timestamp : null;
        channelData.messages = messages; // Store messages directly in the channel object
      }
      
      // Get all threads in the text channel
      const threads = await processThreads(channel, channelData, originalChannel, guild);
      
      if (threads.length > 0) {
        channelData.threadsCount = threads.length;
        channelData.threads = threads; // Store threads directly in the channel object
      }
    }
    // If it's already a thread
    else if (
      channel.type === ChannelType.PublicThread || 
      channel.type === ChannelType.PrivateThread || 
      channel.type === ChannelType.AnnouncementThread
    ) {
      const messages = await fetchMessagesFromChannel(channel, originalChannel, guild);
      
      if (messages.length > 0) {
        // Sort messages chronologically
        messages.sort((a, b) => a.timestamp - b.timestamp);
        
        channelData.messagesCount = messages.length;
        channelData.firstMessageTimestamp = messages.length > 0 ? messages[0].timestamp : null;
        channelData.messages = messages; // Store messages directly in the channel object
        channelData.parentId = channel.parentId;
        channelData.ownerId = channel.ownerId;
      }
    }
    
    return channelData;
  } catch (error) {
    console.error(`Error processing channel ${channel.name}:`, error);
    return null;
  }
}

// Save the full guild export to a single file
function saveFullGuildExport(guildExport, filename) {
  return new Promise((resolve, reject) => {
    try {
      fs.writeFileSync(filename, JSON.stringify(guildExport, null, 2));
      resolve();
    } catch (err) {
      reject(err);
    }
  });
}

// Add a variable to store the status update message reference
let statusMessageRef = null;

// Send status update if we've processed enough new messages
async function checkAndSendStatusUpdate(channel, guild) {
  if (totalMessagesProcessed - lastStatusUpdateCount >= 1000) {
    const timeElapsed = getTimeElapsed();
    const speed = getProcessingSpeed();
    
    const statusMessage = 
      `Status update: Processed ${totalMessagesProcessed.toLocaleString()} messages from ${guild.name}\n` +
      `Time elapsed: ${timeElapsed}\n` +
      `Processing speed: ${speed} messages/second\n` +
      `Rate limit hits: ${rateLimitHits}`;
    
    if (!statusMessageRef) {
      // If this is the first status update, send a new message and store the reference
      statusMessageRef = await channel.send(statusMessage);
    } else {
      // Otherwise, update the existing message
      try {
        await statusMessageRef.edit(statusMessage);
      } catch (error) {
        console.error('Error updating status message, sending a new one:', error);
        // If there's an error (e.g., message deleted), create a new reference
        statusMessageRef = await channel.send(statusMessage);
      }
    }
    
    lastStatusUpdateCount = totalMessagesProcessed;
    
    // Try to trigger garbage collection if available
    if (global.gc) {
      try {
        global.gc();
      } catch (e) {
        // Continue if gc is not available
      }
    }
  }
}

async function fetchMessagesFromChannel(channel, originalChannel, guild) {
  const messages = [];
  let lastMessageId = null;
  let batchCount = 0;
  
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
    
    batchCount++;
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
    
    // Update the total message count
    totalMessagesProcessed += fetchedMessages.size;
    
    // Check if we should send a status update
    await checkAndSendStatusUpdate(originalChannel, guild);
    
    console.log(`Channel ${channel.name}: Fetched ${messages.length} messages so far... (Total: ${totalMessagesProcessed})`);
    
    // Update lastMessageId for pagination
    lastMessageId = fetchedMessages.last().id;
    
    // If we got less than 100 messages, we've reached the end
    if (fetchedMessages.size < 100) {
      break;
    }
    
    // Add a small delay between batches to avoid rate limiting
    // The delay increases as we process more batches from the same channel
    if (batchCount > 5) {
      const delay = Math.min(200 + batchCount * 20, 1000);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
  }
  
  return messages;
}
// Variables to track message references for updates
let channelStatusMessageRef = null;
let progressStatusMessageRef = null;

async function exportGuildMessages(guild, originalMessage) {
  // Create export directory if needed
  if (!fs.existsSync(EXPORT_DIR)) {
    fs.mkdirSync(EXPORT_DIR, { recursive: true });
  }
  
  // Reset message references
  statusMessageRef = null;
  channelStatusMessageRef = null;
  progressStatusMessageRef = null;
  
  // Check for processed channels from previous runs
  const processedChannelIds = new Set();
  
  // Get all channels we want to export, filtered by type
  const channels = Array.from(guild.channels.cache.filter(channel => 
    (channel.type === ChannelType.GuildText || 
     channel.type === ChannelType.GuildForum || 
     channel.type === ChannelType.PublicThread || 
     channel.type === ChannelType.PrivateThread || 
     channel.type === ChannelType.AnnouncementThread) &&
    channel.viewable
  ).values());
  
  // Initialize guild data for a single complete export
  const timestamp = Date.now();
  const guildData = {
    id: guild.id,
    name: guild.name,
    exportStartedAt: new Date().toISOString(),
    timestamp: timestamp,
    totalChannelsToProcess: channels.length,
    channels: []
  };
  
  // Reset counters and set start time
  totalMessagesProcessed = 0;
  lastStatusUpdateCount = 0;
  rateLimitHits = 0;
  exportStartTime = new Date();
  
  // Sort channels by type and name
  channels.sort((a, b) => {
    if (a.type !== b.type) return a.type - b.type;
    return a.name.localeCompare(b.name);
  });
  
  await originalMessage.channel.send(`Found ${channels.length} channels to process (including text channels, forums, and threads).`);
  
  // Process channels one by one to avoid memory issues
  for (let i = 0; i < channels.length; i++) {
    const channel = channels[i];
    
    const channelStatusMessage = `Processing channel ${i+1}/${channels.length}: ${channel.name}\n` +
      `Time elapsed: ${getTimeElapsed()}\n` +
      `Messages processed: ${totalMessagesProcessed.toLocaleString()}`;
    
    // Update or send channel status message
    if (!channelStatusMessageRef) {
      channelStatusMessageRef = await originalMessage.channel.send(channelStatusMessage);
    } else {
      try {
        await channelStatusMessageRef.edit(channelStatusMessage);
      } catch (error) {
        console.error('Error updating channel status message:', error);
        channelStatusMessageRef = await originalMessage.channel.send(channelStatusMessage);
      }
    }
    
    // Process this channel
    const channelData = await processChannel(channel, guild.id, originalMessage.channel, guild);
    
    if (channelData) {
      guildData.channels.push(channelData);
      processedChannelIds.add(channel.id);
    }
    
    // Send a status update every 3 channels
    if ((i + 1) % 3 === 0 || i === channels.length - 1) {
      const progressMessage = `Progress: ${processedChannelIds.size}/${channels.length} channels processed\n` +
        `Total messages: ${totalMessagesProcessed.toLocaleString()}\n` +
        `Processing speed: ${getProcessingSpeed()} messages/second\n` +
        `Rate limit hits: ${rateLimitHits}`;
      
      // Update or send progress status message
      if (!progressStatusMessageRef) {
        progressStatusMessageRef = await originalMessage.channel.send(progressMessage);
      } else {
        try {
          await progressStatusMessageRef.edit(progressMessage);
        } catch (error) {
          console.error('Error updating progress status message:', error);
          progressStatusMessageRef = await originalMessage.channel.send(progressMessage);
        }
      }
    }
    
    // Add a small delay between channels
    if (i < channels.length - 1) {
      const delay = Math.min(1000 + rateLimitHits * 200, 5000);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
    
// Try to trigger garbage collection if available
    if (global.gc) {
      try { global.gc(); } catch (e) { /* Continue */ }
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

async function processChannel(channel, guildId, originalChannel, guild) {
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
      const messages = await fetchMessagesFromChannel(channel, originalChannel, guild);
      
      if (messages.length > 0) {
        // Sort messages chronologically
        messages.sort((a, b) => a.timestamp - b.timestamp);
        channelData.messagesCount = messages.length;
        channelData.messages = messages; // Store messages directly in the channel object
      }
      
      // Get all threads in the forum
      const threads = await processThreads(channel, channelData, originalChannel, guild);
      
      if (threads.length > 0) {
        channelData.threadsCount = threads.length;
        channelData.threads = threads; // Store threads directly in the channel object
      }
    } 
    // If it's a regular text channel
    else if (channel.type === ChannelType.GuildText) {
      // Get messages from the channel
      const messages = await fetchMessagesFromChannel(channel, originalChannel, guild);
      
      if (messages.length > 0) {
        // Sort messages chronologically
        messages.sort((a, b) => a.timestamp - b.timestamp);
        
        channelData.messagesCount = messages.length;
        channelData.firstMessageTimestamp = messages.length > 0 ? messages[0].timestamp : null;
        channelData.messages = messages; // Store messages directly in the channel object
      }
      
      // Get all threads in the text channel
      const threads = await processThreads(channel, channelData, originalChannel, guild);
      
      if (threads.length > 0) {
        channelData.threadsCount = threads.length;
        channelData.threads = threads; // Store threads directly in the channel object
      }
    }
    // If it's already a thread
    else if (
      channel.type === ChannelType.PublicThread || 
      channel.type === ChannelType.PrivateThread || 
      channel.type === ChannelType.AnnouncementThread
    ) {
      const messages = await fetchMessagesFromChannel(channel, originalChannel, guild);
      
      if (messages.length > 0) {
        // Sort messages chronologically
        messages.sort((a, b) => a.timestamp - b.timestamp);
        
        channelData.messagesCount = messages.length;
        channelData.firstMessageTimestamp = messages.length > 0 ? messages[0].timestamp : null;
        channelData.messages = messages; // Store messages directly in the channel object
        channelData.parentId = channel.parentId;
        channelData.ownerId = channel.ownerId;
      }
    }
    
    return channelData;
  } catch (error) {
    console.error(`Error processing channel ${channel.name}:`, error);
    return null;
  }
}

// Save the full guild export to a single file
function saveFullGuildExport(guildExport, filename) {
  return new Promise((resolve, reject) => {
    try {
      fs.writeFileSync(filename, JSON.stringify(guildExport, null, 2));
      resolve();
    } catch (err) {
      reject(err);
    }
  });
}

// Modified: Update the status update function to update existing message
async function checkAndSendStatusUpdate(channel, guild) {
  if (totalMessagesProcessed - lastStatusUpdateCount >= 1000) {
    const timeElapsed = getTimeElapsed();
    const speed = getProcessingSpeed();
    
    const statusMessage = 
      `Status update: Processed ${totalMessagesProcessed.toLocaleString()} messages from ${guild.name}\n` +
      `Time elapsed: ${timeElapsed}\n` +
      `Processing speed: ${speed} messages/second\n` +
      `Rate limit hits: ${rateLimitHits}`;
    
    if (!statusMessageRef) {
      // If this is the first status update, send a new message and store the reference
      statusMessageRef = await channel.send(statusMessage);
    } else {
      // Otherwise, update the existing message
      try {
        await statusMessageRef.edit(statusMessage);
      } catch (error) {
        console.error('Error updating status message, sending a new one:', error);
        // If there's an error (e.g., message deleted), create a new reference
        statusMessageRef = await channel.send(statusMessage);
      }
    }
    
    lastStatusUpdateCount = totalMessagesProcessed;
    
    // Try to trigger garbage collection if available
    if (global.gc) {
      try {
        global.gc();
      } catch (e) {
        // Continue if gc is not available
      }
    }
  }
}

async function exportGuildMessages(guild, originalMessage) {
  // Create export directory if needed
  if (!fs.existsSync(EXPORT_DIR)) {
    fs.mkdirSync(EXPORT_DIR, { recursive: true });
  }
  
  // Reset message references at the start of export
  statusMessageRef = null;
  channelStatusMessageRef = null;
  progressStatusMessageRef = null;
  
  // Check for processed channels from previous runs
  const processedChannelIds = new Set();
  
  // Get all channels we want to export, filtered by type
  const channels = Array.from(guild.channels.cache.filter(channel => 
    (channel.type === ChannelType.GuildText || 
     channel.type === ChannelType.GuildForum || 
     channel.type === ChannelType.PublicThread || 
     channel.type === ChannelType.PrivateThread || 
     channel.type === ChannelType.AnnouncementThread) &&
    channel.viewable
  ).values());
  
  // Initialize guild data for a single complete export
  const timestamp = Date.now();
  const guildData = {
    id: guild.id,
    name: guild.name,
    exportStartedAt: new Date().toISOString(),
    timestamp: timestamp,
    totalChannelsToProcess: channels.length,
    channels: []
  };
  
  // Reset counters and set start time
  totalMessagesProcessed = 0;
  lastStatusUpdateCount = 0;
  rateLimitHits = 0;
  exportStartTime = new Date();
  
  // Sort channels by type and name
  channels.sort((a, b) => {
    if (a.type !== b.type) return a.type - b.type;
    return a.name.localeCompare(b.name);
  });
  
  await originalMessage.channel.send(`Found ${channels.length} channels to process (including text channels, forums, and threads).`);
  
  // Process channels one by one to avoid memory issues
  for (let i = 0; i < channels.length; i++) {
    const channel = channels[i];
    
    const channelStatusMessage = 
      `Processing channel ${i+1}/${channels.length}: ${channel.name}\n` +
      `Time elapsed: ${getTimeElapsed()}\n` +
      `Messages processed: ${totalMessagesProcessed.toLocaleString()}`;
    
    // Update or send channel processing status message
    if (!channelStatusMessageRef) {
      channelStatusMessageRef = await originalMessage.channel.send(channelStatusMessage);
    } else {
      try {
        await channelStatusMessageRef.edit(channelStatusMessage);
      } catch (error) {
        console.error('Error updating channel status message:', error);
        channelStatusMessageRef = await originalMessage.channel.send(channelStatusMessage);
      }
    }
    
    // Process this channel
    const channelData = await processChannel(channel, guild.id, originalMessage.channel, guild);
    
    if (channelData) {
      guildData.channels.push(channelData);
      processedChannelIds.add(channel.id);
    }
    
    // Send/update a status message every 3 channels
    if ((i + 1) % 3 === 0 || i === channels.length - 1) {
      const progressMessage = 
        `Progress: ${processedChannelIds.size}/${channels.length} channels processed\n` +
        `Total messages: ${totalMessagesProcessed.toLocaleString()}\n` +
        `Processing speed: ${getProcessingSpeed()} messages/second\n` +
        `Rate limit hits: ${rateLimitHits}`;
      
      // Update or send progress status message
      if (!progressStatusMessageRef) {
        progressStatusMessageRef = await originalMessage.channel.send(progressMessage);
      } else {
        try {
          await progressStatusMessageRef.edit(progressMessage);
        } catch (error) {
          console.error('Error updating progress status message:', error);
          progressStatusMessageRef = await originalMessage.channel.send(progressMessage);
        }
      }
    }
    
    // Add a small delay between channels
    if (i < channels.length - 1) {
      const delay = Math.min(1000 + rateLimitHits * 200, 5000);
      await new Promise(resolve => setTimeout(resolve, delay));
    }
    
    // Try to trigger garbage collection if available
    if (global.gc) {
      try { global.gc(); } catch (e) { /* Continue */ }
    }
  }
  
  // Sort channels by their first message timestamp
  guildData.channels.sort((a, b) => {
    if (!a.firstMessageTimestamp) return 1;
    if (!b.firstMessageTimestamp) return -1;
    return a.firstMessageTimestamp - b.firstMessageTimestamp;
  });
  
  // Complete the export data
  guildData.exportCompletedAt = new Date().toISOString();
  guildData.totalMessagesExported = totalMessagesProcessed;
  guildData.totalChannelsExported = guildData.channels.length;
  guildData.exportDurationMs = new Date() - exportStartTime;
  
  // Create a single JSON file for the entire guild
  const finalFileName = path.join(EXPORT_DIR, `${guild.name.replace(/[^a-z0-9]/gi, '_')}_${guild.id}_complete_export.json`);
  
  try {
    // Update the progress status message if it exists, otherwise send new one
    const exportMessage = `Creating single JSON export file...`;
    if (progressStatusMessageRef) {
      await progressStatusMessageRef.edit(exportMessage);
    } else {
      await originalMessage.channel.send(exportMessage);
    }
    
    // Write the entire JSON in one operation
    fs.writeFileSync(finalFileName, JSON.stringify(guildData, null, 2));
    
    await originalMessage.reply(
      `Export complete!\n` +
      `✅ Exported ${totalMessagesProcessed.toLocaleString()} messages from ${guildData.channels.length} channels\n` +
      `⏱️ Total time: ${getTimeElapsed()}\n` +
      `📁 All data saved to: ${finalFileName}`
    );
  } catch (error) {
    console.error('Error writing final export file:', error);
    
    // If writing fails due to size, use streaming approach
    const alternativeMessage = `Trying alternative streaming approach for large export...`;
    if (progressStatusMessageRef) {
      await progressStatusMessageRef.edit(alternativeMessage);
    } else {
      await originalMessage.channel.send(alternativeMessage);
    }
    
    await writeGuildDataToStreamingJson(guildData, finalFileName);
    
    await originalMessage.reply(
      `Export complete!\n` +
      `✅ Exported ${totalMessagesProcessed.toLocaleString()} messages from ${guildData.channels.length} channels\n` +
      `⏱️ Total time: ${getTimeElapsed()}\n` +
      `📁 All data saved to: ${finalFileName}`
    );
  }
}

// Helper function to write large JSON data using streaming
async function writeGuildDataToStreamingJson(guildData, filePath) {
  return new Promise((resolve, reject) => {
    try {
      const stream = fs.createWriteStream(filePath);
      
      // Write opening brace and basic guild info
      stream.write('{\n');
      stream.write(`  "id": ${JSON.stringify(guildData.id)},\n`);
      stream.write(`  "name": ${JSON.stringify(guildData.name)},\n`);
      stream.write(`  "exportStartedAt": ${JSON.stringify(guildData.exportStartedAt)},\n`);
      stream.write(`  "exportCompletedAt": ${JSON.stringify(guildData.exportCompletedAt)},\n`);
      stream.write(`  "timestamp": ${guildData.timestamp},\n`);
      stream.write(`  "totalMessagesExported": ${guildData.totalMessagesExported},\n`);
      stream.write(`  "totalChannelsExported": ${guildData.totalChannelsExported},\n`);
      stream.write(`  "exportDurationMs": ${guildData.exportDurationMs},\n`);
      
      // Begin channels array
      stream.write('  "channels": [\n');
      
      // Write each channel
      const channelCount = guildData.channels.length;
      for (let i = 0; i < channelCount; i++) {
        const channelJson = JSON.stringify(guildData.channels[i], null, 2);
        
        // Add proper indentation and commas
        const formattedChannel = channelJson
          .split('\n')
          .map(line => '    ' + line)
          .join('\n');
          
        stream.write(formattedChannel + (i < channelCount - 1 ? ',\n' : '\n'));
        
        // Clear the channel data after writing to help with memory
        guildData.channels[i] = null;
      }
      
      // Close the channels array and the main object
      stream.write('  ]\n');
      stream.write('}\n');
      
      stream.on('finish', () => {
        console.log(`Successfully wrote guild data to ${filePath}`);
        resolve();
      });
      
      stream.on('error', (err) => {
        console.error(`Error writing guild data: ${err}`);
        reject(err);
      });
      
      stream.end();
      
    } catch (error) {
      console.error(`Error in streaming write: ${error}`);
      reject(error);
    }
  });
}

// Login to Discord with proper error handling
client.login(process.env.DISCORD_TOKEN).catch(err => {
  console.error('Failed to login to Discord:', err);
  console.log('Please check your DISCORD_TOKEN in the .env file and ensure the bot is properly configured.');
});