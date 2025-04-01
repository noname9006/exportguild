// Import required libraries
const fs = require('fs');
const path = require('path');
const { Client, GatewayIntentBits, ChannelType } = require('discord.js');
require('dotenv').config();

// Configure the Discord client with necessary intents
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.DirectMessages
  ]
});

// Configuration
const PREFIX = '!'; // Command prefix
const EXPORT_DIR = './exports'; // Directory to save exports
const MAX_RATE_LIMIT_RETRIES = 5; // Maximum retries when rate limited

// Global variables for tracking export progress
let totalMessagesProcessed = 0;
let lastStatusUpdateCount = 0;
let rateLimitHits = 0;
let exportStartTime = null;
let combinedStatusMessageRef = null; // Single message reference for all status updates

// Utility function to clean up objects by removing undefined and null values
function cleanObject(obj) {
  if (!obj) return obj;
  
  Object.keys(obj).forEach(key => {
    if (obj[key] === undefined || obj[key] === null) {
      delete obj[key];
    } else if (typeof obj[key] === 'object' && !Array.isArray(obj[key])) {
      obj[key] = cleanObject(obj[key]);
    }
  });
  
  return obj;
}

// Format the time elapsed since the export began
function getTimeElapsed() {
  const elapsedMs = new Date() - exportStartTime;
  const seconds = Math.floor((elapsedMs / 1000) % 60);
  const minutes = Math.floor((elapsedMs / (1000 * 60)) % 60);
  const hours = Math.floor(elapsedMs / (1000 * 60 * 60));
  
  return `${hours}h ${minutes}m ${seconds}s`;
}

// Calculate the processing speed (messages per second)
function getProcessingSpeed() {
  const elapsedSeconds = (new Date() - exportStartTime) / 1000;
  if (elapsedSeconds <= 0) return "0.00";
  
  const speed = totalMessagesProcessed / elapsedSeconds;
  return speed.toFixed(2);
}

// Function to handle API rate limits with exponential backoff
async function fetchWithRateLimit(fetchFunction, retries = 0) {
  try {
    return await fetchFunction();
  } catch (error) {
    if (error.httpStatus === 429 && retries < MAX_RATE_LIMIT_RETRIES) {
      // Discord's rate limit error
      rateLimitHits++;
      const retryAfter = error.retryAfter || 1; // Time in seconds
      console.log(`Rate limited. Waiting ${retryAfter}s before retry. (Attempt ${retries + 1}/${MAX_RATE_LIMIT_RETRIES})`);
      
      // Wait for the specified time before retrying
      await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
      
      // Recursive retry with exponential backoff
      return fetchWithRateLimit(fetchFunction, retries + 1);
    } else {
      // Either not a rate limit error or we've exceeded max retries
      throw error;
    }
  }
}

// Function to send status updates - consolidated version
async function checkAndSendStatusUpdate(channel, guild, currentChannelInfo = '', progressInfo = '') {
  const timeElapsed = getTimeElapsed();
  const speed = getProcessingSpeed();
  
  // Combine all the status information into one message
  const combinedStatusMessage = 
    `**Guild Export Status**\n` +
    `${currentChannelInfo ? `🔄 ${currentChannelInfo}\n` : ''}` +
    `📊 Processed ${totalMessagesProcessed.toLocaleString()} messages from ${guild.name}\n` +
    `⏱️ Time elapsed: ${timeElapsed}\n` +
    `⚡ Processing speed: ${speed} messages/second\n` +
    `${progressInfo ? `📈 ${progressInfo}\n` : ''}` +
    `🚦 Rate limit hits: ${rateLimitHits}`;
  
  try {
    if (!combinedStatusMessageRef) {
      // If we don't have a reference yet, create a new message
      combinedStatusMessageRef = await channel.send(combinedStatusMessage);
    } else {
      // Otherwise try to update the existing message
      await combinedStatusMessageRef.edit(combinedStatusMessage);
    }
  } catch (error) {
    console.error('Error updating status message, sending a new one:', error);
    // If there's an error (e.g., message was deleted), create a new reference
    try {
      combinedStatusMessageRef = await channel.send(combinedStatusMessage);
    } catch (innerError) {
      console.error('Failed to send new message:', innerError);
      // If we can't send a new message, just log and continue
    }
  }
  
  // Update the tracking counter
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

// Function to fetch messages from a channel with pagination
async function fetchMessagesFromChannel(channel, originalChannel, guild, channelStatusInfo = '') {
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
        content: msg.content,
        authorId: msg.author?.id,
        authorUsername: msg.author?.username,
        authorBot: msg.author?.bot,
        timestamp: msg.createdTimestamp,
        createdAt: msg.createdAt?.toISOString(),
        editedAt: msg.editedAt?.toISOString(),
        attachments: Array.from(msg.attachments.values()).map(attachment => ({
          id: attachment.id,
          name: attachment.name,
          url: attachment.url,
          contentType: attachment.contentType,
          size: attachment.size
        })),
        embeds: msg.embeds.map(embed => ({
          title: embed.title,
          description: embed.description,
          url: embed.url,
          color: embed.color,
          timestamp: embed.timestamp
        })),
        reactions: Array.from(msg.reactions.cache.values()).map(reaction => ({
          emoji: reaction.emoji.name,
          count: reaction.count
        }))
      }));
    });
    
    // Update the total message count
    totalMessagesProcessed += fetchedMessages.size;
    
    // Check if we should send a status update - use the combined update function
    if (totalMessagesProcessed - lastStatusUpdateCount >= 1000) {
      // Update the message count in the progress info
      const progressInfo = `Processing messages: ${messages.length} in current channel (${totalMessagesProcessed.toLocaleString()} total)`;
      await checkAndSendStatusUpdate(originalChannel, guild, channelStatusInfo, progressInfo);
    }
    
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

// Process threads in a channel or forum
async function processThreads(channel, channelData, originalChannel, guild, channelStatusInfo = '') {
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
        // Update the channel info to show we're processing a thread
        const threadStatusInfo = `${channelStatusInfo} - Thread: ${thread.name}`;
        
        const messages = await fetchMessagesFromChannel(thread, originalChannel, guild, threadStatusInfo);
        
        if (messages.length > 0) {
          // Sort messages chronologically
          messages.sort((a, b) => a.timestamp - b.timestamp);
          
          processedCount++;
          
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
  
  // Process archived threads similarly, passing along the status info
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
      
      // Update the status to show we're processing archived threads
      await checkAndSendStatusUpdate(
        originalChannel, 
        guild, 
        `${channelStatusInfo} - Processing archived threads`, 
        `Processed ${threadDataList.length} threads so far`
      );
      
      // Process archived threads in batches too
      const archivedThreadsArray = Array.from(archivedThreads.threads.values());
      
      for (let i = 0; i < archivedThreadsArray.length; i += batchSize) {
        const threadBatch = archivedThreadsArray.slice(i, i + batchSize);
        const batchPromises = threadBatch.map(async (thread) => {
          try {
            // Update the channel info to show we're processing an archived thread
            const threadStatusInfo = `${channelStatusInfo} - Archived thread: ${thread.name}`;
            
            const messages = await fetchMessagesFromChannel(thread, originalChannel, guild, threadStatusInfo);
            
            if (messages.length > 0) {
              // Sort messages chronologically
              messages.sort((a, b) => a.timestamp - b.timestamp);
              
              processedCount++;
              
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
        
        // Wait for each batch to complete before starting the next
        const batchResults = await Promise.all(batchPromises);
        threadDataList.push(...batchResults.filter(Boolean));
        
        // Update lastThreadId for the next batch
        if (archivedThreadsArray.length > 0) {
          lastThreadId = archivedThreadsArray[archivedThreadsArray.length - 1].id;
        }
        
        // Force garbage collection if available
        if (global.gc && i % 3 === 0) {
          try {
            global.gc();
          } catch (e) {
            // Continue if gc is not available
          }
        }
        
        // Brief pause between batches
        if (i + batchSize < archivedThreadsArray.length) {
          await new Promise(resolve => setTimeout(resolve, 200));
        }
      }
      
      // If we got less than the requested limit, we've reached the end
      if (archivedThreads.threads.size < options.limit) {
        hasMoreArchivedThreads = false;
        break;
      }
      
      // Add a small delay between batches to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 500));
    } catch (error) {
      console.error(`Error fetching archived threads for ${channel.name}:`, error);
      hasMoreArchivedThreads = false;
      break;
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

// Process an individual channel
async function processChannel(channel, guildId, originalChannel, guild, channelStatusInfo = '') {
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
      const messages = await fetchMessagesFromChannel(channel, originalChannel, guild, channelStatusInfo);
      
      if (messages.length > 0) {
        // Sort messages chronologically
        messages.sort((a, b) => a.timestamp - b.timestamp);
        channelData.messagesCount = messages.length;
        channelData.messages = messages; // Store messages directly in the channel object
      }
      
      // Get all threads in the forum
      const threads = await processThreads(channel, channelData, originalChannel, guild, channelStatusInfo);
      
      if (threads.length > 0) {
        channelData.threadsCount = threads.length;
        channelData.threads = threads; // Store threads directly in the channel object
      }
    } 
    // If it's a regular text channel
    else if (channel.type === ChannelType.GuildText) {
      // Get messages from the channel
      const messages = await fetchMessagesFromChannel(channel, originalChannel, guild, channelStatusInfo);
      
      if (messages.length > 0) {
        // Sort messages chronologically
        messages.sort((a, b) => a.timestamp - b.timestamp);
        
        channelData.messagesCount = messages.length;
        channelData.firstMessageTimestamp = messages.length > 0 ? messages[0].timestamp : null;
        channelData.messages = messages; // Store messages directly in the channel object
      }
      
      // Get all threads in the text channel
      const threads = await processThreads(channel, channelData, originalChannel, guild, channelStatusInfo);
      
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
      const messages = await fetchMessagesFromChannel(channel, originalChannel, guild, channelStatusInfo);
      
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

// Export all messages from a guild
async function exportGuildMessages(guild, originalMessage) {
  // Create export directory if needed
  if (!fs.existsSync(EXPORT_DIR)) {
    fs.mkdirSync(EXPORT_DIR, { recursive: true });
  }
  
  // IMPORTANT: Explicitly set the reference to null at the start
  combinedStatusMessageRef = null;
  
  // Get all channels we want to export
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
    
    // Create the channel info text
    const currentChannelInfo = `Processing channel ${i+1}/${channels.length}: ${channel.name}`;
    
    // Create the progress info text
    const progressInfo = `Progress: ${i}/${channels.length} channels processed`;
    
    // Update the combined status message with both channel and progress info
    await checkAndSendStatusUpdate(originalMessage.channel, guild, currentChannelInfo, progressInfo);
    
    // Process this channel
    const channelData = await processChannel(channel, guild.id, originalMessage.channel, guild, currentChannelInfo);
    
    if (channelData) {
      guildData.channels.push(channelData);
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
    // Update the status message with the final step info
    await checkAndSendStatusUpdate(originalMessage.channel, guild, 
      "Creating single JSON export file...", 
      `Completed ${guildData.channels.length}/${channels.length} channels`);
    
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
    await checkAndSendStatusUpdate(originalMessage.channel, guild,
      "Trying alternative streaming approach for large export...", 
      `Completed ${guildData.channels.length}/${channels.length} channels`);
    
    await writeGuildDataToStreamingJson(guildData, finalFileName);
    
    await originalMessage.reply(
      `Export complete!\n` +
      `✅ Exported ${totalMessagesProcessed.toLocaleString()} messages from ${guildData.channels.length} channels\n` +
      `⏱️ Total time: ${getTimeElapsed()}\n` +
      `📁 All data saved to: ${finalFileName}`
    );
  }
}

// Helper function to handle rate limits with exponential backoff
async function fetchWithRateLimit(apiCall, retries = 5, initialDelay = 1000) {
  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      return await apiCall();
    } catch (error) {
      // Check if this is a rate limit error
      if (error.httpStatus === 429) {
        rateLimitHits++;
        const retryAfter = error.retryAfter || 1; // Discord provides this in seconds
        console.log(`Rate limited! Waiting ${retryAfter} seconds before retry. Attempt ${attempt + 1}/${retries}`);
        // Wait for the specified time plus a small random offset
        await new Promise(resolve => setTimeout(resolve, retryAfter * 1000 + Math.random() * 500));
      } else if (attempt < retries - 1) {
        // For other errors, also back off but with exponential delay
        const delay = initialDelay * Math.pow(2, attempt) + Math.random() * 500;
        console.error(`Error in API call, retrying in ${delay}ms. Attempt ${attempt + 1}/${retries}. Error: ${error.message}`);
        await new Promise(resolve => setTimeout(resolve, delay));
      } else {
        // If we've exhausted our retries, throw the error
        throw error;
      }
    }
  }
}

// Helper function to clean objects by removing undefined and null values
function cleanObject(obj) {
  return Object.fromEntries(
    Object.entries(obj)
      .filter(([_, value]) => value !== undefined && value !== null)
      .map(([key, value]) => {
        // Clean nested objects recursively
        if (typeof value === 'object' && !Array.isArray(value)) {
          return [key, cleanObject(value)];
        }
        // Clean objects in arrays
        if (Array.isArray(value)) {
          return [key, value.map(item => 
            typeof item === 'object' && item !== null ? cleanObject(item) : item
          )];
        }
        return [key, value];
      })
  );
}

// Helper function to get formatted elapsed time
function getTimeElapsed() {
  const now = new Date();
  const elapsed = now - exportStartTime;
  const hours = Math.floor(elapsed / (1000 * 60 * 60));
  const minutes = Math.floor((elapsed % (1000 * 60 * 60)) / (1000 * 60));
  const seconds = Math.floor((elapsed % (1000 * 60)) / 1000);
  return `${hours}h ${minutes}m ${seconds}s`;
}

// Helper function to calculate processing speed
function getProcessingSpeed() {
  const elapsed = new Date() - exportStartTime;
  const elapsedSeconds = elapsed / 1000;
  // Avoid division by zero
  if (elapsedSeconds < 1) return '0.00';
  return (totalMessagesProcessed / elapsedSeconds).toFixed(2);
}

// Process all threads in a channel
async function processThreads(channel, channelData, originalChannel, guild, channelStatusInfo = '') {
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
        // Update the channel info to show we're processing a thread
        const threadStatusInfo = `${channelStatusInfo} - Thread: ${thread.name}`;
        
        const messages = await fetchMessagesFromChannel(thread, originalChannel, guild, threadStatusInfo);
        
        if (messages.length > 0) {
          // Sort messages chronologically
          messages.sort((a, b) => a.timestamp - b.timestamp);
          
          processedCount++;
          
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
    
    // Update the progress info in the status message
    if (i % 9 === 0 || i + batchSize >= activeThreadsArray.length) {
      const progressInfo = `Processed ${threadDataList.length}/${activeThreadsArray.length} threads`;
      await checkAndSendStatusUpdate(
        originalChannel, 
        guild, 
        channelStatusInfo, 
        progressInfo
      );
    }
    
    // Force garbage collection if available
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
  
  // Process archived threads similarly, passing along the status info
  let lastThreadId = null;
  let hasMoreArchivedThreads = true;
  let archivedThreadsProcessed = 0;
  
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
      
      // Update the status to show we're processing archived threads
      const archivedStatus = `${channelStatusInfo} - Processing archived threads`;
      const progressInfo = `Processed ${threadDataList.length} threads (${archivedThreadsProcessed} archived)`;
      await checkAndSendStatusUpdate(originalChannel, guild, archivedStatus, progressInfo);
      
      // Process archived threads in batches too
      const archivedThreadsArray = Array.from(archivedThreads.threads.values());
      
      for (let i = 0; i < archivedThreadsArray.length; i += batchSize) {
        const threadBatch = archivedThreadsArray.slice(i, i + batchSize);
        const batchPromises = threadBatch.map(async (thread) => {
          try {
            // Update the channel info to show we're processing an archived thread
            const threadStatusInfo = `${channelStatusInfo} - Archived thread: ${thread.name}`;
            
            const messages = await fetchMessagesFromChannel(thread, originalChannel, guild, threadStatusInfo);
            
            if (messages.length > 0) {
              // Sort messages chronologically
              messages.sort((a, b) => a.timestamp - b.timestamp);
              
              processedCount++;
              archivedThreadsProcessed++;
              
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
        
        // Wait for each batch to complete before starting the next
        const batchResults = await Promise.all(batchPromises);
        threadDataList.push(...batchResults.filter(Boolean));
        
        // Update the status periodically
        if (i % 9 === 0 || i + batchSize >= archivedThreadsArray.length) {
          const progressInfo = `Processed ${threadDataList.length} threads (${archivedThreadsProcessed} archived)`;
          await checkAndSendStatusUpdate(
            originalChannel, 
            guild, 
            archivedStatus, 
            progressInfo
          );
        }
        
        // Update lastThreadId for the next batch
        if (archivedThreadsArray.length > 0) {
          lastThreadId = archivedThreadsArray[archivedThreadsArray.length - 1].id;
        }
        
        // Force garbage collection if available
        if (global.gc && i % 3 === 0) {
          try {
            global.gc();
          } catch (e) {
            // Continue if gc is not available
          }
        }
        
        // Brief pause between batches
        if (i + batchSize < archivedThreadsArray.length) {
          await new Promise(resolve => setTimeout(resolve, 200));
        }
      }
      
      // If we got less than the requested limit, we've reached the end
      if (archivedThreads.threads.size < options.limit) {
        hasMoreArchivedThreads = false;
        break;
      }
      
      // Add a small delay between batches to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 500));
    } catch (error) {
      console.error(`Error fetching archived threads for ${channel.name}:`, error);
      hasMoreArchivedThreads = false;
      break;
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

// Initialize the Discord client
client.once('ready', () => {
  console.log(`Bot is ready! Logged in as ${client.user.tag}`);
  console.log(`Current date and time: ${new Date().toISOString()}`);
  console.log(`Current user: ${process.env.USER || 'unknown'}`);
});

// Handle commands
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
      
      // IMPORTANT: Explicitly reset the message reference
      combinedStatusMessageRef = null;
      
      await exportGuildMessages(guild, message);
    } catch (error) {
      console.error('Error during export:', error);
      message.reply('An error occurred during export. Check console for details.');
    }
  } else if (command === 'help') {
    message.reply(`
**Available Commands**
\`${PREFIX}exportguild [guildId]\` - Export all messages from a guild. Uses current guild if no ID provided.
\`${PREFIX}help\` - Show this help message.

**Status**
Bot Version: ${BOT_VERSION}
Current Time: ${new Date().toISOString()}
`);
  }
});

// Handle errors to prevent crashes
client.on('error', (error) => {
  console.error('Discord client error:', error);
});

process.on('unhandledRejection', (error) => {
  console.error('Unhandled promise rejection:', error);
});

// Login to Discord with proper error handling
client.login(process.env.DISCORD_TOKEN).catch(err => {
  console.error('Failed to login to Discord:', err);
  console.log('Please check your DISCORD_TOKEN in the .env file and ensure the bot is properly configured.');
  process.exit(1);
});

// Export functions for testing purposes if needed
module.exports = {
  exportGuildMessages,
  processChannel,
  fetchMessagesFromChannel,
  processThreads
};