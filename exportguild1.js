// Import required libraries
const fs = require('fs');
const path = require('path');
const { Client, GatewayIntentBits, ChannelType } = require('discord.js');
require('dotenv').config();

// Configuration
const PREFIX = '!'; // Command prefix
const EXPORT_DIR = './exports'; // Directory to save exports
const MAX_RATE_LIMIT_RETRIES = 5; // Maximum retries when rate limited
const MESSAGES_BEFORE_SAVE = 10000; // Save every n messages

// Test write permissions to the export directory
console.log(`[DEBUG] Testing write permissions to ${EXPORT_DIR} directory...`);
try {
  // Create export directory if it doesn't exist
  if (!fs.existsSync(EXPORT_DIR)) {
    fs.mkdirSync(EXPORT_DIR, { recursive: true });
    console.log(`[DEBUG] Created export directory: ${EXPORT_DIR}`);
  }
  
  // Test write permissions with a temporary file
  const testFile = path.join(EXPORT_DIR, 'test_write.txt');
  fs.writeFileSync(testFile, 'Test write permissions');
  fs.unlinkSync(testFile); // Delete the test file
  console.log('[DEBUG] Write permissions confirmed for export directory');
} catch (error) {
  console.error('[DEBUG] PERMISSIONS ERROR: Cannot write to export directory:', error);
  console.error('[DEBUG] Error details:', error.stack);
}

// Configure the Discord client with necessary intents
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.DirectMessages
  ]
});

// Global variables for tracking export progress
let totalMessagesProcessed = 0;
let lastStatusUpdateCount = 0;
let rateLimitHits = 0;
let exportStartTime = null;
let combinedStatusMessageRef = null; // Single message reference for all status updates
let statusMessageSequence = 0; // To track status message sequence
let readyMessageShown = false; // Flag to prevent duplicate ready messages

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

function saveExportProgress(guild, currentChannels, outputFileName, originalChannel) {
  try {
    console.log(`[DEBUG] Saving export progress: ${totalMessagesProcessed} messages processed`);
    console.log(`[DEBUG] Channels to save: ${currentChannels.length}`);
    console.log(`[DEBUG] Output file: ${outputFileName}`);
    
    // Report saving action to Discord
    checkAndSendStatusUpdate(
      originalChannel,
      guild,
      "Saving data to file and clearing memory",
      `After processing ${totalMessagesProcessed.toLocaleString()} messages`
    ).catch(err => console.error("Failed to send status update:", err));
    
    // Create or update the export file
    let guildData = {
      id: guild.id,
      name: guild.name,
      exportStartedAt: new Date().toISOString(),
      timestamp: Date.now(),
      channels: currentChannels
    };
    
    // Check if file already exists, and if so, load existing channels
    if (fs.existsSync(outputFileName)) {
      console.log(`[DEBUG] File exists, loading existing data`);
      try {
        const existingData = JSON.parse(fs.readFileSync(outputFileName));
        
        // Preserve the original start time
        guildData.exportStartedAt = existingData.exportStartedAt;
        guildData.timestamp = existingData.timestamp;
        
        // Merge existing channels with new ones
        guildData.channels = [...(existingData.channels || []), ...currentChannels];
        console.log(`[DEBUG] Total channels after merge: ${guildData.channels.length}`);
      } catch (readError) {
        console.error('[DEBUG] Error reading existing file:', readError);
        // Continue with current data only
      }
    } else {
      console.log(`[DEBUG] Creating new file: ${outputFileName}`);
    }
    
    // Force synchronous write to ensure data is saved
    fs.writeFileSync(outputFileName, JSON.stringify(guildData, null, 2));
    console.log(`[DEBUG] Data successfully written to file`);
    
    // Return an empty array after saving to clear memory
    return [];
  } catch (error) {
    console.error('[DEBUG] Error saving export progress:', error);
    return currentChannels; // Return the original array if saving failed
  }
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
  const currentUtcTime = new Date().toISOString(); // Get current UTC time
  statusMessageSequence++; // Increment sequence number
  
  // Combine all the status information into one message
  const combinedStatusMessage = 
    `**Guild Export Status** (#${statusMessageSequence})\n` +
    `${currentChannelInfo ? `🔄 ${currentChannelInfo}\n` : ''}` +
    `📊 Processed ${totalMessagesProcessed.toLocaleString()} messages from ${guild.name}\n` +
    `⏱️ Time elapsed: ${timeElapsed}\n` +
    `⚡ Processing speed: ${speed} messages/second\n` +
    `${progressInfo ? `📈 ${progressInfo}\n` : ''}` +
    `🚦 Rate limit hits: ${rateLimitHits}\n` +
    `⏰ Last update: ${currentUtcTime}`;
  
  try {
    if (!combinedStatusMessageRef) {
      // If we don't have a reference yet, create a new message
      console.log('Creating new status message');
      combinedStatusMessageRef = await channel.send(combinedStatusMessage);
    } else {
      // Otherwise try to update the existing message
      console.log('Updating existing status message');
      await combinedStatusMessageRef.edit(combinedStatusMessage);
    }
  } catch (error) {
    console.error('Error updating status message, sending a new one:', error);
    // If there's an error (e.g., message was deleted), create a new reference
    try {
      console.log('Creating replacement status message after error');
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
async function processThreads(channel, channelData, originalChannel, guild, channelStatusInfo = '', outputFileName) {
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
  
  // Check if we need to save to file based on message count
  if (shouldSaveBasedOnMessageCount()) {
  console.log(`[DEBUG] Save triggered in processThreads at ${totalMessagesProcessed} messages`);
  await saveProcessedDataToFile(guild, outputFileName, threadDataList);
  return [];
}
  
  return threadDataList;
}

// Process an individual channel
async function processChannel(channel, guildId, originalChannel, guild, channelStatusInfo = '', outputFileName) {
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
      const threads = await processThreads(channel, channelData, originalChannel, guild, channelStatusInfo, outputFileName);
      
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
      const threads = await processThreads(channel, channelData, originalChannel, guild, channelStatusInfo, outputFileName);
      
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
    
    // Check if we've reached message threshold and need to save
    if (shouldSaveBasedOnMessageCount()) {
  console.log(`[DEBUG] Save triggered in processChannel at ${totalMessagesProcessed} messages`);
  await saveChannelDataToFile(guild, outputFileName, channelData);
  return null;
}
    
    return channelData;
  } catch (error) {
    console.error(`Error processing channel ${channel.name}:`, error);
    return null;
  }
}

// Function to save channel data to file and free memory
async function saveChannelDataToFile(guild, outputFileName, channelData) {
  try {
    // Check if the file exists
    const fileExists = fs.existsSync(outputFileName);
    
    if (!fileExists) {
      // If file doesn't exist, create it with initial structure
      const initialData = {
        id: guild.id,
        name: guild.name,
        exportStartedAt: new Date().toISOString(),
        timestamp: Date.now(),
        channels: []
      };
      
      fs.writeFileSync(outputFileName, JSON.stringify(initialData));
    }
    
    // Read current data
    const fileContent = fs.readFileSync(outputFileName);
    const guildData = JSON.parse(fileContent);
    
    // Add the channel data
    if (channelData !== null) {
      guildData.channels.push(channelData);
    }
    
    // Update export metadata
    guildData.lastUpdatedAt = new Date().toISOString();
    guildData.totalMessagesExportedSoFar = totalMessagesProcessed;
    
    // Write back to file
    fs.writeFileSync(outputFileName, JSON.stringify(guildData));
    
    console.log(`Saved data to ${outputFileName} (${totalMessagesProcessed.toLocaleString()} messages so far)`);
    
    // Force garbage collection if available
    if (global.gc) {
      try { global.gc(); } catch (e) { /* Continue */ }
    }
    
    return true;
  } catch (error) {
    console.error('Error saving channel data to file:', error);
    return false;
  }
}

// Function to save processed threads data to file
async function saveProcessedDataToFile(guild, outputFileName, threadDataList) {
  try {
    // Check if the file exists
    const fileExists = fs.existsSync(outputFileName);
    
    if (!fileExists) {
      // If file doesn't exist, create it with initial structure
      const initialData = {
        id: guild.id,
        name: guild.name,
        exportStartedAt: new Date().toISOString(),
        timestamp: Date.now(),
        channels: []
      };
      
      fs.writeFileSync(outputFileName, JSON.stringify(initialData));
    }
    
    // Read current data
    const fileContent = fs.readFileSync(outputFileName);
    const guildData = JSON.parse(fileContent);
    
    // Find the last channel to update it with thread data
    if (guildData.channels.length > 0 && threadDataList.length > 0) {
      const lastChannel = guildData.channels[guildData.channels.length - 1];
      
      // Add threads to the last channel
      if (!lastChannel.threads) {
        lastChannel.threads = [];
        lastChannel.threadsCount = 0;
      }
      
      lastChannel.threads = lastChannel.threads.concat(threadDataList);
      lastChannel.threadsCount = lastChannel.threads.length;
    }
    
    // Update export metadata
    guildData.lastUpdatedAt = new Date().toISOString();
    guildData.totalMessagesExportedSoFar = totalMessagesProcessed;
    
    // Write back to file
    fs.writeFileSync(outputFileName, JSON.stringify(guildData));
    
    console.log(`Saved thread data to ${outputFileName} (${totalMessagesProcessed.toLocaleString()} messages so far)`);
    
    // Force garbage collection if available
    if (global.gc) {
      try { global.gc(); } catch (e) { /* Continue */ }
    }
    
    return true;
  } catch (error) {
    console.error('Error saving thread data to file:', error);
    return false;
  }
}

// Initialize the Discord client
client.once('ready', () => {
  console.log(`Bot is ready! Logged in as ${client.user.tag}`);
  console.log(`Current date and time: ${new Date().toISOString()}`);
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
    // Add debug information about the environment
    console.log(`[DEBUG] Current directory: ${process.cwd()}`);
    console.log(`[DEBUG] Export directory: ${path.resolve(EXPORT_DIR)}`);
    console.log(`[DEBUG] Can write to export directory: ${fs.existsSync(EXPORT_DIR) && fs.accessSync(EXPORT_DIR, fs.constants.W_OK)}`);
    
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

// Function to handle periodic saving of data and memory cleanup
async function saveAndCleanupData(guild, originalChannel, outputFileName, data, dataType = 'channel') {
  try {
    // Read current file content
    let fileContent = {};
    if (fs.existsSync(outputFileName)) {
      fileContent = JSON.parse(fs.readFileSync(outputFileName));
    } else {
      // Initialize file with basic structure if it doesn't exist
      fileContent = {
        id: guild.id,
        name: guild.name,
        exportStartedAt: new Date().toISOString(),
        timestamp: Date.now(),
        totalMessagesExportedSoFar: totalMessagesProcessed,
        channels: []
      };
    }
    
    // Update appropriate data
    if (dataType === 'channel' && data) {
      fileContent.channels.push(data);
    } else if (dataType === 'thread' && data && data.length > 0) {
      // Find last channel to add threads to
      if (fileContent.channels.length > 0) {
        const lastChannel = fileContent.channels[fileContent.channels.length - 1];
        if (!lastChannel.threads) {
          lastChannel.threads = [];
        }
        lastChannel.threads.push(...data);
        lastChannel.threadsCount = (lastChannel.threads || []).length;
      }
    }
    
    // Update metadata
    fileContent.lastUpdatedAt = new Date().toISOString();
    fileContent.totalMessagesExportedSoFar = totalMessagesProcessed;
    
    // Write updated content back to file
    await writeGuildDataToStreamingJson(fileContent, outputFileName);
    
    // Send a status message
    await checkAndSendStatusUpdate(
      originalChannel, 
      guild,
      `Saving data to file (${totalMessagesProcessed.toLocaleString()} messages processed)`,
      `Memory cleaned: ${dataType === 'channel' ? 'Channel data' : 'Thread data'}`
    );
    
    // Force garbage collection if available
    if (global.gc) {
      try { global.gc(); } catch (e) { /* Continue */ }
    }
    
    return true;
  } catch (error) {
    console.error(`Error in saveAndCleanupData: ${error}`);
    return false;
  }
}

// Function to update final file with completion information
async function finalizeExportFile(guild, outputFileName, originalMessage) {
  try {
    // Read current file content
    let fileContent = {};
    if (fs.existsSync(outputFileName)) {
      fileContent = JSON.parse(fs.readFileSync(outputFileName));
    } else {
      throw new Error("Output file not found for finalization");
    }
    
    // Update with final metadata
    fileContent.exportCompletedAt = new Date().toISOString();
    fileContent.totalMessagesExported = totalMessagesProcessed;
    fileContent.totalChannelsExported = fileContent.channels.length;
    fileContent.exportDurationMs = new Date() - exportStartTime;
    
    // Write finalized file
    await writeGuildDataToStreamingJson(fileContent, outputFileName);
    
    await originalMessage.reply(
      `Export complete!\n` +
      `✅ Exported ${totalMessagesProcessed.toLocaleString()} messages from ${fileContent.channels.length} channels\n` +
      `⏱️ Total time: ${getTimeElapsed()}\n` +
      `📁 All data saved to: ${outputFileName}`
    );
    
    return true;
  } catch (error) {
    console.error(`Error finalizing export file: ${error}`);
    
    await originalMessage.reply(
      `Export process finished but encountered an error finalizing the file.\n` +
      `✅ Processed ${totalMessagesProcessed.toLocaleString()} messages\n` +
      `⏱️ Total time: ${getTimeElapsed()}\n` +
      `📁 Data saved to: ${outputFileName}\n` +
      `⚠️ Error: ${error.message}`
    );
    
    return false;
  }
}

// Check if we need to save data based on message count
function shouldSaveBasedOnMessageCount() {
  // Save after every MESSAGES_BEFORE_SAVE messages (e.g., at 10K, 20K, 30K)
  if (totalMessagesProcessed > 0 && totalMessagesProcessed % MESSAGES_BEFORE_SAVE === 0) {
    console.log(`[DEBUG] Should save at exact ${MESSAGES_BEFORE_SAVE} multiple: ${totalMessagesProcessed}`);
    return true;
  }
  
  // Also save if we're slightly past a threshold (to catch any edge cases)
  const remainder = totalMessagesProcessed % MESSAGES_BEFORE_SAVE;
  if (remainder > 0 && remainder < 100 && totalMessagesProcessed > MESSAGES_BEFORE_SAVE) {
    console.log(`[DEBUG] Should save slightly past threshold: ${totalMessagesProcessed}`);
    return true;
  }
  
  return false;
}

// Helper function to handle writing large JSON data
async function writeGuildDataToFile(guildData, filePath, isCompleteExport = false) {
  try {
    // Basic guild data to preserve
    const baseGuildData = {
      id: guildData.id,
      name: guildData.name,
      exportStartedAt: guildData.exportStartedAt || new Date().toISOString(),
      timestamp: guildData.timestamp || Date.now(),
      lastUpdatedAt: new Date().toISOString(),
      totalMessagesExportedSoFar: totalMessagesProcessed
    };
    
    // Add completion data if this is the final export
    if (isCompleteExport) {
      baseGuildData.exportCompletedAt = new Date().toISOString();
      baseGuildData.totalMessagesExported = totalMessagesProcessed;
      baseGuildData.exportDurationMs = new Date() - exportStartTime;
    }
    
    // Write the data
    fs.writeFileSync(filePath, JSON.stringify({
      ...baseGuildData,
      channels: guildData.channels || []
    }, null, 2));
    
    return true;
  } catch (error) {
    console.error('Error writing to file:', error);
    
    // If the file is too large, try streaming method
    try {
      await writeGuildDataToStreamingJson(guildData, filePath, isCompleteExport);
      return true;
    } catch (streamError) {
      console.error('Streaming write also failed:', streamError);
      return false;
    }
  }
}

// Helper function to write large JSON data using streaming
async function writeGuildDataToStreamingJson(guildData, filePath, isCompleteExport = false) {
  console.log(`[DEBUG] Starting streaming write to ${filePath}`);
  console.log(`[DEBUG] Guild data channels: ${(guildData.channels || []).length}`);
  console.log(`[DEBUG] Total messages processed: ${totalMessagesProcessed}`);
  return new Promise((resolve, reject) => {
    try {
      const stream = fs.createWriteStream(filePath);
      
      // Write opening brace and basic guild info
      stream.write('{\n');
      stream.write(`  "id": ${JSON.stringify(guildData.id)},\n`);
      stream.write(`  "name": ${JSON.stringify(guildData.name)},\n`);
      stream.write(`  "exportStartedAt": ${JSON.stringify(guildData.exportStartedAt || new Date().toISOString())},\n`);
      stream.write(`  "timestamp": ${guildData.timestamp || Date.now()},\n`);
      stream.write(`  "lastUpdatedAt": ${JSON.stringify(new Date().toISOString())},\n`);
      stream.write(`  "totalMessagesExportedSoFar": ${totalMessagesProcessed},\n`);
      
      // Add completion data if this is the final export
      if (isCompleteExport) {
        stream.write(`  "exportCompletedAt": ${JSON.stringify(new Date().toISOString())},\n`);
        stream.write(`  "totalMessagesExported": ${totalMessagesProcessed},\n`);
        stream.write(`  "exportDurationMs": ${new Date() - exportStartTime},\n`);
      }
      
      // Begin channels array
      stream.write('  "channels": [\n');
      
      // Write each channel
      const channelCount = (guildData.channels || []).length;
      for (let i = 0; i < channelCount; i++) {
        const channelJson = JSON.stringify(guildData.channels[i], null, 2);
        
        // Add proper indentation and commas
        const formattedChannel = channelJson
          .split('\n')
          .map(line => '    ' + line)
          .join('\n');
          
        stream.write(formattedChannel + (i < channelCount - 1 ? ',\n' : '\n'));
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
      console.error(`[DEBUG] Detailed error in streaming write:`, error);
      console.error(`[DEBUG] Error stack:`, error.stack);
      reject(error);
    }
  });
}

// Function to save the current export progress and clear memory
async function saveExportProgress(guild, currentChannels, outputFileName, originalChannel) {
  try {
    // Report saving action
    await checkAndSendStatusUpdate(
      originalChannel,
      guild,
      "Saving data to file and clearing memory",
      `After processing ${totalMessagesProcessed.toLocaleString()} messages`
    );
    
    // Create or update the export file
    let guildData = {
      id: guild.id,
      name: guild.name,
      exportStartedAt: new Date().toISOString(),
      timestamp: Date.now(),
      channels: currentChannels
    };
    
    // Check if file already exists, and if so, load existing channels
    if (fs.existsSync(outputFileName)) {
      try {
        const existingData = JSON.parse(fs.readFileSync(outputFileName));
        
        // Preserve the original start time
        guildData.exportStartedAt = existingData.exportStartedAt;
        guildData.timestamp = existingData.timestamp;
        
        // Merge existing channels with new ones
        guildData.channels = [...existingData.channels, ...currentChannels];
      } catch (readError) {
        console.error('Error reading existing file:', readError);
        // Continue with current data only
      }
    }
    
    // Write to file
    await writeGuildDataToFile(guildData, outputFileName);
    
    // Return an empty array after saving to clear memory
    return [];
  } catch (error) {
    console.error('Error saving export progress:', error);
    return currentChannels; // Return the original array if saving failed
  }
}

// Export all messages from a guild
async function exportGuildMessages(guild, originalMessage) {
  // Create export directory if needed
  if (!fs.existsSync(EXPORT_DIR)) {
    fs.mkdirSync(EXPORT_DIR, { recursive: true });
  }
  
  // Reset status tracking
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
  
  // Create the single output file name for this guild
const outputFileName = path.join(EXPORT_DIR, `${guild.name.replace(/[^a-z0-9]/gi, '_')}_${guild.id}_export.json`);
console.log(`[DEBUG] Output file path: ${outputFileName}`);
console.log(`[DEBUG] Absolute path: ${path.resolve(outputFileName)}`);
  
  // Initialize the output file with empty structure
  const initialGuildData = {
    id: guild.id,
    name: guild.name,
    exportStartedAt: new Date().toISOString(),
    timestamp: Date.now(),
    totalChannelsToProcess: channels.length,
    channels: []
  };
  await writeGuildDataToFile(initialGuildData, outputFileName);
  
  // Reset counters and set start time
  totalMessagesProcessed = 0;
  lastStatusUpdateCount = 0;
  rateLimitHits = 0;
  statusMessageSequence = 0;
  exportStartTime = new Date();
  
  // Sort channels by type and name
  channels.sort((a, b) => {
    if (a.type !== b.type) return a.type - b.type;
    return a.name.localeCompare(b.name);
  });
  
  await originalMessage.channel.send(`Found ${channels.length} channels to process (including text channels, forums, and threads).`);
  
  // Keep a local array of processed channels to periodically save
  let processedChannels = [];
  
  // Process channels one by one
  for (let i = 0; i < channels.length; i++) {
    const channel = channels[i];
    
    // Create the channel info text
    const currentChannelInfo = `Processing channel ${i+1}/${channels.length}: ${channel.name}`;
    
    // Create the progress info text
    const progressInfo = `Progress: ${i}/${channels.length} channels processed`;
    
    // Update the status message
    await checkAndSendStatusUpdate(originalMessage.channel, guild, currentChannelInfo, progressInfo);
    
    // Process this channel
    try {
      const channelData = await processChannel(channel, guild.id, originalMessage.channel, guild, currentChannelInfo, outputFileName);
      
      if (channelData) {
        // Add to our local array
        processedChannels.push(channelData);
        
        // Check if we should save and clear memory
        if (shouldSaveBasedOnMessageCount()) {
  console.log(`[DEBUG] Save triggered in main loop: ${totalMessagesProcessed} messages, ${processedChannels.length} channels`);
  processedChannels = await saveExportProgress(guild, processedChannels, outputFileName, originalMessage.channel);
  console.log(`[DEBUG] After saving, processedChannels.length = ${processedChannels.length}`);
}
      }
    } catch (channelError) {
      console.error(`Error processing channel ${channel.name}:`, channelError);
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
  
  // Save any remaining channels
  if (processedChannels.length > 0) {
    await saveExportProgress(guild, processedChannels, outputFileName, originalMessage.channel);
  }
  
  // Finalize the export with completion information
  try {
    // Read the current file
    const fileContent = fs.readFileSync(outputFileName);
    const finalGuildData = JSON.parse(fileContent);
    
    // Write the final version with completion data
    await writeGuildDataToFile(finalGuildData, outputFileName, true);
    
    await originalMessage.reply(
      `Export complete!\n` +
      `✅ Exported ${totalMessagesProcessed.toLocaleString()} messages from ${finalGuildData.channels.length} channels\n` +
      `⏱️ Total time: ${getTimeElapsed()}\n` +
      `📁 All data saved to: ${outputFileName}`
    );
    
  } catch (error) {
    console.error('Error finalizing export file:', error);
    
    await originalMessage.reply(
      `Export process finished with errors.\n` +
      `✅ Processed ${totalMessagesProcessed.toLocaleString()} messages\n` +
      `⏱️ Total time: ${getTimeElapsed()}\n` +
      `📁 Data saved to: ${outputFileName}\n` +
      `⚠️ Error: ${error.message}`
    );
  }
}

// Process an individual channel
async function processChannel(channel, guildId, originalChannel, guild, channelStatusInfo = '', outputFileName) {
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
      const threads = await processThreads(channel, channelData, originalChannel, guild, channelStatusInfo, outputFileName);
      
      if (threads && threads.length > 0) {
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
      const threads = await processThreads(channel, channelData, originalChannel, guild, channelStatusInfo, outputFileName);
      
      if (threads && threads.length > 0) {
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
  
  // Process active threads in smaller batches
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
    
    // Force garbage collection if available
    if (global.gc && i % 3 === 0) {
      try { global.gc(); } catch (e) { /* Continue */ }
    }
    
    // Brief pause between batches
    if (i + batchSize < activeThreadsArray.length) {
      await new Promise(resolve => setTimeout(resolve, 200));
    }
  }
  
  // Process archived threads
  let lastThreadId = null;
  let hasMoreArchivedThreads = true;
  
  while (hasMoreArchivedThreads) {
    try {
      const options = { limit: 50 };
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
          try { global.gc(); } catch (e) { /* Continue */ }
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