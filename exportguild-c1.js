require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { 
  Client, 
  GatewayIntentBits, 
  Partials, 
  PermissionFlagsBits,
  ChannelType
} = require('discord.js');

// Set up the Discord client with necessary intents to read messages
const client = new Client({
  intents: [
    GatewayIntentBits.Guilds,
    GatewayIntentBits.GuildMessages,
    GatewayIntentBits.MessageContent,
    GatewayIntentBits.GuildMembers
  ],
  partials: [
    Partials.Channel,
    Partials.Message,
    Partials.ThreadMember
  ]
});

// Track active exports to prevent multiple exports in the same guild
const activeExports = new Set();

// Parse excluded channels from environment variable
const excludedChannels = process.env.EX_CHANNELS ? 
  new Set(process.env.EX_CHANNELS.split(',').map(id => id.trim())) : 
  new Set();

// Export threshold - how many messages to process before auto-save
const EXPORT_THRESHOLD = parseInt(process.env.METADATA_EXPORT_THRESHOLD) || 10000;

// Memory limit in MB - default to 500MB if not specified
const MEMORY_LIMIT_MB = parseInt(process.env.MEMORY_LIMIT) || 500;
// Convert to bytes for easier comparison with process.memoryUsage()
const MEMORY_LIMIT_BYTES = MEMORY_LIMIT_MB * 1024 * 1024;

// Memory check frequency in milliseconds
const MEMORY_CHECK_INTERVAL = 10000; // Check every 10 seconds

// Auto-save interval in milliseconds (save every 30 seconds)
const AUTO_SAVE_INTERVAL = 30000;

// Maximum concurrent API requests
const MAX_CONCURRENT_REQUESTS = 5;

client.once('ready', () => {
  console.log(`Bot is ready! Logged in as ${client.user.tag}`);
  console.log(`Memory limit set to: ${MEMORY_LIMIT_MB} MB`);
});

client.on('messageCreate', async (message) => {
  // Check if the message is the export command
  if (message.content.toLowerCase() === '!exportguild') {
    // Verify the user has administrator permissions
    if (!message.member.permissions.has(PermissionFlagsBits.Administrator)) {
      return message.reply('You need administrator permissions to use this command.');
    }

    // Check if an export is already running for this guild
    if (activeExports.has(message.guildId)) {
      return message.reply('An export is already running for this guild!');
    }

    // Set guild as being exported
    activeExports.add(message.guildId);

    try {
      await handleExportGuild(message);
    } catch (error) {
      console.error('Critical error during export:', error);
      message.channel.send(`Critical error during export: ${error.message}`);
    } finally {
      // Remove guild from active exports when done (even if there was an error)
      activeExports.delete(message.guildId);
    }
  }
});

// Function to check current memory usage and return details
function checkMemoryUsage() {
  const memoryUsage = process.memoryUsage();
  const heapUsed = memoryUsage.heapUsed;
  const rss = memoryUsage.rss; // Resident Set Size - total memory allocated
  
  const heapUsedMB = Math.round(heapUsed / 1024 / 1024 * 100) / 100;
  const rssMB = Math.round(rss / 1024 / 1024 * 100) / 100;
  
  return {
    heapUsed,
    rss,
    heapUsedMB,
    rssMB,
    isAboveLimit: rss > MEMORY_LIMIT_BYTES,
    percentOfLimit: Math.round((rss / MEMORY_LIMIT_BYTES) * 100)
  };
}

// Log memory usage
function logMemoryUsage(prefix = '') {
  const memory = checkMemoryUsage();
  console.log(`${prefix} Memory usage: ${memory.rssMB} MB / ${MEMORY_LIMIT_MB} MB (${memory.percentOfLimit}% of limit), Heap: ${memory.heapUsedMB} MB`);
  return memory;
}

async function handleExportGuild(message) {
  const guild = message.guild;
  
  console.log(`Starting export for guild: ${guild.name} (${guild.id})`);
  logMemoryUsage('Initial');
  
  // Create status message
  const statusMessage = await message.channel.send(
    `Guild Export Status (#1)\n` +
    `🔄 Initializing export...`
  );

  // Create a unique filename for this export
  const timestamp = new Date().toISOString();
  const isoTimestamp = timestamp;
  const unixTimestamp = Date.now();
  const sanitizedGuildName = guild.name.replace(/[^a-z0-9]/gi, '-').toLowerCase();
  const filename = `${sanitizedGuildName}-${guild.id}-${timestamp.replace(/[:.]/g, '-')}.json`;
  const filepath = path.join(process.cwd(), filename);
  
  console.log(`Export file will be created at: ${filepath}`);
  
  // Initialize export structure
  const exportData = {
    id: guild.id,
    name: guild.name,
    exportStartedAt: isoTimestamp,
    timestamp: unixTimestamp,
    lastUpdatedAt: isoTimestamp,
    totalMessagesExportedSoFar: 0,
    channels: []
  };
  
  // Create the file immediately with initial data
  fs.writeFileSync(filepath, JSON.stringify(exportData, null, 2));
  console.log(`Created initial export file: ${filename}`);
  
  // Initialize export state
  const exportState = {
    startTime: Date.now(),
    processedMessages: 0,
    totalChannels: 0,
    processedChannels: 0,
    currentChannelIndex: 0,
    currentChannel: null,
    messagesInCurrentChannel: 0,
    rateLimitHits: 0,
    lastStatusUpdateTime: Date.now(),
    lastAutoSaveTime: Date.now(),
    memoryCheckCount: 0,
    memoryTriggeredSaves: 0,
    runningTasksCount: 0,
    filename: filename,
    filepath: filepath,
    exportData: exportData,
    channelsMap: new Map(), // For tracking channels by ID
    saveInProgress: false,
    memoryLimit: MEMORY_LIMIT_BYTES
  };

  // Update the status message initially
  await updateStatusMessage(statusMessage, exportState, guild);
  
  // Set up auto-save timer
  const autoSaveTimer = setInterval(() => {
    if (!exportState.saveInProgress) {
      saveExportProgress(exportState, 'AUTO_SAVE');
    }
  }, AUTO_SAVE_INTERVAL);
  
  // Set up memory check timer
  const memoryCheckTimer = setInterval(() => {
    checkAndHandleMemoryUsage(exportState, 'TIMER_CHECK');
  }, MEMORY_CHECK_INTERVAL);
  
  try {
    // Get all channels in the guild that are actually visible
    const allChannels = await fetchVisibleChannels(guild);
    exportState.totalChannels = allChannels.length;
    
    console.log(`Found ${allChannels.length} visible channels to process`);
    
    // Pre-initialize channels in the export data
    for (const channelObj of allChannels) {
      const channel = channelObj.channel;
      const channelData = {
        id: channel.id,
        name: channel.name,
        type: channel.type,
        messages: []
      };
      
      // Store the channel in our export data and map
      exportState.exportData.channels.push(channelData);
      exportState.channelsMap.set(channel.id, channelData);
    }
    
    // Save the initial channel structure
    console.log("Saving initial channel structure...");
    await saveExportProgress(exportState, 'INITIAL');
    
    // Process channels in parallel with controlled concurrency
    await processChannelsInParallel(allChannels, exportState, statusMessage, guild);
    
    // Update final timestamp
    exportState.exportData.lastUpdatedAt = new Date().toISOString();
    
    // Write the complete export data to file one last time
    console.log("Saving final export data...");
    await saveExportProgress(exportState, 'FINAL');
    
    // Final status update
    await updateStatusMessage(statusMessage, exportState, guild, true);
    
    console.log(`Export completed successfully for guild: ${guild.name} (${guild.id})`);
    logMemoryUsage('Final');
  } catch (error) {
    console.error('Error during export:', error);
    
    // Try to save what we have so far
    try {
      exportState.exportData.lastUpdatedAt = new Date().toISOString();
      await saveExportProgress(exportState, 'ERROR');
    } catch (e) {
      console.error('Error saving partial export data:', e);
    }
    
    await statusMessage.edit(`Error occurred during export: ${error.message}`);
  } finally {
    // Clear timers
    clearInterval(autoSaveTimer);
    clearInterval(memoryCheckTimer);
  }
}

// Function to check memory and handle if above limit
async function checkAndHandleMemoryUsage(exportState, trigger = 'MANUAL') {
  exportState.memoryCheckCount++;
  
  // Check memory usage
  const memory = logMemoryUsage(`Memory check #${exportState.memoryCheckCount} (${trigger})`);
  
  // If above limit and not currently saving, trigger save and memory cleanup
  if (memory.isAboveLimit && !exportState.saveInProgress) {
    console.log(`🚨 Memory usage above limit (${memory.rssMB}MB / ${MEMORY_LIMIT_MB}MB). Triggering save and cleanup...`);
    exportState.memoryTriggeredSaves++;
    await saveExportProgress(exportState, `MEMORY_LIMIT`);
    return true;
  }
  return false;
}

// Function to save export progress to file
async function saveExportProgress(exportState, trigger = 'MANUAL') {
  if (exportState.saveInProgress) return;
  
  exportState.saveInProgress = true;
  
  try {
    // Update timestamp
    exportState.exportData.lastUpdatedAt = new Date().toISOString();
    
    console.log(`Saving export progress (trigger: ${trigger})...`);
    
    // Write to file
    fs.writeFileSync(exportState.filepath, JSON.stringify(exportState.exportData, null, 2));
    
    console.log(`Saved export progress: ${exportState.exportData.totalMessagesExportedSoFar} messages so far`);
    exportState.lastAutoSaveTime = Date.now();
    
    // If this is a memory-triggered save, clean up message arrays to free memory
    if (trigger === 'MEMORY_LIMIT') {
      console.log('Performing memory cleanup...');
      
      // Keep track of how many messages were in memory
      let messagesInMemory = 0;
      
      // Clear message arrays in each channel to free up memory
      for (const channelData of exportState.exportData.channels) {
        messagesInMemory += channelData.messages.length;
        
        // Clear the messages array after they've been saved to file
        channelData.messages = [];
      }
      
      console.log(`Memory cleanup: cleared ${messagesInMemory} messages from memory`);
      
      // Force garbage collection if using node with --expose-gc flag
      if (global.gc) {
        console.log('Forcing garbage collection...');
        global.gc();
      } else {
        console.log('Garbage collection not exposed. Start node with --expose-gc to enable manual garbage collection.');
      }
      
      // Log memory after cleanup
      logMemoryUsage('After cleanup');
    }
  } catch (error) {
    console.error('Error saving export progress:', error);
  } finally {
    exportState.saveInProgress = false;
  }
}

async function fetchVisibleChannels(guild) {
  // Get only visible text-based channels
  const visibleChannels = [];
  
  // Log guild info
  console.log(`Guild: ${guild.name} (${guild.id})`);
  console.log(`Total channels in guild: ${guild.channels.cache.size}`);
  
  // Get text channels that the bot can actually see and read messages in
  const textChannels = guild.channels.cache
    .filter(channel => {
      // Log channel being checked
      console.log(`Checking channel: ${channel.name} (${channel.id}), type: ${channel.type}`);
      
      const isTextChannel = channel.type === ChannelType.GuildText || channel.type === ChannelType.GuildForum;
      const notExcluded = !excludedChannels.has(channel.id);
      const isViewable = channel.viewable;
      const canReadHistory = channel.permissionsFor(guild.members.me)?.has(PermissionFlagsBits.ReadMessageHistory) ?? false;
      
      return isTextChannel && notExcluded && isViewable && canReadHistory;
    })
    .map(channel => ({
      channel,
      isThread: false,
      parentId: null
    }));
  
  visibleChannels.push(...textChannels);
  console.log(`Found ${textChannels.length} text channels to process`);
  
  // Get threads in batches to avoid rate limiting
  const threadChannels = [];
  for (const channelObj of textChannels) {
    const channel = channelObj.channel;
    if (!channel.threads) continue;
    
    console.log(`Fetching threads for channel: ${channel.name} (${channel.id})`);
    
    try {
      // Get active threads
      let activeThreads;
      try {
        activeThreads = await channel.threads.fetchActive();
        console.log(`Found ${activeThreads.threads.size} active threads in ${channel.name}`);
      } catch (e) {
        console.error(`Error fetching active threads for ${channel.name}:`, e);
        activeThreads = { threads: new Map() };
      }
      
      // Get archived threads
      let archivedThreads;
      try {
        archivedThreads = await channel.threads.fetchArchived();
        console.log(`Found ${archivedThreads.threads.size} archived threads in ${channel.name}`);
      } catch (e) {
        console.error(`Error fetching archived threads for ${channel.name}:`, e);
        archivedThreads = { threads: new Map() };
      }
      
      // Add visible threads
      for (const thread of [...activeThreads.threads.values(), ...archivedThreads.threads.values()]) {
        if (!excludedChannels.has(thread.id) && 
            thread.viewable && 
            thread.permissionsFor(guild.members.me).has(PermissionFlagsBits.ReadMessageHistory)) {
          threadChannels.push({
            channel: thread,
            isThread: true,
            parentId: channel.id,
            parentName: channel.name
          });
        }
      }
    } catch (error) {
      console.error(`Error processing threads for channel ${channel.name}:`, error);
    }
  }
  
  visibleChannels.push(...threadChannels);
  console.log(`Found ${threadChannels.length} thread channels to process`);
  
  return visibleChannels;
}

async function processChannelsInParallel(channels, exportState, statusMessage, guild) {
  // Create a queue for processing channels with controlled concurrency
  let currentIndex = 0;
  
  // Process function that takes from the queue
  const processNext = async () => {
    if (currentIndex >= channels.length) return;
    
    const channelIndex = currentIndex++;
    const channelObj = channels[channelIndex];
    const channel = channelObj.channel;
    
    exportState.runningTasksCount++;
    exportState.currentChannel = channel;
    exportState.currentChannelIndex = channelIndex + 1;
    exportState.messagesInCurrentChannel = 0;
    
    console.log(`Processing channel ${channelIndex + 1}/${channels.length}: ${channel.name} (${channel.id})`);
    
    try {
      await fetchMessagesFromChannel(channel, exportState, statusMessage, guild);
    } catch (error) {
      console.error(`Error processing channel ${channel.name}:`, error);
    } finally {
      exportState.runningTasksCount--;
      // Always process next to ensure we continue even after errors
      processNext();
    }
  };
  
  // Start initial batch of tasks
  const initialBatch = Math.min(MAX_CONCURRENT_REQUESTS, channels.length);
  const initialPromises = [];
  
  for (let i = 0; i < initialBatch; i++) {
    initialPromises.push(processNext());
  }
  
  // Wait for all channels to complete processing
  await Promise.all(initialPromises);
  
  // Wait until all concurrent tasks are done
  while (exportState.runningTasksCount > 0) {
    await new Promise(resolve => setTimeout(resolve, 100));
  }
}

async function fetchMessagesFromChannel(channel, exportState, statusMessage, guild) {
  if (!channel.isTextBased()) {
    console.log(`Skipping non-text channel: ${channel.name}`);
    return;
  }
  
  let lastMessageId = null;
  let keepFetching = true;
  let fetchCount = 0;

  // Use a buffer for collecting messages before adding to the main export
  let messageBuffer = [];
  
  console.log(`Starting to fetch messages from channel: ${channel.name} (${channel.id})`);
  
  while (keepFetching) {
    try {
      // Check memory usage every 5 fetch operations
      if (fetchCount % 5 === 0) {
        const memoryExceeded = await checkAndHandleMemoryUsage(exportState, 'FETCH_CYCLE');
        if (memoryExceeded) {
          console.log(`Memory limit reached during channel processing. Cleared buffers.`);
          messageBuffer = []; // Clear the buffer after it's been saved
        }
      }
      
      // Fetch messages - use optimal batch size
      const options = { limit: 100 }; // Max allowed by Discord API
      if (lastMessageId) {
        options.before = lastMessageId;
      }
      
      fetchCount++;
      console.log(`Fetching batch ${fetchCount} from ${channel.name}, options:`, options);
      
      const messages = await channel.messages.fetch(options);
      console.log(`Fetched ${messages.size} messages from ${channel.name}`);
      
      if (messages.size === 0) {
        console.log(`No more messages in ${channel.name}`);
        keepFetching = false;
        continue;
      }
      
      // Save the last message ID for pagination
      lastMessageId = messages.last().id;
      
      // Filter out bot messages
      const nonBotMessages = Array.from(messages.values())
        .filter(message => !message.author.bot);
      
      console.log(`Found ${nonBotMessages.length} non-bot messages in batch`);
      
      // Extract metadata
      const messageMetadata = nonBotMessages.map(message => extractMessageMetadata(message));
      
      // Update counts atomically
      const newMessagesCount = messageMetadata.length;
      messageBuffer.push(...messageMetadata);
      exportState.processedMessages += newMessagesCount;
      exportState.exportData.totalMessagesExportedSoFar += newMessagesCount;
      exportState.messagesInCurrentChannel += newMessagesCount;
      
      // Update status less frequently to reduce overhead
      const currentTime = Date.now();
      if (currentTime - exportState.lastStatusUpdateTime > 1000) {
        exportState.lastStatusUpdateTime = currentTime;
        exportState.exportData.lastUpdatedAt = new Date().toISOString();
        updateStatusMessage(statusMessage, exportState, guild);
      }
      
      // Check if auto-save is needed
      if (currentTime - exportState.lastAutoSaveTime > AUTO_SAVE_INTERVAL && !exportState.saveInProgress) {
        await saveExportProgress(exportState, 'AUTO_SAVE_INTERVAL');
      }
      
      // Check if we need to add to channel data to free up memory
      if (messageBuffer.length >= EXPORT_THRESHOLD) {
        console.log(`Buffer threshold reached. Adding ${messageBuffer.length} messages to channel ${channel.name}`);
        addMessagesToChannel(messageBuffer, channel.id, exportState);
        messageBuffer = [];
        
        // Also save progress
        if (!exportState.saveInProgress) {
          await saveExportProgress(exportState, 'BUFFER_THRESHOLD');
        }
      }
      
      // If we got fewer messages than requested, we've reached the end
      if (messages.size < 100) {
        console.log(`Reached end of messages for ${channel.name}`);
        keepFetching = false;
      }
      
    } catch (error) {
      if (error.code === 10008 || error.code === 50001) {
        // Message or channel not found, skip
        console.log(`Skipping channel ${channel.name}: ${error.message}`);
        keepFetching = false;
      }
      else if (error.httpStatus === 429 || error.code === 'RateLimitedError') {
        exportState.rateLimitHits++;
        // Use the retry_after value from the error or default to 1 second
        const retryAfter = error.retry_after || error.timeout || 1000;
        console.log(`Rate limited in ${channel.name}, waiting ${retryAfter}ms`);
        await new Promise(resolve => setTimeout(resolve, retryAfter));
      } else {
        console.error(`Error fetching messages from ${channel.name}:`, error);
        keepFetching = false;
      }
    }
  }
  
  // Add any remaining messages to the channel
  if (messageBuffer.length > 0) {
    console.log(`Adding remaining ${messageBuffer.length} messages to channel ${channel.name}`);
    addMessagesToChannel(messageBuffer, channel.id, exportState);
  }
  
  console.log(`Completed processing channel: ${channel.name} (${channel.id}), processed ${exportState.messagesInCurrentChannel} messages`);
}

function extractMessageMetadata(message) {
  // Format exactly as in the example
  return {
    id: message.id,
    content: message.content,
    authorId: message.author.id,
    authorUsername: message.author.username,
    authorBot: message.author.bot,
    timestamp: message.createdTimestamp,
    createdAt: new Date(message.createdTimestamp).toISOString(),
    attachments: Array.from(message.attachments.values()).map(att => ({
      id: att.id,
      url: att.url,
      filename: att.name,
      size: att.size
    })),
    embeds: message.embeds.map(embed => ({
      type: embed.type,
      title: embed.title || null
    })),
    reactions: Array.from(message.reactions.cache.values()).map(reaction => ({
      emoji: reaction.emoji.name,
      count: reaction.count
    }))
  };
}

function addMessagesToChannel(messages, channelId, exportState) {
  if (!messages || messages.length === 0) return;
  
  // Find the channel in our map
  const channelData = exportState.channelsMap.get(channelId);
  if (!channelData) {
    console.error(`Channel ${channelId} not found in channels map`);
    return;
  }
  
  // Add messages to the channel's messages array
  channelData.messages.push(...messages);
  
  // Log progress
  console.log(`Added ${messages.length} messages to channel ${channelData.name}. Total: ${exportState.exportData.totalMessagesExportedSoFar}`);
}

// Format the date exactly as requested in your example
function formatDateToUTC() {
  const now = new Date();
  return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')} ${String(now.getUTCHours()).padStart(2, '0')}:${String(now.getUTCMinutes()).padStart(2, '0')}:${String(now.getUTCSeconds()).padStart(2, '0')}`;
}

// Status update with proper formatting
let statusUpdateTimeout = null;

async function updateStatusMessage(statusMessage, exportState, guild, isFinal = false) {
  // Skip updates that are too frequent unless final
  if (!isFinal && statusUpdateTimeout) return;
  
  // Set update throttling
  if (!isFinal) {
    statusUpdateTimeout = setTimeout(() => {
      statusUpdateTimeout = null;
    }, 1000); // Update at most once per second
  }
  
  // Get memory usage for status message
  const memory = checkMemoryUsage();
  
  const currentTime = Date.now();
  const elapsedTime = currentTime - exportState.startTime;
  
  // Calculate time components
  const hours = Math.floor(elapsedTime / (1000 * 60 * 60));
  const minutes = Math.floor((elapsedTime % (1000 * 60 * 60)) / (1000 * 60));
  const seconds = Math.floor((elapsedTime % (1000 * 60)) / 1000);
  
  // Calculate processing speed
  const messagesPerSecond = elapsedTime > 0 ? 
    (exportState.processedMessages / (elapsedTime / 1000)).toFixed(2) : 
    "0.00";
  
  // Current date in the exact format from your example
  const nowFormatted = formatDateToUTC();

  // Calculate export number based on memory-triggered saves
  const exportNumber = exportState.memoryTriggeredSaves + 1;
  
  // Build status message exactly matching your example format
  let status = `Guild Export Status (#${exportNumber})\n`;
  
  if (isFinal) {
    status += `✅ Export completed! All messages saved to ${exportState.filename}\n`;
  } else if (exportState.currentChannel) {
    status += `🔄 Processing channel ${exportState.currentChannelIndex}/${exportState.totalChannels}: ${exportState.currentChannel.name}\n`;
  } else {
    status += `🔄 Initializing export...\n`;
  }
  
  status += `📊 Processed ${exportState.processedMessages.toLocaleString()} messages from ${guild.name}\n`;
  status += `⏱️ Time elapsed: ${hours}h ${minutes}m ${seconds}s\n`;
  status += `⚡ Processing speed: ${messagesPerSecond} messages/second\n`;
  
  if (exportState.currentChannel) {
    status += `📈 Processing messages: ${exportState.messagesInCurrentChannel.toLocaleString()} in current channel (${exportState.processedMessages.toLocaleString()} total)\n`;
  }
  
  status += `🚦 Rate limit hits: ${exportState.rateLimitHits}\n`;
  // Add memory usage info
  status += `💾 Memory: ${memory.rssMB}MB / ${MEMORY_LIMIT_MB}MB (${memory.percentOfLimit}%)\n`;
  status += `⏰ Last update: ${nowFormatted}`;
  
  try {
    await statusMessage.edit(status);
    console.log(`Updated status message`);
  } catch (error) {
    console.error('Error updating status message:', error);
  }
}

// Login to Discord
console.log(`Starting bot with memory limit: ${MEMORY_LIMIT_MB}MB`);
client.login(process.env.DISCORD_TOKEN).catch(error => {
  console.error('Failed to login:', error);
  process.exit(1);
});