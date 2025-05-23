const fs = require('fs');
const path = require('path');
const { 
  PermissionFlagsBits,
  ChannelType
} = require('discord.js');
const config = require('./config');

// Parse excluded channels from environment variable or config
const excludedChannelsArray = config.getConfig('excludedChannels', 'EX_CHANNELS');
const excludedChannels = new Set(excludedChannelsArray);

// Export threshold - how many messages to process before auto-save
const EXPORT_THRESHOLD = config.getConfig('exportThreshold', 'METADATA_EXPORT_THRESHOLD');

// Memory limit in MB
const MEMORY_LIMIT_MB = config.getConfig('memoryLimitMB', 'MEMORY_LIMIT_MB');
// Convert to bytes for easier comparison with process.memoryUsage()
const MEMORY_LIMIT_BYTES = MEMORY_LIMIT_MB * 1024 * 1024;
// Memory scale factor - use only this percentage of the configured limit as effective limit
const MEMORY_SCALE_FACTOR = 0.85;

// Memory check frequency in milliseconds
const MEMORY_CHECK_INTERVAL = config.getConfig('memoryCheckInterval', 'MEMORY_CHECK_INTERVAL');

// Auto-save interval in milliseconds
const AUTO_SAVE_INTERVAL = config.getConfig('autoSaveInterval', 'AUTO_SAVE_INTERVAL');

// Maximum concurrent API requests
const MAX_CONCURRENT_REQUESTS = 3;

// Function to check current memory usage and return details
function checkMemoryUsage() {
  const memoryUsage = process.memoryUsage();
  const heapUsed = memoryUsage.heapUsed;
  const rss = memoryUsage.rss; // Resident Set Size - total memory allocated
  
  const heapUsedMB = Math.round(heapUsed / 1024 / 1024 * 100) / 100;
  const rssMB = Math.round(rss / 1024 / 1024 * 100) / 100;
  
  // Calculate effective limit
  const effectiveLimit = MEMORY_LIMIT_BYTES * MEMORY_SCALE_FACTOR;
  const effectiveLimitMB = Math.round(effectiveLimit / 1024 / 1024 * 100) / 100;
  
  return {
    heapUsed,
    rss,
    heapUsedMB,
    rssMB,
    isAboveLimit: rss > effectiveLimit,
    percentOfLimit: Math.round((rss / MEMORY_LIMIT_BYTES) * 100),
    effectiveLimitMB
  };
}

// Log memory usage
function logMemoryUsage(prefix = '') {
  const memory = checkMemoryUsage();
  console.log(`${prefix} Memory usage: ${memory.rssMB} MB / ${MEMORY_LIMIT_MB} MB (${memory.percentOfLimit}% of limit), Heap: ${memory.heapUsedMB} MB`);
  return memory;
}

// Function for aggressive memory cleanup
async function forceMemoryRelease() {
  console.log('Forcing aggressive memory cleanup...');
  
  // Run garbage collection multiple times if available
  if (global.gc) {
    for (let i = 0; i < 3; i++) {
      console.log(`Forcing garbage collection pass ${i+1}...`);
      global.gc();
      // Small delay between GC calls
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }
  
  // Attempt to force memory compaction in newer Node versions
  if (process.versions.node.split('.')[0] >= 12) {
    try {
      console.log('Attempting to compact heap memory...');
      if (typeof v8 !== 'undefined' && v8.getHeapStatistics && v8.writeHeapSnapshot) {
        const v8 = require('v8');
        const heapBefore = v8.getHeapStatistics().total_heap_size;
        v8.writeHeapSnapshot(); // This can help compact memory in some cases
        const heapAfter = v8.getHeapStatistics().total_heap_size;
        console.log(`Heap size change: ${(heapBefore - heapAfter) / 1024 / 1024} MB`);
      }
    } catch (e) {
      console.error('Error during heap compaction:', e);
    }
  }
  
  // Run another GC pass after compaction
  if (global.gc) {
    global.gc();
  }
}

// Function for performing memory cleanup
async function performMemoryCleanup(exportState) {
  if (exportState.saveInProgress) return;
  
  exportState.saveInProgress = true;
  
  try {
    console.log('Performing memory cleanup...');
    
    // Clear any references to large objects
    global._lastMemoryReport = null; // Clear any references we might have created
    
    // Force garbage collection with enhanced approach
    await forceMemoryRelease();
    
    // Log memory after cleanup
    logMemoryUsage('After cleanup');
    
    // If memory is still too high after cleanup, pause operations briefly
    const memoryAfter = checkMemoryUsage();
    if (memoryAfter.isAboveLimit) {
      console.log('Memory still above limit after cleanup. Pausing operations for 2 seconds...');
      // This pause can help the system actually release memory
      await new Promise(resolve => setTimeout(resolve, 2000));
      
      // One more GC attempt after the pause
      if (global.gc) global.gc();
      
      logMemoryUsage('After pause');
    }
  } catch (error) {
    console.error('Error during memory cleanup:', error);
  } finally {
    exportState.saveInProgress = false;
  }
}

// Function to check memory and handle if above limit
async function checkAndHandleMemoryUsage(exportState, trigger = 'MANUAL') {
  exportState.memoryCheckCount++;
  
  // Check memory usage
  const memory = logMemoryUsage(`Memory check #${exportState.memoryCheckCount} (${trigger})`);
  
  // If above limit and not currently saving, trigger memory cleanup
  if (memory.isAboveLimit && !exportState.saveInProgress) {
    console.log(`🚨 Memory usage above limit (${memory.rssMB}MB / ${memory.effectiveLimitMB}MB). Triggering cleanup...`);
    exportState.memoryTriggeredSaves++;
    await performMemoryCleanup(exportState);
    return true;
  }
  return false;
}

// Function to verify export completeness
async function verifyExportCompleteness(exportState) {
  // Count lines in the file
  let lineCount = 0;
  try {
    // Use manual counting to count lines
    const data = fs.readFileSync(exportState.filepath, 'utf8');
    lineCount = data.split('\n').filter(line => line.trim().length > 0).length;
    
    console.log(`File verification: ${lineCount} lines in file, ${exportState.messagesWrittenToFile} messages reported written`);
    
    // Check for messages that may have been processed but not written
    const nonMessageLines = 2 + exportState.totalChannels; // 2 for start/end metadata + channel metadata
    const expectedMessageLines = exportState.messagesWrittenToFile;
    const expectedTotalLines = nonMessageLines + expectedMessageLines;
    
    if (lineCount !== expectedTotalLines) {
      console.error(`DISCREPANCY: File line count (${lineCount}) doesn't match expected count (${expectedTotalLines})`);
      console.error(`Expected ${nonMessageLines} non-message lines + ${expectedMessageLines} message lines`);
    }
  } catch (error) {
    console.error('Error during file verification:', error);
  }
  
  return lineCount;
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
      exportState.processedChannels++;
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

// Function to write a message to the file
async function writeMessageToFile(message, exportState) {
  try {
    // Add channel ID and message type to the message object
    message.type = 'message';
    
    // Write the message as NDJSON format (one JSON object per line)
    fs.appendFileSync(exportState.filepath, JSON.stringify(message) + '\n');
    exportState.messagesWrittenToFile++;
    return true;
  } catch (error) {
    console.error('Error writing message to file:', error);
    exportState.messageWriteErrors++;
    return false;
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
  
  console.log(`Starting to fetch messages from channel: ${channel.name} (${channel.id})`);
  
  while (keepFetching) {
    try {
      // Check memory usage every 5 fetch operations
      if (fetchCount % 5 === 0) {
        const memoryExceeded = await checkAndHandleMemoryUsage(exportState, 'FETCH_CYCLE');
        if (memoryExceeded) {
          console.log(`Memory limit reached during channel processing.`);
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
      
      // Track all messages encountered
      exportState.messagesTotalProcessed += messages.size;
      
      // Filter out bot messages
      const nonBotMessages = Array.from(messages.values())
        .filter(message => !message.author.bot);
      
      // Track dropped (bot) messages
      exportState.messageDroppedCount += (messages.size - nonBotMessages.length);
      
      console.log(`Found ${nonBotMessages.length} non-bot messages in batch`);
      
      // Extract and write each message immediately
      for (const message of nonBotMessages) {
        const messageData = extractMessageMetadata(message);
        // Add the channel ID to the message data
        messageData.channelId = channel.id;
        await writeMessageToFile(messageData, exportState);
        
        // Update processed counter for status display
        exportState.processedMessages++;
      }
      
      // Update status less frequently to reduce overhead
      const currentTime = Date.now();
      if (currentTime - exportState.lastStatusUpdateTime > 1000) {
        exportState.lastStatusUpdateTime = currentTime;
        updateStatusMessage(statusMessage, exportState, guild);
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
  
  console.log(`Completed processing channel: ${channel.name} (${channel.id}), processed ${exportState.messagesInCurrentChannel} messages`);
}

function extractMessageMetadata(message) {
  // Format with all relevant message data
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

// Format the date exactly as requested 
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
    status += `✅ Export completed! ${exportState.processedMessages.toLocaleString()} non-bot messages saved to ${exportState.filename}\n`;
    status += `📝 Messages written to file: ${exportState.messagesWrittenToFile.toLocaleString()}\n`;
    status += `🤖 Bot messages skipped: ${exportState.messageDroppedCount.toLocaleString()}\n`;
    
    if (exportState.messageWriteErrors > 0) {
      status += `⚠️ Write errors encountered: ${exportState.messageWriteErrors}\n`;
    }
    
    // Add verification
    status += `✓ Verification: ${exportState.processedMessages === exportState.messagesWrittenToFile ? 
      'All messages successfully written' : 
      'DISCREPANCY DETECTED - check logs'}\n`;
  } else if (exportState.currentChannel) {
    status += `🔄 Processing channel ${exportState.currentChannelIndex}/${exportState.totalChannels}: ${exportState.currentChannel.name}\n`;
  } else {
    status += `🔄 Initializing export...\n`;
  }
  
  status += `📊 Processed ${exportState.processedMessages.toLocaleString()} non-bot messages from ${guild.name}\n`;
  status += `⏱️ Time elapsed: ${hours}h ${minutes}m ${seconds}s\n`;
  status += `⚡ Processing speed: ${messagesPerSecond} messages/second (average)\n`;
  
  status += `📈 Progress: ${exportState.processedChannels}/${exportState.totalChannels} channels (${Math.round(exportState.processedChannels / exportState.totalChannels * 100)}%)\n`;
  
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

async function handleExportGuild(message, client) {
  const guild = message.guild;
  
  console.log(`Starting export for guild: ${guild.name} (${guild.id})`);
  logMemoryUsage('Initial');
  
  // Verify the user has administrator permissions
  if (!message.member.permissions.has(PermissionFlagsBits.Administrator)) {
    return message.reply('You need administrator permissions to use this command.');
  }
  
  // Create status message
  const statusMessage = await message.channel.send(
    `Guild Export Status (#1)\n` +
    `🔄 Initializing NDJSON export...`
  );

  // Create a unique filename for this export
  const timestamp = new Date().toISOString();
  const sanitizedGuildName = guild.name.replace(/[^a-z0-9]/gi, '-').toLowerCase();
  const filename = `${sanitizedGuildName}-${guild.id}-${timestamp.replace(/[:.]/g, '-')}.ndjson`;
  const filepath = path.join(process.cwd(), filename);
  
  console.log(`Export file will be created at: ${filepath}`);
  
  // Initialize export state
  const exportState = {
    startTime: Date.now(),
    processedMessages: 0,
    messagesTotalProcessed: 0,
    messagesWrittenToFile: 0,
    messageDroppedCount: 0,
    messageWriteErrors: 0,
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
    saveInProgress: false,
    memoryLimit: MEMORY_LIMIT_BYTES
  };

  // Create the file with a header metadata record
  const initialMetadata = {
    type: 'metadata',
    id: guild.id,
    name: guild.name,
    exportStartedAt: timestamp,
    exportFormat: 'ndjson',
    botVersion: '1.0.0-ndjson'
  };
  
  // Write initial metadata as the first line
  fs.writeFileSync(filepath, JSON.stringify(initialMetadata) + '\n');
  console.log(`Created initial export file: ${filename}`);
  
  // Update the status message initially
  await updateStatusMessage(statusMessage, exportState, guild);
  
  // Set up memory check timer
  const memoryCheckTimer = setInterval(() => {
    checkAndHandleMemoryUsage(exportState, 'TIMER_CHECK');
  }, MEMORY_CHECK_INTERVAL);
  
  try {
    // Get all channels in the guild that are actually visible
    const allChannels = await fetchVisibleChannels(guild);
    exportState.totalChannels = allChannels.length;
    
    console.log(`Found ${allChannels.length} visible channels to process`);
    
    // Write channel metadata to the file
    for (const channelObj of allChannels) {
      const channel = channelObj.channel;
      const channelMetadata = {
        type: 'channel',
        id: channel.id,
        name: channel.name,
        channelType: channel.type,
        isThread: channelObj.isThread,
        parentId: channelObj.parentId,
        parentName: channelObj.parentName
      };
      
      // Write channel metadata as a separate NDJSON line
      fs.appendFileSync(filepath, JSON.stringify(channelMetadata) + '\n');
    }
    
    // Process channels in parallel with controlled concurrency
    await processChannelsInParallel(allChannels, exportState, statusMessage, guild);
    
    // Verify export completeness
    const lineCount = await verifyExportCompleteness(exportState);
    console.log(`Export verification complete. File contains ${lineCount} lines.`);
    
    // Write the final status update metadata
    const finalMetadata = {
      type: 'metadata',
      id: guild.id,
      name: guild.name,
      exportCompletedAt: new Date().toISOString(),
      totalMessagesProcessed: exportState.messagesTotalProcessed,
      totalNonBotMessages: exportState.processedMessages,
      messagesWrittenToFile: exportState.messagesWrittenToFile,
      botMessagesFiltered: exportState.messageDroppedCount,
      writeErrors: exportState.messageWriteErrors,
      fileLineCount: lineCount,
      exportDurationSeconds: Math.floor((Date.now() - exportState.startTime) / 1000),
      channelsProcessed: exportState.totalChannels,
      rateLimitHits: exportState.rateLimitHits
    };
    fs.appendFileSync(filepath, JSON.stringify(finalMetadata) + '\n');
    
    // Final status update
    await updateStatusMessage(statusMessage, exportState, guild, true);
    
    console.log(`Export completed successfully for guild: ${guild.name} (${guild.id})`);
    logMemoryUsage('Final');
  } catch (error) {
    console.error('Error during export:', error);
    
    try {
      // Add error metadata to the file
      const errorMetadata = {
        type: 'error',
        timestamp: new Date().toISOString(),
        error: error.message,
        stage: 'export-process'
      };
      fs.appendFileSync(filepath, JSON.stringify(errorMetadata) + '\n');
    } catch (e) {
      console.error('Error saving error metadata:', e);
    }
    
    await statusMessage.edit(`Error occurred during export: ${error.message}`);
  } finally {
    // Clear timer
    clearInterval(memoryCheckTimer);
  }
}

// Export functions
module.exports = {
  handleExportGuild
};