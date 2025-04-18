// processData.js - Contains functionality to analyze NDJSON files and create CSV reports
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const { createObjectCsvWriter } = require('csv-writer');

// Find the most recent NDJSON file in the root folder
function findMostRecentNDJSON() {
  const files = fs.readdirSync(process.cwd())
    .filter(file => file.endsWith('.ndjson'))
    .map(file => ({
      name: file,
      path: path.join(process.cwd(), file),
      mtime: fs.statSync(path.join(process.cwd(), file)).mtime
    }))
    .sort((a, b) => b.mtime - a.mtime); // Sort by modification time, newest first

  if (files.length === 0) {
    throw new Error('No .ndjson files found in the current directory');
  }

  console.log(`Found ${files.length} NDJSON files, using most recent: ${files[0].name}`);
  return files[0].path;
}

// Format date to YYYY-MM-DD
function formatDate(dateString) {
  const date = new Date(dateString);
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

// Process NDJSON file and create CSV reports
async function processNDJSON(message) {
  try {
    const guild = message.guild;
    
    const statusMessage = await message.channel.send(
      `Data Processing Status\n` +
      `🔄 Finding the most recent NDJSON file...`
    );

    // Find the most recent NDJSON file
    const ndjsonFilePath = findMostRecentNDJSON();
    const fileName = path.basename(ndjsonFilePath);

    // Update status message
    await statusMessage.edit(
      `Data Processing Status\n` +
      `🔄 Processing file: ${fileName}\n` +
      `🔄 Analyzing messages...`
    );

    // Read the NDJSON file line by line
    const fileStream = fs.createReadStream(ndjsonFilePath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });

    // Data structures to track messages by user
    const userMessages = new Map(); // userId -> array of messages
    const userMessageCounts = new Map(); // userId -> total message count
    const userDailyMessageCounts = new Map(); // userId -> Map of date -> message count
    const userSymbolCounts = new Map(); // userId -> total symbol count
    const userDailySymbols = new Map(); // userId -> Map of date -> symbol count
    const usernames = new Map(); // userId -> username
    const allDates = new Set(); // All unique dates in the dataset

    // Process each line
    let lineCount = 0;
    for await (const line of rl) {
      lineCount++;
      if (lineCount % 10000 === 0) {
        await statusMessage.edit(
          `Data Processing Status\n` +
          `🔄 Processing file: ${fileName}\n` +
          `🔄 Analyzed ${lineCount.toLocaleString()} lines...`
        );
      }

      try {
        const data = JSON.parse(line);
        
        // Only process message records
        if (data.type === 'message') {
          const userId = data.authorId;
          const username = data.authorUsername;
          const date = formatDate(data.createdAt);
          const content = data.content || "";
          const symbolCount = content.length;
          
          // Store username
          usernames.set(userId, username);
          
          // Track message count
          userMessageCounts.set(userId, (userMessageCounts.get(userId) || 0) + 1);
          
          // Track message timestamps
          if (!userMessages.has(userId)) {
            userMessages.set(userId, []);
          }
          userMessages.get(userId).push(date);
          
          // Track total symbol count
          userSymbolCounts.set(userId, (userSymbolCounts.get(userId) || 0) + symbolCount);
          
          // Track daily symbol counts
          if (!userDailySymbols.has(userId)) {
            userDailySymbols.set(userId, new Map());
          }
          const dailySymbolsMap = userDailySymbols.get(userId);
          dailySymbolsMap.set(date, (dailySymbolsMap.get(date) || 0) + symbolCount);
          
          // Track daily message counts
          if (!userDailyMessageCounts.has(userId)) {
            userDailyMessageCounts.set(userId, new Map());
          }
          const dailyMsgCountMap = userDailyMessageCounts.get(userId);
          dailyMsgCountMap.set(date, (dailyMsgCountMap.get(date) || 0) + 1);
          
          // Add date to all dates set
          allDates.add(date);
        }
      } catch (error) {
        console.error(`Error processing line ${lineCount}:`, error);
      }
    }

    // Update status
    await statusMessage.edit(
      `Data Processing Status\n` +
      `🔄 Processing file: ${fileName}\n` +
      `✅ Analyzed ${lineCount.toLocaleString()} lines\n` +
      `🔄 Optimizing role fetching...`
    );

    // Get all user IDs
    const userIds = Array.from(usernames.keys());
    const userRoles = new Map(); // userId -> highest role name
    
    // OPTIMIZATION: Fetch all members at once instead of one by one
    console.log(`Fetching role data for ${userIds.length} users in batches...`);
    
    // Create a map of members we have in the cache
    const memberCache = new Map();
    guild.members.cache.forEach(member => {
      memberCache.set(member.user.id, member);
    });
    
    // Batch size for fetching members
    const BATCH_SIZE = 100; // Discord allows fetching up to 100 members at a time
    
    // Process users in batches
    for (let i = 0; i < userIds.length; i += BATCH_SIZE) {
      const batch = userIds.slice(i, i + BATCH_SIZE);
      const missingUserIds = batch.filter(id => !memberCache.has(id));
      
      if (missingUserIds.length > 0) {
        // Update status message for each batch
        await statusMessage.edit(
          `Data Processing Status\n` +
          `🔄 Processing file: ${fileName}\n` +
          `✅ Analyzed ${lineCount.toLocaleString()} lines\n` +
          `🔄 Fetching roles... Batch ${Math.ceil(i/BATCH_SIZE) + 1}/${Math.ceil(userIds.length/BATCH_SIZE)}`
        );
        
        try {
          // Try to fetch the batch of members all at once if possible
          const fetchOptions = { 
            user: missingUserIds, 
            limit: missingUserIds.length,
            withPresences: false
          };
          
          const fetchedMembers = await guild.members.fetch(fetchOptions).catch(e => {
            console.log(`Could not fetch members in batch: ${e.message}`);
            return new Map(); // Return empty map if fetch fails
          });
          
          // Add fetched members to our cache
          fetchedMembers.forEach(member => {
            memberCache.set(member.user.id, member);
          });
          
        } catch (error) {
          console.error('Error batch fetching members:', error);
        }
      }
    }

    console.log(`Completed fetching member data, assigning roles...`);

    // Assign roles from our cache
    for (const userId of usernames.keys()) {
      const member = memberCache.get(userId);
      if (member && member.roles && member.roles.highest) {
        userRoles.set(userId, member.roles.highest.name);
      } else {
        userRoles.set(userId, "Not in server");
      }
    }

    // Sort dates newest to oldest
    const sortedDates = Array.from(allDates).sort((a, b) => new Date(b) - new Date(a));

    // Update status
    await statusMessage.edit(
      `Data Processing Status\n` +
      `🔄 Processing file: ${fileName}\n` +
      `✅ Analyzed ${lineCount.toLocaleString()} lines\n` +
      `✅ Fetched role data\n` +
      `🔄 Preparing CSV reports...`
    );

    // Generate CSV 1: Message counts sorted by user with daily message counts
    const messageCountRows = [];
    for (const [userId, totalMsgCount] of userMessageCounts.entries()) {
      const dailyMsgCountMap = userDailyMessageCounts.get(userId);
      const dailyData = {};
      
      // Add daily message count columns sorted newest to oldest
      for (const date of sortedDates) {
        dailyData[`count_${date}`] = dailyMsgCountMap.get(date) || 0;
      }
      
      messageCountRows.push({
        userId,
        username: usernames.get(userId),
        highestRole: userRoles.get(userId),
        messageCount: totalMsgCount,
        ...dailyData
      });
    }
    
    // Sort by message count (highest to lowest)
    messageCountRows.sort((a, b) => b.messageCount - a.messageCount);

    // Generate CSV 2: Symbol counts sorted by user with daily symbol counts
    const symbolCountRows = [];
    for (const [userId, totalSymbols] of userSymbolCounts.entries()) {
      const dailySymbolsMap = userDailySymbols.get(userId);
      const totalMessages = userMessageCounts.get(userId);
      const avgSymbolsPerMsg = totalMessages > 0 ? Math.round(totalSymbols / totalMessages) : 0;
      const dailyData = {};
      
      // Add daily symbol count columns sorted newest to oldest
      for (const date of sortedDates) {
        dailyData[`symbols_${date}`] = dailySymbolsMap.get(date) || 0;
      }
      
      symbolCountRows.push({
        userId,
        username: usernames.get(userId),
        highestRole: userRoles.get(userId),
        totalMessages,
        totalSymbols,
        avgSymbolsPerMsg,
        ...dailyData
      });
    }
    
    // Sort by total symbols (highest to lowest)
    symbolCountRows.sort((a, b) => b.totalSymbols - a.totalSymbols);

    // Create output filenames based on input file
    const baseName = path.basename(fileName, '.ndjson');
    const messageCountsFile = `${baseName}-message-counts.csv`;
    const symbolCountsFile = `${baseName}-symbol-counts.csv`;

    // Prepare CSV writers
    // For message counts - with dates sorted newest to oldest
    const messageCountHeaders = [
      { id: 'userId', title: 'User ID' },
      { id: 'username', title: 'Username' },
      { id: 'highestRole', title: 'Highest Role' },
      { id: 'messageCount', title: 'Total Messages' }
    ];
    
    // Add date columns sorted newest to oldest
    sortedDates.forEach(date => {
      messageCountHeaders.push({ id: `count_${date}`, title: date });
    });

    const messageCountWriter = createObjectCsvWriter({
      path: messageCountsFile,
      header: messageCountHeaders
    });

    // For symbol counts - with enhanced columns and dates newest to oldest
    const symbolCountHeaders = [
      { id: 'userId', title: 'User ID' },
      { id: 'username', title: 'Username' },
      { id: 'highestRole', title: 'Highest Role' },
      { id: 'totalMessages', title: 'Total Messages' },
      { id: 'totalSymbols', title: 'Total Symbols' },
      { id: 'avgSymbolsPerMsg', title: 'Avg Symbols/Message' }
    ];

    // Add date columns sorted newest to oldest
    sortedDates.forEach(date => {
      symbolCountHeaders.push({ id: `symbols_${date}`, title: date });
    });

    const symbolCountWriter = createObjectCsvWriter({
      path: symbolCountsFile,
      header: symbolCountHeaders
    });

    // Write the files
    await messageCountWriter.writeRecords(messageCountRows);
    await symbolCountWriter.writeRecords(symbolCountRows);

    // Update status with completion message
    await statusMessage.edit(
      `Data Processing Status\n` +
      `✅ Processing complete!\n` +
      `📄 Generated reports:\n` +
      `1. ${messageCountsFile} - Users sorted by message count with daily breakdown\n` +
      `2. ${symbolCountsFile} - Users sorted by symbol count with avg symbols and daily breakdown\n` +
      `📊 Processed data from ${userMessageCounts.size} users with ${lineCount.toLocaleString()} total lines`
    );

    console.log(`Processing complete: Generated ${messageCountsFile} and ${symbolCountsFile}`);
    
  } catch (error) {
    console.error('Error during data processing:', error);
    await message.channel.send(`Error during data processing: ${error.message}`);
  }
}

module.exports = {
  processNDJSON
};