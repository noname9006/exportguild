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
    const userSymbolCounts = new Map(); // userId -> total symbol count
    const userDailySymbols = new Map(); // userId -> Map of date -> symbol count
    const usernames = new Map(); // userId -> username

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
          const dailyMap = userDailySymbols.get(userId);
          dailyMap.set(date, (dailyMap.get(date) || 0) + symbolCount);
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
      `🔄 Preparing CSV reports...`
    );

    // Generate CSV 1: Message counts sorted by user
    const messageCountRows = Array.from(userMessageCounts.entries())
      .map(([userId, count]) => ({
        userId,
        username: usernames.get(userId),
        messageCount: count,
        dates: userMessages.get(userId).join(', ')
      }))
      .sort((a, b) => b.messageCount - a.messageCount);

    // Generate CSV 2: Symbol counts sorted by user
    const symbolCountRows = [];
    for (const [userId, totalSymbols] of userSymbolCounts.entries()) {
      const dailySymbolsMap = userDailySymbols.get(userId);
      const dailyData = {};
      
      // Convert daily symbols map to individual columns
      for (const [date, count] of dailySymbolsMap.entries()) {
        dailyData[`symbols_${date}`] = count;
      }
      
      symbolCountRows.push({
        userId,
        username: usernames.get(userId),
        totalSymbols,
        ...dailyData
      });
    }
    
    // Sort by total symbols
    symbolCountRows.sort((a, b) => b.totalSymbols - a.totalSymbols);

    // Create output filenames based on input file
    const baseName = path.basename(fileName, '.ndjson');
    const messageCountsFile = `${baseName}-message-counts.csv`;
    const symbolCountsFile = `${baseName}-symbol-counts.csv`;

    // Prepare CSV writers
    // For message counts
    const messageCountWriter = createObjectCsvWriter({
      path: messageCountsFile,
      header: [
        { id: 'userId', title: 'User ID' },
        { id: 'username', title: 'Username' },
        { id: 'messageCount', title: 'Message Count' },
        { id: 'dates', title: 'Message Dates' }
      ]
    });

    // For symbol counts
    // Dynamic headers based on dates encountered
    const allDates = new Set();
    userDailySymbols.forEach(userMap => {
      userMap.forEach((count, date) => {
        allDates.add(date);
      });
    });

    const symbolCountHeaders = [
      { id: 'userId', title: 'User ID' },
      { id: 'username', title: 'Username' },
      { id: 'totalSymbols', title: 'Total Symbols' }
    ];

    // Add date columns sorted chronologically
    Array.from(allDates).sort().forEach(date => {
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
      `1. ${messageCountsFile} - Users sorted by message count\n` +
      `2. ${symbolCountsFile} - Users sorted by symbol count\n` +
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