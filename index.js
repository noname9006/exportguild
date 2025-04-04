// index.js - Main bot entry point
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const { 
  Client, 
  GatewayIntentBits, 
  Partials
} = require('discord.js');

// Import modules
const exportGuild = require('./exportguild');
const processData = require('./processData');

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

client.once('ready', () => {
  console.log(`Bot is ready! Logged in as ${client.user.tag}`);
});

// Track active operations to prevent multiple operations in the same guild
const activeOperations = new Set();

// Command handler
client.on('messageCreate', async (message) => {
  // Ignore messages from bots
  if (message.author.bot) return;

  const args = message.content.trim().split(/\s+/);
  const command = args[0].toLowerCase();
  const subCommand = args[1]?.toLowerCase();

  if (command === '!exportguild') {
    // Check if an operation is already running for this guild
    if (activeOperations.has(message.guildId)) {
      return message.reply('An operation is already running for this guild!');
    }
    
    // Set guild as being processed
    activeOperations.add(message.guildId);

    try {
      if (!subCommand || subCommand === 'export') {
        // Export guild data
        await exportGuild.handleExportGuild(message, client);
      } else if (subCommand === 'process') {
        // Process NDJSON data
        await processData.processNDJSON(message);
      } else {
        message.reply('Unknown subcommand. Available commands: `!exportguild` or `!exportguild process`');
      }
    } catch (error) {
      console.error('Critical error:', error);
      message.channel.send(`Critical error during operation: ${error.message}`);
    } finally {
      // Remove guild from active operations when done (even if there was an error)
      activeOperations.delete(message.guildId);
    }
  }
});

// Login to Discord
console.log('Starting Discord bot...');
client.login(process.env.DISCORD_TOKEN).catch(error => {
  console.error('Failed to login:', error);
  process.exit(1);
});