# exportguild

Discord bot for exporting guild data

## Installation

1. Clone the repository:
```bash
git clone https://github.com/noname9006/exportguild.git
cd exportguild
```

2. Install dependencies:
```bash
npm install
```

3. Create a `.env` file with your Discord bot token:
```
DISCORD_TOKEN=your_bot_token_here
```

## Usage

Start the bot:
```bash
npm start
```

## Bot Commands

### Export Commands

#### `!exportguild` or `!exportguild export`
Exports all messages from the guild to an NDJSON file.

**Required Permission:** Administrator

**What it does:**
- Fetches all messages from visible text channels and threads
- Filters out bot messages (only exports human messages)
- Saves data to a `.ndjson` file in the current directory
- Provides real-time progress updates including:
  - Processing speed (messages/second)
  - Memory usage
  - Channel progress
  - Total messages processed

**Exported Data Includes:**
- Message ID, content, and timestamp
- Author ID and username
- Attachments (URL, filename, size)
- Embeds (type, title)
- Reactions (emoji, count)
- Channel metadata

#### `!exportguild process [--noroles]`
Processes the most recent NDJSON export file and generates CSV reports.

**Required Permission:** Administrator (inherited from exportguild command)

**What it does:**
- Analyzes the most recent `.ndjson` file in the directory
- Creates multiple CSV reports with user statistics
- Optional `--noroles` flag to skip role information

**Generated Reports:**
- User message counts and activity
- Daily message statistics per user
- Symbol/character counts
- Days on server calculations

### Channel Management Commands

#### `!channellist`
Generates a hierarchical list of all channels and threads in the guild.

**Required Permission:** Administrator

**What it does:**
- Lists all categories, channels, and threads
- Shows forum channels and their posts
- Indicates archived/locked status
- Provides statistics on channel counts
- Creates clickable channel links

**Display Format:**
- Categories with nested channels
- Thread status indicators (📁 archived, 🔒 locked, 💬 active)
- Forum posts (📊 forum, 📌 posts)
- Statistics summary

#### `!ex` or `!ex list`
Lists all channels excluded from export.

**Required Permission:** Administrator

**What it does:**
- Shows all channels currently excluded from the export process
- Displays channel names and IDs

#### `!ex add <channel>`
Adds channel(s) to the exclusion list.

**Required Permission:** Administrator

**Usage Examples:**
```
!ex add #channel-name
!ex add 123456789012345678
!ex add https://discord.com/channels/server_id/channel_id
```

**What it does:**
- Accepts channel mentions, IDs, or URLs
- Can add multiple channels at once
- Validates channels exist in the server
- Updates exclusion list

#### `!ex remove <channel>`
Removes channel(s) from the exclusion list.

**Required Permission:** Administrator

**Usage Examples:**
```
!ex remove #channel-name
!ex remove 123456789012345678
```

**What it does:**
- Accepts channel mentions, IDs, or URLs
- Can remove multiple channels at once
- Updates exclusion list

## Permissions Required

The bot requires the following Discord permissions:
- **Administrator permission** for users running commands
- **Read Message History** - to fetch historical messages
- **View Channels** - to see server channels
- **Send Messages** - to respond with status updates

The bot uses these Discord intents:
- `Guilds` - to access server information
- `GuildMessages` - to read messages
- `MessageContent` - to access message content
- `GuildMembers` - to access member information

## Data Export Format

The bot exports data in **NDJSON** (Newline Delimited JSON) format, where each line is a valid JSON object.

**File naming:** `{guild-name}-{guild-id}-{timestamp}.ndjson`

**Example export structure:**
```json
{"type":"metadata","id":"123...","name":"Server Name","exportStartedAt":"2024-..."}
{"type":"channel","id":"456...","name":"general","channelType":0}
{"type":"message","id":"789...","content":"Hello","authorId":"111...","timestamp":1234567890}
{"type":"metadata","exportCompletedAt":"2024-...","totalNonBotMessages":1500}
```

## Features

- **Memory Management:** Automatic memory cleanup and monitoring to handle large servers
- **Rate Limit Handling:** Automatically handles Discord API rate limits
- **Progress Tracking:** Real-time status updates during export
- **Concurrent Processing:** Parallel channel processing for faster exports
- **Thread Support:** Exports both active and archived threads
- **Forum Support:** Exports forum channels and posts
- **Exclusion List:** Manage which channels to exclude from exports

## License

ISC
