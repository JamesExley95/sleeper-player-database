# Sleeper Player Database

This repository contains NFL player data from the Sleeper API.

## Files

- `players_simple.json` - Simple player ID to name mapping
- `players_detailed.json` - Detailed player information

## Usage in Make.com

### Simple Lookup
URL: `https://raw.githubusercontent.com/YOUR_USERNAME/sleeper-player-database/main/players_simple.json`

### Detailed Lookup  
URL: `https://raw.githubusercontent.com/YOUR_USERNAME/sleeper-player-database/main/players_detailed.json`

## Data Structure

### Simple Format
```json
{
  "metadata": {...},
  "players": {
    "4066": "Calvin Ridley",
    "8134": "D'Andre Swift"
  }
}
```

### Detailed Format
```json
{
  "metadata": {...},
  "players": {
    "4066": {
      "name": "Calvin Ridley",
      "position": "WR", 
      "team": "TEN",
      "status": "Active"
    }
  }
}
```

## Last Updated
2025-08-20 17:36:36

Total Players: 11400
