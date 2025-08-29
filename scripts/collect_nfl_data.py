#!/usr/bin/env python3
"""
DEBUGGED NFL Data Collection Script
Version: 3.1.1-debug
"""

import nfl_data_py as nfl
import json
import os
import hashlib
from datetime import datetime
import requests

# Debug: Print script version and location immediately
SCRIPT_VERSION = "3.1.1-debug"
print(f"=== SCRIPT VERSION: {SCRIPT_VERSION} ===")
print(f"Script location: {__file__}")
print(f"Working directory: {os.getcwd()}")
print(f"Environment: {os.environ.get('GITHUB_ACTIONS', 'local')}")

def debug_file_status(filepath):
    """Debug helper to check file status"""
    if os.path.exists(filepath):
        stat = os.stat(filepath)
        with open(filepath, 'r') as f:
            content = f.read()
        content_hash = hashlib.md5(content.encode()).hexdigest()
        print(f"File exists: {filepath}")
        print(f"Size: {stat.st_size} bytes")
        print(f"Modified: {datetime.fromtimestamp(stat.st_mtime)}")
        print(f"Content hash: {content_hash[:16]}")
        
        # Check first few lines for version info
        lines = content.split('\n')[:10]
        for line in lines:
            if 'version' in line.lower() or 'last_updated' in line.lower():
                print(f"Version info: {line.strip()}")
    else:
        print(f"File does not exist: {filepath}")

def load_nfl_data():
    """Load NFL data with enhanced debugging"""
    try:
        print("=== DATA LOADING DEBUG ===")
        print("Loading NFL weekly data for 2024...")
        weekly_data = nfl.import_weekly_data([2024], columns=[
            'player_id', 'player_name', 'position', 'recent_team', 
            'fantasy_points_ppr', 'passing_yards', 'rushing_yards', 
            'receiving_yards', 'passing_tds', 'rushing_tds', 'receiving_tds'
        ])
        print(f"Loaded {len(weekly_data)} total weekly records")
        
        # Debug: Check data structure
        print("Sample raw data:")
        print(weekly_data.head(3).to_string())
        print(f"Unique players in raw data: {weekly_data['player_name'].nunique()}")
        print(f"Unique player_ids: {weekly_data['player_id'].nunique()}")
        
        return weekly_data
    except Exception as e:
        print(f"ERROR loading NFL data: {e}")
        return None

def process_players(weekly_data, target_counts):
    """Process and deduplicate players with extensive debugging"""
    if weekly_data is None or weekly_data.empty:
        print("ERROR: No data to process")
        return []
    
    print(f"=== DEDUPLICATION DEBUG ===")
    print(f"Processing {len(weekly_data)} weekly records...")
    
    # Debug: Show some raw data
    print("Raw data sample:")
    sample_data = weekly_data[['player_name', 'position', 'recent_team', 'fantasy_points_ppr']].head(10)
    print(sample_data.to_string())
    
    # Create player summaries by aggregating all their weekly data
    player_stats = {}
    duplicate_keys_found = []
    
    for idx, row in weekly_data.iterrows():
        player_name = row.get('player_name', 'Unknown')
        position = row.get('position', 'Unknown')
        team = row.get('recent_team', 'Unknown')
        
        # Skip invalid entries
        if not player_name or player_name == 'Unknown' or not position:
            continue
        
        # Create unique key: name + position (allows same name, different positions)
        unique_key = f"{player_name}_{position}"
        
        if unique_key not in player_stats:
            player_stats[unique_key] = {
                'player_name': player_name,
                'position': position,
                'team': team,
                'total_fantasy_points': 0,
                'total_passing_yards': 0,
                'total_rushing_yards': 0,
                'total_receiving_yards': 0,
                'total_passing_tds': 0,
                'total_rushing_tds': 0,
                'total_receiving_tds': 0,
                'games_played': 0
            }
        else:
            # Track duplicate key occurrences
            if unique_key not in duplicate_keys_found:
                duplicate_keys_found.append(unique_key)
        
        # Aggregate stats
        stats = player_stats[unique_key]
        stats['total_fantasy_points'] += row.get('fantasy_points_ppr', 0) or 0
        stats['total_passing_yards'] += row.get('passing_yards', 0) or 0
        stats['total_rushing_yards'] += row.get('rushing_yards', 0) or 0
        stats['total_receiving_yards'] += row.get('receiving_yards', 0) or 0
        stats['total_passing_tds'] += row.get('passing_tds', 0) or 0
        stats['total_rushing_tds'] += row.get('rushing_tds', 0) or 0
        stats['total_receiving_tds'] += row.get('receiving_tds', 0) or 0
        stats['games_played'] += 1
        
        # Use most recent team
        stats['team'] = team
    
    print(f"Raw weekly records: {len(weekly_data)}")
    print(f"Unique players after aggregation: {len(player_stats)}")
    print(f"Duplicate keys processed: {len(duplicate_keys_found)}")
    
    # Debug: Show some aggregated players
    print("Sample aggregated players:")
    sample_players = list(player_stats.items())[:5]
    for key, stats in sample_players:
        print(f"  {key}: {stats['total_fantasy_points']:.1f} pts, {stats['games_played']} games")
    
    # Filter to fantasy-relevant players
    fantasy_players = []
    for unique_key, stats in player_stats.items():
        if is_fantasy_relevant(stats):
            fantasy_players.append(stats)
    
    print(f"Fantasy-relevant players: {len(fantasy_players)}")
    
    # Sort by total fantasy points for each position
    sorted_players = {'QB': [], 'RB': [], 'WR': [], 'TE': []}
    
    for player in fantasy_players:
        position = player['position']
        if position in sorted_players:
            sorted_players[position].append(player)
    
    # Debug position distribution
    print("Players by position before sorting:")
    for pos in sorted_players:
        print(f"  {pos}: {len(sorted_players[pos])} players")
    
    # Sort each position by fantasy points
    for position in sorted_players:
        sorted_players[position].sort(
            key=lambda x: x['total_fantasy_points'], 
            reverse=True
        )
    
    # Select top players for each position
    final_players = []
    for position, count in target_counts.items():
        if position in sorted_players:
            selected = sorted_players[position][:count]
            final_players.extend(selected)
            print(f"Selected top {len(selected)} {position}s")
            
            # Debug: Show top 3 players for each position
            if selected:
                print(f"  Top 3 {position}s:")
                for i, player in enumerate(selected[:3]):
                    print(f"    {i+1}. {player['player_name']} ({player['team']}) - {player['total_fantasy_points']:.1f} pts")
    
    print(f"Final player list: {len(final_players)} players")
    return final_players

def is_fantasy_relevant(player_stats):
    """Determine if a player is fantasy relevant"""
    total_points = player_stats['total_fantasy_points']
    position = player_stats['position']
    games_played = player_stats['games_played']
    
    # Minimum thresholds by position
    min_thresholds = {
        'QB': {'points': 50, 'games': 4},
        'RB': {'points': 30, 'games': 3},
        'WR': {'points': 25, 'games': 3},
        'TE': {'points': 20, 'games': 3}
    }
    
    if position not in min_thresholds:
        return False
    
    threshold = min_thresholds[position]
    is_relevant = (total_points >= threshold['points'] and 
                   games_played >= threshold['games'])
    
    return is_relevant

def create_player_json(processed_players):
    """Create the final JSON structure with debugging"""
    print(f"=== JSON CREATION DEBUG ===")
    print(f"Creating JSON for {len(processed_players)} players")
    
    players_dict = {}
    player_names_created = []
    
    for i, player_data in enumerate(processed_players):
        # Create truly unique player ID
        player_name = player_data['player_name'].replace(' ', '_').replace('.', '').replace("'", '')
        position = player_data['position']
        team = player_data['team']
        
        player_id = f"{player_name}_{position}_{team}_{i:03d}"
        
        # Debug: Track first 10 players created
        if i < 10:
            player_names_created.append(f"{player_data['player_name']} ({position})")
        
        # Calculate projections and ADP
        total_points = player_data['total_fantasy_points']
        games_played = max(player_data['games_played'], 1)
        projected_points = (total_points / games_played) * 17  # Project to 17 games
        
        # Generate realistic ADP (will be replaced by real data later)
        position_adp_bases = {'QB': 80, 'RB': 40, 'WR': 50, 'TE': 120}
        base_adp = position_adp_bases.get(position, 100)
        adp_overall = base_adp + (i % 20) * 8  # More realistic distribution
        
        players_dict[player_id] = {
            "player_name": player_data['player_name'],
            "position": position,
            "team": team,
            "projected_points_ppr": round(projected_points, 1),
            "adp_overall": adp_overall,
            "adp_position": i % 30 + 1,
            "total_fantasy_points_2024": round(total_points, 1),
            "games_played_2024": games_played,
            "passing_yards_2024": player_data.get('total_passing_yards', 0),
            "rushing_yards_2024": player_data.get('total_rushing_yards', 0),
            "receiving_yards_2024": player_data.get('total_receiving_yards', 0),
            "total_tds_2024": (player_data.get('total_passing_tds', 0) + 
                             player_data.get('total_rushing_tds', 0) + 
                             player_data.get('total_receiving_tds', 0)),
            "risk_score": min(round(abs(projected_points - 100) / 10, 1), 10.0),
            "last_updated": datetime.now().isoformat()
        }
    
    print("First 10 players created:")
    for name in player_names_created:
        print(f"  {name}")
    
    print(f"Total unique player IDs created: {len(players_dict)}")
    
    # Debug: Check for any duplicates in final dataset
    player_names_final = [p['player_name'] for p in players_dict.values()]
    unique_names = set(player_names_final)
    if len(player_names_final) != len(unique_names):
        print(f"WARNING: Duplicate names in final dataset!")
        from collections import Counter
        name_counts = Counter(player_names_final)
        duplicates = {name: count for name, count in name_counts.items() if count > 1}
        print(f"Duplicates found: {duplicates}")
    else:
        print("✓ No duplicate names in final dataset")
    
    return players_dict

def save_database_with_debug(database):
    """Save database with extensive debugging"""
    print(f"=== FILE SAVE DEBUG ===")
    
    # Ensure directory exists
    os.makedirs('json_data', exist_ok=True)
    print(f"Directory 'json_data' exists: {os.path.exists('json_data')}")
    
    filepath = 'json_data/players.json'
    
    # Check existing file before overwrite
    print("BEFORE save:")
    debug_file_status(filepath)
    
    try:
        # Write the file
        print(f"Writing to: {os.path.abspath(filepath)}")
        with open(filepath, 'w') as f:
            json.dump(database, f, indent=2)
        
        print("✓ File write completed successfully")
        
        # Verify file was written correctly
        print("AFTER save:")
        debug_file_status(filepath)
        
        # Verify content integrity
        with open(filepath, 'r') as f:
            loaded_data = json.load(f)
        
        print(f"Verification - Players in saved file: {len(loaded_data.get('players', {}))}")
        print(f"Verification - Metadata version: {loaded_data.get('metadata', {}).get('version', 'missing')}")
        
        # Show first few player names to verify diversity
        players = loaded_data.get('players', {})
        if players:
            first_five_names = []
            for i, (player_id, player_data) in enumerate(players.items()):
                if i < 5:
                    first_five_names.append(f"{player_data.get('player_name', 'Unknown')} ({player_data.get('position', 'Unknown')})")
            print("First 5 players in saved file:")
            for name in first_five_names:
                print(f"  {name}")
        
        return True
        
    except Exception as e:
        print(f"ERROR saving file: {e}")
        print(f"Error type: {type(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main execution function with comprehensive debugging"""
    print(f"=== MAIN EXECUTION START ===")
    print(f"Script version: {SCRIPT_VERSION}")
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    # Target player counts for each position
    target_counts = {
        'QB': 35,
        'RB': 75, 
        'WR': 85,
        'TE': 35
    }
    
    print(f"Target counts: {target_counts}")
    
    # Load and process data
    weekly_data = load_nfl_data()
    if weekly_data is None:
        print("FATAL ERROR: Failed to load NFL data")
        return
    
    processed_players = process_players(weekly_data, target_counts)
    
    if not processed_players:
        print("FATAL ERROR: No players processed successfully")
        return
    
    # Create JSON structure
    players_dict = create_player_json(processed_players)
    
    # Create final database structure
    database = {
        "metadata": {
            "script_version": SCRIPT_VERSION,
            "last_updated": datetime.now().isoformat(),
            "version": "3.1.1",  # Updated version number
            "total_players": len(players_dict),
            "position_breakdown": {
                pos: len([p for p in players_dict.values() if p['position'] == pos])
                for pos in ['QB', 'RB', 'WR', 'TE', 'K', 'DEF']
            },
            "data_collection_method": "nflverse_2024_aggregated_debugged",
            "next_update": "Weekly Tuesday 6AM UTC",
            "debug_info": {
                "working_directory": os.getcwd(),
                "github_actions": os.environ.get('GITHUB_ACTIONS', 'false'),
                "python_version": os.sys.version
            }
        },
        "players": players_dict
    }
    
    # Save to file with debugging
    success = save_database_with_debug(database)
    
    if success:
        print(f"\n✅ SUCCESS: Created database with {len(players_dict)} unique players")
        print(f"Script version: {SCRIPT_VERSION}")
        print(f"Database version: {database['metadata']['version']}")
        print("\nPosition breakdown:")
        for pos, count in database['metadata']['position_breakdown'].items():
            if count > 0:
                print(f"  {pos}: {count} players")
        
        # Show sample players to verify diversity
        print(f"\nSample players (first 5):")
        for i, (player_id, player_data) in enumerate(list(players_dict.items())[:5]):
            print(f"  {player_data['position']}: {player_data['player_name']} ({player_data['team']})")
    else:
        print("FATAL ERROR: Failed to save database")
        return
    
    print("\n" + "="*60)
    
    # Enhanced AI analysis would run here but we'll skip it for debugging
    print("Skipping AI analysis for debugging focus")
    
    print(f"\nDEBUG SESSION COMPLETE")
    print(f"Script version: {SCRIPT_VERSION}")

if __name__ == "__main__":
    main()
