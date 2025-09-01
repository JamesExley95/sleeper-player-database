#!/usr/bin/env python3
"""
NFL Data Collection Script - Production Version
Version: 3.2 - Cleaned from working debug version
"""

import nfl_data_py as nfl
import json
import os
import hashlib
from datetime import datetime
import requests

SCRIPT_VERSION = "3.2"

def load_nfl_data():
    """Load NFL data using the working API calls from debug version"""
    print("Loading NFL data...")
    
    datasets = {}
    
    try:
        # Use same API calls that worked in debug version
        print("  Loading rosters...")
        datasets['rosters'] = nfl.import_rosters([2024])
        
        print("  Loading weekly data...")
        datasets['weekly'] = nfl.import_weekly_data([2024])
        
        print("  Loading seasonal data...")
        datasets['seasonal'] = nfl.import_seasonal_data([2024])
        
        print("  Loading IDs...")
        datasets['ids'] = nfl.import_ids()
        
        print("Data loading complete")
        return datasets
        
    except Exception as e:
        print(f"Error loading NFL data: {e}")
        raise

def aggregate_players(datasets):
    """Aggregate player data with the same deduplication logic from debug version"""
    print("Aggregating player data...")
    
    player_data = {}
    
    # Process rosters (primary source)
    if datasets.get('rosters') is not None:
        for _, player in datasets['rosters'].iterrows():
            if player['position'] in ['QB', 'RB', 'WR', 'TE']:
                key = f"{player['player_name']}_{player['position']}"
                
                if key not in player_data:
                    player_data[key] = {
                        'player_name': player['player_name'],
                        'position': player['position'],
                        'team': player['team'],
                        'season_data': []
                    }
                
                player_data[key]['team'] = player['team']  # Update team
    
    # Add weekly stats
    if datasets.get('weekly') is not None:
        for _, stats in datasets['weekly'].iterrows():
            if stats['position'] in ['QB', 'RB', 'WR', 'TE']:
                key = f"{stats['player_name']}_{stats['position']}"
                
                if key in player_data:
                    player_data[key]['season_data'].append({
                        'week': stats.get('week', 0),
                        'fantasy_points': stats.get('fantasy_points_ppr', 0)
                    })
    
    # Add seasonal totals
    if datasets.get('seasonal') is not None:
        for _, seasonal in datasets['seasonal'].iterrows():
            if seasonal['position'] in ['QB', 'RB', 'WR', 'TE']:
                key = f"{seasonal['player_name']}_{seasonal['position']}"
                
                if key in player_data:
                    player_data[key]['total_fantasy_points'] = seasonal.get('fantasy_points_ppr', 0)
                    player_data[key]['games_played'] = seasonal.get('games', 0)
    
    print(f"Aggregated {len(player_data)} unique players")
    return player_data

def create_players_json(player_data):
    """Create final players JSON with projections and rankings"""
    print("Creating player objects...")
    
    players_dict = {}
    all_players = list(player_data.values())
    
    # Sort by total fantasy points for ranking
    all_players.sort(key=lambda x: x.get('total_fantasy_points', 0), reverse=True)
    
    for i, player in enumerate(all_players):
        # Calculate metrics
        total_points = player.get('total_fantasy_points', 0)
        games = player.get('games_played', 1)
        avg_per_game = total_points / max(games, 1)
        
        # Generate projections based on performance
        projected_points = calculate_projection(player['position'], total_points, avg_per_game)
        adp = calculate_adp(player['position'], projected_points, i + 1)
        risk_score = calculate_risk(player['position'], avg_per_game, games)
        
        # Create player ID
        name_clean = player['player_name'].replace(' ', '.').replace("'", "")[:10]
        player_id = f"{name_clean}_{player['position']}_{i+1:03d}"
        
        # Create player object
        players_dict[player_id] = {
            'player_name': player['player_name'],
            'position': player['position'],
            'team': player['team'],
            'fantasy_points_season': round(total_points, 1),
            'projected_points_ppr': round(projected_points, 1),
            'games_played': games,
            'avg_points_per_game': round(avg_per_game, 1),
            'adp_overall': adp,
            'risk_score': risk_score,
            'injury_status': 'healthy',
            'last_updated': datetime.now().isoformat()
        }
    
    print(f"Created {len(players_dict)} player objects")
    return players_dict

def calculate_projection(position, total_points, avg_per_game):
    """Calculate next season projection"""
    if total_points > 0:
        return avg_per_game * 17  # Project over 17 games
    
    # Default projections by position
    defaults = {'QB': 280, 'RB': 180, 'WR': 160, 'TE': 120}
    return defaults.get(position, 100)

def calculate_adp(position, projected_points, overall_rank):
    """Calculate ADP based on projection and position"""
    if position == 'QB':
        if projected_points > 300: return 45
        elif projected_points > 250: return 85
        else: return 150
    elif position == 'RB':
        if projected_points > 250: return 15
        elif projected_points > 180: return 55
        else: return 120
    elif position == 'WR':
        if projected_points > 220: return 25
        elif projected_points > 160: return 70
        else: return 140
    else:  # TE
        if projected_points > 180: return 50
        elif projected_points > 120: return 90
        else: return 180

def calculate_risk(position, avg_points, games):
    """Calculate risk score 1-10"""
    base_risk = 5
    
    if games < 10: base_risk += 2
    elif games > 15: base_risk -= 1
    
    if avg_points < 5: base_risk += 2
    elif avg_points > 15: base_risk -= 1
    
    return max(1, min(10, base_risk))

def create_database(players_dict):
    """Create final database structure"""
    position_counts = {}
    for player in players_dict.values():
        pos = player['position']
        position_counts[pos] = position_counts.get(pos, 0) + 1
    
    database = {
        "metadata": {
            "script_version": SCRIPT_VERSION,
            "last_updated": datetime.now().isoformat(),
            "version": "3.2",
            "total_players": len(players_dict),
            "position_breakdown": {
                "QB": position_counts.get('QB', 0),
                "RB": position_counts.get('RB', 0),
                "WR": position_counts.get('WR', 0),
                "TE": position_counts.get('TE', 0),
                "K": 0,
                "DEF": 0
            },
            "data_collection_status": "completed"
        },
        "players": players_dict
    }
    
    return database

def save_database(database, filename='json_data/players.json'):
    """Save database to file"""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w') as f:
        json.dump(database, f, indent=2, ensure_ascii=False)
    
    file_size = os.path.getsize(filename)
    print(f"Database saved: {filename}")
    print(f"File size: {file_size / 1024:.2f} KB")
    print(f"Total players: {database['metadata']['total_players']}")

def main():
    """Main execution"""
    print(f"=== NFL Data Collection Script v{SCRIPT_VERSION} ===")
    
    try:
        # Load data using working API calls
        datasets = load_nfl_data()
        
        # Aggregate with same logic that worked
        player_data = aggregate_players(datasets)
        
        # Create players JSON
        players_dict = create_players_json(player_data)
        
        # Create final database
        database = create_database(players_dict)
        
        # Save
        save_database(database)
        
        print(f"\nSUCCESS: {len(players_dict)} unique players processed")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        raise

if __name__ == "__main__":
    main()

def create_players_json(player_data):
    """Create final players JSON with projections and rankings"""
    print("Creating player objects...")
    
    players_dict = {}
    all_players = list(player_data.values())
    
    # Sort by total fantasy points for ranking
    all_players.sort(key=lambda x: x.get('total_fantasy_points', 0), reverse=True)
    
    for i, player in enumerate(all_players):
        # Calculate metrics
        total_points = player.get('total_fantasy_points', 0)
        games = player.get('games_played', 1)
        avg_per_game = total_points / max(games, 1)
        
        # Generate projections based on performance
        projected_points = calculate_projection(player['position'], total_points, avg_per_game)
        adp = calculate_adp(player['position'], projected_points, i + 1)
        risk_score = calculate_risk(player['position'], avg_per_game, games)
        
        # Create player ID
        name_clean = player['player_name'].replace(' ', '.').replace("'", "")[:10]
        player_id = f"{name_clean}_{player['position']}_{i+1:03d}"
        
        # Create player object
        players_dict[player_id] = {
            'player_name': player['player_name'],
            'position': player['position'],
            'team': player['team'],
            'fantasy_points_season': round(total_points, 1),
            'projected_points_ppr': round(projected_points, 1),
            'games_played': games,
            'avg_points_per_game': round(avg_per_game, 1),
            'adp_overall': adp,
            'risk_score': risk_score,
            'injury_status': 'healthy',
            'last_updated': datetime.now().isoformat()
        }
    
    print(f"Created {len(players_dict)} player objects")
    return players_dict

def calculate_projection(position, total_points, avg_per_game):
    """Calculate next season projection"""
    if total_points > 0:
        return avg_per_game * 17  # Project over 17 games
    
    # Default projections by position
    defaults = {'QB': 280, 'RB': 180, 'WR': 160, 'TE': 120}
    return defaults.get(position, 100)

def calculate_adp(position, projected_points, overall_rank):
    """Calculate ADP based on projection and position"""
    if position == 'QB':
        if projected_points > 300: return 45
        elif projected_points > 250: return 85
        else: return 150
    elif position == 'RB':
        if projected_points > 250: return 15
        elif projected_points > 180: return 55
        else: return 120
    elif position == 'WR':
        if projected_points > 220: return 25
        elif projected_points > 160: return 70
        else: return 140
    else:  # TE
        if projected_points > 180: return 50
        elif projected_points > 120: return 90
        else: return 180

def calculate_risk(position, avg_points, games):
    """Calculate risk score 1-10"""
    base_risk = 5
    
    if games < 10: base_risk += 2
    elif games > 15: base_risk -= 1
    
    if avg_points < 5: base_risk += 2
    elif avg_points > 15: base_risk -= 1
    
    return max(1, min(10, base_risk))

def create_database(players_dict):
    """Create final database structure"""
    position_counts = {}
    for player in players_dict.values():
        pos = player['position']
        position_counts[pos] = position_counts.get(pos, 0) + 1
    
    database = {
        "metadata": {
            "script_version": SCRIPT_VERSION,
            "last_updated": datetime.now().isoformat(),
            "version": "3.2",
            "total_players": len(players_dict),
            "position_breakdown": {
                "QB": position_counts.get('QB', 0),
                "RB": position_counts.get('RB', 0),
                "WR": position_counts.get('WR', 0),
                "TE": position_counts.get('TE', 0),
                "K": 0,
                "DEF": 0
            },
            "data_collection_status": "completed"
        },
        "players": players_dict
    }
    
    return database

def save_database(database, filename='json_data/players.json'):
    """Save database to file"""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, 'w') as f:
        json.dump(database, f, indent=2, ensure_ascii=False)
    
    file_size = os.path.getsize(filename)
    print(f"Database saved: {filename}")
    print(f"File size: {file_size / 1024:.2f} KB")
    print(f"Total players: {database['metadata']['total_players']}")

def main():
    """Main execution"""
    print(f"=== NFL Data Collection Script v{SCRIPT_VERSION} ===")
    
    try:
        # Load data using working API calls
        datasets = load_nfl_data()
        
        # Aggregate with same logic that worked
        player_data = aggregate_players(datasets)
        
        # Create players JSON
        players_dict = create_players_json(player_data)
        
        # Create final database
        database = create_database(players_dict)
        
        # Save
        save_database(database)
        
        print(f"\nSUCCESS: {len(players_dict)} unique players processed")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        raise

if __name__ == "__main__":
    main()
