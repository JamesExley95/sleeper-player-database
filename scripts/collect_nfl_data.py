#!/usr/bin/env python3
"""
NFL Data Collection Script - Production Version
Version: 3.2
Consolidated from debugged version with core fixes preserved
"""

import nfl_data_py as nfl
import json
import os
import hashlib
from datetime import datetime
import requests

SCRIPT_VERSION = "3.2"

def load_nfl_data():
    """Load and combine NFL data from multiple sources"""
    print("Loading NFL data...")
    
    current_year = 2024
    years = [current_year]
    
    # Load comprehensive NFL datasets
    datasets = {}
    
    try:
        # Core player data
        print("  - Loading roster data...")
        datasets['rosters'] = nfl.import_rosters(years)
        
        print("  - Loading weekly stats...")
        datasets['weekly'] = nfl.import_weekly_data(years)
        
        print("  - Loading seasonal stats...")  
        datasets['seasonal'] = nfl.import_seasonal_data(years)
        
        print("  - Loading player IDs...")
        datasets['ids'] = nfl.import_ids()
        
        # Advanced metrics
        print("  - Loading advanced stats...")
        try:
            datasets['advanced'] = nfl.import_pbp_data(years, columns=['player_id', 'player_name', 'passer_rating', 'cpoe'])
        except Exception as e:
            print(f"    Advanced stats unavailable: {e}")
            datasets['advanced'] = None
            
    except Exception as e:
        print(f"Error loading NFL data: {e}")
        raise
    
    print(f"Data loading complete. Processing datasets...")
    return datasets

def process_players(datasets):
    """Process and aggregate player data with proper deduplication"""
    print("Processing player data...")
    
    # Step 1: Aggregate all player records by unique player identifier
    player_aggregations = {}
    
    # Process roster data (primary source)
    if 'rosters' in datasets and datasets['rosters'] is not None:
        for _, player in datasets['rosters'].iterrows():
            if player['position'] in ['QB', 'RB', 'WR', 'TE']:
                # Create unique key for deduplication
                key = f"{player['player_name']}_{player['position']}"
                
                if key not in player_aggregations:
                    player_aggregations[key] = {
                        'player_name': player['player_name'],
                        'position': player['position'],
                        'team': player['team'],
                        'records': []
                    }
                
                player_aggregations[key]['records'].append({
                    'source': 'roster',
                    'team': player['team'],
                    'season': player.get('season', 2024)
                })
    
    # Enhance with weekly stats
    if 'weekly' in datasets and datasets['weekly'] is not None:
        for _, stats in datasets['weekly'].iterrows():
            if stats['position'] in ['QB', 'RB', 'WR', 'TE']:
                key = f"{stats['player_name']}_{stats['position']}"
                
                if key in player_aggregations:
                    player_aggregations[key]['records'].append({
                        'source': 'weekly_stats',
                        'fantasy_points': stats.get('fantasy_points_ppr', 0),
                        'week': stats.get('week', 0)
                    })
    
    # Enhance with seasonal data
    if 'seasonal' in datasets and datasets['seasonal'] is not None:
        for _, seasonal in datasets['seasonal'].iterrows():
            if seasonal['position'] in ['QB', 'RB', 'WR', 'TE']:
                key = f"{seasonal['player_name']}_{seasonal['position']}"
                
                if key in player_aggregations:
                    player_aggregations[key]['records'].append({
                        'source': 'seasonal',
                        'fantasy_points': seasonal.get('fantasy_points_ppr', 0),
                        'games': seasonal.get('games', 0)
                    })
    
    print(f"Unique players after aggregation: {len(player_aggregations)}")
    
    # Step 2: Create final player objects
    processed_players = []
    
    for key, aggregated_data in player_aggregations.items():
        # Calculate aggregated stats from all records
        total_fantasy_points = sum(r.get('fantasy_points', 0) for r in aggregated_data['records'] if 'fantasy_points' in r)
        total_games = sum(r.get('games', 0) for r in aggregated_data['records'] if 'games' in r)
        weekly_records = [r for r in aggregated_data['records'] if r['source'] == 'weekly_stats']
        
        # Generate projections and risk scores
        avg_points = total_fantasy_points / max(total_games, 1) if total_games > 0 else 0
        projected_season = avg_points * 17 if avg_points > 0 else generate_projection(aggregated_data['position'])
        
        player_obj = {
            'player_name': aggregated_data['player_name'],
            'position': aggregated_data['position'],
            'team': aggregated_data['team'],
            'fantasy_points_season': round(total_fantasy_points, 1),
            'projected_points_ppr': round(projected_season, 1),
            'games_played': total_games,
            'avg_points_per_game': round(avg_points, 1),
            'weekly_records': len(weekly_records),
            'adp_overall': generate_adp(aggregated_data['position'], projected_season),
            'risk_score': calculate_risk_score(aggregated_data['position'], avg_points, len(weekly_records)),
            'injury_status': 'healthy',  # Default - would be enhanced with injury API
            'last_updated': datetime.now().isoformat()
        }
        
        processed_players.append(player_obj)
    
    # Sort by projected points for consistent ordering
    processed_players.sort(key=lambda x: x['projected_points_ppr'], reverse=True)
    
    print(f"Final processed players: {len(processed_players)}")
    print(f"Positions: QB={sum(1 for p in processed_players if p['position'] == 'QB')}, "
          f"RB={sum(1 for p in processed_players if p['position'] == 'RB')}, "
          f"WR={sum(1 for p in processed_players if p['position'] == 'WR')}, "
          f"TE={sum(1 for p in processed_players if p['position'] == 'TE')}")
    
    return processed_players

def generate_projection(position):
    """Generate realistic fantasy projections by position"""
    base_projections = {
        'QB': 280,  # ~16.5 ppg
        'RB': 180,  # ~10.6 ppg  
        'WR': 160,  # ~9.4 ppg
        'TE': 120   # ~7.1 ppg
    }
    return base_projections.get(position, 100)

def generate_adp(position, projected_points):
    """Generate realistic ADP based on position and projections"""
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

def calculate_risk_score(position, avg_points, weekly_records):
    """Calculate risk score based on consistency and sample size"""
    base_risk = 5
    
    # Adjust for sample size
    if weekly_records < 5:
        base_risk += 2
    elif weekly_records > 12:
        base_risk -= 1
        
    # Adjust for position volatility
    volatility = {'QB': 0, 'RB': 1, 'WR': 2, 'TE': 1}
    base_risk += volatility.get(position, 1)
    
    # Adjust for performance level
    if avg_points < 5:
        base_risk += 2
    elif avg_points > 15:
        base_risk -= 1
    
    return max(1, min(10, base_risk))

def create_player_json(processed_players):
    """Create JSON structure with unique player IDs"""
    players_dict = {}
    
    for i, player in enumerate(processed_players):
        # Create unique, consistent player ID
        name_short = player['player_name'].replace(' ', '.').replace("'", "")[:10]
        player_id = f"{name_short}_{player['position']}_{i+1:03d}"
        
        players_dict[player_id] = player
    
    database = {
        "metadata": {
            "script_version": SCRIPT_VERSION,
            "last_updated": datetime.now().isoformat(),
            "version": "3.2",
            "total_players": len(processed_players),
            "position_breakdown": {
                "QB": sum(1 for p in processed_players if p['position'] == 'QB'),
                "RB": sum(1 for p in processed_players if p['position'] == 'RB'), 
                "WR": sum(1 for p in processed_players if p['position'] == 'WR'),
                "TE": sum(1 for p in processed_players if p['position'] == 'TE'),
                "K": 0,
                "DEF": 0
            },
            "data_collection_status": "completed",
            "next_update": "automated_weekly"
        },
        "players": players_dict
    }
    
    return database

def save_database(database, filename='json_data/players.json'):
    """Save database with basic verification"""
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    try:
        with open(filename, 'w') as f:
            json.dump(database, f, indent=2, ensure_ascii=False)
        
        # Verify save
        file_size = os.path.getsize(filename)
        print(f"Database saved successfully: {filename}")
        print(f"File size: {file_size / 1024:.2f} KB")
        print(f"Total players: {database['metadata']['total_players']}")
        
        return True
        
    except Exception as e:
        print(f"Error saving database: {e}")
        return False

def main():
    """Main execution flow"""
    print(f"=== NFL Data Collection Script v{SCRIPT_VERSION} ===")
    
    try:
        # Load data
        datasets = load_nfl_data()
        
        # Process with proper deduplication 
        processed_players = process_players(datasets)
        
        if not processed_players:
            raise Exception("No players processed - check data sources")
        
        # Create JSON database
        database = create_player_json(processed_players)
        
        # Save to file
        success = save_database(database)
        
        if success:
            print(f"\n=== SUCCESS ===")
            print(f"Unique players in database: {len(processed_players)}")
            print(f"Data collection completed successfully")
        else:
            raise Exception("Database save failed")
            
    except Exception as e:
        print(f"\n=== ERROR ===")
        print(f"Script failed: {e}")
        raise

if __name__ == "__main__":
    main()
