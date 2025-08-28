# scripts/collect_nfl_data.py
# Automated NFL data collection using nfl_data_py

import nfl_data_py as nfl
import pandas as pd
import json
import os
from datetime import datetime, timedelta
import requests

def calculate_current_week():
    """Calculate current NFL week based on season start"""
    season_start = datetime(2025, 9, 4)  # Approximate 2025 season start
    now = datetime.now()
    
    if now < season_start:
        return 1
    
    weeks_elapsed = (now - season_start).days // 7
    return min(max(weeks_elapsed + 1, 1), 18)  # Weeks 1-18

def fetch_position_limits():
    """Load position limits from config file"""
    try:
        with open('json_config/enhanced_settings.json', 'r') as f:
            settings = json.load(f)
            return settings.get('position_limits', {
                'QB': 25, 'RB': 50, 'WR': 50, 'TE': 25, 
                'K': 15, 'DEF': 25, 'DL': 25, 'LB': 25, 'DB': 30
            })
    except FileNotFoundError:
        print("Settings file not found, using default position limits")
        return {
            'QB': 25, 'RB': 50, 'WR': 50, 'TE': 25, 
            'K': 15, 'DEF': 25, 'DL': 25, 'LB': 25, 'DB': 30
        }

def fetch_adp_data():
    """Simulate ADP data - in production would fetch from Fantasy Football Calculator"""
    print("Note: Simulating ADP data. In production, integrate with FantasyPros API")
    # This would be replaced with actual ADP API calls
    return {}

def collect_nfl_rosters():
    """Collect current NFL rosters using nfl_data_py"""
    print("Collecting NFL roster data...")
    
    try:
        # Get current season weekly data (includes roster info)
        current_year = datetime.now().year
        weekly_data = nfl.import_weekly_data([current_year])
        
        print(f"Collected {len(weekly_data)} total weekly roster entries")
        return weekly_data
        
    except Exception as e:
        print(f"Failed to collect current weekly data: {e}")
        print("Trying previous season as fallback...")
        
        try:
            # Fallback to previous year if current not available
            weekly_data = nfl.import_weekly_data([current_year - 1])
            print(f"Using {current_year - 1} weekly data: {len(weekly_data)} entries")
            return weekly_data
        except Exception as e2:
            print(f"Fallback also failed: {e2}")
            print("Trying seasonal data as final fallback...")
            
            try:
                # Try seasonal data instead
                seasonal_data = nfl.import_seasonal_data([current_year - 1])
                print(f"Using {current_year - 1} seasonal data: {len(seasonal_data)} entries")
                return seasonal_data
            except Exception as e3:
                print(f"All methods failed: {e3}")
                return pd.DataFrame()

def collect_injury_data():
    """Collect current injury reports"""
    print("Collecting injury data...")
    
    try:
        current_year = datetime.now().year
        current_week = calculate_current_week()
        
        # Get weekly data which may include injury information
        weekly_data = nfl.import_weekly_data([current_year])
        
        # For now, simulate injury data since nfl_data_py may not have explicit injury functions
        print("Note: Simulating injury data. NFL_data_py may not have direct injury import function")
        return pd.DataFrame()
            
    except Exception as e:
        print(f"Failed to collect injury data: {e}")
        return pd.DataFrame()

def process_players(rosters_df, position_limits, adp_data):
    """Process and filter players based on fantasy relevance"""
    print("Processing player data...")
    
    if rosters_df.empty:
        print("No roster data to process")
        return {}
    
    # Filter for fantasy-relevant positions
    fantasy_positions = list(position_limits.keys())
    fantasy_rosters = rosters_df[rosters_df['position'].isin(fantasy_positions)].copy()
    
    print(f"Filtered to {len(fantasy_rosters)} fantasy-relevant players")
    
    # Group by position and apply limits
    processed_players = {}
    position_counts = {pos: 0 for pos in position_limits.keys()}
    
    # Sort by position and available columns for better player selection
    # Check which columns actually exist first
    available_columns = fantasy_rosters.columns.tolist()
    sort_columns = ['position']
    
    # Add optional columns if they exist
    if 'depth_chart_position' in available_columns:
        sort_columns.append('depth_chart_position')
    if 'years_exp' in available_columns:
        sort_columns.append('years_exp')
    elif 'season' in available_columns:  # Alternative sorting column
        sort_columns.append('season')
    
    fantasy_rosters = fantasy_rosters.sort_values(sort_columns, ascending=[True] * len(sort_columns))
    
    player_index = 0
    processed_count = 0
    
    for _, player in fantasy_rosters.iterrows():
        position = player['position']
        
        # Apply position limits
        if position_counts[position] < position_limits[position]:
            # Create unique player ID - use player_id from nfl_data_py if available
            nfl_player_id = player.get('player_id')
            player_name = player.get('player_name', player.get('player_display_name', f'player_{player_index}'))
            
            if nfl_player_id:
                player_id = str(nfl_player_id)
            else:
                # Fallback to name-based ID with index to ensure uniqueness
                clean_name = player_name.lower().replace(' ', '_').replace('.', '').replace("'", '')
                player_id = f"{clean_name}_{position.lower()}_{position_counts[position]}"
            
            # Check for duplicate IDs and make unique if necessary
            original_player_id = player_id
            counter = 1
            while player_id in processed_players:
                player_id = f"{original_player_id}_{counter}"
                counter += 1
            
            # Calculate ADP data (simulated) - fix the position count reference
            simulated_adp = 50 + position_counts[position] * 8 + (player_index % 50)
            
            processed_players[player_id] = {
                'player_id': player_id,
                'player_name': player_name,
                'team': player.get('team', player.get('recent_team', 'FA')),
                'position': position,
                'team_id': player.get('team_id'),
                'team_name': player.get('team_name'),
                'adp_overall': simulated_adp,
                'adp_position': position_counts[position] + 1,
                'adp_round': (simulated_adp - 1) // 12 + 1,
                'adp_pick': ((simulated_adp - 1) % 12) + 1,
                'tier': min(((position_counts[position] // 5) + 1), 5),
                'projected_points_standard': max(150 - (position_counts[position] * 8), 80),
                'projected_points_ppr': max(180 - (position_counts[position] * 8), 100),
                'projected_points_halfppr': max(165 - (position_counts[position] * 8), 90),
                'risk_score': min(max(3 + (position_counts[position] // 10), 3), 9),
                'bye_week': (position_counts[position] % 14) + 4,  # Weeks 4-17
                'strengths': get_position_strengths(position, position_counts[position]),
                'concerns': get_position_concerns(position, position_counts[position]),
                'contextual_analysis_reach': f"Before {max(simulated_adp - 12, 1)}.{((max(simulated_adp - 12, 1) - 1) % 12) + 1}",
                'contextual_analysis_value': f"Picks {max(simulated_adp - 6, 1)}-{simulated_adp + 6}",
                'contextual_analysis_steal': f"After {simulated_adp + 12}.{((simulated_adp + 12 - 1) % 12) + 1}",
                'contextual_analysis_keeper': "Yes - solid production" if position_counts[position] < 15 else "Maybe - depth piece",
                'contextual_analysis_redraft': get_redraft_analysis(position, position_counts[position]),
                'last_updated': datetime.now().isoformat(),
                'status': 'active',
                'injury_status': 'healthy',
                'years_exp': player.get('years_exp', 0),
                'age': player.get('age'),
                'height': player.get('weight'),
                'weight': player.get('weight'),
                'college': player.get('college'),
                'depth_chart_position': position_counts[position] + 1
            }
            
            position_counts[position] += 1
            processed_count += 1
        
        player_index += 1
    
    print(f"DEBUG: Processed {processed_count} players, dictionary has {len(processed_players)} entries")
    
    print("Player counts by position:")
    total_players = 0
    for pos, count in position_counts.items():
        print(f"  {pos}: {count}/{position_limits[pos]}")
        total_players += count
    
    print(f"Total players processed: {total_players}")
    return processed_players, position_counts

def get_position_strengths(position, rank):
    """Generate position-specific strengths based on ranking"""
    strengths_map = {
        'QB': ["Elite arm talent", "Dual-threat ability", "Proven production"] if rank < 10 else ["Game manager", "System fit"],
        'RB': ["Workhorse usage", "Pass-catching ability", "Red zone touches"] if rank < 20 else ["Handcuff value", "Goal line work"],
        'WR': ["Target share", "Red zone looks", "Big play ability"] if rank < 20 else ["Deep threat", "Slot specialist"],
        'TE': ["Reliable target", "Red zone presence", "Blocking ability"] if rank < 10 else ["Streaming option", "Matchup dependent"]
    }
    return ",".join(strengths_map.get(position, ["Solid contributor"])[:2])

def get_position_concerns(position, rank):
    """Generate position-specific concerns based on ranking"""
    concerns_map = {
        'QB': ["Age concerns", "Injury history"] if rank < 10 else ["Inconsistent play", "Limited weapons"],
        'RB': ["Heavy workload", "Injury concerns"] if rank < 20 else ["Limited touches", "Committee backfield"],
        'WR': ["Target competition", "QB play"] if rank < 20 else ["Boom/bust", "Limited snaps"],
        'TE': ["Inconsistent targets", "Injury prone"] if rank < 10 else ["Low floor", "Streaming risk"]
    }
    return ",".join(concerns_map.get(position, ["Depth concerns"])[:2])

def get_redraft_analysis(position, rank):
    """Generate redraft league analysis"""
    if rank < 5:
        return f"{position}1 with high ceiling"
    elif rank < 15:
        return f"Solid {position}2 option"
    elif rank < 25:
        return f"Flex play with upside"
    else:
        return "Deep league consideration"

def update_injury_data(processed_players, injury_df):
    """Update players with current injury status"""
    if injury_df.empty:
        return processed_players
    
    print("Updating injury statuses...")
    
    # Create mapping of player names to injury status
    injury_map = {}
    for _, injury in injury_df.iterrows():
        player_name = injury.get('full_name', injury.get('player_name', ''))
        if player_name:
            injury_map[player_name.lower()] = injury.get('report_status', 'Unknown')
    
    # Update player injury statuses
    injured_count = 0
    for player_id, player_data in processed_players.items():
        player_name = player_data['player_name'].lower()
        if player_name in injury_map:
            player_data['injury_status'] = injury_map[player_name]
            if injury_map[player_name] != 'healthy':
                injured_count += 1
    
    print(f"Updated injury status for {injured_count} players")
    return processed_players

def save_players_data(processed_players, position_counts):
    """Save processed players to JSON file"""
    current_week = calculate_current_week()
    
    print(f"DEBUG: About to save {len(processed_players)} players to JSON")
    print(f"DEBUG: Position counts being saved: {position_counts}")
    
    # Verify we have the expected number of players
    expected_total = sum(position_counts.values())
    actual_total = len(processed_players)
    
    if expected_total != actual_total:
        print(f"WARNING: Expected {expected_total} players but have {actual_total}")
        
    # Debug: Print first few player IDs
    player_ids = list(processed_players.keys())[:5]
    print(f"DEBUG: First 5 player IDs: {player_ids}")
    
    players_data = {
        'metadata': {
            'last_updated': datetime.now().isoformat(),
            'version': '3.0',
            'total_players': len(processed_players),  # Use actual count, not expected
            'position_breakdown': position_counts,
            'data_collection_status': 'completed',
            'data_source': 'nfl_data_py_automated',
            'season': datetime.now().year,
            'week': current_week,
            'collection_type': 'github_actions_automated'
        },
        'players': processed_players
    }
    
    # Ensure directory exists
    os.makedirs('json_data', exist_ok=True)
    
    # Save to file
    with open('json_data/players.json', 'w') as f:
        json.dump(players_data, f, indent=2)
    
    print(f"Successfully saved {len(processed_players)} players to json_data/players.json")
    print(f"File size: {os.path.getsize('json_data/players.json') / 1024:.2f} KB")

def save_injury_data(injury_df):
    """Save injury data to JSON file"""
    current_week = calculate_current_week()
    
    injury_data = {
        'metadata': {
            'last_updated': datetime.now().isoformat(),
            'active_injuries': len(injury_df) if not injury_df.empty else 0,
            'season': datetime.now().year,
            'week': current_week
        },
        'current_injuries': {},
        'injury_history': {},
        'return_timeline': {}
    }
    
    # Process injury data if available
    if not injury_df.empty:
        for _, injury in injury_df.iterrows():
            player_name = injury.get('full_name', injury.get('player_name', 'Unknown'))
            if player_name != 'Unknown':
                injury_data['current_injuries'][player_name] = {
                    'team': injury.get('team', 'Unknown'),
                    'position': injury.get('position', 'Unknown'),
                    'report_status': injury.get('report_status', 'Unknown'),
                    'last_updated': datetime.now().isoformat()
                }
    
    # Save to file
    with open('json_data/injuries.json', 'w') as f:
        json.dump(injury_data, f, indent=2)
    
    print(f"Saved injury data to json_data/injuries.json")

def main():
    """Main execution function"""
    print("=== AUTOMATED NFL DATA COLLECTION STARTED ===")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Current week: {calculate_current_week()}")
    
    # Load configuration
    position_limits = fetch_position_limits()
    print(f"Position limits: {position_limits}")
    
    # Collect data
    rosters_df = collect_nfl_rosters()
    injury_df = collect_injury_data()
    adp_data = fetch_adp_data()
    
    # Process players
    if not rosters_df.empty:
        processed_players, position_counts = process_players(rosters_df, position_limits, adp_data)
        
        # Update with injury data
        processed_players = update_injury_data(processed_players, injury_df)
        
        # Save data
        save_players_data(processed_players, position_counts)
        save_injury_data(injury_df)
        
        print("=== DATA COLLECTION COMPLETED SUCCESSFULLY ===")
        print(f"Total players processed: {len(processed_players)}")
        print("Files updated: json_data/players.json, json_data/injuries.json")
        
    else:
        print("=== DATA COLLECTION FAILED ===")
        print("No roster data available")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
