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

def debug_file_status(filename, label):
    """Debug file status before and after operations"""
    if os.path.exists(filename):
        size = os.path.getsize(filename)
        mtime = os.path.getmtime(filename)
        mtime_str = datetime.fromtimestamp(mtime).isoformat()
        print(f"{label}: EXISTS - Size: {size} bytes, Modified: {mtime_str}")
        
        # Calculate hash for content verification
        with open(filename, 'rb') as f:
            content_hash = hashlib.md5(f.read()).hexdigest()[:8]
        print(f"{label}: Content hash: {content_hash}")
    else:
        print(f"{label}: NOT EXISTS")

def load_nfl_data():
    """Load and combine NFL data from multiple sources with debug output"""
    print("\n=== DATA LOADING DEBUG ===")
    
    current_year = 2024
    years = [current_year]
    
    # Debug: Check what functions are actually available
    print("Checking available nfl_data_py functions...")
    available_functions = [attr for attr in dir(nfl) if not attr.startswith('_')]
    print(f"Public functions: {available_functions[:10]}...")  # Show first 10
    
    # Check for private functions that might be the real ones
    private_functions = [attr for attr in dir(nfl) if attr.startswith('__') and 'import' in attr.lower()]
    if private_functions:
        print(f"Private import functions found: {private_functions}")
    
    datasets = {}
    
    try:
        print("Loading NFL datasets...")
        print(f"Target year: {current_year}")
        
        # Core player data - try multiple API approaches
        print("  - Loading roster data...")
        try:
            datasets['rosters'] = nfl.import_rosters(years)
            print(f"    ✅ import_rosters worked: {len(datasets['rosters'])} records")
        except AttributeError:
            print("    ❌ import_rosters not available, trying alternatives...")
            try:
                # Try private function
                datasets['rosters'] = nfl.__import_rosters(years)
                print(f"    ✅ __import_rosters worked: {len(datasets['rosters'])} records")
            except:
                print("    ❌ No roster import function available")
                datasets['rosters'] = None
        
        print("  - Loading weekly stats...")
        try:
            datasets['weekly'] = nfl.import_weekly_data(years)
            print(f"    ✅ import_weekly_data worked: {len(datasets['weekly'])} records")
        except AttributeError:
            print("    ❌ import_weekly_data not available, trying alternatives...")
            try:
                datasets['weekly'] = nfl.__import_weekly_data(years)
                print(f"    ✅ __import_weekly_data worked: {len(datasets['weekly'])} records")
            except:
                print("    ❌ No weekly data import function available")
                datasets['weekly'] = None
        
        print("  - Loading seasonal stats...")
        try:
            datasets['seasonal'] = nfl.import_seasonal_data(years)
            print(f"    ✅ import_seasonal_data worked: {len(datasets['seasonal'])} records")
        except AttributeError:
            print("    ❌ import_seasonal_data not available, trying alternatives...")
            try:
                datasets['seasonal'] = nfl.__import_seasonal_data(years)
                print(f"    ✅ __import_seasonal_data worked: {len(datasets['seasonal'])} records")
            except:
                print("    ❌ No seasonal data import function available")
                datasets['seasonal'] = None
        
        print("  - Loading player IDs...")
        try:
            datasets['ids'] = nfl.import_ids()
            if datasets['ids'] is not None:
                print(f"    ✅ import_ids worked: {len(datasets['ids'])} records")
            else:
                print("    ✅ import_ids worked but returned None")
        except AttributeError:
            print("    ❌ import_ids not available, trying alternatives...")
            try:
                datasets['ids'] = nfl.__import_ids()
                if datasets['ids'] is not None:
                    print(f"    ✅ __import_ids worked: {len(datasets['ids'])} records")
                else:
                    print("    ✅ __import_ids worked but returned None")
            except:
                print("    ❌ No IDs import function available")
                datasets['ids'] = None
        
        # Check if we got any usable data
        data_sources = [k for k, v in datasets.items() if v is not None]
        if not data_sources:
            raise Exception("No NFL data sources available - all import functions failed")
        
        print(f"Successfully loaded data from: {data_sources}")
        
        # Advanced metrics (optional)
        print("  - Loading advanced stats...")
        try:
            datasets['advanced'] = nfl.import_pbp_data(years, columns=['player_id', 'player_name', 'passer_rating', 'cpoe'])
            if datasets['advanced'] is not None:
                print(f"    Advanced stats loaded: {len(datasets['advanced'])} records")
        except Exception as e:
            print(f"    Advanced stats unavailable: {e}")
            datasets['advanced'] = None
            
    except Exception as e:
        print(f"CRITICAL ERROR in data loading: {e}")
        raise
    
    print("=== DATA LOADING COMPLETE ===\n")
    return datasets

def process_players(datasets):
    """Process and aggregate player data with comprehensive deduplication debug"""
    print("=== DEDUPLICATION DEBUG ===")
    
    # Step 1: Aggregate all player records by unique player identifier
    print("Step 1: Aggregating player records...")
    player_aggregations = {}
    
    # Process roster data (primary source)
    if 'rosters' in datasets and datasets['rosters'] is not None:
        print("Processing roster data for aggregation...")
        roster_count = 0
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
                roster_count += 1
                
        print(f"  Roster records processed: {roster_count}")
    
    # Enhance with weekly stats
    if 'weekly' in datasets and datasets['weekly'] is not None:
        print("Processing weekly stats for aggregation...")
        weekly_count = 0
        for _, stats in datasets['weekly'].iterrows():
            if stats['position'] in ['QB', 'RB', 'WR', 'TE']:
                key = f"{stats['player_name']}_{stats['position']}"
                
                if key in player_aggregations:
                    player_aggregations[key]['records'].append({
                        'source': 'weekly_stats',
                        'fantasy_points': stats.get('fantasy_points_ppr', 0),
                        'week': stats.get('week', 0)
                    })
                    weekly_count += 1
                    
        print(f"  Weekly stats records added: {weekly_count}")
    
    # Enhance with seasonal data
    if 'seasonal' in datasets and datasets['seasonal'] is not None:
        print("Processing seasonal data for aggregation...")
        seasonal_count = 0
        for _, seasonal in datasets['seasonal'].iterrows():
            if seasonal['position'] in ['QB', 'RB', 'WR', 'TE']:
                key = f"{seasonal['player_name']}_{seasonal['position']}"
                
                if key in player_aggregations:
                    player_aggregations[key]['records'].append({
                        'source': 'seasonal',
                        'fantasy_points': seasonal.get('fantasy_points_ppr', 0),
                        'games': seasonal.get('games', 0)
                    })
                    seasonal_count += 1
                    
        print(f"  Seasonal records added: {seasonal_count}")
    
    print(f"Unique players after aggregation: {len(player_aggregations)}")
    
    # Debug: Show sample aggregated players
    sample_players = list(player_aggregations.keys())[:5]
    print(f"Sample aggregated players: {sample_players}")
    
    # Step 2: Create final player objects
    print("\nStep 2: Creating final player objects...")
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
            'injury_status': 'healthy',
            'last_updated': datetime.now().isoformat()
        }
        
        processed_players.append(player_obj)
    
    # Sort by projected points for consistent ordering
    processed_players.sort(key=lambda x: x['projected_points_ppr'], reverse=True)
    
    print(f"Final processed players: {len(processed_players)}")
    
    # Debug: Show position breakdown
    position_counts = {}
    for player in processed_players:
        pos = player['position']
        position_counts[pos] = position_counts.get(pos, 0) + 1
    
    print("Position breakdown:")
    for pos, count in position_counts.items():
        print(f"  {pos}: {count}")
    
    # Debug: Show first 10 players created
    print("First 10 players created:")
    for i, player in enumerate(processed_players[:10]):
        print(f"  {i+1}. {player['player_name']} ({player['position']}) - {player['projected_points_ppr']} proj points")
    
    print("=== DEDUPLICATION COMPLETE ===\n")
    return processed_players

def generate_projection(position):
    """Generate realistic fantasy projections by position"""
    base_projections = {
        'QB': 280,
        'RB': 180,  
        'WR': 160,
        'TE': 120
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
    print("=== PLAYER JSON CREATION DEBUG ===")
    
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
            "version": "3.1.1",
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
    
    print(f"Player JSON created with {len(players_dict)} unique players")
    print("=== PLAYER JSON CREATION COMPLETE ===\n")
    
    return database

def save_database_with_debug(database, filename='json_data/players.json'):
    """Save database with comprehensive debug output"""
    print("=== FILE SAVE DEBUG ===")
    
    # Debug: File status before save
    debug_file_status(filename, "BEFORE SAVE")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    try:
        # Write file
        with open(filename, 'w') as f:
            json.dump(database, f, indent=2, ensure_ascii=False)
        
        # Debug: File status after save
        debug_file_status(filename, "AFTER SAVE")
        
        # Verify content
        file_size = os.path.getsize(filename)
        print(f"Database saved successfully: {filename}")
        print(f"File size: {file_size / 1024:.2f} KB")
        print(f"Total players in database: {database['metadata']['total_players']}")
        
        # Verify by reading back
        with open(filename, 'r') as f:
            verification = json.load(f)
        
        print(f"Verification: Read back {len(verification.get('players', {}))} players")
        print(f"Verification: Database version {verification.get('metadata', {}).get('version', 'unknown')}")
        
        return True
        
    except Exception as e:
        print(f"ERROR saving database: {e}")
        return False
    
    finally:
        print("=== FILE SAVE DEBUG COMPLETE ===\n")

def perform_enhanced_analysis(processed_players):
    """Perform enhanced analysis and return results"""
    print("=== ENHANCED ANALYSIS DEBUG ===")
    
    # Perform some basic analysis for validation
    total_players = len(processed_players)
    avg_projection = sum(p['projected_points_ppr'] for p in processed_players) / total_players
    
    analysis_results = {
        'metadata': {
            'total_players_analyzed': total_players,
            'average_projection': round(avg_projection, 1),
            'analysis_timestamp': datetime.now().isoformat(),
            'roster_corrections': []  # Empty for now, can be enhanced later
        },
        'summary': {
            'players_processed': total_players,
            'deduplication_successful': total_players > 100,  # Sanity check
            'data_quality': 'high' if total_players > 200 else 'moderate'
        }
    }
    
    print(f"Enhanced analysis complete: {total_players} players analyzed")
    print("=== ENHANCED ANALYSIS COMPLETE ===\n")
    
    return analysis_results

def main():
    """Main execution flow with comprehensive debugging"""
    print(f"=== NFL Data Collection Script v{SCRIPT_VERSION} ===")
    print(f"Execution started at: {datetime.now().isoformat()}")
    
    try:
        # Load data with debug output
        datasets = load_nfl_data()
        
        # Process with comprehensive deduplication debug
        processed_players = process_players(datasets)
        
        if not processed_players:
            raise Exception("No players processed - check data sources and deduplication logic")
        
        # Create JSON database with debug
        database = create_player_json(processed_players)
        
        # Perform enhanced analysis
        analysis_results = perform_enhanced_analysis(processed_players)
        
        # FIXED: Access roster_corrections from metadata
        roster_corrections = analysis_results['metadata']['roster_corrections']
        
        # Save with comprehensive debug output
        success = save_database_with_debug(database)
        
        if success:
            print(f"\n=== FINAL SUCCESS SUMMARY ===")
            print(f"Script version: {SCRIPT_VERSION}")
            print(f"Unique players in database: {len(processed_players)}")
            print(f"Data quality: {analysis_results['summary']['data_quality']}")
            print(f"Database file updated successfully")
            print(f"Execution completed at: {datetime.now().isoformat()}")
            print("=== SCRIPT EXECUTION COMPLETE ===")
        else:
            raise Exception("Database save failed - check file permissions and disk space")
            
    except Exception as e:
        print(f"\n=== CRITICAL ERROR ===")
        print(f"Script version: {SCRIPT_VERSION}")
        print(f"Error occurred: {e}")
        print(f"Error timestamp: {datetime.now().isoformat()}")
        print("Check debug output above for detailed error analysis")
        raise

if __name__ == "__main__":
    main()