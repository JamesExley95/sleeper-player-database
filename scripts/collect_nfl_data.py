def perform_enhanced_analysis(players_dict):
    """Perform enhanced AI analysis with real ADP data and roster verification"""
    print("=== ENHANCED AI ANALYSIS ===")
    
    # Get real ADP data from Fantasy Football Calculator
    print("Fetching real ADP data...")
    real_adp_data = fetch_real_adp_data()
    
    if real_adp_data:
        print(f"Successfully fetched ADP for {len(real_adp_data)} players")
        
        # Update player ADP with real data
        updated_count = 0
        for player_id, player_data in players_dict.items():
            player_name = player_data['player_name']
            
            # Try to find this player in real ADP data
            for adp_player in real_adp_data:
                if similar_player_names(player_name, adp_player['name']):
                    player_data['adp_overall'] = adp_player['adp']
                    player_data['adp_position'] = adp_player.get('position_rank', player_data['adp_position'])
                    updated_count += 1
                    break
        
        print(f"Updated ADP for {updated_count} players with real data")
    else:
        print("Using projected ADP data (real ADP fetch failed)")
    
    # Perform AI analysis
    analysis_prompt = create_analysis_prompt(players_dict)
    
    try:
        analysis_results = {
            "metadata": {
                "analysis_type": "enhanced_weekly_insights",
                "total_players_analyzed": len(players_dict),
                "real_adp_players": len(real_adp_data) if real_adp_data else 0,
                "analysis_timestamp": datetime.now().isoformat(),
                "roster_corrections": []  # This is where roster corrections will be stored
            },
            "insights": {
                "must_starts": [],
                "sleeper_candidates": [],
                "bust_warnings": [],
                "injury_concerns": [],
                "trade_targets": []
            }
        }
        
        # Simplified analysis for debugging - categorize players by projected points
        qbs = [p for p in players_dict.values() if p['position'] == 'QB']
        rbs = [p for p in players_dict.values() if p['position'] == 'RB'] 
        wrs = [p for p in players_dict.values() if p['position'] == 'WR']
        tes = [p for p in players_dict.values() if p['position'] == 'TE']
        
        # Sort by projected points
        qbs.sort(key=lambda x: x['projected_points_ppr'], reverse=True)
        rbs.sort(key=lambda x: x['projected_points_ppr'], reverse=True)
        wrs.sort(key=lambda x: x['projected_points_ppr'], reverse=True)
        tes.sort(key=lambda x: x['projected_points_ppr'], reverse=True)
        
        # Must starts (top performers by position)
        must_starts = qbs[:5] + rbs[:12] + wrs[:15] + tes[:4]
        analysis_results["insights"]["must_starts"] = [
            {
                "player_name": p["player_name"],
                "position": p["position"], 
                "team": p["team"],
                "projected_points": p["projected_points_ppr"],
                "reasoning": f"Top {p['position']} with {p['projected_points_ppr']:.1f} projected points"
            }
            for p in must_starts
        ]
        
        # Sleeper candidates (middle tier with upside)
        sleeper_candidates = qbs[8:15] + rbs[20:35] + wrs[25:45] + tes[8:15]
        analysis_results["insights"]["sleeper_candidates"] = [
            {
                "player_name": p["player_name"],
                "position": p["position"],
                "team": p["team"], 
                "projected_points": p["projected_points_ppr"],
                "reasoning": f"Solid {p['position']} option with upside potential"
            }
            for p in sleeper_candidates
        ]
        
        print(f"Analysis complete:")
        print(f"  Must starts identified: {len(analysis_results['insights']['must_starts'])}")
        print(f"  Sleeper candidates: {len(analysis_results['insights']['sleeper_candidates'])}")
        
        # FIXED: Access roster_corrections from metadata
        roster_corrections = analysis_results['metadata']['roster_corrections']  # This is the correct path
        print(f"  Roster corrections: {len(roster_corrections)}")
        
        return analysis_results
        
    except Exception as e:
        print(f"ERROR in enhanced analysis: {e}")
        import traceback
        traceback.print_exc()
        
        # Return minimal analysis structure on error
        return {
            "metadata": {
                "analysis_type": "basic_fallback", 
                "total_players_analyzed": len(players_dict),
                "analysis_timestamp": datetime.now().isoformat(),
                "roster_corrections": [],
                "error": str(e)
            },
            "insights": {
                "must_starts": [],
                "sleeper_candidates": [],
                "bust_warnings": [],
                "injury_concerns": [],
                "trade_targets": []
            }
        }


def fetch_real_adp_data():
    """Fetch real ADP data from Fantasy Football Calculator"""
    try:
        url = "https://www.fantasyfootballcalculator.com/api/v1/adp/ppr?teams=12&year=2025"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if 'players' in data:
                return data['players']
        
        # Fallback URL or method
        return None
        
    except Exception as e:
        print(f"Failed to fetch real ADP data: {e}")
        return None


def similar_player_names(name1, name2):
    """Check if two player names are similar enough to be the same player"""
    # Simple similarity check - you could make this more sophisticated
    name1_clean = name1.lower().replace('.', '').replace(' ', '')
    name2_clean = name2.lower().replace('.', '').replace(' ', '')
    
    # Exact match
    if name1_clean == name2_clean:
        return True
    
    # Check if one name contains the other (handles nicknames, etc.)
    if name1_clean in name2_clean or name2_clean in name1_clean:
        return True
    
    # Check initials + last name matching
    parts1 = name1.split()
    parts2 = name2.split()
    if len(parts1) >= 2 and len(parts2) >= 2:
        if parts1[-1].lower() == parts2[-1].lower():  # Same last name
            if parts1[0][0].lower() == parts2[0][0].lower():  # Same first initial
                return True
    
    return False


def create_analysis_prompt(players_dict):
    """Create a prompt for AI analysis (placeholder for now)"""
    return f"Analyze {len(players_dict)} fantasy football players for weekly insights"
