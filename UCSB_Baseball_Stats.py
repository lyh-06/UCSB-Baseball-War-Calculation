"""
UCSB Baseball Data Scraper (2015-2025): In-State vs Out-of-State Player Analysis
Focused on extracting performance averages without visualizations
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import os
from tqdm import tqdm
import numpy as np

class UCSBBaseballScraper:
    def __init__(self, start_year = 2015, end_year = 2025):
        self.start_year = start_year
        self.end_year = end_year
        self.base_url = "https://ucsbgauchos.com/sports/baseball/stats"
        self.player_data = []
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        # Define the position adjustments for each position
        # The values here are hypothetical and should be adjusted based on actual data analysis
        self.position_adjustments = {
            'C': 12.5,      # Catcher
            '1B': -12.5,    # First Base
            '2B': 2.5,      # Second Base
            '3B': 2.5,      # Third Base
            'SS': 7.5,      # Shortstop
            'LF': -7.5,     # Left Field
            'CF': 2.5,      # Center Field
            'RF': -7.5,     # Right Field
            'DH': -17.5,    # Designated Hitter
            'P': 0,         # Pitcher (handled separately)
            'RP': 0,        # Relief Pitcher
            'SP': 0,        # Starting Pitcher
            'OF': -2.5,     # Generic Outfield (average of OF positions)
            'IF': 0,        # Generic Infield (not used in calculations)
            'UTL': 0,       # Utility (will use primary position)
            'UTIL': 0,      # Utility (will use primary position)
            'INF': 0,       # Infield (will use primary position)
            'OF/IF': 0,     # Two-way player (will use primary position)
            'Unknown': 0    # Default
        }
        
        # Create directory for data
        os.makedirs('ucsb_data', exist_ok = True)

    def get_page(self, url):
        try:
            time.sleep(0.5)  # Rate limiting
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()  # Raises an exception for HTTP errors
            return response.content
        except requests.exceptions.RequestException as e:
            print(f"Failed to retrieve data from {url}: {e}")
            return None

    def get_player_data(self, year):
        """Get player data for a specific year."""
        # Get player for specific year and extract player links
        roster_url = f"{self.base_url}/{year}"

        print(f"Fetching roster for {year} from {roster_url}")
        html_content = self.get_page(roster_url)
        if not html_content:
            return []
        
        # Now parse HTML with BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        player_rows = soup.find_all('tr', class_=lambda c: c and ('stat_meets_min' in c or 'stat_doesnt_meet_min' in c))

        players_data = []
        for row in player_rows:
            try:
                # Initialize induvidual data dictionary
                player_data = {
                    'year': year,
                    'position': 'Unknown',  # Default value
                    'is_in_state': False    # Default value for is_in_state
                }

                # Get jersey number
                jersey_cell = row.find('td', class_= 'text-center hide-on-medium-down')
                if jersey_cell:
                    player_data['jersey'] = jersey_cell.text.strip()
                
                # Get player name and ID
                name_cell = row.find('th', class_='text-no-wrap')
                if name_cell:
                    player_link = name_cell.find('a', class_='hide-on-medium-down')
                    if player_link:
                        player_data['name'] = player_link.text.strip()
                        player_data['id'] = player_link.get('data-player-id')

                        # Extract bio link for later state extraction
                        bio_link = row.select_one('a[href*="/roster/"]')
                        if bio_link:
                            player_data['bio_url'] = bio_link['href']  # Fixed: Use dictionary access instead of function call
                            if player_data['bio_url'] and player_data['bio_url'].startswith('/'):
                                player_data['bio_url'] = f"https://ucsbgauchos.com{player_data['bio_url']}"
                        
                        html_content = self.get_page(player_data['bio_url'])
            
                        soup1 = BeautifulSoup(html_content, 'html.parser')
                        
                        # Extract position
                        field_labels = soup1.find_all('span', class_='sidearm-roster-player-field-label')

                        for label in field_labels:
                            if label.text.strip() == 'Position':
                                position = label.find_next('span').text.strip()
                                player_data['position'] = self.normalize_position(position)
                                break

                # Collect performance metrics
                metrics = {
                    # Batting Stats
                    'AVG': {'data_label': 'AVG', 'key': 'batting_AVG'},
                    'OBP': {'data_label': 'OB%', 'key': 'batting_OBP'},
                    'SLG': {'data_label': 'SLG%', 'key': 'batting_SLG'},
                    'OPS': {'data_label': 'OPS', 'key': 'batting_OPS'},
                    'RBI': {'data_label': 'RBI', 'key': 'batting_RBI'},
                    'R': {'data_label': 'R', 'key': 'batting_R'},
                    'H': {'data_label': 'H', 'key': 'batting_H'},
                    '2B': {'data_label': '2B', 'key': 'batting_2B'},
                    '3B': {'data_label': '3B', 'key': 'batting_3B'},
                    'HR': {'data_label': 'HR', 'key': 'batting_HR'},
                    'BB': {'data_label': 'BB', 'key': 'batting_BB'},
                    'SO': {'data_label': 'SO', 'key': 'batting_SO'},
                    'AB': {'data_label': 'AB', 'key': 'batting_AB'},
                    'SB': {'data_label': 'SB', 'key': 'batting_SB'},
                    # Pitching Stats
                    'ERA': {'data_label': 'ERA', 'key': 'pitching_ERA'},
                    'WHIP': {'data_label': 'WHIP', 'key': 'pitching_WHIP'},
                    'W-L': {'data_label': 'W-L', 'key': 'pitching_WL'},
                    'APP-GS': {'data_label': 'APP-GS', 'key': 'pitching_APP_GS'},
                    'CG': {'data_label': 'CG', 'key': 'pitching_CG'},
                    'SHO': {'data_label': 'SHO', 'key': 'pitching_SHO'},
                    'SV': {'data_label': 'SV', 'key': 'pitching_SV'},
                    'IP': {'data_label': 'IP', 'key': 'pitching_IP'},
                    'H': {'data_label': 'H', 'key': 'pitching_H'},
                    'R': {'data_label': 'R', 'key': 'pitching_R'},
                    'ER': {'data_label': 'ER', 'key': 'pitching_ER'},
                    'BB': {'data_label': 'BB', 'key': 'pitching_BB'},
                    'SO': {'data_label': 'SO', 'key': 'pitching_SO'},
                    '2B': {'data_label': '2B', 'key': 'pitching_2B'},
                    '3B': {'data_label': '3B', 'key': 'pitching_3B'},
                    'HR': {'data_label': 'HR', 'key': 'pitching_HR'},
                    'B/AVG': {'data_label': 'B/AVG', 'key': 'pitching_BAVG'},
                    'WP': {'data_label': 'WP', 'key': 'pitching_WP'},
                    'HBP': {'data_label': 'HBP', 'key': 'pitching_HBP'},
                    'BK': {'data_label': 'BK', 'key': 'pitching_BK'},
                    'SFA': {'data_label': 'SFA', 'key': 'pitching_SFA'},
                    'SHA': {'data_label': 'SHA', 'key': 'pitching_SHA'}
                }

                # Extract each metric from table
                for metric_key, metric_info in metrics.items():
                    cell = row.find('td', attrs={'data-label': metric_info['data_label']})
                    if cell:
                        value = cell.text.strip()

                        # Special handling for SB which might be in format "5-6"
                        if metric_key == 'SB' and '-' in value:
                            sb_parts = value.split('-')
                            if len(sb_parts) == 2:
                                try:
                                    player_data['batting_SB_successful'] = int(sb_parts[0])
                                    player_data['batting_SB_attempted'] = int(sb_parts[1])
                                    player_data[metric_info['key']] = value
                                except ValueError:
                                    player_data[metric_info['key']] = value
                        else:
                            # Try to convert to appropriate type
                            if metric_key in ['AVG', 'OBP', 'SLG', 'OPS', 'ERA', 'WHIP', 'B/AVG']:
                                try:
                                    player_data[metric_info['key']] = float(value)
                                except ValueError:
                                    player_data[metric_info['key']] = value
                            elif metric_key in ['RBI', 'R', 'H', '2B', '3B', 'HR', 'BB', 'SO', 'AB','IP', 'HBP', 'WP', 'BK', 'SFA', 'SHA', 'SV']:
                                try:
                                    player_data[metric_info['key']] = int(value)
                                except ValueError:
                                    player_data[metric_info['key']] = value
                            else:
                                player_data[metric_info['key']] = value

                players_data.append(player_data)
            except Exception as e:
                print(f"Error processing player data: {e}")
                continue
        
        return players_data
    
    def get_player_hometown(self, player_data):
        """Extract hometown/state info from player bio page"""
        if not player_data.get('bio_url'):
            # If no bio URL, default to California player
            player_data['hometown'] = 'Unknown'
            player_data['state'] = 'California'  # Default to California
            player_data['is_in_state'] = True
            return player_data
        
        time.sleep(0.5)
        html_content = self.get_page(player_data['bio_url'])
        if not html_content:
            # If bio page can't be fetched, default to California player
            player_data['hometown'] = 'Unknown'
            player_data['state'] = 'California'  # Default to California
            player_data['is_in_state'] = True
            return player_data
        
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract hometown from bio page
        
        bio_text = soup.get_text()
        if bio_text:
            hometown, state = self.extract_hometown_state(soup)
            player_data['hometown'] = hometown if hometown else 'Unknown'
            player_data['state'] = state if state else 'California'  # Default to California if state not found

        # Extract position
        if player_data['position'] == 'Unknown':
            position_elem = soup.select_one('.sidearm-roster-player-position')
            if position_elem:
                player_data['position'] = self.normalize_position(position_elem.text.strip())

        # Determine if player is in state or not
        if player_data.get('state'):
            player_data['is_in_state'] = self.is_california_state(player_data['state'])
        else:
            player_data['is_in_state'] = True  # Default to California player if state not found
            player_data['state'] = 'California'

        return player_data
    
    def extract_hometown_state(self, soup):
        # Fallback to HTML element
        field_labels = soup.find_all('span', class_='sidearm-roster-player-field-label')

        for label in field_labels:
            if label.text.strip() == 'Hometown':
                location = label.find_next('span').text.strip()
                if ',' in location:
                    return tuple(part.strip() for part in location.split(',', 1))
        
        return None, None
    
    def is_california_state(self, state):
        """Check if the state is California"""
        california_states = ['CA', 'California', 'Calif.', 'Calif', 'Calf.', 'Calf']
        return state.strip() in california_states
    
    def normalize_position(self, position_str):
        """Normalize position string to standard format"""

        if not position_str:
            return 'Unknown'
        
        position_str = position_str.strip().upper()

        # If player has multiple positions, take the first one as primary
        if '/' in position_str:
            position_str = position_str.split('/')[0].strip()

        if 'C' == position_str:
            return 'C'
        elif 'CATCHER' in position_str:
            return 'C'
        elif '1B' in position_str:
            return '1B'
        elif '2B' in position_str:
            return '2B'
        elif '3B' in position_str:
            return '3B'
        elif 'SS' in position_str:
            return 'SS'
        elif 'LF' in position_str:
            return 'LF'
        elif 'CF' in position_str:
            return 'CF'
        elif 'RF' in position_str:
            return 'RF'
        elif 'OF' in position_str:
            return 'OF'
        elif 'OUTFIELDER' in position_str:
            return 'OF'
        elif 'OUFIELD' in position_str:
            return 'OF'
        elif 'INF' in position_str:
            return 'IF'
        elif 'DH' in position_str:
            return 'DH'
        elif 'P' == position_str:
            return 'P'
        elif 'PITCHER' in position_str:
            return 'P'
        elif 'RHP' in position_str or 'LHP' in position_str:
            return 'P'
        elif 'SP' in position_str:
            return 'SP'
        elif 'RP' in position_str:
            return 'RP'
        elif 'UTIL' in position_str:
            return 'UTIL'
        elif 'UT' in position_str:
            return 'UTIL'
        else:
            return 'Unknown'
    
    def calculate_war(self, player_data):
        """Calculate WAR value for a player"""

        # Constants for WAR calculation
        runs_per_win = 10.0
        replacement_level = 0.0
        league_avg_woba = 0.320
        woba_scale = 1.2

        position = player_data.get('position')
        war_value = 0.0

        # Adjust for position
        pos_adjustment = self.position_adjustments.get(position, 0)

        # Pitcher calculation
        if position in ['P', 'SP', 'RP']:
            # Calculate pitcher WAR if we have ERA and IP
            era = player_data.get('pitching_ERA', None)
            ip = player_data.get('pitching_IP', 0)

            if isinstance(ip, str):
                try:
                    ip = float(ip)
                except ValueError:
                    ip = 0

            if era is not None and ip > 0:
                # Simplified WAR based on ERA
                league_avg_era = 4.0  # Approximate NCAA average
                era_difference = league_avg_era - era
                war_value = era_difference * (ip / 9) / runs_per_win
                
                # Apply leverage multiplier for relievers
                if position == 'RP' and player_data.get('pitching_SV'):
                    war_value *= 1.5  # Closers get a boost  
        else:
            # Position player calculation
            # Calculate if we have OBP, SLG, and AB
            obp = player_data.get('batting_OBP')
            slg = player_data.get('batting_SLG')
            ab = player_data.get('batting_AB')

            if obp is not None and slg is not None and ab is not None:
                # Calculate appox wOBA
                woba = obp * 1.75 + slg * 0.7

                # Calculate plate appearances
                bb = player_data.get('batting_BB', 0)
                hbp = player_data.get('batting_HBP', 0)
                pa = ab + bb + hbp

                # Offensive runs above average
                offensive_runs = (woba - league_avg_woba) * woba_scale * pa / 100

                # Apply position adjustment
                position_runs = pos_adjustment * pa / 600

                # Calculate WAR
                war_value = (offensive_runs + position_runs) / runs_per_win + replacement_level

                # Adjustments for certains positions
                if position == 'C':
                    # Catchers get a bonus for framing/game management
                    war_value += 0.5
                elif position in ['SS', '2B']:
                    # Middle infielders get a slight boost
                    war_value += 0.3
            
        return war_value
        
    def process_all_years(self):
        """Process data for all years in the specfied range"""
        all_players = []

        for year in range(self.start_year, self.end_year + 1):
            players = self.get_player_data(year)

            for player in tqdm(players, desc=f"Processing players from {year}"):
                player = self.get_player_hometown(player)

                # Calculate WAR for each player
                player['WAR'] = self.calculate_war(player)

                all_players.append(player)
        
        df = pd.DataFrame(all_players)
        
        # Ensure 'is_in_state' column exists and is a boolean
        if 'is_in_state' not in df.columns:
            df['is_in_state'] = True  # Default: assume all players are in-state
        
        # Save data to CSV
        df.to_csv('final_ucsb_player_metrics.csv', index=False)
        print("Data saved to final_ucsb_player_metrics.csv")
        return df
    
    def analyze_war_by_position(self, data=None):
        """Analyze WAR differences between in-state and out-of-state players by position"""
        if data is None:
            if os.path.exists('final_ucsb_player_metrics.csv'):
                data = pd.read_csv('final_ucsb_player_metrics.csv')
            else:
                data = self.process_all_years()
        
        # Ensure 'is_in_state' column exists
        if 'is_in_state' not in data.columns:
            print("Warning: 'is_in_state' column missing. Setting all players to in-state by default.")
            data['is_in_state'] = True
        
        # Filter out players without WAR or position
        data = data[~data['WAR'].isna() & ~data['position'].isna()]

        # Group by position and in-state/OOS
        grouped = data.groupby(['position', 'is_in_state'])
        position_stats = grouped['WAR'].agg(['mean', 'std', 'count']).reset_index()

        # Pivot to get in-state and OOS in separate columns
        pivot_table = pd.pivot_table(position_stats, values=['mean', 'std', 'count'], index='position', columns='is_in_state')

        # Create summary DataFrame
        result_df = pd.DataFrame({
            'Position': pivot_table.index,
            'In-State WAR': pivot_table[('mean', True)].values,
            'In-State Count': pivot_table[('count', True)].values,
            'Out-of-State WAR': pivot_table[('mean', False)].values if ('mean', False) in pivot_table else [0] * len(pivot_table.index),
            'Out-of-State Count': pivot_table[('count', False)].values if ('count', False) in pivot_table else [0] * len(pivot_table.index)
        })

        # Calculate difference and percentage difference (fixed syntax error)
        result_df['WAR Difference'] = result_df['In-State WAR'] - result_df['Out-of-State WAR']
        result_df['%Difference'] = 100 * result_df['WAR Difference'] / result_df['Out-of-State WAR'].replace(0, np.nan)

        # Sort by absolute differences
        result_df = result_df.sort_values(by='WAR Difference', ascending=False)

        # Save to CSV
        result_df.to_csv('final_ucsb_war_analysis.csv', index=False)
        print("WAR analysis saved to final_ucsb_war_analysis.csv")

        return result_df
                  
def main():
    scraper = UCSBBaseballScraper(start_year=2015, end_year=2025)

    # Process all years and save data
    player_data = scraper.process_all_years()
    print("Player data processing complete")
    print(player_data.head())
    
    # Analyze WAR by position
    war_analysis = scraper.analyze_war_by_position(player_data)
    print("WAR analysis complete")
    print(war_analysis.head())

if __name__ == "__main__":
    main()