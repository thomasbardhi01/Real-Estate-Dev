import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime
import json
import cloudscraper

# Failed properties to scrape
FAILED_PROPERTIES = ["3870", "7347", "11967", "679", "2296"]

# Create session
session = cloudscraper.create_scraper(browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False})
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
})

base_url = "https://braintree.patriotproperties.com"

def clean_text(text):
    if not text:
        return None
    cleaned = ' '.join(text.split())
    return cleaned if cleaned and cleaned != '--' else None

def establish_session():
    # Visit home and do a search
    session.get(f"{base_url}/default.asp", timeout=30)
    time.sleep(2)
    
    search_data = {
        "SearchLUC": "101",
        "SearchLUCDescription": "ONE FAM",
        "SearchSubmitted": "yes",
        "cmdGo": "Go",
    }
    session.post(f"{base_url}/SearchResults.asp", data=search_data, timeout=30)
    time.sleep(2)

def scrape_property(account_number):
    # Initialize empty property
    property_data = {
        'account_number': account_number,
        'scrape_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'location': None, 'parcel_id': None, 'old_parcel_id': None,
        'owner': None, 'owner_2': None, 'mailing_address': None,
        'city': None, 'state': None, 'zip': None, 'zoning': None,
        'is_llc': False, 'is_trust': False, 'sale_date': None,
        'sale_price': None, 'legal_reference': None, 'seller': None,
        'assessment_year': None, 'building_value': None,
        'xtra_features_value': None, 'land_value': None,
        'total_value': None, 'land_area': None, 'year_built': None,
        'style': None, 'bedrooms': None, 'bathrooms': None,
        'half_baths': None, 'three_quarter_baths': None,
        'total_rooms': None, 'units': None, 'exterior': None,
        'roof': None, 'property_class': None,
        'sales_history': [], 'historical_assessments': [],
    }
    
    # Load pages
    session.get(f"{base_url}/Summary.asp?AccountNumber={account_number}", timeout=30)
    time.sleep(2)
    
    response = session.get(f"{base_url}/summary-bottom.asp?AccountNumber={account_number}", timeout=30)
    if response.status_code != 200:
        return None
        
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract basic info
    top_table = soup.find('table', {'border': '1'})
    if top_table:
        location_cell = top_table.find('font', string=re.compile(r'Location'))
        if location_cell:
            location_value = location_cell.find_next('font', {'color': '#0000FF'})
            if location_value:
                property_data['location'] = clean_text(location_value.get_text())
        
        parcel_cell = top_table.find('font', string=re.compile(r'Parcel ID'))
        if parcel_cell:
            parcel_value = parcel_cell.find_next('font', {'color': '#0000FF'})
            if parcel_value:
                property_data['parcel_id'] = clean_text(parcel_value.get_text())
    
    # Extract mailing info
    mailing_header = soup.find('font', string=re.compile(r'Current Property Mailing Address'))
    if mailing_header:
        mailing_table = mailing_header.find_parent('td').find_next('table', {'border': '1'})
        if mailing_table:
            # Owner
            owner_cell = mailing_table.find('font', string=re.compile(r'^Owner$'))
            if owner_cell:
                owner_td = owner_cell.find_parent('td')
                if owner_td:
                    next_td = owner_td.find_next_sibling('td')
                    if next_td:
                        owner_fonts = next_td.find_all('font', {'color': '#0000FF'})
                        owner_values = [clean_text(f.get_text()) for f in owner_fonts if clean_text(f.get_text())]
                        if owner_values:
                            property_data['owner'] = owner_values[0]
                            full_owner = ' '.join(owner_values).upper()
                            property_data['is_llc'] = 'LLC' in full_owner
                            property_data['is_trust'] = 'TRUST' in full_owner or ' TR' in full_owner
            
            # Other fields
            for field_name, property_key in [
                ('Address', 'mailing_address'), ('City', 'city'),
                ('State', 'state'), ('Zip', 'zip'), ('Zoning', 'zoning')
            ]:
                cell = mailing_table.find('font', string=re.compile(f'^{field_name}$'))
                if cell:
                    td = cell.find_parent('td').find_next_sibling('td')
                    if td:
                        value_font = td.find('font', {'color': '#0000FF'})
                        if value_font:
                            property_data[property_key] = clean_text(value_font.get_text())
    
    # Extract sales info
    sales_header = soup.find('font', string=re.compile(r'Current Property Sales Information'))
    if sales_header:
        sales_table = sales_header.find_parent('td').find_next('table', {'border': '1'})
        if sales_table:
            # Sale date
            sale_date_cell = sales_table.find('font', string=re.compile(r'Sale Date'))
            if sale_date_cell:
                value_font = sale_date_cell.find_parent('td').find_next_sibling('td').find('font', {'color': '#0000FF'})
                if value_font:
                    property_data['sale_date'] = clean_text(value_font.get_text())
            
            # Seller
            seller_cell = sales_table.find('font', string=re.compile(r'Grantor.*Seller'))
            if seller_cell:
                value_font = seller_cell.find_parent('td').find_next_sibling('td').find('font', {'color': '#0000FF'})
                if value_font:
                    property_data['seller'] = clean_text(value_font.get_text())
            
            # Sale price
            sale_price_cell = sales_table.find('font', string=re.compile(r'^Sale\s*Price$'))
            if sale_price_cell:
                value_font = sale_price_cell.find_parent('td').find_next_sibling('td').find('font', {'color': '#0000FF'})
                if value_font:
                    price_text = clean_text(value_font.get_text())
                    try:
                        property_data['sale_price'] = int(price_text.replace(',', '').replace('$', ''))
                    except:
                        property_data['sale_price'] = price_text
    
    # Extract assessment info
    assessment_header = soup.find('font', string=re.compile(r'Current\s*Property Assessment'))
    if assessment_header:
        assessment_table = assessment_header.find_parent('td').find_next('table', {'border': '1'})
        if assessment_table:
            # Year
            year_cell = assessment_table.find('font', string=re.compile(r'^Year$'))
            if year_cell:
                value_font = year_cell.find_parent('td').find_next_sibling('td').find('font', {'color': '#0000FF'})
                if value_font:
                    property_data['assessment_year'] = clean_text(value_font.get_text())
            
            # Values
            for field_name, property_key in [
                ('Building\s*Value', 'building_value'),
                ('Xtra Features\s*Value', 'xtra_features_value'),
                ('Land\s*Value', 'land_value'),
                ('Total\s*Value', 'total_value')
            ]:
                cell = assessment_table.find('font', string=re.compile(field_name))
                if cell:
                    value_font = cell.find_parent('td').find_next_sibling('td').find('font', {'color': '#0000FF'})
                    if value_font:
                        value_text = clean_text(value_font.get_text())
                        try:
                            property_data[property_key] = int(value_text.replace(',', '').replace('$', ''))
                        except:
                            pass
            
            # Land Area
            land_area_cell = assessment_table.find('font', string=re.compile(r'Land\s*Area'))
            if land_area_cell:
                value_font = land_area_cell.find_parent('td').find_next_sibling('td').find('font', {'color': '#0000FF'})
                if value_font:
                    property_data['land_area'] = clean_text(value_font.get_text())
    
    # Extract narrative
    narrative_header = soup.find('font', string=re.compile(r'Narrative Description'))
    if narrative_header:
        narrative_table = narrative_header.find_parent('td').find_next('table', {'border': '1'})
        if narrative_table:
            narrative_text = narrative_table.get_text()
            
            patterns = {
                'property_class': (r'classified as\s*([^,]+?)(?:\s*with|\s*$)', 1),
                'style': (r'with a\(n\)\s*([^,]+?)\s*style', 1),
                'year_built': (r'built about\s*(\d{4})', 0),
                'exterior': (r'having\s*([^,]+?)\s*exterior', 1),
                'roof': (r'and\s*([^,]+?)\s*roof', 1),
                'units': (r'with\s*(\d+)\s*unit\(s\)', 0),
                'total_rooms': (r'(\d+)\s*total room\(s\)', 0),
                'bedrooms': (r'(\d+)\s*total bedroom\(s\)', 0),
                'bathrooms': (r'(\d+)\s*total bath\(s\)', 0),
                'half_baths': (r'(\d+)\s*total half bath\(s\)', 0),
                'three_quarter_baths': (r'(\d+)\s*total 3/4 bath\(s\)', 0),
            }
            
            for field, (pattern, value_type) in patterns.items():
                match = re.search(pattern, narrative_text, re.IGNORECASE)
                if match:
                    value = match.group(1).strip()
                    if value_type == 0:  # Integer
                        try:
                            property_data[field] = int(value)
                        except:
                            property_data[field] = value
                    else:  # String
                        property_data[field] = value
    
    return property_data if property_data['location'] else None

# Main execution
print("Quick retry scraper for 5 failed properties")
print("=" * 50)

# Load existing data
print("Loading existing data...")
with open('braintree_properties_final.json', 'r') as f:
    existing_data = json.load(f)
print(f"Loaded {len(existing_data)} existing properties")

existing_df = pd.read_csv('braintree_properties_final.csv')

# Establish session
print("\nEstablishing session...")
establish_session()

# Scrape failed properties
new_properties = []
for i, account_num in enumerate(FAILED_PROPERTIES):
    print(f"\nProcessing {account_num} ({i+1}/5)...")
    
    # Try up to 3 times
    for attempt in range(3):
        if attempt > 0:
            print(f"  Retry {attempt}...")
            establish_session()  # Re-establish session
            
        property_data = scrape_property(account_num)
        if property_data:
            print(f"  ✓ Success!")
            new_properties.append(property_data)
            break
        
        time.sleep(5)
    else:
        print(f"  ✗ Failed after 3 attempts")
    
    time.sleep(10)  # Wait between properties

# Append to JSON
if new_properties:
    print(f"\nAppending {len(new_properties)} properties to JSON...")
    existing_data.extend(new_properties)
    with open('braintree_properties_final.json', 'w') as f:
        json.dump(existing_data, f, indent=2)
    
    # Append to CSV
    print(f"Appending to CSV...")
    new_df = pd.DataFrame(new_properties)
    
    # Convert list fields to JSON strings for CSV
    if 'historical_assessments' in new_df.columns:
        new_df['historical_assessments'] = new_df['historical_assessments'].apply(
            lambda x: json.dumps(x) if x else ''
        )
    if 'sales_history' in new_df.columns:
        new_df['sales_history'] = new_df['sales_history'].apply(
            lambda x: json.dumps(x) if x else ''
        )
    
    # Append to existing CSV
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    combined_df.to_csv('braintree_properties_final.csv', index=False)
    
    print(f"\nTotal properties now: {len(combined_df)}")
    print(f"Successfully added: {[p['account_number'] for p in new_properties]}")
else:
    print("\nNo properties were successfully scraped")