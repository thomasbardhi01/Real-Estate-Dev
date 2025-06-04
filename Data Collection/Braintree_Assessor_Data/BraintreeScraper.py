import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re
from datetime import datetime
import json
import random
import os
from threading import Lock
import logging
from typing import Dict, List, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import cloudscraper

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class SlowBraintreePropertyScraper:
    def __init__(self):
        """Initialize the scraper with VERY conservative settings"""
        self.base_url = "https://braintree.patriotproperties.com"
        self.properties = []
        self.failed_properties = {}
        self.session = None
        self.request_count = 0
        self.last_request_time = 0
        
        # Session management
        self.current_search_page = 1
        self.luc_code = None
        self.luc_desc = None
        self.session_valid = False
        
        # Create a single session that we'll reuse
        self._create_session()
        
        # Statistics
        self.stats = {
            'total_attempted': 0,
            'successful': 0,
            'http_errors': 0,
            'empty_responses': 0,
            'extraction_errors': 0,
            'session_resets': 0
        }
        
        # Create debug directory
        os.makedirs('debug_failed_properties', exist_ok=True)
    
    def _create_session(self):
        """Create a single session with cookies and proper headers"""
        self.session = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        
        # Randomize user agent
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        
        self.session.headers.update({
            'User-Agent': random.choice(user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Upgrade-Insecure-Requests': '1',
        })
        
        # Add retry strategy
        adapter = HTTPAdapter(
            pool_connections=1,
            pool_maxsize=1,
            max_retries=Retry(
                total=3,
                backoff_factor=2,
                status_forcelist=[500, 502, 503, 504]
            )
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Mark session as invalid until we establish it
        self.session_valid = False
    
    def _wait_between_requests(self):
        """Implement smart waiting between requests"""
        self.request_count += 1
        
        # Base delay
        base_delay = random.uniform(5, 8)
        
        # Add extra delay every 10 requests
        if self.request_count % 10 == 0:
            extra_delay = random.uniform(20, 30)
            logger.info(f"Taking a longer break ({extra_delay:.1f}s) after {self.request_count} requests...")
            time.sleep(extra_delay)
        
        # Add even longer delay every 50 requests
        if self.request_count % 50 == 0:
            long_delay = random.uniform(60, 90)
            logger.info(f"Taking a long break ({long_delay:.1f}s) after {self.request_count} requests...")
            time.sleep(long_delay)
            # Force session refresh after long break
            self.session_valid = False
        
        time.sleep(base_delay)
    
    def _establish_session(self):
        """Establish a session by visiting the home page and doing a search"""
        logger.info("Establishing new session...")
        try:
            # First visit home page
            response = self.session.get(f"{self.base_url}/default.asp", timeout=30)
            if response.status_code != 200:
                logger.error(f"Failed to load home page: HTTP {response.status_code}")
                return False
            
            time.sleep(3)
            
            # Now do a search to establish the session
            if self.luc_code and self.luc_desc:
                logger.info(f"Performing search to establish session (LUC: {self.luc_code})...")
                search_url = f"{self.base_url}/SearchResults.asp"
                search_data = {
                    "SearchLUC": self.luc_code,
                    "SearchLUCDescription": self.luc_desc,
                    "SearchSubmitted": "yes",
                    "cmdGo": "Go",
                }
                
                response = self.session.post(search_url, data=search_data, timeout=30)
                if response.status_code == 200:
                    logger.info("Session established successfully")
                    self.session_valid = True
                    self.stats['session_resets'] += 1
                    time.sleep(3)
                    return True
                else:
                    logger.error(f"Failed to establish session via search: HTTP {response.status_code}")
                    return False
            else:
                # If no search params, just mark session as potentially valid
                self.session_valid = True
                return True
                
        except Exception as e:
            logger.error(f"Error establishing session: {e}")
            return False
    
    def _is_session_expired(self, html_content):
        """Check if the response indicates session has expired"""
        if not html_content:
            return True
        
        # Check for session timeout message
        if "Either no search has been executed or your session has timed out" in html_content:
            return True
        
        # Check for too-short responses that might indicate an error page
        if len(html_content) < 1000:
            return True
            
        return False
    
    def _refresh_session_with_search(self):
        """Refresh the session by redoing the search"""
        logger.info("Session expired - refreshing...")
        
        # Create new session
        self._create_session()
        
        # Re-establish with search
        if self._establish_session():
            logger.info("Session refreshed successfully")
            return True
        else:
            logger.error("Failed to refresh session")
            return False
    
    def load_previous_results(self, filename='braintree_properties.json'):
        """Load previously scraped properties"""
        try:
            with open(filename, 'r') as f:
                self.properties = json.load(f)
                logger.info(f"Loaded {len(self.properties)} previous properties")
        except FileNotFoundError:
            logger.info("No previous results found, starting fresh")
        except Exception as e:
            logger.error(f"Error loading previous results: {e}")
    
    def search_properties(self, luc_code="101", start_page=1, end_page=None, 
                         luc_desc="ONE FAM", resume_from_property=None):
        """Main search function with resume capability"""
        
        # Store search params for session management
        self.luc_code = luc_code
        self.luc_desc = luc_desc
        
        # Establish initial session
        if not self._establish_session():
            logger.error("Could not establish initial session")
            return []
        
        # Get all property links first
        if resume_from_property:
            # Load property links from file if resuming
            try:
                with open('property_links.json', 'r') as f:
                    all_property_links = json.load(f)
                logger.info(f"Loaded {len(all_property_links)} property links from file")
            except:
                logger.error("Could not load property links. Please run without resume first.")
                return []
        else:
            # Collect all property links
            all_property_links = self._collect_all_property_links(luc_code, luc_desc, start_page, end_page)
            
            # Save property links for potential resume
            with open('property_links.json', 'w') as f:
                json.dump(all_property_links, f)
            logger.info(f"Saved {len(all_property_links)} property links to file")
        
        # Process properties
        self._process_properties_slowly(all_property_links, resume_from_property)
        
        return self.properties
    
    def _collect_all_property_links(self, luc_code, luc_desc, start_page, end_page):
        """Collect all property links from search pages"""
        logger.info("Collecting property links from search pages...")
        
        # Get total pages if not specified
        if end_page is None:
            end_page = self._get_total_pages(luc_code, luc_desc)
            logger.info(f"Will process {end_page} pages")
        
        all_links = []
        
        for page in range(start_page, end_page + 1):
            logger.info(f"Processing search page {page}/{end_page}")
            self.current_search_page = page
            
            try:
                links = self._process_search_page(page, luc_code, luc_desc)
                if links:
                    all_links.extend(links)
                    logger.info(f"  Found {len(links)} properties on page {page}")
                else:
                    logger.warning(f"  No properties found on page {page}")
                
                self._wait_between_requests()
                
            except Exception as e:
                logger.error(f"Error on page {page}: {e}")
                # Continue with next page
        
        # Remove duplicates
        unique_links = list(set(all_links))
        logger.info(f"Total unique properties found: {len(unique_links)}")
        
        return unique_links
    
    def _process_search_page(self, page, luc_code, luc_desc):
        """Process a single search results page"""
        if page == 1:
            # First page requires POST
            search_url = f"{self.base_url}/SearchResults.asp"
            search_data = {
                "SearchLUC": luc_code,
                "SearchLUCDescription": luc_desc,
                "SearchSubmitted": "yes",
                "cmdGo": "Go",
            }
            
            try:
                response = self.session.post(search_url, data=search_data, timeout=30)
            except Exception as e:
                logger.error(f"Error posting search: {e}")
                return []
        else:
            # Other pages use GET
            search_url = f"{self.base_url}/SearchResults.asp?page={page}"
            
            try:
                response = self.session.get(search_url, timeout=30)
            except Exception as e:
                logger.error(f"Error getting page {page}: {e}")
                return []
        
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            return self._extract_property_links(soup)
        else:
            logger.error(f"Page {page} returned status {response.status_code}")
            return []
    
    def _extract_property_links(self, soup):
        """Extract property links from search results"""
        links = []
        
        property_links = soup.find_all('a', {'target': '_top'})
        
        for link in property_links:
            href = link.get('href', '')
            if 'Summary.asp?AccountNumber=' in href:
                match = re.search(r'AccountNumber=(\d+)', href)
                if match:
                    account_num = match.group(1)
                    links.append(account_num)
        
        return links
    
    def _get_total_pages(self, luc_code, luc_desc):
        """Get total number of pages"""
        search_url = f"{self.base_url}/SearchResults.asp"
        search_data = {
            "SearchLUC": luc_code,
            "SearchLUCDescription": luc_desc,
            "SearchSubmitted": "yes",
            "cmdGo": "Go",
        }
        
        try:
            response = self.session.post(search_url, data=search_data, timeout=30)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            page_text = soup.get_text()
            match = re.search(r'Print page \d+ of (\d+)', page_text)
            if match:
                return int(match.group(1))
            
            # Default
            return 183
            
        except Exception as e:
            logger.error(f"Error getting total pages: {e}")
            return 183
    
    def _process_properties_slowly(self, account_numbers, resume_from=None):
        """Process properties one by one with long delays"""
        
        # Get already processed accounts
        existing_accounts = {p['account_number'] for p in self.properties}
        
        # Filter out already processed
        if resume_from:
            try:
                start_idx = account_numbers.index(resume_from)
                account_numbers = account_numbers[start_idx:]
                logger.info(f"Resuming from property {resume_from} (index {start_idx})")
            except ValueError:
                logger.warning(f"Resume property {resume_from} not found, starting from beginning")
        
        account_numbers = [acc for acc in account_numbers if acc not in existing_accounts]
        
        if not account_numbers:
            logger.info("All properties already scraped")
            return
        
        total = len(account_numbers)
        logger.info(f"Processing {total} properties...")
        
        consecutive_failures = 0
        max_consecutive_failures = 5
        
        for idx, account_number in enumerate(account_numbers):
            logger.info(f"\n[{idx+1}/{total}] Processing property {account_number}")
            
            try:
                property_data = self._scrape_property_details_with_retry(account_number)
                
                if property_data:
                    self.properties.append(property_data)
                    self.stats['successful'] += 1
                    consecutive_failures = 0  # Reset failure counter
                    logger.info(f"  ✓ Successfully scraped property {account_number}")
                    
                    # Save progress every 10 properties
                    if len(self.properties) % 10 == 0:
                        self._save_progress()
                else:
                    consecutive_failures += 1
                    logger.warning(f"  ✗ Failed to scrape property {account_number}")
                    
                    # If too many consecutive failures, try refreshing session
                    if consecutive_failures >= max_consecutive_failures:
                        logger.warning(f"Too many consecutive failures ({consecutive_failures}), refreshing session...")
                        if self._refresh_session_with_search():
                            consecutive_failures = 0
                        else:
                            logger.error("Failed to refresh session, waiting before retry...")
                            time.sleep(60)
                
            except Exception as e:
                logger.error(f"  ✗ Exception scraping {account_number}: {e}")
                self.failed_properties[account_number] = str(e)
                consecutive_failures += 1
            
            # Always wait between properties
            self._wait_between_requests()
            
            # Print progress summary every 25 properties
            if (idx + 1) % 25 == 0:
                self._print_progress_summary()
    
    def _scrape_property_details_with_retry(self, account_number, max_retries=3):
        """Scrape property details with session retry logic"""
        for attempt in range(max_retries):
            result = self._scrape_property_details(account_number)
            
            # If we got a valid result, return it
            if result and result.get('location'):  # Check if we got actual data
                return result
            
            # Check if it was a session issue
            if self.stats['empty_responses'] > self.stats['successful'] and attempt < max_retries - 1:
                logger.info(f"Attempt {attempt + 1} failed, refreshing session...")
                if self._refresh_session_with_search():
                    time.sleep(5)
                    continue
                else:
                    break
        
        return None
    
    def _scrape_property_details(self, account_number):
        """Scrape individual property details"""
        self.stats['total_attempted'] += 1
        
        property_data = {
            'account_number': account_number,
            'scrape_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'location': None,
            'parcel_id': None,
            'old_parcel_id': None,
            'owner': None,
            'owner_2': None,
            'mailing_address': None,
            'city': None,
            'state': None,
            'zip': None,
            'zoning': None,
            'is_llc': False,
            'is_trust': False,
            'sale_date': None,
            'sale_price': None,
            'legal_reference': None,
            'seller': None,
            'assessment_year': None,
            'building_value': None,
            'xtra_features_value': None,
            'land_value': None,
            'total_value': None,
            'land_area': None,
            'year_built': None,
            'style': None,
            'bedrooms': None,
            'bathrooms': None,
            'half_baths': None,
            'three_quarter_baths': None,
            'total_rooms': None,
            'units': None,
            'exterior': None,
            'roof': None,
            'property_class': None,
            'sales_history': [],
            'historical_assessments': [],
        }
        
        try:
            # Load main page first
            main_url = f"{self.base_url}/Summary.asp?AccountNumber={account_number}"
            main_response = self.session.get(main_url, timeout=30)
            
            if main_response.status_code != 200:
                self.stats['http_errors'] += 1
                self.failed_properties[account_number] = f"Main page HTTP {main_response.status_code}"
                return None
            
            # Small delay before loading bottom frame
            time.sleep(2)
            
            # Load bottom frame
            bottom_url = f"{self.base_url}/summary-bottom.asp?AccountNumber={account_number}"
            response = self.session.get(bottom_url, timeout=30)
            
            if response.status_code == 200:
                # Check for session expiry
                if self._is_session_expired(response.text):
                    self.stats['empty_responses'] += 1
                    self.failed_properties[account_number] = "Session expired"
                    self._save_debug_html(account_number, response.text, "session_expired")
                    logger.warning(f"Session expired for property {account_number}")
                    
                    # Log first 200 chars of response for debugging
                    logger.debug(f"Response preview: {response.text[:200]}")
                    
                    return None
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract data
                self._extract_property_data_fixed(soup, property_data)
                
                # Small delay before getting history
                time.sleep(2)
                
                # Get sales history
                try:
                    property_data['sales_history'] = self._scrape_sales_history(account_number)
                except:
                    pass
                
                time.sleep(2)
                
                # Get historical assessments
                try:
                    property_data['historical_assessments'] = self._scrape_historical_assessments(account_number)
                except:
                    pass
                
                return property_data
                
            else:
                self.stats['http_errors'] += 1
                self.failed_properties[account_number] = f"Bottom frame HTTP {response.status_code}"
                return None
                
        except Exception as e:
            self.stats['extraction_errors'] += 1
            self.failed_properties[account_number] = f"Exception: {str(e)}"
            logger.error(f"Error details: {e}")
            return None
    
    def _save_debug_html(self, account_number, html_content, error_type):
        """Save HTML of failed properties"""
        filename = f"debug_failed_properties/{account_number}_{error_type}.html"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(html_content)
            # Also log first 300 chars for immediate debugging
            logger.debug(f"HTML preview for {account_number}: {html_content[:300]}")
        except:
            pass
    
    def _clean_text(self, text):
        """Clean text"""
        if not text:
            return None
        cleaned = ' '.join(text.split())
        return cleaned if cleaned and cleaned != '--' else None
    
    def _extract_property_data_fixed(self, soup, property_data):
        """Extract property data - same as before"""
        # 1. Extract top table information
        top_table = soup.find('table', {'border': '1'})
        if top_table:
            location_cell = top_table.find('font', string=re.compile(r'Location'))
            if location_cell:
                location_value = location_cell.find_next('font', {'color': '#0000FF'})
                if location_value:
                    property_data['location'] = self._clean_text(location_value.get_text())
            
            parcel_cell = top_table.find('font', string=re.compile(r'Parcel ID'))
            if parcel_cell:
                parcel_value = parcel_cell.find_next('font', {'color': '#0000FF'})
                if parcel_value:
                    property_data['parcel_id'] = self._clean_text(parcel_value.get_text())
        
        # 2. Extract Old Parcel ID
        old_parcel_text = soup.find('font', string=re.compile(r'Old Parcel ID'))
        if old_parcel_text:
            old_parcel_value = old_parcel_text.find_next('font', {'color': '#0000FF'})
            if old_parcel_value:
                value = self._clean_text(old_parcel_value.get_text())
                if value and value != '--':
                    property_data['old_parcel_id'] = value
        
        # 3. Extract Mailing Address Section
        mailing_header = soup.find('font', string=re.compile(r'Current Property Mailing Address'))
        if mailing_header:
            mailing_table = mailing_header.find_parent('td').find_next('table', {'border': '1'})
            if mailing_table:
                # Owner
                owner_cell = mailing_table.find('font', string=re.compile(r'^Owner$'))
                if owner_cell:
                    owner_td = owner_cell.find_parent('td')
                    if owner_td:
                        owner_values = []
                        next_td = owner_td.find_next_sibling('td')
                        if next_td:
                            owner_fonts = next_td.find_all('font', {'color': '#0000FF'})
                            for font in owner_fonts:
                                text = self._clean_text(font.get_text())
                                if text:
                                    owner_values.append(text)
                        
                        if owner_values:
                            property_data['owner'] = owner_values[0]
                            if len(owner_values) > 1:
                                property_data['owner_2'] = ' '.join(owner_values[1:])
                            
                            full_owner = ' '.join(owner_values).upper()
                            property_data['is_llc'] = 'LLC' in full_owner
                            property_data['is_trust'] = 'TRUST' in full_owner or ' TR' in full_owner or 'TRS' in full_owner
                
                # Other fields
                for field_name, property_key in [
                    ('Address', 'mailing_address'),
                    ('City', 'city'),
                    ('State', 'state'),
                    ('Zip', 'zip'),
                    ('Zoning', 'zoning')
                ]:
                    cell = mailing_table.find('font', string=re.compile(f'^{field_name}$'))
                    if cell:
                        value_font = cell.find_parent('td').find_next_sibling('td').find('font', {'color': '#0000FF'})
                        if value_font:
                            property_data[property_key] = self._clean_text(value_font.get_text())
        
        # 4. Extract Sales Information
        sales_header = soup.find('font', string=re.compile(r'Current Property Sales Information'))
        if sales_header:
            sales_table = sales_header.find_parent('td').find_next('table', {'border': '1'})
            if sales_table:
                for field_name, property_key in [
                    ('Sale Date', 'sale_date'),
                    ('Legal.*Reference', 'legal_reference'),
                    ('Grantor.*Seller', 'seller')
                ]:
                    cell = sales_table.find('font', string=re.compile(field_name))
                    if cell:
                        value_font = cell.find_parent('td').find_next_sibling('td').find('font', {'color': '#0000FF'})
                        if value_font:
                            property_data[property_key] = self._clean_text(value_font.get_text())
                
                # Sale price
                sale_price_cell = sales_table.find('font', string=re.compile(r'^Sale\s*Price$'))
                if sale_price_cell:
                    sale_price_value = sale_price_cell.find_parent('td').find_next_sibling('td').find('font', {'color': '#0000FF'})
                    if sale_price_value:
                        price_text = self._clean_text(sale_price_value.get_text())
                        try:
                            property_data['sale_price'] = int(price_text.replace(',', '').replace('$', ''))
                        except:
                            property_data['sale_price'] = price_text
        
        # 5. Extract Assessment Information
        assessment_header = soup.find('font', string=re.compile(r'Current\s*Property Assessment'))
        if assessment_header:
            assessment_table = assessment_header.find_parent('td').find_next('table', {'border': '1'})
            if assessment_table:
                # Year
                year_cell = assessment_table.find('font', string=re.compile(r'^Year$'))
                if year_cell:
                    year_value = year_cell.find_parent('td').find_next_sibling('td').find('font', {'color': '#0000FF'})
                    if year_value:
                        property_data['assessment_year'] = self._clean_text(year_value.get_text())
                
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
                            value_text = self._clean_text(value_font.get_text())
                            try:
                                property_data[property_key] = int(value_text.replace(',', '').replace('$', ''))
                            except:
                                pass
                
                # Land Area
                land_area_cell = assessment_table.find('font', string=re.compile(r'Land\s*Area'))
                if land_area_cell:
                    land_area_value = land_area_cell.find_parent('td').find_next_sibling('td').find('font', {'color': '#0000FF'})
                    if land_area_value:
                        property_data['land_area'] = self._clean_text(land_area_value.get_text())
        
        # 6. Extract Narrative Description
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
    
    def _scrape_sales_history(self, account_number):
        """Scrape sales history"""
        url = f"{self.base_url}/g_sales.asp?AccountNumber={account_number}"
        history = []
        
        try:
            response = self.session.get(url, timeout=15)
            if response.status_code == 200 and not self._is_session_expired(response.text):
                soup = BeautifulSoup(response.text, 'html.parser')
                table = soup.find('table')
                
                if table:
                    rows = table.find_all('tr')[1:]
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 4:
                            history.append({
                                'sale_date': cells[0].get_text(strip=True),
                                'sale_price': cells[1].get_text(strip=True),
                                'legal_reference': cells[2].get_text(strip=True),
                                'grantor': cells[3].get_text(strip=True)
                            })
        except:
            pass
        
        return history
    
    def _scrape_historical_assessments(self, account_number):
        """Scrape historical assessments"""
        # Visit property page first
        self.session.get(f"{self.base_url}/Summary.asp?AccountNumber={account_number}", timeout=10)
        time.sleep(1)
        
        url = f"{self.base_url}/g_previous.asp"
        
        try:
            response = self.session.get(url, timeout=15)
            if response.status_code == 200 and not self._is_session_expired(response.text):
                soup = BeautifulSoup(response.text, 'html.parser')
                historical_data = []
                
                table = soup.find('table')
                if table:
                    rows = table.find_all('tr')[1:]
                    for row in rows:
                        cells = row.find_all('td')
                        if len(cells) >= 8:
                            year_text = cells[0].get_text(strip=True)
                            total_text = cells[7].get_text(strip=True)
                            
                            if year_text and total_text and year_text.isdigit():
                                try:
                                    historical_data.append({
                                        'year': int(year_text),
                                        'total_value': int(total_text.replace(',', ''))
                                    })
                                except:
                                    continue
                
                return historical_data
        except:
            pass
        
        return []
    
    def _save_progress(self):
        """Save current progress"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save JSON
        with open(f'braintree_properties_progress_{timestamp}.json', 'w') as f:
            json.dump(self.properties, f, indent=2)
        
        logger.info(f"Progress saved: {len(self.properties)} properties")
    
    def _print_progress_summary(self):
        """Print progress summary"""
        logger.info("\n" + "="*50)
        logger.info("PROGRESS SUMMARY")
        logger.info("="*50)
        logger.info(f"Total attempted: {self.stats['total_attempted']}")
        logger.info(f"Successful: {self.stats['successful']} ({self.stats['successful']/max(self.stats['total_attempted'],1)*100:.1f}%)")
        logger.info(f"HTTP errors: {self.stats['http_errors']}")
        logger.info(f"Empty responses: {self.stats['empty_responses']}")
        logger.info(f"Extraction errors: {self.stats['extraction_errors']}")
        logger.info(f"Session resets: {self.stats['session_resets']}")
        logger.info("="*50 + "\n")
    
    def save_results(self, filename='braintree_properties.csv'):
        """Save final results"""
        if not self.properties:
            logger.warning("No properties to save")
            return
        
        df = pd.DataFrame(self.properties)
        
        # Save CSV
        df_csv = df.copy()
        if 'historical_assessments' in df_csv.columns:
            df_csv['historical_assessments'] = df_csv['historical_assessments'].apply(
                lambda x: json.dumps(x) if x else ''
            )
        if 'sales_history' in df_csv.columns:
            df_csv['sales_history'] = df_csv['sales_history'].apply(
                lambda x: json.dumps(x) if x else ''
            )
        
        df_csv.to_csv(filename, index=False)
        logger.info(f"Saved {len(df)} properties to {filename}")
        
        # Save JSON
        json_filename = filename.replace('.csv', '.json')
        with open(json_filename, 'w') as f:
            json.dump(self.properties, f, indent=2)
        
        # Save failed properties
        if self.failed_properties:
            with open('failed_properties_final.json', 'w') as f:
                json.dump(self.failed_properties, f, indent=2)
        
        # Print field summary
        logger.info("\nField capture summary:")
        for col in df.columns:
            if col not in ['historical_assessments', 'sales_history']:
                non_null = df[col].notna().sum()
                logger.info(f"  {col}: {non_null}/{len(df)} ({non_null/len(df)*100:.1f}%)")
        
        # Final stats
        self._print_progress_summary()


# Usage
if __name__ == "__main__":
    scraper = SlowBraintreePropertyScraper()
    
    # Load previous results
    scraper.load_previous_results('braintree_properties.json')
    
    print("\nEnhanced Braintree Scraper with Session Management")
    print("="*50)
    print("This scraper includes:")
    print("- Automatic session detection and refresh")
    print("- Retry logic for failed properties")
    print("- Smart session management")
    print("- Progress tracking and resume capability")
    print("="*50 + "\n")
    
    # Option to resume from a specific property
    resume_from = None  # Set to account number to resume, e.g., "7141"
    
    # Start scraping
    start_time = time.time()
    
    try:
        properties = scraper.search_properties(
            luc_code="101",
            start_page=1,
            end_page=None,  # Process all pages
            resume_from_property=resume_from
        )
    except KeyboardInterrupt:
        logger.info("\nScraping interrupted by user")
        scraper._save_progress()
    
    # Save final results
    scraper.save_results('braintree_properties_final.csv')
    
    elapsed = time.time() - start_time
    print(f"\nCompleted in {elapsed/3600:.1f} hours")
    print(f"Successfully scraped: {len(scraper.properties)} properties")
    print(f"Session resets: {scraper.stats['session_resets']}")