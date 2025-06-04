import json
import pandas as pd
import numpy as np
from datetime import datetime
import re
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from collections import defaultdict

class BraintreeDataQualityChecker:
    def __init__(self, json_file_path):
        self.json_file_path = json_file_path
        self.properties = []
        self.quality_report = {
            'total_records': 0,
            'completeness': {},
            'validity_issues': defaultdict(list),
            'consistency_issues': defaultdict(list),
            'accuracy_issues': defaultdict(list),
            'duplicate_issues': defaultdict(list),
            'data_anomalies': defaultdict(list),
            'summary_stats': {},
            'recommendations': []
        }
        
    def load_data(self):
        """Load the JSON data file"""
        try:
            with open(self.json_file_path, 'r') as f:
                self.properties = json.load(f)
            self.quality_report['total_records'] = len(self.properties)
            print(f"Loaded {len(self.properties)} properties")
            return True
        except Exception as e:
            print(f"Error loading data: {e}")
            return False
    
    def check_completeness(self):
        """Check data completeness for each field"""
        print("\n=== Checking Data Completeness ===")
        
        # Define required fields and their importance
        required_fields = {
            'critical': ['account_number', 'location', 'parcel_id', 'owner', 
                        'assessment_year', 'total_value'],
            'important': ['sale_date', 'sale_price', 'building_value', 'land_value',
                         'year_built', 'bedrooms', 'bathrooms', 'style', 'land_area'],
            'optional': ['owner_2', 'old_parcel_id', 'legal_reference', 'seller',
                        'half_baths', 'three_quarter_baths', 'units', 'exterior', 'roof']
        }
        
        all_fields = set()
        for prop in self.properties:
            all_fields.update(prop.keys())
        
        field_completeness = {}
        for field in all_fields:
            non_null_count = sum(1 for prop in self.properties 
                               if prop.get(field) is not None and str(prop.get(field)).strip())
            completeness_pct = (non_null_count / len(self.properties)) * 100
            field_completeness[field] = {
                'count': non_null_count,
                'percentage': completeness_pct,
                'missing': len(self.properties) - non_null_count
            }
        
        # Check critical fields
        for field in required_fields['critical']:
            if field in field_completeness:
                if field_completeness[field]['percentage'] < 100:
                    self.quality_report['validity_issues']['missing_critical_data'].append({
                        'field': field,
                        'missing_count': field_completeness[field]['missing'],
                        'completeness': field_completeness[field]['percentage']
                    })
        
        self.quality_report['completeness'] = field_completeness
        
        # Print summary
        print("\nField Completeness Summary:")
        for category, fields in required_fields.items():
            print(f"\n{category.upper()} Fields:")
            for field in fields:
                if field in field_completeness:
                    pct = field_completeness[field]['percentage']
                    print(f"  {field}: {pct:.1f}% complete")
    
    def check_validity(self):
        """Check data validity and format consistency"""
        print("\n=== Checking Data Validity ===")
        
        for idx, prop in enumerate(self.properties):
            # Check account number format
            if not str(prop.get('account_number', '')).isdigit():
                self.quality_report['validity_issues']['invalid_account_number'].append({
                    'index': idx,
                    'account_number': prop.get('account_number'),
                    'location': prop.get('location')
                })
            
            # Check parcel ID format (should match pattern like "2032 0 102")
            parcel_id = prop.get('parcel_id', '')
            if parcel_id and not re.match(r'^\d{4}\s+\d+\s+\d+$', str(parcel_id)):
                self.quality_report['validity_issues']['invalid_parcel_format'].append({
                    'index': idx,
                    'parcel_id': parcel_id,
                    'location': prop.get('location')
                })
            
            # Check ZIP code format
            zip_code = prop.get('zip', '')
            if zip_code and not re.match(r'^\d{5}(-\d{4})?$', str(zip_code)):
                self.quality_report['validity_issues']['invalid_zip'].append({
                    'index': idx,
                    'zip': zip_code,
                    'location': prop.get('location')
                })
            
            # Check year_built validity
            year_built = prop.get('year_built')
            if year_built:
                try:
                    year = int(year_built)
                    current_year = datetime.now().year
                    if year < 1600 or year > current_year:
                        self.quality_report['validity_issues']['invalid_year_built'].append({
                            'index': idx,
                            'year_built': year,
                            'location': prop.get('location')
                        })
                except:
                    self.quality_report['validity_issues']['non_numeric_year'].append({
                        'index': idx,
                        'year_built': year_built,
                        'location': prop.get('location')
                    })
            
            # Check numeric fields
            numeric_fields = ['building_value', 'land_value', 'total_value', 'bedrooms', 
                            'bathrooms', 'total_rooms', 'units']
            for field in numeric_fields:
                value = prop.get(field)
                if value is not None:
                    try:
                        num_val = float(value)
                        if num_val < 0:
                            self.quality_report['validity_issues']['negative_values'].append({
                                'index': idx,
                                'field': field,
                                'value': value,
                                'location': prop.get('location')
                            })
                    except:
                        self.quality_report['validity_issues']['non_numeric_values'].append({
                            'index': idx,
                            'field': field,
                            'value': value,
                            'location': prop.get('location')
                        })
            
            # Check date formats
            date_fields = ['sale_date', 'scrape_date']
            for field in date_fields:
                date_val = prop.get(field)
                if date_val:
                    # Try to parse various date formats
                    valid_date = False
                    for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S']:
                        try:
                            datetime.strptime(str(date_val), fmt)
                            valid_date = True
                            break
                        except:
                            continue
                    
                    if not valid_date and date_val:
                        self.quality_report['validity_issues']['invalid_date_format'].append({
                            'index': idx,
                            'field': field,
                            'value': date_val,
                            'location': prop.get('location')
                        })
    
    def check_consistency(self):
        """Check data consistency and relationships"""
        print("\n=== Checking Data Consistency ===")
        
        for idx, prop in enumerate(self.properties):
            # Check if total_value = building_value + land_value + xtra_features_value
            building = prop.get('building_value', 0) or 0
            land = prop.get('land_value', 0) or 0
            xtra = prop.get('xtra_features_value', 0) or 0
            total = prop.get('total_value', 0) or 0
            
            try:
                calculated_total = int(building) + int(land) + int(xtra)
                if abs(calculated_total - int(total)) > 1:  # Allow $1 rounding difference
                    self.quality_report['consistency_issues']['value_mismatch'].append({
                        'index': idx,
                        'location': prop.get('location'),
                        'building_value': building,
                        'land_value': land,
                        'xtra_features_value': xtra,
                        'total_value': total,
                        'calculated_total': calculated_total,
                        'difference': calculated_total - int(total)
                    })
            except:
                pass
            
            # Check bathroom consistency
            bathrooms = prop.get('bathrooms', 0) or 0
            half_baths = prop.get('half_baths', 0) or 0
            three_quarter = prop.get('three_quarter_baths', 0) or 0
            
            # Check if bedrooms <= total_rooms
            bedrooms = prop.get('bedrooms', 0) or 0
            total_rooms = prop.get('total_rooms', 0) or 0
            
            try:
                if int(bedrooms) > int(total_rooms):
                    self.quality_report['consistency_issues']['room_count_mismatch'].append({
                        'index': idx,
                        'location': prop.get('location'),
                        'bedrooms': bedrooms,
                        'total_rooms': total_rooms
                    })
            except:
                pass
            
            # Check if LLC/Trust flags match owner names
            is_llc = prop.get('is_llc', False)
            is_trust = prop.get('is_trust', False)
            owner = str(prop.get('owner', '')).upper()
            owner_2 = str(prop.get('owner_2', '')).upper()
            
            combined_owner = f"{owner} {owner_2}"
            
            if is_llc and 'LLC' not in combined_owner:
                self.quality_report['consistency_issues']['llc_flag_mismatch'].append({
                    'index': idx,
                    'location': prop.get('location'),
                    'owner': prop.get('owner'),
                    'is_llc': is_llc
                })
            
            if is_trust and not any(term in combined_owner for term in ['TRUST', ' TR', 'TRST']):
                self.quality_report['consistency_issues']['trust_flag_mismatch'].append({
                    'index': idx,
                    'location': prop.get('location'),
                    'owner': prop.get('owner'),
                    'is_trust': is_trust
                })
    
    def check_duplicates(self):
        """Check for duplicate records"""
        print("\n=== Checking for Duplicates ===")
        
        # Check for duplicate account numbers
        account_numbers = [prop.get('account_number') for prop in self.properties]
        duplicates = set([x for x in account_numbers if account_numbers.count(x) > 1])
        
        if duplicates:
            for dup in duplicates:
                dup_props = [prop for prop in self.properties 
                           if prop.get('account_number') == dup]
                self.quality_report['duplicate_issues']['duplicate_accounts'].append({
                    'account_number': dup,
                    'count': len(dup_props),
                    'locations': [p.get('location') for p in dup_props]
                })
        
        # Check for duplicate parcel IDs
        parcel_ids = [prop.get('parcel_id') for prop in self.properties if prop.get('parcel_id')]
        dup_parcels = set([x for x in parcel_ids if parcel_ids.count(x) > 1])
        
        if dup_parcels:
            for dup in dup_parcels:
                dup_props = [prop for prop in self.properties 
                           if prop.get('parcel_id') == dup]
                self.quality_report['duplicate_issues']['duplicate_parcels'].append({
                    'parcel_id': dup,
                    'count': len(dup_props),
                    'account_numbers': [p.get('account_number') for p in dup_props]
                })
    
    def check_anomalies(self):
        """Check for data anomalies and outliers"""
        print("\n=== Checking for Anomalies ===")
        
        # Prepare data for statistical analysis
        df = pd.DataFrame(self.properties)
        
        # Convert numeric fields
        numeric_fields = ['building_value', 'land_value', 'total_value', 
                         'bedrooms', 'bathrooms', 'total_rooms', 'year_built']
        
        for field in numeric_fields:
            if field in df.columns:
                df[field] = pd.to_numeric(df[field], errors='coerce')
        
        # Check for outliers using IQR method
        for field in numeric_fields:
            if field in df.columns:
                data = df[field].dropna()
                if len(data) > 0:
                    Q1 = data.quantile(0.25)
                    Q3 = data.quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    
                    outliers = df[(df[field] < lower_bound) | (df[field] > upper_bound)]
                    
                    if not outliers.empty:
                        for idx, row in outliers.iterrows():
                            self.quality_report['data_anomalies'][f'{field}_outliers'].append({
                                'account_number': row.get('account_number'),
                                'location': row.get('location'),
                                'value': row.get(field),
                                'lower_bound': lower_bound,
                                'upper_bound': upper_bound
                            })
        
        # Check for unusual sale prices
        for prop in self.properties:
            if prop.get('sale_price'):
                try:
                    price = int(prop.get('sale_price'))
                    # Flag suspiciously low prices (likely family transfers)
                    if 0 < price < 1000:
                        self.quality_report['data_anomalies']['suspicious_sale_price'].append({
                            'account_number': prop.get('account_number'),
                            'location': prop.get('location'),
                            'sale_price': price,
                            'sale_date': prop.get('sale_date'),
                            'likely_reason': 'Family transfer or quit claim'
                        })
                except:
                    pass
        
        # Calculate summary statistics
        self.quality_report['summary_stats'] = {
            'total_value': {
                'mean': df['total_value'].mean() if 'total_value' in df else None,
                'median': df['total_value'].median() if 'total_value' in df else None,
                'std': df['total_value'].std() if 'total_value' in df else None,
                'min': df['total_value'].min() if 'total_value' in df else None,
                'max': df['total_value'].max() if 'total_value' in df else None
            },
            'year_built': {
                'oldest': df['year_built'].min() if 'year_built' in df else None,
                'newest': df['year_built'].max() if 'year_built' in df else None,
                'median': df['year_built'].median() if 'year_built' in df else None
            },
            'property_size': {
                'avg_bedrooms': df['bedrooms'].mean() if 'bedrooms' in df else None,
                'avg_bathrooms': df['bathrooms'].mean() if 'bathrooms' in df else None,
                'avg_total_rooms': df['total_rooms'].mean() if 'total_rooms' in df else None
            }
        }
    
    def check_historical_data(self):
        """Check historical assessments and sales history integrity"""
        print("\n=== Checking Historical Data ===")
        
        for idx, prop in enumerate(self.properties):
            # Check historical assessments
            hist_assessments = prop.get('historical_assessments', [])
            if hist_assessments:
                # Check for chronological order
                years = [h.get('year') for h in hist_assessments if h.get('year')]
                if years != sorted(years, reverse=True):
                    self.quality_report['consistency_issues']['historical_order'].append({
                        'account_number': prop.get('account_number'),
                        'location': prop.get('location'),
                        'issue': 'Historical assessments not in chronological order'
                    })
                
                # Check for unrealistic value changes
                for i in range(len(hist_assessments) - 1):
                    curr_year = hist_assessments[i].get('year')
                    curr_val = hist_assessments[i].get('total_value', 0)
                    next_year = hist_assessments[i + 1].get('year')
                    next_val = hist_assessments[i + 1].get('total_value', 0)
                    
                    try:
                        if curr_val and next_val and curr_year and next_year:
                            year_diff = int(curr_year) - int(next_year)
                            if year_diff == 1:  # Consecutive years
                                change_pct = abs((int(curr_val) - int(next_val)) / int(next_val) * 100)
                                if change_pct > 50:  # More than 50% change in one year
                                    self.quality_report['data_anomalies']['extreme_value_change'].append({
                                        'account_number': prop.get('account_number'),
                                        'location': prop.get('location'),
                                        'year_from': next_year,
                                        'year_to': curr_year,
                                        'value_from': next_val,
                                        'value_to': curr_val,
                                        'change_pct': change_pct
                                    })
                    except:
                        pass
    
    def generate_recommendations(self):
        """Generate recommendations based on quality checks"""
        print("\n=== Generating Recommendations ===")
        
        recommendations = []
        
        # Check completeness issues
        critical_missing = [issue for issue in self.quality_report['validity_issues'].get('missing_critical_data', [])]
        if critical_missing:
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Completeness',
                'issue': f"Critical fields missing data: {', '.join([i['field'] for i in critical_missing])}",
                'recommendation': 'Re-scrape properties with missing critical data or implement data recovery procedures'
            })
        
        # Check validity issues
        if self.quality_report['validity_issues'].get('invalid_date_format'):
            recommendations.append({
                'priority': 'MEDIUM',
                'category': 'Validity',
                'issue': 'Inconsistent date formats detected',
                'recommendation': 'Standardize all date fields to ISO format (YYYY-MM-DD) for consistency'
            })
        
        # Check consistency issues
        if self.quality_report['consistency_issues'].get('value_mismatch'):
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Consistency',
                'issue': f"{len(self.quality_report['consistency_issues']['value_mismatch'])} properties have value calculation mismatches",
                'recommendation': 'Review value calculation logic and verify with source data'
            })
        
        # Check duplicate issues
        if self.quality_report['duplicate_issues'].get('duplicate_accounts'):
            recommendations.append({
                'priority': 'HIGH',
                'category': 'Integrity',
                'issue': f"{len(self.quality_report['duplicate_issues']['duplicate_accounts'])} duplicate account numbers found",
                'recommendation': 'Investigate and resolve duplicate records, ensure unique constraints'
            })
        
        # Check anomalies
        suspicious_sales = self.quality_report['data_anomalies'].get('suspicious_sale_price', [])
        if suspicious_sales:
            recommendations.append({
                'priority': 'LOW',
                'category': 'Data Quality',
                'issue': f"{len(suspicious_sales)} properties with suspicious sale prices (< $1,000)",
                'recommendation': 'Flag these as non-arms-length transactions for special handling in analysis'
            })
        
        self.quality_report['recommendations'] = recommendations
    
    def generate_report(self, output_file='data_quality_report.json'):
        """Generate comprehensive quality report"""
        print("\n=== Generating Quality Report ===")
        
        # Create summary
        total_issues = 0
        for category in ['validity_issues', 'consistency_issues', 'duplicate_issues', 'data_anomalies']:
            for issue_type, issues in self.quality_report[category].items():
                total_issues += len(issues)
        
        self.quality_report['summary'] = {
            'total_records': self.quality_report['total_records'],
            'total_issues': total_issues,
            'data_quality_score': max(0, 100 - (total_issues / self.quality_report['total_records'] * 100)),
            'report_generated': datetime.now().isoformat()
        }
        
        # Save report
        with open(output_file, 'w') as f:
            json.dump(self.quality_report, f, indent=2, default=str)
        
        print(f"\nQuality report saved to: {output_file}")
        
        # Print summary
        print("\n" + "="*50)
        print("DATA QUALITY SUMMARY")
        print("="*50)
        print(f"Total Records: {self.quality_report['total_records']}")
        print(f"Total Issues Found: {total_issues}")
        print(f"Data Quality Score: {self.quality_report['summary']['data_quality_score']:.1f}%")
        
        print("\nIssue Breakdown:")
        for category in ['validity_issues', 'consistency_issues', 'duplicate_issues', 'data_anomalies']:
            category_total = sum(len(issues) for issues in self.quality_report[category].values())
            if category_total > 0:
                print(f"\n{category.replace('_', ' ').title()}: {category_total} issues")
                for issue_type, issues in self.quality_report[category].items():
                    if issues:
                        print(f"  - {issue_type}: {len(issues)}")
        
        print("\nTop Recommendations:")
        for rec in sorted(self.quality_report['recommendations'], 
                         key=lambda x: {'HIGH': 0, 'MEDIUM': 1, 'LOW': 2}[x['priority']]):
            print(f"\n[{rec['priority']}] {rec['category']}: {rec['issue']}")
            print(f"  → {rec['recommendation']}")
    
    def create_visualizations(self, output_dir='quality_visualizations'):
        """Create data quality visualizations"""
        Path(output_dir).mkdir(exist_ok=True)
        
        # Create completeness heatmap
        plt.figure(figsize=(12, 8))
        completeness_data = {k: v['percentage'] for k, v in self.quality_report['completeness'].items()}
        
        # Sort by completeness
        sorted_fields = sorted(completeness_data.items(), key=lambda x: x[1], reverse=True)
        fields = [f[0] for f in sorted_fields]
        percentages = [f[1] for f in sorted_fields]
        
        colors = ['green' if p >= 90 else 'yellow' if p >= 70 else 'red' for p in percentages]
        
        plt.barh(fields, percentages, color=colors)
        plt.xlabel('Completeness %')
        plt.title('Field Completeness Analysis')
        plt.tight_layout()
        plt.savefig(f'{output_dir}/completeness_analysis.png')
        plt.close()
        
        print(f"\nVisualizations saved to {output_dir}/")
    
    def run_all_checks(self):
        """Run all quality checks"""
        if not self.load_data():
            return
        
        self.check_completeness()
        self.check_validity()
        self.check_consistency()
        self.check_duplicates()
        self.check_anomalies()
        self.check_historical_data()
        self.generate_recommendations()
        self.generate_report()
        self.create_visualizations()

# Usage
if __name__ == "__main__":
    # Replace with your actual JSON file path
    checker = BraintreeDataQualityChecker('braintree_single_fam_20250529_ALL.json')
    checker.run_all_checks()
    
    # You can also access specific reports
    # print(json.dumps(checker.quality_report['validity_issues'], indent=2))