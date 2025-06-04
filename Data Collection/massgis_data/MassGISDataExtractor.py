import requests
import os
import logging

# Setup
output_dir = "massgis_data"
os.makedirs(output_dir, exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(message)s')

# MassGIS corrected URLs (verified working links only)
urls = {
    # boundaries
    'municipalities': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/townssurvey_shp.zip',
    'counties': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/counties.zip',
    'congressional_districts': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/CONGRESSMA118.zip',

    # property & administrative
    'property_parcels': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/l3parcels/L3_AGGREGATE_SHP_20250101.zip',
    'zip_codes': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/zipcodes_nt.zip',
    'school_districts': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/schooldistricts.zip',

    # environmental + hazards
    'flood_zones_nfhl': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/nfhl.zip',
    'wetlands': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/wetlandsdep.zip',
    'protected_open_space': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/openspace.zip',
    'aquifers': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/aquifers.zip',
    'water_supply_protection': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/swp_zones.zip',
    'contaminated_sites': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/c21e_pt.zip',
    'underground_storage_tanks': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/ust.zip',

    # transportation
    'roads': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/MassDOT_Roads_SHP.zip',
    'railways': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/trains.zip',
    'trails': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/trails.zip',

    # civic facilities
    'schools': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/schools.zip',
    'hospitals': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/acute_care_hospitals.zip',
    'libraries': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/libraries.zip',
    'fire_stations': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/firestations_pt.zip',
    'police_stations': 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/state/policestations.zip',
}

# Download all layers
session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})

print(f"Downloading {len(urls)} MassGIS layers...")
print("Note: property_parcels is 5.7 GB, may take a while!\n")

for i, (name, url) in enumerate(urls.items(), 1):
    print(f"[{i}/{len(urls)}] Downloading {name}...", end='', flush=True)
    try:
        response = session.get(url, timeout=600, stream=True)
        if response.status_code == 200:
            filename = os.path.join(output_dir, f"{name}.zip")
            total_size = int(response.headers.get('content-length', 0))
            downloaded = 0
            
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            size_mb = total_size / (1024 * 1024)
                            print(f"\r[{i}/{len(urls)}] Downloading {name}... {percent:.1f}% ({size_mb:.1f} MB)", end='', flush=True)
            
            print(f"\r[{i}/{len(urls)}] ✓ {name}.zip downloaded successfully")
        else:
            print(f"\r[{i}/{len(urls)}] ✗ Failed {name}: HTTP {response.status_code}")
    except Exception as e:
        print(f"\r[{i}/{len(urls)}] ✗ Error {name}: {e}")

print(f"\nDone! Files saved to: {output_dir}/")
print("\nNote: Airports layer must be downloaded manually from:")
print("https://geo-massdot.opendata.arcgis.com/datasets/17eb7e286f4e4942aeef500f5ef6bfcd_0")