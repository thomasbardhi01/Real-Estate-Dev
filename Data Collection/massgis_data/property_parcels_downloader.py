import requests
import os
import time
import ssl
import urllib3

# Disable SSL warnings if needed
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def download_with_resume(url, filename, chunk_size=1024*1024):  # 1MB chunks
    """Download large file with resume capability"""
    headers = {'User-Agent': 'Mozilla/5.0'}
    mode = 'wb'
    resume_pos = 0
    
    # Check if partial download exists
    if os.path.exists(filename):
        resume_pos = os.path.getsize(filename)
        mode = 'ab'
        headers['Range'] = f'bytes={resume_pos}-'
        print(f"Resuming download from {resume_pos / (1024*1024):.1f} MB")
    
    session = requests.Session()
    session.headers.update(headers)
    
    # Retry logic
    max_retries = 5
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Try with SSL verification first, then without if it fails
            try:
                response = session.get(url, stream=True, timeout=30)
            except ssl.SSLError:
                print("SSL verification failed, trying without verification...")
                response = session.get(url, stream=True, timeout=30, verify=False)
            
            response.raise_for_status()
            
            # Get total file size
            total_size = int(response.headers.get('content-length', 0))
            if resume_pos > 0:
                total_size += resume_pos
            
            print(f"Total file size: {total_size / (1024*1024*1024):.2f} GB")
            
            # Download the file
            downloaded = resume_pos
            start_time = time.time()
            
            with open(filename, mode) as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        # Progress update
                        elapsed = time.time() - start_time
                        if elapsed > 0:
                            speed = (downloaded - resume_pos) / elapsed / (1024*1024)  # MB/s
                            percent = (downloaded / total_size) * 100 if total_size > 0 else 0
                            print(f"\rProgress: {percent:.1f}% ({downloaded/(1024*1024*1024):.2f} GB / {total_size/(1024*1024*1024):.2f} GB) - Speed: {speed:.1f} MB/s", end='', flush=True)
            
            print("\n✓ Download completed successfully!")
            return True
            
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            retry_count += 1
            print(f"\n✗ Connection error (attempt {retry_count}/{max_retries}): {e}")
            if retry_count < max_retries:
                wait_time = retry_count * 5
                print(f"Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                # Update resume position
                if os.path.exists(filename):
                    resume_pos = os.path.getsize(filename)
                    headers['Range'] = f'bytes={resume_pos}-'
                    mode = 'ab'
            else:
                print("Max retries reached. Download failed.")
                return False
                
        except Exception as e:
            print(f"\n✗ Unexpected error: {e}")
            return False
    
    return False

# Main download
if __name__ == "__main__":
    # Create output directory
    output_dir = "massgis_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Property parcels URL and filename
    url = 'https://s3.us-east-1.amazonaws.com/download.massgis.digital.mass.gov/shapefiles/l3parcels/L3_AGGREGATE_SHP_20250101.zip'
    filename = os.path.join(output_dir, 'property_parcels.zip')
    
    print("MassGIS Property Parcels Downloader")
    print("=" * 40)
    print("This will download the 5.7 GB property parcels file.")
    print("The download supports resume if interrupted.\n")
    
    # Start download
    success = download_with_resume(url, filename)
    
    if success:
        print(f"\nFile saved to: {filename}")
        print("You can now extract the ZIP file to access the shapefile data.")
    else:
        print("\nDownload failed. You can run this script again to resume.")
        print("\nAlternative options:")
        print("1. Try downloading directly in a web browser:")
        print(f"   {url}")
        print("\n2. Use wget with resume support:")
        print(f"   wget -c '{url}' -O '{filename}'")
        print("\n3. Use curl with resume support:")
        print(f"   curl -C - -L '{url}' -o '{filename}'")