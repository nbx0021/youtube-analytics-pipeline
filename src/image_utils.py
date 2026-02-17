import requests
from PIL import Image
from io import BytesIO
from sklearn.cluster import KMeans
import numpy as np

def get_dominant_color(image_url):
    """
    Downloads an image and returns its dominant color as a HEX string (e.g., '#FF0000').
    Uses K-Means clustering to ignore small details and find the 'Main Theme'.
    """
    if not image_url:
        return None

    try:
        # 1. Download Image (In-Memory)
        response = requests.get(image_url, timeout=5)
        response.raise_for_status()
        img = Image.open(BytesIO(response.content))
        
        # 2. Resize to speed up processing (50x50 pixels is enough)
        img = img.resize((50, 50))
        
        # 3. Convert to RGB and numpy array
        if img.mode != 'RGB':
            img = img.convert('RGB')
        
        # Reshape image data to a list of RGB pixels
        # (50 * 50 pixels, 3 color channels)
        img_array = np.array(img).reshape((50 * 50, 3))

        # 4. Use K-Means Clustering to find the "Center" color
        # n_clusters=1 means "Find the single most common average color"
        kmeans = KMeans(n_clusters=1, n_init=10)
        kmeans.fit(img_array)
        
        # Get RGB values of the center
        dominant_color = kmeans.cluster_centers_[0].astype(int)
        
        # 5. Convert RGB to HEX
        hex_color = '#{:02x}{:02x}{:02x}'.format(*dominant_color)
        return hex_color

    except Exception as e:
        print(f"⚠️ Thumbnail Error ({image_url}): {e}")
        return "#000000" # Return Black on error