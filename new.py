import numpy as np
from keras.models import load_model
from PIL import Image
import io
import requests

# Load the trained model
model = load_model('marine_model.h5')

# Labels
labels = ['Non Oil Spill', 'Oil Spill']

# Define danger levels based on prediction confidence
DANGER_THRESHOLDS = {
    "Low": 0.50,
    "Medium": 0.70,
    "High": 0.85,
    "Critical": 0.95
}

def load_and_preprocess_image(image):
    """Load, preprocess, and normalize image."""
    try:
        img = Image.open(io.BytesIO(image))

        # Convert to RGB to ensure 3 channels
        img = img.convert("RGB")
        
        # Resize with anti-aliasing
        img = img.resize((150, 150), Image.Resampling.LANCZOS)  
        
        # Convert to NumPy array and normalize
        img_array = np.array(img, dtype=np.float32) / 255.0  
        
        # Expand dimensions for batch processing
        img_array = np.expand_dims(img_array, axis=0)

        print(f"Processed image shape: {img_array.shape}")  # Debugging
        return img_array
    except Exception as e:
        print(f"Error loading and processing image: {e}")
        return None

def determine_danger_level(confidence):
    """Assign a danger level based on model confidence."""
    for level, threshold in reversed(DANGER_THRESHOLDS.items()):
        if confidence >= threshold:
            return level
    return "Low"

def predict_from_url(image_url):
    """Fetch image from URL, predict oil spill, and determine danger level."""
    try:
        response = requests.get(image_url, timeout=10)  # Set timeout for robustness
        if response.status_code == 200:
            img = load_and_preprocess_image(response.content)
            if img is not None:
                prediction = model.predict(img)[0][0]  # Extract probability
                confidence = float(prediction)

                print(f"Raw prediction confidence: {confidence}")  # Debugging

                # Adjusting threshold if needed
                threshold = 0.65  # Try adjusting this
                result_label = labels[int(confidence > threshold)]

                print(f"Prediction: {result_label}")
                if result_label == "Oil Spill":
                    danger_level = determine_danger_level(confidence)
                    print(f"Danger Level: {danger_level} (Confidence: {confidence:.2f})")
                else:
                    print(f"Confidence: {confidence:.2f}")
            else:
                print("Error processing the image. Please ensure it's a valid image format.")
        else:
            print("Failed to fetch image from the URL. Please check the URL and try again.")
    except Exception as e:
        print(f"Error fetching image from URL: {e}")

# Get user input for image URL and run prediction
image_url = input("Enter the image URL: ")
predict_from_url(image_url)
