import time
import uuid
import json
import logging
import math
from collections import deque
from typing import Dict, Deque, Tuple
import paho.mqtt.client as mqtt

# Configuration
class Config:
    APP_NAME = "OilSpillMonitorApp 1.2"
    # Thresholds
    SPEED_DROP_THRESHOLD_PERCENT = 50
    COURSE_CHANGE_THRESHOLD_DEG = 45
    DRIFT_SPEED_THRESHOLD = 0.5  # ~1 knot
    # Timing
    VESSEL_HISTORY_LENGTH = 5
    ALERT_COOLDOWN_MINUTES = 10
    # Danger level thresholds
    DANGER_LEVELS = {
        1: "Low",
        2: "Medium",
        3: "High",
        4: "Critical"
    }
    # MQTT
    MQTT_BROKER = "meri.digitraffic.fi"
    MQTT_PORT = 443
    MQTT_TOPIC = "vessels-v2/+/location"

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# Global state
vessel_location_history: Dict[str, Deque[dict]] = {}
last_alert_times: Dict[str, float] = {}

def calculate_danger_level(anomalies: list) -> Tuple[int, str]:
    """Determine danger level based on active anomalies."""
    danger_scores = {
        "speed_drop": 1,
        "course_change": 2,
        "drifting": 3
    }
    
    total_score = sum(danger_scores.get(a, 0) for a in anomalies)
    
    if total_score >= 4:
        return 4, Config.DANGER_LEVELS[4]
    if total_score >= 3:
        return 3, Config.DANGER_LEVELS[3]
    if total_score >= 2:
        return 2, Config.DANGER_LEVELS[2]
    return 1, Config.DANGER_LEVELS[1]

def get_map_link(lat: float, lon: float) -> str:
    """Generate Google Maps link for coordinates with 6 decimal places precision."""
    return f"https://www.google.com/maps/search/?api=1&query={lat:.6f},{lon:.6f}"

def send_alert(mmsi: str, reason: str, details: dict):
    """Trigger alert with danger level and location info."""
    current_time = time.time()
    cooldown = Config.ALERT_COOLDOWN_MINUTES * 60
    
    # Calculate danger level
    danger_level, danger_label = calculate_danger_level(details["anomalies"])
    
    # Prepare location info using dictionary with explicit lat/lon keys
    current_pos = details["positions"]["current"]
    location_info = {
        "coordinates": {
            "lat": current_pos["lat"],
            "lon": current_pos["lon"]
        },
        "google_maps": get_map_link(current_pos["lat"], current_pos["lon"]),
        "approx_location": "At sea"  # Could integrate with geocoding service
    }
    
    alert_data = {
        "vessel_id": mmsi,
        "timestamp": details["timestamp"],
        "danger_level": danger_label,
        "danger_score": danger_level,
        "location": location_info,
        "indicators": details["anomalies"],
        "metrics": {
            "speed": details["speed"],
            "course": details["course"],
            "drifting_speed": details.get("drifting_speed")
        }
    }
    
    if current_time - last_alert_times.get(mmsi, 0) >= cooldown:
        logging.warning(f"ALERT ({danger_label}): Vessel {mmsi}\n"
                        f"Reason: {reason}\n"
                        f"Location: {location_info['google_maps']}\n"
                        f"Coordinates: {current_pos['lat']:.6f}°N, {current_pos['lon']:.6f}°E\n"
                        f"Details: {json.dumps(alert_data, indent=2, default=str)}")
        last_alert_times[mmsi] = current_time
    else:
        logging.info(f"Suppressed {danger_label} alert for {mmsi} in cooldown")

def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between coordinates in meters."""
    R = 6371000  # Earth radius in meters
    φ1 = math.radians(lat1)
    φ2 = math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)

    a = math.sin(Δφ/2)**2 + math.cos(φ1) * math.cos(φ2) * math.sin(Δλ/2)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

def normalize_course_diff(cog1: float, cog2: float) -> float:
    """Calculate minimal course difference accounting for circular nature."""
    diff = abs(cog1 - cog2) % 360
    return diff if diff <= 180 else 360 - diff

def check_for_anomalies(mmsi: str, current: dict, previous: dict):
    """Analyze vessel data for potential oil spill indicators."""
    anomalies = []
    details = {
        "timestamp": current["time"],
        "positions": {
            "current": {"lat": current["lat"], "lon": current["lon"]},
            "previous": {"lat": previous["lat"], "lon": previous["lon"]}
        },
        "speed": {"current": current["sog"], "previous": previous["sog"]},
        "course": {"current": current["cog"], "previous": previous["cog"]},
        "anomalies": anomalies
    }

    # Navigation status check (0 = under way using engine)
    if current["navStat"] != 0:
        return

    time_diff = current["time"] - previous["time"]
    if time_diff <= 0:
        return

    # Speed drop detection
    if previous["sog"] > 0:
        speed_drop = (previous["sog"] - current["sog"]) / previous["sog"] * 100
        if speed_drop >= Config.SPEED_DROP_THRESHOLD_PERCENT:
            anomalies.append("speed_drop")
            details["speed_drop_percent"] = speed_drop

    # Course change detection
    course_change = normalize_course_diff(current["cog"], previous["cog"])
    if course_change >= Config.COURSE_CHANGE_THRESHOLD_DEG:
        anomalies.append("course_change")
        details["course_change_deg"] = course_change

    # Drift detection: when both speeds are zero but vessel has moved
    if current["sog"] == 0 and previous["sog"] == 0:
        distance = haversine(previous["lat"], previous["lon"], current["lat"], current["lon"])
        drift_speed = distance / time_diff
        if drift_speed > Config.DRIFT_SPEED_THRESHOLD:
            anomalies.append("drifting")
            details["drifting_speed"] = drift_speed

    if anomalies:
        reason = ", ".join(anomalies).replace("_", " ").title()
        send_alert(mmsi, reason, details)

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        logging.info("Connected to MQTT broker")
        client.subscribe(Config.MQTT_TOPIC)
    else:
        logging.error(f"Connection failed: {reason_code}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        topic_parts = msg.topic.split('/')
        if len(topic_parts) < 3:
            return

        mmsi = topic_parts[1]
        history = vessel_location_history.setdefault(
            mmsi, deque(maxlen=Config.VESSEL_HISTORY_LENGTH)
        )

        current_data = {
    "time": payload.get("time", int(time.time())),
    "sog": payload.get("sog", 0),
    "cog": payload.get("cog", 0),
    "navStat": payload.get("navStat", 0),
    "lat": payload.get("lat", 0),
    "lon": payload.get("lon", 0)
}


        # Validate and format position data to 6 decimal places
        if -90 <= current_data["lat"] <= 90 and -180 <= current_data["lon"] <= 180:
            current_data["lat"] = round(current_data["lat"], 6)
            current_data["lon"] = round(current_data["lon"], 6)
            history.append(current_data)
            if len(history) >= 2:
                check_for_anomalies(mmsi, current_data, history[-2])
        else:
            logging.warning(f"Invalid coordinates for {mmsi}")

    except json.JSONDecodeError:
        logging.error("Invalid JSON payload")
    except Exception as e:
        logging.error(f"Processing error: {str(e)}")

def main():
    client = mqtt.Client(
        client_id=f"{Config.APP_NAME}_{uuid.uuid4()}",
        transport="websockets",
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )
    client.tls_set()
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(Config.MQTT_BROKER, Config.MQTT_PORT)
        client.loop_forever()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    except Exception as e:
        logging.critical(f"Fatal error: {str(e)}")

if __name__ == "__main__":
    main()
