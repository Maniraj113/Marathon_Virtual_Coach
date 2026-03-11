import os
import requests
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

class StravaService:
    """
    Service for interacting with the Strava API.
    """
    def __init__(self, client_id=None, client_secret=None, refresh_token=None):
        self.client_id = client_id or os.getenv("STRAVA_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("STRAVA_CLIENT_SECRET")
        self.refresh_token = refresh_token
        self.access_token = None

    def refresh_access_token(self):
        """Refreshes the Strava access token."""
        if not self.client_id or not self.client_secret or not self.refresh_token:
            logger.error("Strava credentials or refresh token missing.")
            return False

        url = "https://www.strava.com/oauth/token"
        payload = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self.refresh_token,
            'grant_type': 'refresh_token'
        }

        try:
            response = requests.post(url, data=payload)
            response.raise_for_status()
            data = response.json()
            self.access_token = data['access_token']
            # Note: We should ideally update the stored refresh_token if a new one is returned
            if 'refresh_token' in data:
                self.refresh_token = data['refresh_token']
            return True
        except Exception as e:
            logger.error(f"Failed to refresh Strava token: {e}")
            return False

    def get_activity_streams(self, activity_id):
        """
        Fetches streams (time-series data) for a specific activity.
        Typically includes: time, distance, latlng, altitude, velocity_smooth, heartrate, cadence, watts, temp, moving, grade_smooth.
        """
        if not self.access_token and not self.refresh_access_token():
            return None

        url = f"https://www.strava.com/api/v3/activities/{activity_id}/streams"
        params = {
            'keys': 'time,distance,velocity_smooth,heartrate,cadence',
            'key_by_type': 'true'
        }
        headers = {'Authorization': f'Bearer {self.access_token}'}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to fetch Strava streams for activity {activity_id}: {e}")
            return None

    def analyze_pace_consistency(self, streams):
        """
        Calculates pace consistency from velocity_smooth stream.
        Returns a simplified metric of variance.
        """
        if not streams or 'velocity_smooth' not in streams:
            return None
        
        velocities = streams['velocity_smooth']['data']
        if not velocities:
            return None
            
        # Example analysis: variance in moving segments
        import statistics
        try:
            # Filter out very low speeds (likely stopped)
            active_speeds = [v for v in velocities if v > 1.0] # > 1m/s (~16 min/km)
            if len(active_speeds) < 10:
                return "Insufficient data for consistency analysis"
                
            avg_speed = statistics.mean(active_speeds)
            stdev = statistics.stdev(active_speeds)
            coefficient_of_variation = (stdev / avg_speed) * 100
            
            return {
                "avg_pace_m_s": round(avg_speed, 2),
                "stdev_m_s": round(stdev, 2),
                "consistency_score_cv": round(coefficient_of_variation, 2), # Lower is better/more consistent
                "interpretation": "Highly consistent" if coefficient_of_variation < 5 else "Variable" if coefficient_of_variation < 15 else "Highly variable"
            }
        except:
            return "Error calculating consistency"
