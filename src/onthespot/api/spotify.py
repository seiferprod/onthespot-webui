import json
import os
import re
import requests
import threading
import time
import traceback
import uuid
from librespot.audio.decoders import AudioQuality
from librespot.core import Session
from librespot.zeroconf import ZeroconfServer
from ..otsconfig import config, cache_dir
from ..runtimedata import get_logger, account_pool, pending, download_queue, pending_lock
from ..utils import make_call, conv_list_format

logger = get_logger("api.spotify")
BASE_URL = "https://api.spotify.com/v1"

# Cache for album track IDs to avoid repeated API calls and race conditions
_album_track_ids_cache = {}
_album_track_ids_cache_lock = threading.Lock()

# Cache for Spotify Web API client-credentials token
_spotify_app_token = {"access_token": None, "expires_at": 0}
_spotify_app_token_lock = threading.Lock()


def _mask_value(value, visible=6):
    if not value:
        return ""
    if len(value) <= visible:
        return value
    return f"...{value[-visible:]}"


def _spotify_get_app_access_token():
    env_client_id = os.environ.get("ONTHESPOT_SPOTIFY_CLIENT_ID", "").strip()
    env_client_secret = os.environ.get("ONTHESPOT_SPOTIFY_CLIENT_SECRET", "").strip()
    
    # Correction : On cherche d'abord dans la config globale, puis dans les comptes
    cfg_client_id = config.get("spotify_client_id", "").strip()
    cfg_client_secret = config.get("spotify_client_secret", "").strip()

    if not cfg_client_id:
        for acc in config.get('accounts', []):
            if acc.get('service') == 'spotify' and acc.get('client_id'):
                cfg_client_id = acc.get('client_id')
                cfg_client_secret = acc.get('client_secret')
                break

    client_id = env_client_id or cfg_client_id
    client_secret = env_client_secret or cfg_client_secret
    
    id_source = "env" if env_client_id else ("config" if cfg_client_id else "missing")
    secret_source = "env" if env_client_secret else ("config" if cfg_client_secret else "missing")
    
    if not client_id or not client_secret:
        return None

    now = time.time()
    with _spotify_app_token_lock:
        if _spotify_app_token["access_token"] and _spotify_app_token["expires_at"] > now + 30:
            return _spotify_app_token["access_token"]

        try:
            resp = requests.post(
                "https://accounts.spotify.com/api/token",
                data={"grant_type": "client_credentials"},
                auth=(client_id, client_secret),
                timeout=10,
            )
            data = resp.json()
            access_token = data.get("access_token")
            expires_in = int(data.get("expires_in", 0))
            if not access_token:
                return None

            _spotify_app_token["access_token"] = access_token
            _spotify_app_token["expires_at"] = now + expires_in
            return access_token
        except Exception as e:
            logger.error(f"Spotify app token request failed: {e}")
            return None


def spotify_new_session():
    os.makedirs(os.path.join(cache_dir(), 'sessions'), exist_ok=True)

    uuid_uniq = str(uuid.uuid4())
    session_json_path = os.path.join(os.path.join(cache_dir(), 'sessions'),
                         f"ots_login_{uuid_uniq}.json")

    # MODIFICATION : Utilise ton Client ID si disponible
    custom_id = ""
    for acc in config.get('accounts', []):
        if acc.get('service') == 'spotify' and acc.get('client_id'):
            custom_id = acc.get('client_id')
            break
            
    CLIENT_ID: str = custom_id if custom_id else "65b708073fc0480ea92a077233ca87bd"
    
    logger.info(f"Zeroconf starting with Client ID: {_mask_value(CLIENT_ID)}")
    
    ZeroconfServer._ZeroconfServer__default_get_info_fields['clientID'] = CLIENT_ID
    zs_builder = ZeroconfServer.Builder()
    zs_builder.device_name = 'OnTheSpot'
    zs_builder.conf.stored_credentials_file = session_json_path
    zs = zs_builder.create()

    while True:
        time.sleep(1)
        if zs.has_valid_session():
            try:
                with open(session_json_path, 'r') as file:
                    zeroconf_login = json.load(file)
                
                cfg_copy = config.get('accounts').copy()
                new_user = {
                    "uuid": uuid_uniq,
                    "service": "spotify",
                    "active": True,
                    "client_id": custom_id, # On garde le lien
                    "login": {
                        "username": zeroconf_login["username"],
                        "credentials": zeroconf_login["credentials"],
                        "type": zeroconf_login["type"],
                    }
                }
                zs.close()
                cfg_copy.append(new_user)
                config.set('accounts', cfg_copy)
                config.save()
                return True
            except Exception as e:
                logger.error(f"Zeroconf login error: {e}")
                return False

# ... (Le reste de tes fonctions spotify_login_user, etc. reste identique)
