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
    """Récupère le token d'accès via Client Credentials en utilisant tes clés perso."""
    env_client_id = os.environ.get("ONTHESPOT_SPOTIFY_CLIENT_ID", "").strip()
    env_client_secret = os.environ.get("ONTHESPOT_SPOTIFY_CLIENT_SECRET", "").strip()
    
    # On cherche d'abord dans la config racine
    cfg_client_id = config.get("spotify_client_id", "").strip()
    cfg_client_secret = config.get("spotify_client_secret", "").strip()

    # Si pas trouvé, on cherche à l'intérieur de l'objet "accounts" (ton cas avec Nano)
    if not cfg_client_id:
        for acc in config.get('accounts', []):
            if acc.get('service') == 'spotify' and acc.get('client_id'):
                cfg_client_id = acc.get('client_id')
                cfg_client_secret = acc.get('client_secret')
                break

    client_id = env_client_id or cfg_client_id
    client_secret = env_client_secret or cfg_client_secret
    
    if not client_id or not client_secret:
        logger.warning("Spotify app creds missing; skipping client-credentials token.")
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
            logger.info("Spotify app token refreshed using custom Client ID.")
            return access_token
        except Exception as e:
            logger.error(f"Spotify app token request failed: {e}")
            return None


def _spotify_get_public_api_headers(token, context):
    app_token = _spotify_get_app_access_token()
    if app_token:
        return {"Authorization": f"Bearer {app_token}"}, "app"
    return {"Authorization": f"Bearer {token.tokens().get('user-read-email')}"}, "session"


def _spotify_extract_year(value):
    if not value:
        return None
    match = re.search(r'(\d{4})', str(value))
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def spotify_get_playlist_updated_year(headers, playlist_id, tracks_total):
    if not isinstance(tracks_total, int) or tracks_total <= 0:
        return None

    def _fetch_added_at(offset):
        resp = make_call(
            f"{BASE_URL}/playlists/{playlist_id}/tracks",
            params={
                'offset': str(max(0, offset)),
                'limit': '1',
                'fields': 'items(added_at)'
            },
            headers=headers,
            skip_cache=True,
        )
        if not resp or not resp.get('items'):
            return None
        return resp['items'][0].get('added_at')

    first_added_at = _fetch_added_at(0)
    last_added_at = _fetch_added_at(tracks_total - 1)
    years = []
    for added_at in (first_added_at, last_added_at):
        year = _spotify_extract_year(added_at)
        if year is not None:
            years.append(year)
    return str(max(years)) if years else None


class MirrorSpotifyPlayback:
    def __init__(self):
        self.thread = None
        self.is_running = False

    def start(self):
        if self.thread is None:
            logger.info('Starting SpotifyMirrorPlayback')
            self.is_running = True
            self.thread = threading.Thread(target=self.run)
            self.thread.start()

    def stop(self):
        if self.thread is not None:
            self.is_running = False
            self.thread.join()
            self.thread = None

    def run(self):
        from ..accounts import get_account_token
        while self.is_running:
            time.sleep(5)
            try:
                token_obj = get_account_token('spotify')
                token = token_obj.tokens()
            except:
                continue
            
            url = f"{BASE_URL}/me/player/currently-playing"
            try:
                resp = requests.get(url, headers={"Authorization": f"Bearer {token.get('user-read-currently-playing')}"})
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('currently_playing_type') == 'track':
                        item_id = data['item']['id']
                        if item_id not in pending and item_id not in download_queue:
                            # Logique simplifiée pour l'ajout en file d'attente
                            with pending_lock:
                                pending[item_id] = {
                                    'local_id': item_id,
                                    'item_service': 'spotify',
                                    'item_type': 'track',
                                    'item_id': item_id,
                                    'parent_category': 'track'
                                }
                            logger.info(f'Mirror added track: {item_id}')
            except Exception as e:
                logger.debug(f"Mirror playback error: {e}")


def spotify_new_session():
    """Initialise une nouvelle session via Zeroconf avec ton Client ID perso."""
    os.makedirs(os.path.join(cache_dir(), 'sessions'), exist_ok=True)
    uuid_uniq = str(uuid.uuid4())
    session_json_path = os.path.join(cache_dir(), 'sessions', f"ots_login_{uuid_uniq}.json")

    # Récupération de l'ID perso pour Zeroconf
    custom_id = ""
    for acc in config.get('accounts', []):
        if acc.get('service') == 'spotify' and acc.get('client_id'):
            custom_id = acc.get('client_id')
            break
            
    CLIENT_ID = custom_id if custom_id else "65b708073fc0480ea92a077233ca87bd"
    logger.info(f"Starting Zeroconf with Client ID: {_mask_value(CLIENT_ID)}")

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
                    "client_id": custom_id, # On préserve l'ID pour le futur
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
                logger.error(f"Login error: {e}")
                return False


def spotify_login_user(account):
    try:
        uuid_str = account['uuid']
        username = account['login']['username']
        session_dir = os.path.join(cache_dir(), "sessions")
        os.makedirs(session_dir, exist_ok=True)
        session_json_path = os.path.join(session_dir, f"ots_login_{uuid_str}.json")

        with open(session_json_path, 'w') as file:
            json.dump(account['login'], file)

        session_config = Session.Configuration.Builder().set_stored_credential_file(session_json_path).build()
        session = Session.Builder(conf=session_config).stored_file(session_json_path).create()
        
        account_type = session.get_user_attribute("type")
        bitrate = "320k" if account_type == "premium" else "160k"
        
        account_pool.append({
            "uuid": uuid_str,
            "username": username,
            "service": "spotify",
            "status": "active",
            "account_type": account_type,
            "bitrate": bitrate,
            "login": {"session": session, "session_path": session_json_path}
        })
        return True
    except Exception as e:
        logger.error(f"Spotify login failed: {e}")
        return False


def spotify_re_init_session(account, max_retries=4, force=False):
    import gc
    session_json_path = os.path.join(cache_dir(), "sessions", f"ots_login_{account['uuid']}.json")
    username = account.get('username', 'unknown')

    logger.info(f"Resetting session for {username}")
    old_session = account.get('login', {}).get('session')
    if old_session:
        try: old_session.close()
        except: pass

    gc.collect()
    time.sleep(1)

    for attempt in range(max_retries):
        try:
            s_config = Session.Configuration.Builder().set_stored_credential_file(session_json_path).build()
            session = Session.Builder(conf=s_config).stored_file(session_json_path).create()
            
            account['login']['session'] = session
            account['status'] = 'active'
            logger.info(f"Session re-initialized for {username}")
            return session
        except Exception as e:
            logger.warning(f"Retry {attempt+1} failed: {e}")
            time.sleep(2)
    
    account['status'] = 'error'
    return None

def spotify_get_token(parsing_index):
    try:
        token = account_pool[parsing_index]['login']['session']
        if not token or isinstance(token, str):
            raise AttributeError
        return token
    except:
        return spotify_re_init_session(account_pool[parsing_index])

def spotify_get_artist_album_ids(token, artist_id):
    items = []
    offset = 0
    limit = 50
    while True:
        headers, _ = _spotify_get_public_api_headers(token, "artist albums")
        url = f'{BASE_URL}/artists/{artist_id}/albums?include_groups=album,single&limit={limit}&offset={offset}'
        data = make_call(url, headers=headers)
        if not data: break
        items.extend(data['items'])
        offset += limit
        if data['total'] <= offset: break
    return [album['id'] for album in items]

def spotify_get_playlist_data(token, playlist_id):
    headers = {"Authorization": f"Bearer {token.tokens().get('user-read-email')}"}
    resp = make_call(f'{BASE_URL}/playlists/{playlist_id}', headers=headers, skip_cache=True)
    img = resp['images'][0]['url'] if resp.get('images') else ''
    return resp['name'], resp['owner']['display_name'], img

def spotify_get_playlist_items(token, playlist_id):
    items = []
    offset = 0
    while True:
        url = f'{BASE_URL}/playlists/{playlist_id}/tracks?offset={offset}&limit=100'
        headers = {"Authorization": f"Bearer {token.tokens().get('user-read-email')}"}
        resp = make_call(url, headers=headers, skip_cache=True)
        if not resp: break
        items.extend(resp['items'])
        offset += 100
        if resp['total'] <= offset: break
    return items

def spotify_get_liked_songs(token):
    items = []
    offset = 0
    while True:
        url = f'{BASE_URL}/me/tracks?offset={offset}&limit=50'
        headers = {"Authorization": f"Bearer {token.tokens().get('user-library-read')}"}
        resp = make_call(url, headers=headers)
        if not resp: break
        items.extend(resp['items'])
        offset += 50
        if resp['total'] <= offset: break
    return items
