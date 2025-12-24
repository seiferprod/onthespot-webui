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

# Cache pour le token Spotify Web API
_spotify_app_token = {"access_token": None, "expires_at": 0}
_spotify_app_token_lock = threading.Lock()

def _mask_value(value, visible=6):
    if not value: return ""
    return f"...{value[-visible:]}" if len(value) > visible else value

def _spotify_get_app_access_token():
    """Récupère le token via tes clés API perso stockées dans la config."""
    cfg_client_id = config.get("spotify_client_id", "").strip()
    cfg_client_secret = config.get("spotify_client_secret", "").strip()

    if not cfg_client_id:
        for acc in config.get('accounts', []):
            if acc.get('service') == 'spotify' and acc.get('client_id'):
                cfg_client_id = acc.get('client_id')
                cfg_client_secret = acc.get('client_secret')
                break

    if not cfg_client_id or not cfg_client_secret:
        return None

    now = time.time()
    with _spotify_app_token_lock:
        if _spotify_app_token["access_token"] and _spotify_app_token["expires_at"] > now + 30:
            return _spotify_app_token["access_token"]
        try:
            resp = requests.post("https://accounts.spotify.com/api/token",
                data={"grant_type": "client_credentials"},
                auth=(cfg_client_id, cfg_client_secret), timeout=10)
            data = resp.json()
            _spotify_app_token["access_token"] = data.get("access_token")
            _spotify_app_token["expires_at"] = now + int(data.get("expires_in", 0))
            return _spotify_app_token["access_token"]
        except Exception:
            return None

def _spotify_get_public_api_headers(token, context):
    app_token = _spotify_get_app_access_token()
    if app_token:
        return {"Authorization": f"Bearer {app_token}"}, "app"
    return {"Authorization": f"Bearer {token.tokens().get('user-read-email')}"}, "session"

# --- FONCTIONS INDISPENSABLES POUR WEB.PY ---

def spotify_get_track_metadata(token, track_id):
    headers, _ = _spotify_get_public_api_headers(token, "track metadata")
    return make_call(f"{BASE_URL}/tracks/{track_id}", headers=headers)

def spotify_get_podcast_episode_metadata(token, episode_id):
    headers, _ = _spotify_get_public_api_headers(token, "episode metadata")
    return make_call(f"{BASE_URL}/episodes/{episode_id}", headers=headers)

def spotify_search(token, query, types, limit=10):
    headers, _ = _spotify_get_public_api_headers(token, "search")
    params = {'q': query, 'type': types, 'limit': limit}
    return make_call(f"{BASE_URL}/search", params=params, headers=headers)

def _spotify_extract_year(value):
    if not value: return None
    match = re.search(r'(\d{4})', str(value))
    return int(match.group(1)) if match else None

def spotify_get_playlist_updated_year(headers, playlist_id, tracks_total):
    if not isinstance(tracks_total, int) or tracks_total <= 0: return None
    def _f(offset):
        r = make_call(f"{BASE_URL}/playlists/{playlist_id}/tracks",
                      params={'offset': str(max(0, offset)), 'limit': '1', 'fields': 'items(added_at)'},
                      headers=headers, skip_cache=True)
        return r['items'][0].get('added_at') if r and r.get('items') else None
    years = [y for y in [_spotify_extract_year(_f(0)), _spotify_extract_year(_f(tracks_total-1))] if y]
    return str(max(years)) if years else None

class MirrorSpotifyPlayback:
    def __init__(self):
        self.thread = None
        self.is_running = False
    def start(self):
        if self.thread is None:
            self.is_running = True
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()
    def stop(self):
        self.is_running = False
    def run(self):
        from ..accounts import get_account_token
        while self.is_running:
            time.sleep(10)
            try:
                t_obj = get_account_token('spotify')
                r = requests.get(f"{BASE_URL}/me/player/currently-playing", 
                                 headers={"Authorization": f"Bearer {t_obj.tokens().get('user-read-currently-playing')}"})
                if r.status_code == 200:
                    tid = r.json()['item']['id']
                    with pending_lock:
                        if tid not in pending:
                            pending[tid] = {'local_id': tid, 'item_service': 'spotify', 'item_type': 'track', 'item_id': tid, 'parent_category': 'track'}
            except: continue

def spotify_new_session():
    os.makedirs(os.path.join(cache_dir(), 'sessions'), exist_ok=True)
    uuid_uniq = str(uuid.uuid4())
    path = os.path.join(cache_dir(), 'sessions', f"ots_login_{uuid_uniq}.json")
    custom_id = ""
    for acc in config.get('accounts', []):
        if acc.get('service') == 'spotify' and acc.get('client_id'):
            custom_id = acc.get('client_id')
            break
    cid = custom_id if custom_id else "65b708073fc0480ea92a077233ca87bd"
    ZeroconfServer._ZeroconfServer__default_get_info_fields['clientID'] = cid
    zs = ZeroconfServer.Builder().set_device_name('OnTheSpot').set_stored_credentials_file(path).create()
    while not zs.has_valid_session(): time.sleep(1)
    with open(path, 'r') as f: zl = json.load(f)
    cfg = config.get('accounts').copy()
    cfg.append({"uuid": uuid_uniq, "service": "spotify", "active": True, "client_id": custom_id, "login": zl})
    zs.close()
    config.set('accounts', cfg); config.save()
    return True

def spotify_login_user(account):
    try:
        u = account['uuid']
        path = os.path.join(cache_dir(), "sessions", f"ots_login_{u}.json")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f: json.dump(account['login'], f)
        sess = Session.Builder(conf=Session.Configuration.Builder().set_stored_credential_file(path).build()).stored_file(path).create()
        at = sess.get_user_attribute("type")
        account_pool.append({"uuid": u, "username": account['login']['username'], "service": "spotify", "status": "active", 
                             "account_type": at, "bitrate": "320k" if at == "premium" else "160k", "login": {"session": sess, "session_path": path}})
        return True
    except: return False

def spotify_re_init_session(account):
    path = os.path.join(cache_dir(), "sessions", f"ots_login_{account['uuid']}.json")
    try:
        s = Session.Builder(conf=Session.Configuration.Builder().set_stored_credential_file(path).build()).stored_file(path).create()
        account['login']['session'], account['status'] = s, 'active'
        return s
    except: return None

def spotify_get_token(idx):
    try:
        t = account_pool[idx]['login']['session']
        if not t or isinstance(t, str): raise AttributeError
        return t
    except: return spotify_re_init_session(account_pool[idx])

def spotify_get_artist_album_ids(token, artist_id):
    items, offset = [], 0
    while True:
        h, _ = _spotify_get_public_api_headers(token, "artist albums")
        d = make_call(f'{BASE_URL}/artists/{artist_id}/albums?include_groups=album,single&limit=50&offset={offset}', headers=h)
        if not d or not d.get('items'): break
        items.extend(d['items']); offset += 50
        if d['total'] <= offset: break
    return [a['id'] for a in items]

def spotify_get_playlist_data(token, pid):
    h = {"Authorization": f"Bearer {token.tokens().get('user-read-email')}"}
    r = make_call(f'{BASE_URL}/playlists/{pid}', headers=h, skip_cache=True)
    return r['name'], r['owner']['display_name'], (r['images'][0]['url'] if r.get('images') else '')

def spotify_get_playlist_items(token, pid):
    items, offset = [], 0
    while True:
        h = {"Authorization": f"Bearer {token.tokens().get('user-read-email')}"}
        r = make_call(f'{BASE_URL}/playlists/{pid}/tracks?offset={offset}&limit=100', headers=h, skip_cache=True)
        if not r or not r.get('items'): break
        items.extend(r['items']); offset += 100
        if r['total'] <= offset: break
    return items

def spotify_get_liked_songs(token):
    items, offset = [], 0
    while True:
        h = {"Authorization": f"Bearer {token.tokens().get('user-library-read')}"}
        r = make_call(f'{BASE_URL}/me/tracks?offset={offset}&limit=50', headers=h)
        if not r or not r.get('items'): break
        items.extend(r['items']); offset += 50
        if r['total'] <= offset: break
    return items
