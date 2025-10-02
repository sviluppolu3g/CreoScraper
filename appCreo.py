import os, re, time, io, csv, zipfile, shutil
from urllib.parse import urljoin, urlparse
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import gradio as gr

BASE_URL   = "https://www.creokitchens.it/it/cucine"
SITE_ROOT  = "https://www.creokitchens.it"
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"
REQUEST_TIMEOUT = (10, 25)
SLEEP_BETWEEN_REQUESTS = 0.3
MAX_IMAGE_BYTES = 40 * 1024 * 1024
GLOBAL_PER_PAGE_TIMEOUT = 120

def build_session():
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    retries = Retry(total=4, connect=4, read=4, backoff_factor=0.5,
                    status_forcelist=[429,500,502,503,504],
                    allowed_methods=frozenset(["GET","HEAD"]))
    adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter); s.mount("https://", adapter)
    return s

session = build_session()

def slugify(text, maxlen=80):
    text = re.sub(r"\s+", " ", text or "").strip()
    text = text.replace("/", "-").replace("\\", "-")
    text = re.sub(r"[^0-9A-Za-zÀ-ÖØ-öø-ÿ _\-\.\(\)]", "", text)
    text = text[:maxlen]
    text = re.sub(r"\s+", " ", text).strip()
    return text or "senza_nome"

def get_soup(url):
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200: return None
        return BeautifulSoup(r.text, "html.parser")
    except requests.RequestException:
        return None

def is_kitchen_detail_url(href_abs: str) -> bool:
    try:
        u = urlparse(href_abs)
        if u.netloc != urlparse(SITE_ROOT).netloc or u.query or u.fragment: return False
        path = u.path
        if not path.startswith("/it/cucine/"): return False
        seg = [s for s in path.split("/") if s]
        if len(seg) != 3: return False  # ["it","cucine","slug"]
        if "." in seg[-1]: return False
        return True
    except Exception:
        return False

def head_ok_image(url):
    try:
        hr = session.head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if hr.status_code >= 400: return False
        ct = (hr.headers.get("Content-Type") or "").lower()
        if "image" not in ct: return False
        clen = hr.headers.get("Content-Length")
        if clen and clen.isdigit() and int(clen) > MAX_IMAGE_BYTES: return False
        return True
    except requests.RequestException:
        return False

def infer_ext(url):
    path = urlparse(url).path
    ext = os.path.splitext(path)[1]
    return ext.split("?")[0] if (ext and len(ext) <= 5) else ".jpg"

def download_image(url, dest_path):
    if not head_ok_image(url): return False
    try:
        with session.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
            r.raise_for_status()
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            total = 0
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024*64):
                    if not chunk: continue
                    f.write(chunk); total += len(chunk)
                    if total > MAX_IMAGE_BYTES: return False
        return True
    except requests.RequestException:
        return False

def list_kitchens():
    # ritorna [(nome, url)] ordinati
    listing = get_soup(BASE_URL)
    if not listing: return []
    slug2url = {}
    for a in listing.select("a.gb-item-link"):
        href = a.get("href") or ""
        abs_url = urljoin(BASE_URL, href)
        if is_kitchen_detail_url(abs_url):
            slug = urlparse(abs_url).path.rstrip("/").split("/")[-1]
            slug2url.setdefault(slug, abs_url)
    resolved = []
    for slug, url in slug2url.items():
        s = get_soup(url); time.sleep(0.02)
        name = slugify(s.find("h1").get_text(strip=True)) if (s and s.find("h1")) else slugify(slug)
        resolved.append((name, url))
    resolved.sort(key=lambda t: t[0].lower())
    return resolved

# Carica scelte al load
KITCHENS = list_kitchens()
CHOICES  = [name for name, _ in KITCHENS]
NAME2URL = {name:url for name,url in KITCHENS}

def scrape(selected_names, images_per_kitchen):
    if not selected_names:
        return None, "Seleziona almeno una cucina."
    # cartella temp
    OUT_DIR = "/tmp/creo_cucine"
    ZIP_PATH = "/tmp/creo_cucine.zip"
    if os.path.exists(OUT_DIR): shutil.rmtree(OUT_DIR)
    os.makedirs(OUT_DIR, exist_ok=True)
    if os.path.exists(ZIP_PATH): os.remove(ZIP_PATH)

    log_lines = [f"Avvio: {len(selected_names)} cucine | {images_per_kitchen} immagini/cucina"]

    manifest = []
    for idx, name in enumerate(selected_names, 1):
        url = NAME2URL.get(name)
        if not url:
            log_lines.append(f"- {idx}/{len(selected_names)} {name}: URL non trovato, salto.")
            continue
        start_t = time.time()
        soup = get_soup(url)
        if not soup:
            log_lines.append(f"- {idx}/{len(selected_names)} {name}: pagina non caricata, salto.")
            continue

        h1 = soup.find("h1")
        dirname = slugify(h1.get_text(strip=True)) if h1 else slugify(name)
        kdir = os.path.join(OUT_DIR, dirname)
        os.makedirs(kdir, exist_ok=True)

        # descrizione
        desc_container = soup.select_one(".gb-text-and-link")
        paras = [p.get_text(" ", strip=True) for p in (desc_container.find_all("p") if desc_container else []) if p.get_text(" ", strip=True)]
        desc = "\n\n".join(paras).strip()
        with io.open(os.path.join(kdir, "descrizione.txt"), "w", encoding="utf-8") as f:
            f.write(desc)

        # immagini: href degli <a.gb-item-link> in .gb-media-wrapper
        wrappers = soup.select(".gb-media-wrapper")
        anchors = []
        for w in wrappers:
            anchors.extend(w.select("a.gb-item-link"))

        seen = set(); ordered = []
        for a in anchors:
            href = a.get("href")
            if not href: continue
            abs_url = urljoin(url, href)
            if abs_url.lower().endswith((".jpg", ".jpeg", ".png", ".webp")) and abs_url not in seen:
                seen.add(abs_url)
                ordered.append(abs_url)
            if len(ordered) >= images_per_kitchen:
                break

        saved = 0
        for i, img_url in enumerate(ordered, 1):
            if time.time() - start_t > GLOBAL_PER_PAGE_TIMEOUT:
                break
            dest = os.path.join(kdir, f"{i:02d}{infer_ext(img_url)}")
            if download_image(img_url, dest):
                saved += 1
            time.sleep(0.1)

        manifest.append({"kitchen_name": dirname, "url": url, "description_chars": len(desc), "images_saved": saved})
        log_lines.append(f"- {idx}/{len(selected_names)} {dirname}: trovate {len(ordered)}, salvate {saved}")
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    # manifest & zip
    man_path = os.path.join(OUT_DIR, "manifest.csv")
    with io.open(man_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["kitchen_name","url","description_chars","images_saved"])
        w.writeheader(); w.writerows(manifest)

    with zipfile.ZipFile(ZIP_PATH, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(OUT_DIR):
            for file in files:
                full = os.path.join(root, file)
                rel = os.path.relpath(full, OUT_DIR)
                z.write(full, arcname=os.path.join("creo_cucine", rel))

    log_lines.append(f"ZIP pronto: {ZIP_PATH}")
    return ZIP_PATH, "\n".join(log_lines)

with gr.Blocks(title="Creo Kitchens Scraper") as demo:
    gr.Markdown("### Scarica descrizioni e immagini (full-size) dalle cucine selezionate")
    with gr.Row():
        kitchen_sel = gr.CheckboxGroup(choices=CHOICES, label="Cucine")
        img_num = gr.Slider(1, 12, value=3, step=1, label="Immagini per cucina")
    run_btn = gr.Button("Avvia scraping", variant="primary")
    with gr.Row():
        zip_out = gr.File(label="ZIP risultato")
        log_out = gr.Textbox(label="Log", lines=12)
    run_btn.click(fn=scrape, inputs=[kitchen_sel, img_num], outputs=[zip_out, log_out])

if __name__ == "__main__":
    demo.launch()
