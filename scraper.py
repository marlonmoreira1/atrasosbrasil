import logging
from azure.storage.blob import BlobServiceClient
import pandas as pd
from typing import Dict, Callable
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from bs4 import BeautifulSoup
import time
import os
from io import BytesIO
from datetime import datetime, timedelta
import random

# ──────────────────────────────────────────────
# Configurações gerais
# ──────────────────────────────────────────────
MAX_RETRIES = 5          # tentativas por chamada
RETRY_DELAY  = 5         # segundos base entre tentativas
LOAD_MORE_TIMEOUT = 10   # segundos para o botão "carregar mais"
TABLE_TIMEOUT    = 60    # segundos para a tabela aparecer (ads atrasam o render)


# ──────────────────────────────────────────────
# Browser / contexto Playwright (singleton)
# ──────────────────────────────────────────────
_playwright = None
_browser    = None
_page       = None

def get_page():
    """Retorna (e cria, se necessário) a página Playwright reutilizável."""
    global _playwright, _browser, _page
    if _page is None:
        _playwright = sync_playwright().start()
        _browser = _playwright.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = _browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 800},
            locale="pt-BR",
        )
        # Mascara o webdriver para dificultar detecção
        context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        _page = context.new_page()
    return _page


def close_browser():
    global _playwright, _browser, _page
    if _browser:
        _browser.close()
    if _playwright:
        _playwright.stop()
    _browser = _playwright = _page = None


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────
def _jitter(base: float = RETRY_DELAY) -> float:
    """Delay com jitter para parecer mais humano e evitar rate-limit."""
    return base + random.uniform(1, 4)


def fechar_overlay(page):
    """Fecha o banner de cookies / overlay OneTrust, se aparecer."""
    try:
        page.wait_for_selector(".onetrust-pc-dark-filter", timeout=5_000)
        page.click("#onetrust-accept-btn-handler")
        logging.info("Overlay fechado.")
    except Exception:
        logging.debug("Overlay não encontrado ou já dispensado.")


# ──────────────────────────────────────────────
# Coleta principal  (com retry)
# ──────────────────────────────────────────────
def obter_voos(url: str) -> pd.DataFrame:
    """
    Acessa a URL do FlightRadar24, clica em 'carregar mais' até acabar,
    extrai a tabela e retorna um DataFrame.
    Toda a função é envolta em retry para lidar com captcha / falhas de rede.
    """
    page = get_page()

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info(f"[{attempt}/{MAX_RETRIES}] Acessando: {url}")

            page.goto(url, wait_until="domcontentloaded", timeout=45_000)
            time.sleep(_jitter(2))  # pausa inicial "humana"

            fechar_overlay(page)

            # ── Clica em "Load earlier flights" enquanto existir ──
            while True:
                try:
                    btn = page.wait_for_selector(
                        "button.btn.btn-table-action.btn-flights-load",
                        timeout=LOAD_MORE_TIMEOUT * 1_000,
                        state="visible",
                    )
                    btn.scroll_into_view_if_needed()
                    btn.click()
                    time.sleep(_jitter(1))
                except PlaywrightTimeoutError:
                    break  # não há mais botão

            # ── Aguarda qualquer <tr> dentro da tabela aparecer ──
            # Espera pela linha de dados, não pela tabela vazia — mais confiável
            table_selector = "table.table.table-condensed.table-hover.data-table"
            row_selector   = f"{table_selector} tbody tr"
            page.wait_for_selector(
                row_selector,
                timeout=TABLE_TIMEOUT * 1_000,
                state="attached",   # attached = no DOM, não precisa estar visível
            )

            # Pequena pausa extra para demais linhas popularem
            time.sleep(2)

            html_content = page.inner_html(table_selector)
            flights = _parse_table(html_content)

            if not flights:
                raise ValueError("Tabela encontrada mas sem voos — possível captcha ou página vazia.")

            logging.info(f"OK: {len(flights)} voos coletados de {url}")
            return pd.DataFrame(flights)

        except (PlaywrightTimeoutError, ValueError, Exception) as exc:
            logging.warning(f"Tentativa {attempt} falhou para {url}: {exc}")
            # Screenshot de diagnostico para inspecionar o que o bot esta vendo
            try:
                page.screenshot(path=f"debug_attempt_{attempt}.png", full_page=True)
                logging.info(f"Screenshot salvo: debug_attempt_{attempt}.png")
            except Exception:
                pass
            if attempt < MAX_RETRIES:
                wait = _jitter(RETRY_DELAY * attempt)  # back-off exponencial suave
                logging.info(f"Aguardando {wait:.1f}s antes de tentar novamente…")
                time.sleep(wait)
            else:
                logging.error(f"Todas as {MAX_RETRIES} tentativas falharam para {url}. Retornando DataFrame vazio.")
                return pd.DataFrame(columns=[
                    "Time", "Flight", "From", "Airline",
                    "Aircraft", "Status", "Delay_status", "date_flight",
                ])


def _garantir_aba(page, label: str, path_slug: str):
    """
    Garante que a aba correta (Arrivals / Departures) está ativa.
    O FR24 às vezes carrega na aba General mesmo com a URL correta.
    """
    try:
        # Verifica se a tabela já está visível — se sim, aba já está certa
        page.wait_for_selector(
            "table.table.table-condensed.table-hover.data-table",
            timeout=5_000,
            state="visible",
        )
        logging.debug(f"Aba {label} já ativa.")
    except PlaywrightTimeoutError:
        # Tabela não apareceu — tenta clicar na aba manualmente
        logging.info(f"Clicando na aba {label} manualmente…")
        try:
            aba = page.locator(f"a[href*='/{path_slug}']").first
            aba.click()
            time.sleep(_jitter(2))
        except Exception as e:
            logging.warning(f"Não foi possível clicar na aba {label}: {e}")


def _parse_table(html_content: str) -> list:
    """Extrai lista de dicionários de voos a partir do HTML da tabela."""
    soup = BeautifulSoup(html_content, "html.parser")
    table = soup.find(
        "table",
        class_="table table-condensed table-hover data-table m-n-t-15",
    )
    flights = []

    if not table:
        return flights

    tbody = table.find("tbody")
    if not tbody:
        return flights

    for row in tbody.find_all("tr"):
        columns = row.find_all("td")
        if len(columns) <= 1:
            continue

        status_div  = row.find("div", class_="state-block")
        status_color = status_div.get("class")[1] if status_div else "unknown"
        data_date    = row.get("data-date")

        try:
            first_date_obj = datetime.strptime(data_date, "%A, %b %d").replace(
                year=datetime.now().year
            )
            first_date_str = first_date_obj.strftime("%Y-%m-%d")
        except (TypeError, ValueError):
            first_date_str = None

        flights.append({
            "Time":         columns[0].get_text(strip=True),
            "Flight":       columns[1].get_text(strip=True),
            "From":         columns[2].get_text(strip=True),
            "Airline":      columns[3].get_text(strip=True),
            "Aircraft":     columns[4].get_text(strip=True),
            "Status":       columns[6].get_text(strip=True),
            "Delay_status": status_color,
            "date_flight":  first_date_str,
        })

    return flights


# ──────────────────────────────────────────────
# Aeroportos
# ──────────────────────────────────────────────
brazil_airports = {
    "SSA": "Salvador - Aeroporto Internacional de Salvador",
    "GRU": "São Paulo - Aeroporto Internacional de Guarulhos",
    "CGH": "São Paulo - Aeroporto de Congonhas",
    "BSB": "Brasília - Aeroporto Internacional de Brasília",
    "SDU": "Rio de Janeiro - Aeroporto Internacional de Santos Dumont",
    "GIG": "Rio de Janeiro - Aeroporto Internacional do Galeão",
    "CNF": "Belo Horizonte - Aeroporto Internacional de Confins",
    "FOR": "Fortaleza - Aeroporto Internacional Pinto Martins",
    "REC": "Recife - Aeroporto Internacional dos Guararapes",
    "CWB": "Curitiba - Aeroporto Internacional Afonso Pena",
    "BEL": "Belém - Aeroporto Internacional de Belém",
    "MAO": "Manaus - Aeroporto Internacional Eduardo Gomes",
    "VIX": "Vitória - Aeroporto de Vitória",
    "FLN": "Florianópolis - Aeroporto Internacional Hercílio Luz",
    "GYN": "Goiânia - Aeroporto Internacional Santa Genoveva",
    "NAT": "Natal - Aeroporto Internacional Aluízio Alves",
    "MCZ": "Maceió - Aeroporto Internacional Zumbi dos Palmares",
    "CGR": "Campo Grande - Aeroporto Internacional de Campo Grande",
    "SLZ": "São Luís - Aeroporto Internacional de São Luís",
    "CGB": "Cuiabá - Aeroporto Internacional Marechal Rondon",
    "THE": "Teresina - Aeroporto de Teresina",
    "AJU": "Aracaju - Aeroporto de Aracaju",
    "PVH": "Porto Velho - Aeroporto Internacional de Porto Velho",
    "BVB": "Boa Vista - Aeroporto Internacional de Boa Vista",
    "RBR": "Rio Branco - Aeroporto Internacional de Rio Branco",
    "PMW": "Palmas - Aeroporto de Palmas",
    "JPA": "João Pessoa - Aeroporto Internacional Presidente Castro Pinto",
    "POA": "Porto Alegre - Aeroporto Internacional Salgado Filho",
    "MCP": "Aeroporto Internacional de Macapá - Alberto Alcolumbre",
    "VCP": "Campinas - Aeroporto Internacional de Viracopos",
    "BPS": "Porto Seguro - Aeroporto de Porto Seguro",
    "NVT": "Navegantes - Aeroporto Internacional de Navegantes",
    "IGU": "Foz do Iguaçu - Aeroporto Internacional de Foz do Iguaçu",
    "CXJ": "Caxias do Sul - Aeroporto Regional Hugo Cantergiani",
    "LDB": "Londrina - Aeroporto de Londrina",
    "JOI": "Joinville - Aeroporto de Joinville",
    "UDI": "Uberlândia - Aeroporto de Uberlândia",
    "RAO": "Ribeirão Preto - Aeroporto Leite Lopes",
    "MGF": "Maringá - Aeroporto de Maringá",
}


# ──────────────────────────────────────────────
# Orquestração
# ──────────────────────────────────────────────
def collect_data_from_airports(
    airports: Dict[str, str],
    collect_function: Callable[[str], pd.DataFrame],
) -> pd.DataFrame:
    """
    Itera sobre os aeroportos, coleta chegadas e partidas de cada um
    e retorna um DataFrame combinado.
    """
    all_data = []

    for airport, nome in airports.items():
        logging.info(f"Coletando dados para: {airport} - {nome}")

        for tipo, path in [("Chegada", "arrivals"), ("Partida", "departures")]:
            url = f"https://www.flightradar24.com/data/airports/{airport.lower()}/{path}"
            df  = collect_function(url)
            df["Tipo"]      = tipo
            df["Aeroporto"] = nome
            all_data.append(df)
            time.sleep(_jitter(2))  # pausa entre requisições

        logging.info(f"Concluído: {airport} - {nome}\n---")

    return pd.concat(all_data, ignore_index=True)


# ──────────────────────────────────────────────
# Execução principal
# ──────────────────────────────────────────────
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        df_final = collect_data_from_airports(brazil_airports, obter_voos)
    finally:
        close_browser()

    # Filtra apenas voos de ontem
    data_hoje   = datetime.today()
    data_ontem  = data_hoje - timedelta(days=1)
    data_filtro = data_ontem.strftime("%Y-%m-%d")

    voos = df_final[df_final["date_flight"] == data_filtro]

    # ── Upload para Azure Blob Storage ──
    connect_str    = os.environ["CONNECT_STR"]
    container_name = os.environ["CONTAINER_NAME"]

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client    = blob_service_client.get_container_client(container_name)

    parquet_buffer = BytesIO()
    voos.to_parquet(parquet_buffer, index=False)

    blob_name   = f"voos_{data_filtro}_bronze.parquet"
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(parquet_buffer.getvalue(), overwrite=True)

    logging.info(f"Upload concluído: {blob_name}")
