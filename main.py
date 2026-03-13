import pandas as pd
import httpx
import asyncio
import numpy as np
import streamlit as st
# ==============================================================================
# CONSTANTES Y CONFIGURACIÓN DE API ATTIO
# ==============================================================================

ATTIO_API_KEY = st.secrets["ATTIO_API_KEY"]
DEALS_ID = "dbcd94bf-ec33-4f00-a7c8-74f57a559869"
DEAL_FLOW_ID = "54265eb6-d53d-465d-ad35-4e823e135629"
BASE_URL = "https://api.attio.com/v2"
HEADERS = {
    "Authorization": f"Bearer {ATTIO_API_KEY}",
    "Content-Type": "application/json"
}

# ==============================================================================
# FUNCIONES DE EXTRACCIÓN Y TRANSFORMACIÓN (LÓGICA CORE)
# ==============================================================================

def extract_value(attr_list):
    """Extrae el dato real de los valores de atributo de Attio."""
    if not attr_list: return None
    extracted = []
    for item in attr_list:
        attr_type = item.get("attribute_type", "")
        val = None
        if attr_type == "status": val = item.get("status", {}).get("title")
        elif attr_type == "select": val = item.get("option", {}).get("title")
        elif attr_type == "domain": val = item.get("domain")
        elif attr_type == "location":
            val = ", ".join(filter(None, [
                item.get("line_1"), item.get("locality"),
                item.get("region"), item.get("postcode"),
                item.get("country_code"),
            ]))
        elif attr_type == "personal-name": val = item.get("full_name")
        elif attr_type == "email-address": val = item.get("email_address")
        elif attr_type == "phone-number": val = item.get("phone_number")
        elif attr_type == "record-reference": val = item.get("target_record_id")
        elif attr_type == "actor-reference": val = item.get("referenced_actor_id")
        elif attr_type == "interaction": val = item.get("interacted_at")
        elif attr_type == "currency": val = item.get("currency_value")
        elif attr_type in ("text", "number", "date", "timestamp", "checkbox", "rating"):
            val = item.get("value")
        else: val = item.get("value")
        if val is not None: extracted.append(str(val))
    return extracted[0] if len(extracted) == 1 else extracted or None

async def fetch_data(client, url, payload=None):
    all_data = []
    limit, offset = 100, 0
    while True:
        current_payload = payload.copy() if payload else {}
        current_payload.update({"limit": limit, "offset": offset})
        response = await client.post(url, headers=HEADERS, json=current_payload)
        response.raise_for_status()
        data = response.json().get("data", [])
        all_data.extend(data)
        if len(data) < limit: break
        offset += limit
    return all_data

def transform_attio_to_df(attio_data):
    """Convierte la respuesta JSON de Attio en un DataFrame de Pandas."""
    rows = []
    for record in attio_data:
        record_id = record.get("id", {}).get("record_id") or record.get("parent_record_id")
        row = {"record_id": record_id, "created_at": record.get("created_at")}
        values_source = record.get("entry_values", {}) or record.get("values", {})
        for attr_name, attr_list in values_source.items():
            row[attr_name] = extract_value(attr_list)
        rows.append(row)
    return pd.DataFrame(rows)

def get_combined_dataframe():
    async def run_parallel_fetches():
        async with httpx.AsyncClient(timeout=60.0) as client:
            records_task = fetch_data(client, f"{BASE_URL}/objects/{DEALS_ID}/records/query", 
                                    payload={"$or": [{"stage": "Menorca 2026"}, {"stage": "Leads Menorca 2026"}]})
            entries_task = fetch_data(client, f"{BASE_URL}/lists/{DEAL_FLOW_ID}/entries/query")
            return await asyncio.gather(records_task, entries_task)

    raw_records, raw_entries = asyncio.run(run_parallel_fetches())
    df_rec = transform_attio_to_df(raw_records)
    df_ent = transform_attio_to_df(raw_entries)
    
    if df_rec.empty or df_ent.empty: return pd.DataFrame()
    return pd.merge(df_rec, df_ent, on="record_id")

# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Decelera Funnel Report", layout="wide")

# CSS para mejorar la estética general
st.markdown("""
    <style>
    .stDataFrame { border: 1px solid #e6e9ef; border-radius: 10px; }
    h3 { padding-top: 1.5rem; }
    </style>
    """, unsafe_allow_html=True)

st.title("📊 Reporte de Funnel - Menorca 2026")

# --- 2. VARIABLES Y PARÁMETROS ---
df = get_combined_dataframe()
col_fecha = 'created_at_y'
col_ref = 'reference_3'
col_status = 'status'
col_stage = 'stage'
col_reason = 'reason'

orden_referencias = [
    'Referral', 'Contacted by LinkedIn', 'Event', 
    'Mail from Decelera Team', 'Decelera Newsletter', 
    'Social media (LinkedIn, X, Instagram...)', 'Google', 'Press', 'Other'
]

orden_status = ["Contacted", "Initial screening", "First interaction", "Deep dive", "Pre-committee", "Not qualified", "Killed"]

grupos_referencias = {
    "INVESTMENT": ['Referral', 'Contacted by LinkedIn', 'Event'],
    "MARKETING": [
        'Mail from Decelera Team', 'Decelera Newsletter', 
        'Social media (LinkedIn, X, Instagram...)', 'Google', 'Press'
    ]
}

# --- 3. PROCESAMIENTO DE DATOS ---
@st.cache_data
def load_and_clean_data(ttl=300):
    # Usamos la función que ya tienes definida en tu entorno
    df_clean = get_combined_dataframe() 
    df_clean[col_fecha] = pd.to_datetime(df_clean[col_fecha]).dt.date
    df_clean[col_ref] = df_clean[col_ref].fillna("Other")
    df_clean[col_reason] = df_clean[col_reason].fillna("")
    df_clean[col_ref] = pd.Categorical(df_clean[col_ref], categories=orden_referencias, ordered=True)
    df_clean[col_status] = pd.Categorical(df_clean[col_status], categories=orden_status, ordered=True)
    return df_clean

def asignar_batch_y_prioridad(fila):
    fecha = fila[col_fecha]
    stage = str(fila[col_stage])
    f12, f14, f16 = pd.Timestamp(2026, 2, 12).date(), pd.Timestamp(2026, 2, 14).date(), pd.Timestamp(2026, 2, 16).date()

    if stage == "Leads Menorca 2026" and fecha == f16:
        return "0. MIGRACIÓN INICIAL (16 Feb - Menorca)", 0
    if f12 <= fecha <= f14:
        return "1. BATCH (Feb 12 - Feb 14)", 1
    if fecha >= f16:
        dias = (fecha - f16).days
        num_sem = dias // 7
        ini = f16 + pd.Timedelta(days=num_sem * 7)
        fin = ini + pd.Timedelta(days=6)
        return f"Semana {num_sem + 2}: ({ini.strftime('%d %b')} - {fin.strftime('%d %b')})", (num_sem + 2)
    return "Otros", 99

def calcular_metricas_funnel(sub_df):
    outreach = len(sub_df)
    responded_df = sub_df[sub_df[col_status] != "Contacted"]
    responded = len(responded_df)
    init_scr_df = responded_df[responded_df[col_status] != "Not qualified"]
    init_scr = len(init_scr_df)
    pre_comm_mask = (sub_df[col_status] == "Pre-committee") | (sub_df[col_reason] == "Pre-committee")
    deep_dive_mask = pre_comm_mask | ((sub_df[col_status] == "Deep dive") & (sub_df[col_reason] == "Signals (In play)"))
    first_int_mask = deep_dive_mask | (sub_df[col_status].isin(["Stand by", "First interaction"]))
    
    return [outreach, responded, init_scr, len(sub_df[first_int_mask]), len(sub_df[deep_dive_mask]), len(sub_df[pre_comm_mask])]

def format_pct(val, prev):
    if prev <= 0: return f"{val} (0%)"
    return f"{val} ({(val / prev) * 100:.0f}%)"

# --- 4. FUNCIÓN DE ESTILO (NUEVA) ---
def style_dataframe(df):
    """Aplica color gris a las filas de TOTAL y negrita."""
    def apply_row_style(row):
        if "TOTAL" in str(row["Source"]):
            return ['background-color: #f0f2f6; font-weight: bold; color: #31333F'] * len(row)
        return [''] * len(row)
    
    return df.style.apply(apply_row_style, axis=1)

# --- 5. EJECUCIÓN PRINCIPAL ---
if st.sidebar.button("🔄 Refrescar datos ahora"):
    st.cache_data.clear()
    st.rerun()

try:
    df = load_and_clean_data()
    df[['Batch', 'Prioridad']] = df.apply(lambda x: pd.Series(asignar_batch_y_prioridad(x)), axis=1)
    df = df[df['Batch'] != "Otros"]
    df = df.sort_values(by=['Prioridad', col_fecha])

    if df.empty:
        st.warning("No hay datos disponibles para los criterios seleccionados.")
    else:
        # Sidebar
        batch_list = df['Batch'].unique()
        selected_batch = st.sidebar.selectbox("Selecciona un Batch/Semana", batch_list)

        grupo = df[df['Batch'] == selected_batch]
        st.subheader(f"📍 {selected_batch}")

        def generar_tabla_bloque(fuentes):
            filas = []
            columnas = ["Source", "Outreach", "Responded", "Init. Scr.", "First Int.", "Deep Dive", "Pre-comm"]
            
            for ref in fuentes:
                subset = grupo[grupo[col_ref] == ref]
                if len(subset) == 0:
                    counts = ["0", "0 (0%)", "0 (0%)", "0 (0%)", "0 (0%)", "0 (0%)"]
                else:
                    c = calcular_metricas_funnel(subset)
                    counts = [str(c[0])] + [format_pct(c[i], c[i-1]) for i in range(1, len(c))]
                filas.append([ref] + counts)
            
            subset_bloque = grupo[grupo[col_ref].isin(fuentes)]
            c_s = calcular_metricas_funnel(subset_bloque)
            fila_subtotal = ["TOTAL GRUPO"] + [str(c_s[0])] + [format_pct(c_s[i], c_s[i-1]) for i in range(1, len(c_s))]
            filas.append(fila_subtotal)
            
            return pd.DataFrame(filas, columns=columnas)

        # Mostrar Tablas con el nuevo estilo
        st.markdown("### 💰 Investment Sources")
        df_inv = generar_tabla_bloque(grupos_referencias["INVESTMENT"])
        st.dataframe(style_dataframe(df_inv), use_container_width=True, hide_index=True)

        st.markdown("### 📢 Marketing Sources")
        df_mkt = generar_tabla_bloque(grupos_referencias["MARKETING"])
        st.dataframe(style_dataframe(df_mkt), use_container_width=True, hide_index=True)

        # Total Batch
        st.divider()
        c_b = calcular_metricas_funnel(grupo)
        totales = {
            "Métrica": ["Outreach", "Responded", "Init. Screening", "First Int.", "Deep Dive", "Pre-committee"],
            "Cantidad": [c_b[0], c_b[1], c_b[2], c_b[3], c_b[4], c_b[5]],
            "Conversión": ["-"] + [f"{(c_b[i]/c_b[i-1]*100):.1f}%" if c_b[i-1]>0 else "0%" for i in range(1,6)]
        }
        st.markdown(f"### 🏆 TOTAL {selected_batch}")
        st.dataframe(pd.DataFrame(totales), use_container_width=True, hide_index=True)

except Exception as e:
    st.error(f"Error al procesar el reporte: {e}")