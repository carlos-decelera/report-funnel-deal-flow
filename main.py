import streamlit as st
import pandas as pd
import httpx
import asyncio

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Decelera Funnel Report", layout="wide")

# CSS para mejorar la estética
st.markdown("""
    <style>
    .stDataFrame { border: 1px solid #e6e9ef; border-radius: 10px; }
    h3 { padding-top: 1.5rem; }
    </style>
    """, unsafe_allow_html=True)

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
    if not attr_list: return None
    extracted = []
    for item in attr_list:
        attr_type = item.get("attribute_type", "")
        val = None
        if attr_type == "status": val = item.get("status", {}).get("title")
        elif attr_type == "select": val = item.get("option", {}).get("title")
        elif attr_type == "domain": val = item.get("domain")
        elif attr_type == "location":
            val = ", ".join(filter(None, [item.get("line_1"), item.get("locality"), item.get("country_code")]))
        elif attr_type == "personal-name": val = item.get("full_name")
        elif attr_type == "email-address": val = item.get("email_address")
        elif attr_type in ("text", "number", "date", "timestamp", "checkbox"):
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
    rows = []
    for record in attio_data:
        record_id = record.get("id", {}).get("record_id") or record.get("parent_record_id")
        row = {"record_id": record_id, "created_at": record.get("created_at")}
        values_source = record.get("entry_values", {}) or record.get("values", {})
        for attr_name, attr_list in values_source.items():
            row[attr_name] = extract_value(attr_list)
        rows.append(row)
    return pd.DataFrame(rows)

def get_combined_dataframe_raw():
    """Lógica pura de extracción sin caché."""
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

# --- LÓGICA DE NEGOCIO ---

def asignar_batch_y_prioridad(fila, col_fecha, col_stage):
    fecha = fila[col_fecha]
    stage = str(fila[col_stage])
    f12 = pd.Timestamp(2026, 2, 12).date()
    f14 = pd.Timestamp(2026, 2, 14).date()
    f16 = pd.Timestamp(2026, 2, 16).date()

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

# --- CACHÉ MAESTRO ---

@st.cache_data(ttl=300)
def load_and_clean_data():
    """Obtiene datos de la API y los procesa COMPLETAMENTE."""
    df_clean = get_combined_dataframe_raw()
    
    if df_clean.empty:
        return df_clean

    col_fecha = 'created_at_y'
    col_stage = 'stage'
    
    # 1. Limpieza y Formateo
    df_clean[col_fecha] = pd.to_datetime(df_clean[col_fecha]).dt.date
    df_clean['reference_3'] = df_clean['reference_3'].fillna("Other")
    df_clean['reason'] = df_clean['reason'].fillna("")
    
    # 2. Clasificación (Se hace dentro del caché para que sea instantáneo luego)
    df_clean[['Batch', 'Prioridad']] = df_clean.apply(
        lambda x: pd.Series(asignar_batch_y_prioridad(x, col_fecha, col_stage)), axis=1
    )
    
    # 3. Filtrado y Orden
    df_clean = df_clean[df_clean['Batch'] != "Otros"]
    df_clean = df_clean.sort_values(by=['Prioridad', col_fecha])
    
    return df_clean

# --- HELPERS DE INTERFAZ ---

def calcular_metricas_funnel(sub_df, col_status, col_reason):
    outreach = len(sub_df)
    responded_df = sub_df[sub_df[col_status] != "Contacted"]
    responded = len(responded_df)
    init_scr_df = responded_df[responded_df[col_status] != "Not qualified"]
    init_scr = len(init_scr_df)
    
    pre_comm_mask = (sub_df[col_status] == "Pre-committee") | (sub_df[col_reason] == "Pre-committee")
    deep_dive_mask = pre_comm_mask | ((sub_df[col_status] == "Deep dive") & (sub_df[col_reason] == "Signals (In play)"))
    first_int_mask = deep_dive_mask | (sub_df[col_status].isin(["Stand by", "First interaction"]))
    
    return [outreach, responded, init_scr, len(sub_df[first_int_mask]), len(sub_df[deep_dive_mask]), len(sub_df[pre_comm_mask])]

def style_dataframe(df):
    """Aplica estilo dinámico detectando el ancho real del DataFrame."""
    def apply_row_style(row):
        # Si la fila es la de separación o la de TOTAL
        if "TOTAL" in str(row.iloc[0]) or "---" in str(row.iloc[0]):
            return ['background-color: #f0f2f6; font-weight: bold; color: #31333F'] * len(row)
        return [''] * len(row)
    
    return df.style.apply(apply_row_style, axis=1)

# ==============================================================================
# EJECUCIÓN (UI)
# ==============================================================================

st.title("📊 Reporte de Funnel - Menorca 2026")

# Botón de refresco
if st.sidebar.button("🔄 Refrescar datos de Attio"):
    st.cache_data.clear()
    st.rerun()

# ... (Mantén todas las funciones de extracción y caché anteriores igual)

try:
    with st.spinner("Cargando..."):
        df = load_and_clean_data()

    if df.empty:
        st.warning("No hay datos.")
    else:
        # Variables de control
        col_ref = 'reference_3'
        grupos_referencias = {
            "INVESTMENT": ['Referral', 'Contacted by LinkedIn', 'Event'],
            "MARKETING": ['Mail from Decelera Team', 'Decelera Newsletter', 'Social media (LinkedIn, X, Instagram...)', 'Google', 'Press', 'Other']
        }

        selected_batch = st.sidebar.selectbox("Selecciona un Batch", df['Batch'].unique())
        grupo = df[df['Batch'] == selected_batch]
        st.subheader(f"📍 {selected_batch}")

        def generar_tabla(fuentes):
            filas = []
            columnas = ["Source", "Outreach", "Responded", "Init. Scr.", "First Int.", "Deep Dive", "Pre-comm"]
            
            def fmt(v, p): return f"{v} ({(v/p*100):.0f}%)" if p > 0 else f"{v} (0%)"

            for ref in fuentes:
                subset = grupo[grupo[col_ref] == ref]
                c = calcular_metricas_funnel(subset)
                filas.append([ref, str(c[0]), fmt(c[1],c[0]), fmt(c[2],c[1]), fmt(c[3],c[2]), fmt(c[4],c[3]), fmt(c[5],c[4])])
            
            # Total del bloque
            subset_bloque = grupo[grupo[col_ref].isin(fuentes)]
            c_s = calcular_metricas_funnel(subset_bloque)
            filas.append(["TOTAL GRUPO", str(c_s[0]), fmt(c_s[1],c_s[0]), fmt(c_s[2],c_s[1]), fmt(c_s[3],c_s[2]), fmt(c_s[4],c_s[3]), fmt(c_s[5],c_s[4])])
            
            return pd.DataFrame(filas, columns=columnas)

        # Visualización
        st.markdown("### 💰 Investment Sources")
        st.dataframe(style_dataframe(generar_tabla(grupos_referencias["INVESTMENT"])), use_container_width=True)

        st.markdown("### 📢 Marketing Sources")
        st.dataframe(style_dataframe(generar_tabla(grupos_referencias["MARKETING"])), use_container_width=True)

except Exception as e:
    st.error(f"Error: {e}")