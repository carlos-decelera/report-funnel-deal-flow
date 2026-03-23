import streamlit as st
import pandas as pd
import httpx
import asyncio
import plotly.express as px

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

MAPA_OWNERS = {
        '7f0c4189-764d-453a-8d6b-e416adf7583b': 'Raquel Polgrabia',
        '7f35b25b-4398-4f28-bcf3-1bf59c2b04d4': 'Alejandro Perez',
        '8bd199e1-4aac-485c-b70f-a9b7679286d1': 'Diego Navarro',
        '648bf97f-8d29-4965-ab20-6b4cc63f37ee': 'Carlota L',
        'c8d13743-d7e8-4e9e-b967-3d8e6ac3750e': 'Lorenzo Hurtado de Saracho',
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
        elif attr_type == "actor-reference":
            val = item.get("referenced_actor_id")
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
    df_clean = get_combined_dataframe_raw()
    
    if df_clean.empty:
        return df_clean

    col_fecha = 'created_at_y'
    col_stage = 'stage'
    
    # 1. Limpieza
    df_clean[col_fecha] = pd.to_datetime(df_clean[col_fecha]).dt.date
    df_clean['reference_3'] = df_clean['reference_3'].fillna("Other")
    df_clean['reason'] = df_clean['reason'].fillna("")

    if 'owner' in df_clean.columns:
        df_clean['owner_name'] = df_clean['owner'].map(MAPA_OWNERS).dropna()
    else:
        df_clean['owner_name'] = "No definido"
    
    # 2. Clasificación original
    df_clean[['Batch', 'Prioridad']] = df_clean.apply(
        lambda x: pd.Series(asignar_batch_y_prioridad(x, col_fecha, col_stage)), axis=1
    )
    
    # 3. Filtrar "Otros" antes de crear el Total
    df_clean = df_clean[df_clean['Batch'] != "Otros"]

    # --- NUEVA LÓGICA: Crear el registro "TOTAL" ---
    df_total = df_clean.copy()
    df_total['Batch'] = "🌍 TOTAL ACUMULADO"
    df_total['Prioridad'] = -1  # Para que salga el primero al ordenar
    
    # Combinamos ambos
    df_final = pd.concat([df_total, df_clean], ignore_index=True)
    
    # 4. Ordenar por Prioridad
    df_final = df_final.sort_values(by=['Prioridad', col_fecha])
    
    return df_final

# ---- HELPERS DE TIEMPO EN STATUS -----

async def fetch_status_history(client, entry_id):
    """Consulta el historial de cambios del atributo 'status' para un registro."""
    url = f"{BASE_URL}/lists/{DEAL_FLOW_ID}/entries/{entry_id}/attributes/status/values"
    params = {"show_historic": "true"}
    response = await client.get(url, headers=HEADERS, params=params)
    if response.status_code != 200: return []
    return response.json().get("data", [])

@st.cache_data(ttl=600)
def get_avg_time_per_status_cached(entry_data_list):
    """
    entry_data_list: debe ser una lista de tuplas [(entry_id, owner_id), ...]
    """
    async def fetch_all_histories():
        all_durations = []
        CHUNK_SIZE = 50 
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            for i in range(0, len(entry_data_list), CHUNK_SIZE):
                batch = entry_data_list[i:i + CHUNK_SIZE]
                # batch[0] es entry_id, batch[1] es owner_id
                tasks = [fetch_status_history(client, item[0]) for item in batch]
                histories = await asyncio.gather(*tasks)

                for idx, history in enumerate(histories):
                    owner_id = batch[idx][1] # Recuperamos el owner de esta empresa
                    owner_name = MAPA_OWNERS.get(owner_id)
                    
                    for value in history:
                        status_title = value.get("status", {}).get("title")
                        start = pd.to_datetime(value["active_from"])
                        end = pd.to_datetime(value["active_until"]) if value.get("active_until") else pd.to_datetime("now", utc=True)
                        
                        duration = (end - start).total_seconds() / 86400
                        if status_title:
                            all_durations.append({
                                "Status": status_title, 
                                "Days": duration,
                                "Owner": owner_name
                            })
        return all_durations

    data = asyncio.run(fetch_all_histories())
    if not data: return pd.DataFrame()
    
    df_durations = pd.DataFrame(data)
    # Agrupamos por Status Y por Owner
    return df_durations.groupby(["Status", "Owner"])["Days"].mean().reset_index()

# --- HELPERS DE INTERFAZ ---

def calcular_metricas_funnel(sub_df):
    # Definimos las columnas aquí dentro para no tener que pasarlas como argumentos
    col_status = 'status'
    col_reason = 'reason'
    
    outreach = len(sub_df)
    responded_df = sub_df[(sub_df[col_status] != "Contacted") | (sub_df[col_reason] == "Did not answer")]
    responded = len(responded_df)
    
    pre_comm_mask = (sub_df[col_status] == "Pre-committee") | (sub_df[col_reason] == "Pre-comitee")
    deep_dive_mask = pre_comm_mask | ((sub_df[col_status] == "Deep dive") | (sub_df[col_reason] == "Signals (In play)") | (sub_df[col_status] == "Invested"))
    first_int_mask = deep_dive_mask | (sub_df[col_status].isin(["Stand by", "First interaction"]))
    init_scr_mask = first_int_mask | (sub_df[col_status] == "Initial screening") | (sub_df[col_reason] == "Signals (Qualified)")
    
    return [outreach, responded, len(sub_df[init_scr_mask]), len(sub_df[first_int_mask]), len(sub_df[deep_dive_mask]), len(sub_df[pre_comm_mask])]

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

        st.subheader(f"Estado actual: {selected_batch}")

        counts = grupo["status"].value_counts()

        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Contacted", counts.get("Contacted", 0))
        with col2:
            st.metric("Initial Screening", counts.get("Initial screening", 0))
        with col3:
            st.metric("First Interaction", counts.get("First interaction", 0))
        with col4:
            st.metric("Deep Dive", counts.get("Deep dive", 0))
        with col5:
            st.metric("Pre-committee", counts.get("Pre-committee", 0))

        st.divider()

        # --- CREACIÓN DE PESTAÑAS ---
        tab_control, tab_detalle = st.tabs(["🎯 Dashboard Control", "📊 Reporte Detallado"])

        # --- PESTAÑA 1: DASHBOARD CONTROL ---

        with tab_control:
            st.subheader(f"Control de Conversión - {selected_batch}")

            # 1. Preparar datos por grupos
            def get_metrics_dict(df_filtro):
                c = calcular_metricas_funnel(df_filtro)
                # c = [outreach, responded, init_scr, first_int, deep_dive, pre_comm]
                return {
                    "Outreach": c[0],
                    "Responded": c[1],
                    "Deep Dive": c[4],
                    "Pre-committee": c[5]
                }

            # Filtrar dataframes por canal
            df_inv = grupo[grupo[col_ref].isin(grupos_referencias["INVESTMENT"])]
            df_mkt = grupo[grupo[col_ref].isin(grupos_referencias["MARKETING"])]
            # 'Other' son los que no están en ninguna de las listas anteriores
            df_oth = grupo[~grupo[col_ref].isin(grupos_referencias["INVESTMENT"] + grupos_referencias["MARKETING"])]

            m_inv = get_metrics_dict(df_inv)
            m_mkt = get_metrics_dict(df_mkt)
            m_oth = get_metrics_dict(df_oth)
            m_tot = get_metrics_dict(grupo)

            # 2. Construir la matriz de datos (similar a la foto)
            # Función para formatear valor + % (CVR respecto a la fila anterior del funnel seleccionado)
            def fmt_val(val, prev):
                if prev > 0:
                    return f"{val} ({int(val/prev*100)}%)"
                return f"{val} (0%)"

            data_matriz = {
                "Etapa": ["Outreach", "Responded", "Deep Dive", "Pre-committee"],
                "Inversión": [
                    str(m_inv["Outreach"]),
                    fmt_val(m_inv["Responded"], m_inv["Outreach"]),
                    fmt_val(m_inv["Deep Dive"], m_inv["Responded"]),
                    fmt_val(m_inv["Pre-committee"], m_inv["Deep Dive"])
                ],
                "Marketing": [
                    str(m_mkt["Outreach"]),
                    fmt_val(m_mkt["Responded"], m_mkt["Outreach"]),
                    fmt_val(m_mkt["Deep Dive"], m_mkt["Responded"]),
                    fmt_val(m_mkt["Pre-committee"], m_mkt["Deep Dive"])
                ],
                "Other": [
                    str(m_oth["Outreach"]),
                    fmt_val(m_oth["Responded"], m_oth["Outreach"]),
                    fmt_val(m_oth["Deep Dive"], m_oth["Responded"]),
                    fmt_val(m_oth["Pre-committee"], m_oth["Deep Dive"])
                ],
                "TOTAL": [
                    str(m_tot["Outreach"]),
                    fmt_val(m_tot["Responded"], m_tot["Outreach"]),
                    fmt_val(m_tot["Deep Dive"], m_tot["Responded"]),
                    fmt_val(m_tot["Pre-committee"], m_tot["Deep Dive"])
                ]
            }

            df_matriz = pd.DataFrame(data_matriz)

            # 3. Mostrar la tabla con estilo
            st.markdown("### 📊 Matriz de Rendimiento por Canal")
            st.table(df_matriz)

            # --- SECCIÓN: TIEMPO MEDIO POR OWNER ---
            st.divider()
            st.markdown("### ⏱️ Tiempo Medio por Estado y Owner")
            
            with st.spinner("Analizando historial por equipo..."):
                # Preparamos los datos: lista de (id_entrada, id_owner)
                # IMPORTANTE: Cambia 'owner' por el nombre real de tu columna de owner
                col_tecnica_owner = 'owner' 
                
                datos_input = grupo[['entry_id', col_tecnica_owner]].dropna().values.tolist()
                
                if not datos_input:
                    st.info("No hay datos suficientes para segmentar por Owner.")
                else:
                    df_tiempos_owner = get_avg_time_per_status_cached(datos_input)

                    if df_tiempos_owner.empty:
                        st.info("No se encontró historial para este grupo.")
                    else:
                        # Ordenar funnel
                        orden_funnel = ["Contacted", "Initial screening", "First interaction", "Deep dive", "Pre-committee"]
                        df_tiempos_owner['Status'] = pd.Categorical(df_tiempos_owner['Status'], categories=orden_funnel, ordered=True)
                        df_tiempos_owner = df_tiempos_owner.sort_values(['Status', 'Owner']).dropna()

                        # Gráfica de barras agrupadas (barmode='group')
                        fig_owner_time = px.bar(
                            df_tiempos_owner,
                            x='Status',
                            y='Days',
                            color='Owner', # Diferenciamos por color de Owner
                            barmode='group', # Las pone una al lado de la otra
                            text=df_tiempos_owner['Days'].apply(lambda x: f"{x:.1f}"),
                            color_discrete_sequence=px.colors.qualitative.Bold
                        )
                        
                        fig_owner_time.update_layout(
                            xaxis_title=None,
                            yaxis_title="Días Promedio",
                            legend_title="Responsable",
                            height=500
                        )
                        
                        st.plotly_chart(fig_owner_time, use_container_width=True)

            # --- GRÁFICO PIE CHART: ORIGEN DE DEEP DIVES ---
            st.divider()
            st.markdown("### 🎯 Origen de compañías en Deep Dives")

            # 1. Limpieza de nulos en reference_3 antes de filtrar
            grupo['reference_3'] = grupo['reference_3'].fillna("Other")

            # 2. Filtro CORREGIDO (Agrupando el OR con paréntesis)
            # Queremos: Que esté en el Batch seleccionado Y (que sea status Deep Dive O reason Signals)
            mask_deep_dive = (
                (grupo['status'] == "Deep dive") | 
                (grupo['reason'] == "Signals (In play)") |
                (grupo['status'] == "Pre-committee") |
                (grupo['reason'] == "Pre-comitee") |
                (grupo['status'] == "Invested")
            )
            
            df_deep_dives = grupo[mask_deep_dive].copy()

            # Doble check: Si reference_3 es un string vacío, poner Other
            df_deep_dives['reference_3'] = df_deep_dives['reference_3'].replace("", "Other")

            if df_deep_dives.empty:
                st.info("No hay datos de Deep Dive que coincidan con los filtros.")
            else:
                # 3. Agrupar y contar
                df_pie = df_deep_dives['reference_3'].value_counts().reset_index()
                df_pie.columns = ['Fuente', 'Cantidad']

                # 4. Gráfico interactivo
                import plotly.express as px
                
                fig = px.pie(
                    df_pie, 
                    values='Cantidad', 
                    names='Fuente', 
                    hole=0.5,
                    color_discrete_sequence=px.colors.qualitative.Safe # Colores más profesionales
                )

                fig.update_traces(
                    textinfo='percent+value',  # Muestra el % y el número absoluto
                    textposition='inside'      # Fuerza el texto dentro de las porciones
                )
                
                fig.update_layout(
                    showlegend=True,
                    legend=dict(
                        orientation="v",      # Vertical
                        yanchor="top",
                        y=1,
                        xanchor="left",
                        x=1.05                # Lo mueve a la derecha del gráfico
                    ),
                    margin=dict(l=0, r=100, t=30, b=0) # Añadimos margen derecho para la leyenda
                )

                st.plotly_chart(fig, use_container_width=True)
                
            # --- GRÁFICO PIE CHART: MOTIVOS DE DESCARTE (REASON) ---
            st.divider()
            st.markdown("### ❌ Motivos de Descarte")

            # 1. Filtramos para obtener solo los registros que tienen una razón especificada
            # Excluimos vacíos para que el gráfico sea relevante
            df_reasons = grupo[grupo['reason'].str.strip() != ""].copy()

            if df_reasons.empty:
                st.info("No hay datos de 'reason' para mostrar en este Batch.")
            else:
                # 2. Agrupar y contar
                df_pie_reason = df_reasons['reason'].value_counts().reset_index()
                df_pie_reason.columns = ['Motivo', 'Cantidad']

                # 3. Gráfico con escala de rojos
                # Usamos px.colors.sequential.Reds para los tonos rojos
                fig_reason = px.pie(
                    df_pie_reason, 
                    values='Cantidad', 
                    names='Motivo', 
                    hole=0.5,
                    color_discrete_sequence=px.colors.sequential.Reds_r # _r para que los más comunes sean más oscuros
                )

                fig_reason.update_traces(
                    textinfo='percent+value',  # Muestra el % y el número absoluto
                    textposition='inside'      # Fuerza el texto dentro de las porciones
                )
                
                fig_reason.update_layout(
                    showlegend=True,
                    legend=dict(
                        orientation="v",      # Vertical
                        yanchor="top",
                        y=1,
                        xanchor="left",
                        x=1.05                # Lo mueve a la derecha del gráfico
                    ),
                    margin=dict(l=0, r=100, t=30, b=0) # Añadimos margen derecho para la leyenda
                )

                st.plotly_chart(fig_reason, use_container_width=True)

                # --- NUEVA GRÁFICA: ANÁLISIS DE RED FLAGS ---
                st.divider()
                st.markdown("### 🚩 Análisis de Red Flags")

                # 1. Filtrar registros que tengan red flags (no nulos ni vacíos)
                df_flags = grupo[grupo['red_flags_form_7'].fillna("").str.strip() != ""].copy()

                if df_flags.empty:
                    st.info("No se han registrado Red Flags en este Batch.")
                else:
                    # 2. Lógica para procesar el texto: 
                    # Separar por salto de línea -> Expandir a filas individuales -> Limpiar espacios
                    all_flags = (
                        df_flags['red_flags_form_7']
                        .str.split('\n')
                        .explode()
                        .str.strip()
                    )
                    
                    # Filtrar posibles strings vacíos tras el split
                    all_flags = all_flags[all_flags != ""]

                    if not all_flags.empty:
                        # 3. Contar ocurrencias y calcular % manualmente
                        df_counts = all_flags.value_counts().reset_index()
                        df_counts.columns = ['Red Flag', 'Frecuencia']
                        
                        total_flags = df_counts['Frecuencia'].sum()
                        # Creamos una columna de texto ya formateada para evitar errores de Plotly
                        df_counts['Etiqueta'] = df_counts.apply(
                            lambda x: f"{int(x['Frecuencia'])}<br>{(x['Frecuencia']/total_flags*100):.1f}%", 
                            axis=1
                        )
                        
                        df_counts = df_counts.sort_values(by='Frecuencia', ascending=False)

                        # 4. Crear gráfico
                        fig_flags = px.bar(
                            df_counts,
                            x='Red Flag',
                            y='Frecuencia',
                            text='Etiqueta', # Usamos nuestra columna pre-calculada
                            color='Frecuencia',
                            color_continuous_scale='Reds'
                        )

                        # 5. Configurar visualización de las etiquetas
                        fig_flags.update_traces(
                            textposition='outside',
                            textfont=dict(size=11),
                            cliponaxis=False
                        )

                        # 6. Layout
                        fig_flags.update_layout(
                            showlegend=False,
                            coloraxis_showscale=False,
                            height=600, 
                            xaxis_title=None,
                            yaxis_title="Frecuencia",
                            xaxis=dict(tickangle=45, automargin=True),
                            margin=dict(l=50, r=50, t=50, b=150) # Aumentado margen inferior por si los textos son muy largos
                        )

                        st.plotly_chart(fig_flags, use_container_width=True)
                    else:
                        st.info("El campo Red Flags está vacío para este grupo.")
                
        # --- PESTAÑA 2: REPORTE DETALLADO (Tu lógica original) ---
        with tab_detalle:
            st.subheader(f"📍 Detalle de {selected_batch}")

            # 1. TOTAL CONSOLIDADO
            def generar_tabla_total(df_grupo):
                columnas = ["Métrica", "Outreach", "Responded", "Init. Scr.", "First Int.", "Deep Dive", "Pre-comm"]
                c_funnel = calcular_metricas_funnel(df_grupo)
                def fmt(v, p): return f"{v} ({(v/p*100):.0f}%)" if p > 0 else f"{v} (0%)"
                
                fila_total = [
                    "TOTAL ABSOLUTO", 
                    str(c_funnel[0]), 
                    fmt(c_funnel[1], c_funnel[0]), 
                    fmt(c_funnel[2], c_funnel[1]), 
                    fmt(c_funnel[3], c_funnel[2]), 
                    fmt(c_funnel[4], c_funnel[3]), 
                    fmt(c_funnel[5], c_funnel[4])
                ]
                return pd.DataFrame([fila_total], columns=columnas)

            st.markdown(f"### 🏆 TOTAL CONSOLIDADO")
            df_total_general = generar_tabla_total(grupo)
            st.dataframe(style_dataframe(df_total_general), use_container_width=True)
            
            st.markdown("---") 

            # 2. TABLAS DETALLADAS POR FUENTE
            def generar_tabla(fuentes):
                filas = []
                columnas = ["Source", "Outreach", "Responded", "Init. Scr.", "First Int.", "Deep Dive", "Pre-comm"]
                def fmt(v, p): return f"{v} ({(v/p*100):.0f}%)" if p > 0 else f"{v} (0%)"

                for ref in fuentes:
                    subset = grupo[grupo[col_ref] == ref]
                    c_f = calcular_metricas_funnel(subset)
                    filas.append([ref, str(c_f[0]), fmt(c_f[1],c_f[0]), fmt(c_f[2],c_f[1]), fmt(c_f[3],c_f[2]), fmt(c_f[4],c_f[3]), fmt(c_f[5],c_f[4])])
                
                subset_bloque = grupo[grupo[col_ref].isin(fuentes)]
                c_s = calcular_metricas_funnel(subset_bloque)
                filas.append(["TOTAL GRUPO", str(c_s[0]), fmt(c_s[1],c_s[0]), fmt(c_s[2],c_s[1]), fmt(c_s[3],c_s[2]), fmt(c_s[4],c_s[3]), fmt(c_s[5],c_s[4])])
                return pd.DataFrame(filas, columns=columnas)

            st.markdown("### 💰 Investment Sources")
            st.dataframe(style_dataframe(generar_tabla(grupos_referencias["INVESTMENT"])), use_container_width=True)

            st.markdown("### 📢 Marketing Sources")
            st.dataframe(style_dataframe(generar_tabla(grupos_referencias["MARKETING"])), use_container_width=True)

except Exception as e:
    st.error(f"Error: {e}")