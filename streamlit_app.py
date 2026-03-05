import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
from datetime import datetime

# Configuração da página
st.set_page_config(page_title="Mapa de Clima USA", layout="wide")
st.title("📍 Visualizador de Dados Climáticos - USA")

# Carregar dados
@st.cache_data
def load_data():
    df = pd.read_csv('weather_grid_usa_full.csv')
    df['date'] = pd.to_datetime(df['date'])
    return df

df = load_data()

# Sidebar para filtros
st.sidebar.header("Filtros")

# Filtro de data
dates = sorted(df['date'].unique())
selected_date = st.sidebar.selectbox(
    "Selecionar Data",
    dates,
    format_func=lambda x: x.strftime('%d/%m/%Y')
)

# Filtro de estado
states = sorted(df['state'].unique())
selected_states = st.sidebar.multiselect(
    "Selecionar Estados",
    states,
    default=states
)

# Filtrar dados
filtered_df = df[
    (df['date'] == selected_date) & 
    (df['state'].isin(selected_states))
].drop_duplicates(subset=['latitude', 'longitude'])

st.sidebar.info(f"Pontos para visualizar: {len(filtered_df)}")

# Criar mapa
m = folium.Map(
    location=[39.8283, -98.5795],  # Centro dos EUA
    zoom_start=4,
    tiles="OpenStreetMap"
)

# Adicionar marcadores
for idx, row in filtered_df.iterrows():
    # Criar popup com informações
    popup_text = f"""
    <b>{row['state']}</b><br>
    Temperatura Máx: {row['temperature_2m_max']:.1f}°C<br>
    Temperatura Mín: {row['temperature_2m_min']:.1f}°C<br>
    Precipitação: {row['precipitation_sum']:.1f}mm<br>
    Vento Máx: {row['windspeed_10m_max']:.1f} km/h<br>
    Data: {row['date'].strftime('%d/%m/%Y')}
    """
    
    # Cor do marcador baseada na temperatura
    temp = row['temperature_2m_mean']
    if temp < 0:
        color = 'blue'
    elif temp < 10:
        color = 'cyan'
    elif temp < 20:
        color = 'green'
    elif temp < 30:
        color = 'orange'
    else:
        color = 'red'
    
    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=8,
        popup=folium.Popup(popup_text, max_width=300),
        color=color,
        fill=True,
        fillColor=color,
        fillOpacity=0.7,
        weight=2
    ).add_to(m)

# Mostrar mapa
st_folium(m, width=1400, height=600)

# Mostrar tabela de dados
st.subheader("Dados Detalhados")
display_df = filtered_df[[
    'state', 'latitude', 'longitude', 'date',
    'temperature_2m_max', 'temperature_2m_min', 'temperature_2m_mean',
    'precipitation_sum', 'windspeed_10m_max'
]].copy()

display_df.columns = [
    'Estado', 'Latitude', 'Longitude', 'Data',
    'Temp. Máx (°C)', 'Temp. Mín (°C)', 'Temp. Média (°C)',
    'Precipitação (mm)', 'Vento Máx (km/h)'
]

st.dataframe(display_df, use_container_width=True)

# Estatísticas
st.subheader("Estatísticas")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Temperatura Máxima",
        f"{filtered_df['temperature_2m_max'].max():.1f}°C"
    )

with col2:
    st.metric(
        "Temperatura Mínima",
        f"{filtered_df['temperature_2m_min'].min():.1f}°C"
    )

with col3:
    st.metric(
        "Precipitação Total",
        f"{filtered_df['precipitation_sum'].sum():.1f}mm"
    )

with col4:
    st.metric(
        "Pontos Únicos",
        len(filtered_df)
    )
