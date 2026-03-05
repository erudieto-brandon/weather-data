import folium

# Coordenadas
latitude = 30.3
longitude = -87.7

# Criando mapa centralizado no ponto
mapa = folium.Map(
    location=[latitude, longitude],
    zoom_start=8
)

# Adicionando marcador
folium.Marker(
    location=[latitude, longitude],
    popup="Alabama, US",
    tooltip="Clique aqui"
).add_to(mapa)

# Salvando mapa em arquivo HTML
mapa.save("mapa_alabama.html")

print("Mapa gerado: mapa_alabama.html")