import matplotlib.pyplot as plt

# Coordenadas
latitude = 30.3
longitude = -87.7

# Criando o gráfico
plt.figure()
plt.scatter(longitude, latitude)

# Ajustes do gráfico
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Localização - Alabama, US")
plt.grid(True)

plt.show()