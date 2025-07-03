import pandas as pd
import folium
from folium import plugins

# Read the CSV, skip BOM if present
# Read the CSV, skip empty lines and extra columns
df = pd.read_csv("LANES.csv", usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8])
# df = pd.read_csv("LANES.csv", encoding="utf-8-sig")

# Color mapping for O_BUS and D_BUS
bus_color_map = {"HDS": "yellow", "HDP": "orange", "-": "black"}

# Color mapping for COLR
line_color_map = {"blue": "blue", "red": "red"}

# Function to scale line width based on Total_trl
def scale_line_width(val, min_width=1, max_width=10):
    try:
        val = int(val)
    except Exception:
        return min_width
    # Scale between min and max width
    min_val = df["Total_trl"].apply(pd.to_numeric, errors='coerce').min()
    max_val = df["Total_trl"].apply(pd.to_numeric, errors='coerce').max()
    if pd.isnull(val) or pd.isnull(min_val) or pd.isnull(max_val):
        return min_width
    if max_val == min_val:
        return min_width
    return min_width + (max_width - min_width) * (val - min_val) / (max_val - min_val)

# Initialize map at the center of the points
center_lat = df[["From Lat", "To Lat"]].apply(pd.to_numeric, errors='coerce').stack().mean()
center_lng = df[["From Lng", "To Lng"]].apply(pd.to_numeric, errors='coerce').stack().mean()
m = folium.Map(location=[center_lat, center_lng], zoom_start=5, tiles="cartodbpositron")

for idx, row in df.iterrows():
    try:
        from_lat = float(row["From Lat"])
        from_lng = float(row["From Lng"])
        to_lat = float(row["To Lat"]) if not pd.isnull(row["To Lat"]) else None
        to_lng = float(row["To Lng"]) if not pd.isnull(row["To Lng"]) else None
    except Exception:
        continue

    # Get line color
    line_color = line_color_map.get(str(row["Colr"]).strip().lower(), "gray")

    # Get O_BUS and D_BUS color
    o_bus_color = bus_color_map.get(str(row["O_BUS"]).strip(), "black")
    d_bus_color = bus_color_map.get(str(row["D_BUS"]).strip(), "black")

    # Line width
    width = scale_line_width(row["Total_trl"])

    # Draw line if destination coordinates exist
    if to_lat is not None and to_lng is not None:
        folium.PolyLine(
            locations=[(from_lat, from_lng), (to_lat, to_lng)],
            color=line_color,
            weight=width,
            opacity=0.8,
            tooltip=f"{row['LANE']} ({row['Total_trl']})"
        ).add_to(m)

        # Draw start marker (O_BUS)
        folium.CircleMarker(
            location=(from_lat, from_lng),
            radius=6,
            color="black",
            fill=True,
            fill_color=o_bus_color,
            fill_opacity=1,
            tooltip=f"Start: {row['O_BUS']}"
        ).add_to(m)

        # Draw end marker (D_BUS)
        folium.CircleMarker(
            location=(to_lat, to_lng),
            radius=6,
            color="black",
            fill=True,
            fill_color=d_bus_color,
            fill_opacity=1,
            tooltip=f"End: {row['D_BUS']}"
        ).add_to(m)
    else:
        # If destination not available, just plot the start marker
        folium.CircleMarker(
            location=(from_lat, from_lng),
            radius=6,
            color="black",
            fill=True,
            fill_color=o_bus_color,
            fill_opacity=1,
            tooltip=f"Start: {row['O_BUS']}"
        ).add_to(m)

# Add LayerControl and Fullscreen
folium.LayerControl().add_to(m)
plugins.Fullscreen().add_to(m)

# Save to HTML
m.save("LH_Lanes_Map.html")
print("Map saved to LH_Lanes_Map.html")
