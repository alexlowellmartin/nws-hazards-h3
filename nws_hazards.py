import fused

@fused.udf
def udf():
    import pandas as pd
    import geopandas as gpd
    from owslib.wfs import WebFeatureService
    import json

    # WFS URL for the Watch Warning Advisory (WWA) service
    wfs_url = "https://mapservices.weather.noaa.gov/eventdriven/services/WWA/watch_warn_adv/MapServer/WFSServer"

    # Create WFS service
    wfs = WebFeatureService(url=wfs_url, version='2.0.0')

    # Fetch data for WatchesWarnings in GeoJSON format
    hazards_response = wfs.getfeature(typename='watch_warn_adv:WatchesWarnings', outputFormat='GEOJSON')
    hazards_geojson = hazards_response.read().decode('utf-8')

    # Create GeoDataFrames from GeoJSON content
    try:
        hazards_gdf = gpd.GeoDataFrame.from_features(json.loads(hazards_geojson)["features"])
        hazards_gdf.set_crs(epsg=4326, inplace=True)
        print(hazards_gdf)
    except Exception as e:
        print("Error processing Hazards GeoJSON:", e)
        
    
    hazards_cmap = {
        "Tsunami Warning": {"hex": "#fd6347", "rgb": [253, 99, 71]},
        "Tornado Warning": {"hex": "#ff0000", "rgb": [255, 0, 0]},
        "Extreme Wind Warning": {"hex": "#ff8c00", "rgb": [255, 140, 0]},
        "Severe Thunderstorm Warning": {"hex": "#ffa500", "rgb": [255, 165, 0]},
        "Flash Flood Warning": {"hex": "#8b0000", "rgb": [139, 0, 0]},
        "Flash Flood Statement": {"hex": "#8b0000", "rgb": [139, 0, 0]},
        "Severe Weather Statement": {"hex": "#00ffff", "rgb": [0, 255, 255]},
        "Shelter In Place Warning": {"hex": "#fa8072", "rgb": [250, 128, 114]},
        "Evacuation Immediate": {"hex": "#7fff00", "rgb": [127, 255, 0]},
        "Civil Danger Warning": {"hex": "#ffb6c1", "rgb": [255, 182, 193]},
        "Nuclear Power Plant Warning": {"hex": "#4b0082", "rgb": [75, 0, 130]},
        "Radiological Hazard Warning": {"hex": "#4b0082", "rgb": [75, 0, 130]},
        "Hazardous Materials Warning": {"hex": "#4b0082", "rgb": [75, 0, 130]},
        "Fire Warning": {"hex": "#a0522d", "rgb": [160, 82, 45]},
        "Civil Emergency Message": {"hex": "#ffb6c1", "rgb": [255, 182, 193]},
        "Law Enforcement Warning": {"hex": "#c0c0c0", "rgb": [192, 192, 192]},
        "Storm Surge Warning": {"hex": "#b524f7", "rgb": [181, 36, 247]},
        "Hurricane Force Wind Warning": {"hex": "#cd5c5c", "rgb": [205, 92, 92]},
        "Hurricane Warning": {"hex": "#dc143c", "rgb": [220, 20, 60]},
        "Typhoon Warning": {"hex": "#dc143c", "rgb": [220, 20, 60]},
        "Special Marine Warning": {"hex": "#ffa500", "rgb": [255, 165, 0]},
        "Blizzard Warning": {"hex": "#ff4500", "rgb": [255, 69, 0]},
        "Snow Squall Warning": {"hex": "#c71585", "rgb": [199, 21, 133]},
        "Ice Storm Warning": {"hex": "#8b008b", "rgb": [139, 0, 139]},
        "Winter Storm Warning": {"hex": "#ff69b4", "rgb": [255, 105, 180]},
        "High Wind Warning": {"hex": "#daa520", "rgb": [218, 165, 32]},
        "Tropical Storm Warning": {"hex": "#b22222", "rgb": [178, 34, 34]},
        "Storm Warning": {"hex": "#9400d3", "rgb": [148, 0, 211]},
        "Tsunami Advisory": {"hex": "#d2691e", "rgb": [210, 105, 30]},
        "Tsunami Watch": {"hex": "#ff00ff", "rgb": [255, 0, 255]},
        "Avalanche Warning": {"hex": "#1e90ff", "rgb": [30, 144, 255]},
        "Earthquake Warning": {"hex": "#8b4513", "rgb": [139, 69, 19]},
        "Volcano Warning": {"hex": "#2f4f4f", "rgb": [47, 79, 79]},
        "Ashfall Warning": {"hex": "#a9a9a9", "rgb": [169, 169, 169]},
        "Coastal Flood Warning": {"hex": "#228b22", "rgb": [34, 139, 34]},
        "Lakeshore Flood Warning": {"hex": "#228b22", "rgb": [34, 139, 34]},
        "Flood Warning": {"hex": "#00ff00", "rgb": [0, 255, 0]},
        "High Surf Warning": {"hex": "#228b22", "rgb": [34, 139, 34]},
        "Dust Storm Warning": {"hex": "#ffe4c4", "rgb": [255, 228, 196]},
        "Blowing Dust Warning": {"hex": "#ffe4c4", "rgb": [255, 228, 196]},
        "Lake Effect Snow Warning": {"hex": "#008b8b", "rgb": [0, 139, 139]},
        "Excessive Heat Warning": {"hex": "#c71585", "rgb": [199, 21, 133]},
        "Tornado Watch": {"hex": "#ffff00", "rgb": [255, 255, 0]},
        "Severe Thunderstorm Watch": {"hex": "#db7093", "rgb": [219, 112, 147]},
        "Flash Flood Watch": {"hex": "#2e8b57", "rgb": [46, 139, 87]},
        "Gale Warning": {"hex": "#dda0dd", "rgb": [221, 160, 221]},
        "Flood Statement": {"hex": "#00ff00", "rgb": [0, 255, 0]},
        "Wind Chill Warning": {"hex": "#b0c4de", "rgb": [176, 196, 222]},
        "Extreme Cold Warning": {"hex": "#0000ff", "rgb": [0, 0, 255]},
        "Hard Freeze Warning": {"hex": "#9400d3", "rgb": [148, 0, 211]},
        "Freeze Warning": {"hex": "#483d8b", "rgb": [72, 61, 139]},
        "Red Flag Warning": {"hex": "#ff1493", "rgb": [255, 20, 147]},
        "Storm Surge Watch": {"hex": "#db7ff7", "rgb": [219, 127, 247]},
        "Hurricane Watch": {"hex": "#ff00ff", "rgb": [255, 0, 255]},
        "Hurricane Force Wind Watch": {"hex": "#9932cc", "rgb": [153, 50, 204]},
        "Typhoon Watch": {"hex": "#ff00ff", "rgb": [255, 0, 255]},
        "Tropical Storm Watch": {"hex": "#f08080", "rgb": [240, 128, 128]},
        "Storm Watch": {"hex": "#ffe4b5", "rgb": [255, 228, 181]},
        "Hurricane Local Statement": {"hex": "#ffe4b5", "rgb": [255, 228, 181]},
        "Typhoon Local Statement": {"hex": "#ffe4b5", "rgb": [255, 228, 181]},
        "Tropical Storm Local Statement": {"hex": "#ffe4b5", "rgb": [255, 228, 181]},
        "Tropical Depression Local Statement": {"hex": "#ffe4b5", "rgb": [255, 228, 181]},
        "Avalanche Advisory": {"hex": "#cd853f", "rgb": [205, 133, 63]},
        "Winter Weather Advisory": {"hex": "#7b68ee", "rgb": [123, 104, 238]},
        "Wind Chill Advisory": {"hex": "#afeeee", "rgb": [175, 238, 238]},
        "Heat Advisory": {"hex": "#ff7f50", "rgb": [255, 127, 80]},
        "Urban and Small Stream Flood Advisory": {"hex": "#00ff7f", "rgb": [0, 255, 127]},
        "Small Stream Flood Advisory": {"hex": "#00ff7f", "rgb": [0, 255, 127]},
        "Arroyo and Small Stream Flood Advisory": {"hex": "#00ff7f", "rgb": [0, 255, 127]},
        "Flood Advisory": {"hex": "#00ff7f", "rgb": [0, 255, 127]},
        "Hydrologic Advisory": {"hex": "#00ff7f", "rgb": [0, 255, 127]},
        "Lakeshore Flood Advisory": {"hex": "#7cfc00", "rgb": [124, 252, 0]},
        "Coastal Flood Advisory": {"hex": "#7cfc00", "rgb": [124, 252, 0]},
        "High Surf Advisory": {"hex": "#ba55d3", "rgb": [186, 85, 211]},
        "Heavy Freezing Spray Warning": {"hex": "#00bfff", "rgb": [0, 191, 255]},
        "Dense Fog Advisory": {"hex": "#708090", "rgb": [112, 128, 144]},
        "Dense Smoke Advisory": {"hex": "#f0e68c", "rgb": [240, 230, 140]},
        "Small Craft Advisory": {"hex": "#d8bfd8", "rgb": [216, 191, 216]},
        "Brisk Wind Advisory": {"hex": "#d8bfd8", "rgb": [216, 191, 216]},
        "Hazardous Seas Warning": {"hex": "#d8bfd8", "rgb": [216, 191, 216]},
        "Dust Advisory": {"hex": "#bdb76b", "rgb": [189, 183, 107]},
        "Blowing Dust Advisory": {"hex": "#bdb76b", "rgb": [189, 183, 107]},
        "Lake Wind Advisory": {"hex": "#d2b48c", "rgb": [210, 180, 140]},
        "Wind Advisory": {"hex": "#d2b48c", "rgb": [210, 180, 140]},
        "Frost Advisory": {"hex": "#6495ed", "rgb": [100, 149, 237]},
        "Ashfall Advisory": {"hex": "#696969", "rgb": [105, 105, 105]},
        "Freezing Fog Advisory": {"hex": "#008080", "rgb": [0, 128, 128]},
        "Freezing Spray Advisory": {"hex": "#00bfff", "rgb": [0, 191, 255]},
        "Low Water Advisory": {"hex": "#a52a2a", "rgb": [165, 42, 42]},
        "Local Area Emergency": {"hex": "#c0c0c0", "rgb": [192, 192, 192]},
        "Avalanche Watch": {"hex": "#f4a460", "rgb": [244, 164, 96]},
        "Blizzard Watch": {"hex": "#adff2f", "rgb": [173, 255, 47]},
        "Rip Current Statement": {"hex": "#40e0d0", "rgb": [64, 224, 208]},
        "Beach Hazards Statement": {"hex": "#40e0d0", "rgb": [64, 224, 208]},
        "Gale Watch": {"hex": "#ffc0cb", "rgb": [255, 192, 203]},
        "Winter Storm Watch": {"hex": "#4682b4", "rgb": [70, 130, 180]},
        "Hazardous Seas Watch": {"hex": "#483d8b", "rgb": [72, 61, 139]},
        "Heavy Freezing Spray Watch": {"hex": "#bc8f8f", "rgb": [188, 143, 143]},
        "Coastal Flood Watch": {"hex": "#66cdaa", "rgb": [102, 205, 170]},
        "Lakeshore Flood Watch": {"hex": "#66cdaa", "rgb": [102, 205, 170]},
        "Flood Watch": {"hex": "#2e8b57", "rgb": [46, 139, 87]},
        "High Wind Watch": {"hex": "#b8860b", "rgb": [184, 134, 11]},
        "Excessive Heat Watch": {"hex": "#800000", "rgb": [128, 0, 0]},
        "Extreme Cold Watch": {"hex": "#0000ff", "rgb": [0, 0, 255]},
        "Wind Chill Watch": {"hex": "#5f9ea0", "rgb": [95, 158, 160]},
        "Lake Effect Snow Watch": {"hex": "#87cefa", "rgb": [135, 206, 250]},
        "Hard Freeze Watch": {"hex": "#4169e1", "rgb": [65, 105, 225]},
        "Freeze Watch": {"hex": "#00ffff", "rgb": [0, 255, 255]},
        "Fire Weather Watch": {"hex": "#ffdead", "rgb": [255, 222, 173]},
        "Extreme Fire Danger": {"hex": "#e9967a", "rgb": [233, 150, 122]},
        "911 Telephone Outage": {"hex": "#c0c0c0", "rgb": [192, 192, 192]},
        "Coastal Flood Statement": {"hex": "#6b8e23", "rgb": [107, 142, 35]},
        "Lakeshore Flood Statement": {"hex": "#6b8e23", "rgb": [107, 142, 35]},
        "Special Weather Statement": {"hex": "#ffe4b5", "rgb": [255, 228, 181]},
        "Marine Weather Statement": {"hex": "#ffdAB9", "rgb": [255, 239, 213]},
        "Air Quality Alert": {"hex": "#808080", "rgb": [128, 128, 128]},
        "Air Stagnation Advisory": {"hex": "#808080", "rgb": [128, 128, 128]},
        "Hazardous Weather Outlook": {"hex": "#eee8aa", "rgb": [238, 232, 170]},
        "Hydrologic Outlook": {"hex": "#90ee90", "rgb": [144, 238, 144]},
        "Short Term Forecast": {"hex": "#98fb98", "rgb": [152, 251, 152]},
        "Administrative Message": {"hex": "#c0c0c0", "rgb": [192, 192, 192]},
        "Test": {"hex": "#f0ffff", "rgb": [240, 255, 255]},
        "Child Abduction Emergency": {"hex": "#ffffff", "rgb": [255, 255, 255]},
        "Blue Alert": {"hex": "#ffffff", "rgb": [255, 255, 255]}
    }

    # Create new column in the hazards GeoDataFrame with the corresponding HEX colors
    hazards_gdf['color_hex'] = hazards_gdf['Hazard_Type'].map(lambda x: hazards_cmap[x]['hex'])
    return hazards_gdf


gdf = fused.run(udf=udf, engine="local")
gdf.explore(color=gdf['color_hex'], style_kwds={'fillOpacity': 0.5})