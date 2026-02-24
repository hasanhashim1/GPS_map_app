#!/usr/bin/env python3
# By Hasan Hashim
# Generates OFFLINE HTML maps with embedded tiles

import streamlit as st
import json
import math
import base64
import time
import urllib.request
import os
import io
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration
TILE_SERVER = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
USER_AGENT = "MyMapApp/1.0 (contact@email.com)"

# Event type configurations
EVENT_TYPES = {
    'gps_trackpoints': {
        'name': 'GPS Trackpoints',
        'color': '#2980b9',
        'icon': 'circle',
        'description': 'Main GPS route points'
    },
    'device_events': {
        'name': 'Device Events',
        'color': '#9b59b6',
        'icon': 'phone',
        'description': 'Device connected/disconnected events'
    },
    'door_events': {
        'name': 'Door Events',
        'color': '#27ae60',
        'icon': 'door',
        'description': 'Door open/closed events'
    },
    'ignition_events': {
        'name': 'Ignition Events',
        'color': '#e67e22',
        'icon': 'key',
        'description': 'Ignition on/off/crank events'
    },
    'gear_shift_events': {
        'name': 'Gear Shift Events',
        'color': '#3498db',
        'icon': 'gear',
        'description': 'Gear shift changes'
    },
    'seat_belt_events': {
        'name': 'Seat Belt Events',
        'color': '#f39c12',
        'icon': 'shield',
        'description': 'Seat belt fastened/unfastened'
    },
    'wifi_events': {
        'name': 'Wi-Fi Events',
        'color': '#1abc9c',
        'icon': 'wifi',
        'description': 'Wi-Fi connected/disconnected'
    }
}


def get_csv_headers(file_content):
    """Extract CSV headers from first row."""
    lines = file_content.decode('utf-8').split('\n')
    if lines:
        parts = lines[0].strip().split(',')
        return [p.strip('"').strip() for p in parts]
    return []


def get_csv_preview(file_content, max_rows=5):
    """Get first N rows of CSV as list of dicts for preview."""
    lines = file_content.decode('utf-8').split('\n')
    if not lines:
        return []
    headers = [p.strip('"').strip() for p in lines[0].strip().split(',')]
    rows = []
    for line in lines[1:max_rows + 1]:
        if not line.strip():
            continue
        parts = line.strip().split(',')
        parts = [p.strip('"').strip() for p in parts]
        row = {}
        for i, header in enumerate(headers):
            row[header] = parts[i] if i < len(parts) else ''
        rows.append(row)
    return rows


def parse_csv(file_content, datetime_col, lat_col, lon_col, selected_columns, all_headers):
    """Parse CSV and extract points with user-selected columns."""
    points = []
    lines = file_content.decode('utf-8').split('\n')

    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.strip().split(',')
        parts = [p.strip('"').strip() for p in parts]

        max_col = max(datetime_col, lat_col, lon_col)
        if len(parts) > max_col:
            try:
                datetime_str = parts[datetime_col].strip()
                lat_str = parts[lat_col].strip()
                lon_str = parts[lon_col].strip()

                if lat_str in ['NaN', '', 'nan'] or lon_str in ['NaN', '', 'nan']:
                    continue

                lat = float(lat_str)
                lon = float(lon_str)

                # Build extra fields from selected columns
                extra = {}
                for col_name in selected_columns:
                    if col_name in all_headers:
                        col_idx = all_headers.index(col_name)
                        # Skip lat/lon/datetime columns — already stored separately
                        if col_idx in (datetime_col, lat_col, lon_col):
                            continue
                        if col_idx < len(parts):
                            extra[col_name] = parts[col_idx].strip()

                points.append({
                    'datetime': datetime_str,
                    'lat': lat,
                    'lon': lon,
                    'extra': extra
                })
            except (ValueError, IndexError):
                continue
    return points


def get_bounds(all_points):
    """Calculate bounding box with generous padding for tile coverage."""
    lats = [p['lat'] for p in all_points]
    lons = [p['lon'] for p in all_points]

    if not lats or not lons:
        return None

    min_lat, max_lat = min(lats), max(lats)
    min_lon, max_lon = min(lons), max(lons)

    lat_range = max_lat - min_lat
    lon_range = max_lon - min_lon
    lat_pad = max(lat_range * 0.5, 0.1)
    lon_pad = max(lon_range * 0.5, 0.15)

    return {
        'min_lat': min_lat - lat_pad,
        'max_lat': max_lat + lat_pad,
        'min_lon': min_lon - lon_pad,
        'max_lon': max_lon + lon_pad
    }


def lat_lon_to_tile(lat, lon, zoom):
    """Convert lat/lon to tile coordinates."""
    lat_rad = math.radians(lat)
    n = 2 ** zoom
    x = int((lon + 180) / 360 * n)
    y = int((1 - math.asinh(math.tan(lat_rad)) / math.pi) / 2 * n)
    return x, y


def get_required_tiles(bounds, zoom_levels):
    """Get list of required tiles with viewport padding."""
    tiles = []
    for zoom in zoom_levels:
        min_x, max_y = lat_lon_to_tile(bounds['min_lat'], bounds['min_lon'], zoom)
        max_x, min_y = lat_lon_to_tile(bounds['max_lat'], bounds['max_lon'], zoom)

        # Add extra tile padding — more at lower zooms to fill the viewport
        if zoom <= 8:
            pad = 4
        elif zoom <= 10:
            pad = 3
        elif zoom <= 12:
            pad = 2
        else:
            pad = 1

        n = 2 ** zoom
        for x in range(max(0, min_x - pad), min(n - 1, max_x + pad) + 1):
            for y in range(max(0, min_y - pad), min(n - 1, max_y + pad) + 1):
                tiles.append((zoom, x, y))

    return tiles


def download_tile(tile_info):
    """Download a single tile and return as base64."""
    zoom, x, y = tile_info
    url = TILE_SERVER.format(z=zoom, x=x, y=y)
    key = f"{zoom}/{x}/{y}"

    try:
        req = urllib.request.Request(url, headers={'User-Agent': USER_AGENT})
        with urllib.request.urlopen(req, timeout=30) as response:
            data = response.read()
            return key, base64.b64encode(data).decode('ascii')
    except Exception as e:
        return key, None


def download_leaflet():
    """Download Leaflet JS and CSS."""
    js_url = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
    css_url = "https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"

    req_js = urllib.request.Request(js_url, headers={'User-Agent': USER_AGENT})
    req_css = urllib.request.Request(css_url, headers={'User-Agent': USER_AGENT})

    with urllib.request.urlopen(req_js) as r:
        leaflet_js = r.read().decode('utf-8')

    with urllib.request.urlopen(req_css) as r:
        leaflet_css = r.read().decode('utf-8')

    return leaflet_js, leaflet_css


def create_html_multi_events(events_data, all_event_configs, tile_cache, leaflet_js, leaflet_css, case_info, zoom_levels):
    """Generate offline HTML with embedded tiles and multiple event types."""

    events_json = {}
    for event_key, event_points in events_data.items():
        if event_points:
            events_json[event_key] = event_points

    events_json_str = json.dumps(events_json)
    tiles_json = json.dumps(tile_cache)
    event_configs_json = json.dumps(all_event_configs)

    case_number = case_info['case_number']
    item_number = case_info['item_number']
    date_desc = case_info['date_desc']

    total_points = sum(len(points) for points in events_data.values())

    min_zoom = min(zoom_levels)
    max_zoom = max(zoom_levels)

    # Build legend items HTML
    legend_items_html = ""
    for event_key, config in all_event_configs.items():
        if event_key in events_data and events_data[event_key]:
            legend_items_html += f'<div class="legend-item"><div class="legend-marker" style="background: {config["color"]};"></div>{config["name"]} ({len(events_data[event_key])} points)</div>'

    # Build control checkboxes HTML
    controls_html = ""
    for event_key, config in all_event_configs.items():
        if event_key in events_data and events_data[event_key]:
            checked = 'checked' if event_key == 'gps_trackpoints' else ''
            controls_html += f'''
        <div class="control-item">
            <input type="checkbox" id="show_{event_key}" {checked}>
            <label for="show_{event_key}">{config['name']}</label>
        </div>'''

    html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GPS Map with Events - Case {case_number} - {item_number}</title>
    <style>
{leaflet_css}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        html, body {{ height: 100%; width: 100%; font-family: Arial, sans-serif; overflow: hidden; }}
        .header {{ background: #1a252f; color: white; padding: 12px 20px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px; height: 55px; }}
        .header h1 {{ font-size: 16px; font-weight: 600; }}
        .header .info {{ font-size: 12px; opacity: 0.85; }}
        .controls {{ background: #2c3e50; color: white; padding: 8px 20px; display: flex; gap: 15px; flex-wrap: wrap; height: 40px; align-items: center; }}
        #map {{ position: absolute; top: 95px; bottom: 0; left: 0; right: 0; }}
        .control-item {{ display: flex; align-items: center; gap: 6px; font-size: 12px; }}
        .control-item input[type="checkbox"] {{ width: 16px; height: 16px; cursor: pointer; }}
        .control-item label {{ cursor: pointer; white-space: nowrap; }}
        .legend {{ background: white; padding: 10px; border-radius: 5px; box-shadow: 0 1px 5px rgba(0,0,0,0.4); font-size: 11px; max-width: 250px; max-height: 400px; overflow-y: auto; }}
        .legend h4 {{ margin: 0 0 8px 0; font-size: 13px; font-weight: 600; }}
        .legend-item {{ display: flex; align-items: center; gap: 8px; margin: 4px 0; }}
        .legend-marker {{ width: 12px; height: 12px; border-radius: 50%; border: 2px solid white; box-shadow: 0 0 2px rgba(0,0,0,0.3); }}
        .info-box {{ background: white; padding: 10px; border-radius: 5px; box-shadow: 0 1px 5px rgba(0,0,0,0.4); font-size: 11px; max-width: 200px; }}
        .info-box h4 {{ margin: 0 0 8px 0; font-size: 13px; font-weight: 600; }}
        .info-box div {{ margin: 3px 0; }}
        .zoom-info {{ background: white; padding: 5px 10px; border-radius: 3px; box-shadow: 0 1px 5px rgba(0,0,0,0.4); font-size: 12px; font-weight: 600; }}
        .popup-label {{ font-weight: 600; color: #2c3e50; }}
    </style>
</head>
<body>
    <div class="header">
        <div>
            <h1>&#x1F4CD; GPS Map with Events - Case {case_number} - {item_number}</h1>
            <div class="info">Date: {date_desc} | Total Points: {total_points:,} | Offline Map (Zoom {min_zoom}-{max_zoom})</div>
        </div>
    </div>

    <div class="controls">
        {controls_html}
    </div>

    <div id="map"></div>

    <script>
{leaflet_js}

        // Embedded data
        var eventsData = {events_json_str};
        var tileCache = {tiles_json};

        // Event type configurations
        var eventConfigs = {event_configs_json};

        // Custom offline tile layer using base64 embedded tiles
        L.TileLayer.Offline = L.TileLayer.extend({{
            createTile: function(coords, done) {{
                var tile = document.createElement('img');
                var key = coords.z + '/' + coords.x + '/' + coords.y;
                if (tileCache[key]) {{
                    tile.onload = function() {{ done(null, tile); }};
                    tile.onerror = function(e) {{ done(e, tile); }};
                    tile.src = 'data:image/png;base64,' + tileCache[key];
                }} else {{
                    tile.src = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8/5+hHgAHggJ/PchI7wAAAABJRU5ErkJggg==';
                    setTimeout(function() {{ done(null, tile); }}, 0);
                }}
                tile.alt = '';
                tile.setAttribute('role', 'presentation');
                return tile;
            }}
        }});

        // Initialize map
        var map = L.map('map', {{
            minZoom: {min_zoom},
            maxZoom: {max_zoom},
            zoomControl: true
        }});

        // Add offline tile layer
        new L.TileLayer.Offline('', {{
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors | Case {case_number}'
        }}).addTo(map);

        // Create layer groups for each event type
        var eventLayers = {{}};
        var allBounds = [];

        // Process each event type
        Object.keys(eventsData).forEach(function(eventKey) {{
            var points = eventsData[eventKey];
            var config = eventConfigs[eventKey];

            if (!points || points.length === 0) return;

            var layer = L.layerGroup();
            eventLayers[eventKey] = layer;

            // Add to map by default only for GPS trackpoints
            if (eventKey === 'gps_trackpoints') {{
                layer.addTo(map);
            }}

            // Create markers for each point
            points.forEach(function(p, i) {{
                allBounds.push([p.lat, p.lon]);

                var marker = L.circleMarker([p.lat, p.lon], {{
                    radius: 7,
                    fillColor: config.color,
                    color: '#ffffff',
                    weight: 2,
                    opacity: 1,
                    fillOpacity: 1
                }})
                .bindPopup(function() {{
                    var html = '<strong>' + config.name + ' #' + (i+1) + '</strong><br>';
                    html += '<span class="popup-label">Time:</span> ' + p.datetime + '<br>';
                    if (p.extra) {{
                        Object.keys(p.extra).forEach(function(key) {{
                            html += '<span class="popup-label">' + key + ':</span> ' + (p.extra[key] || 'N/A') + '<br>';
                        }});
                    }}
                    html += '<span class="popup-label">Latitude:</span> ' + p.lat.toFixed(6) + '<br>';
                    html += '<span class="popup-label">Longitude:</span> ' + p.lon.toFixed(6);
                    return html;
                }}());

                layer.addLayer(marker);
            }});
        }});

        // Fit map to all points
        if (allBounds.length > 0) {{
            var bounds = L.latLngBounds(allBounds);
            map.fitBounds(bounds, {{padding: [40, 40]}});
        }}

        // Setup control handlers for each event type
        Object.keys(eventLayers).forEach(function(eventKey) {{
            var checkbox = document.getElementById('show_' + eventKey);
            if (checkbox) {{
                checkbox.addEventListener('change', function() {{
                    if (this.checked) {{
                        map.addLayer(eventLayers[eventKey]);
                    }} else {{
                        map.removeLayer(eventLayers[eventKey]);
                    }}
                }});
            }}
        }});

        // Legend
        var legend = L.control({{position: 'bottomleft'}});
        legend.onAdd = function() {{
            var div = L.DomUtil.create('div', 'legend');
            div.innerHTML = '<h4>Event Types</h4>{legend_items_html}';
            return div;
        }};
        legend.addTo(map);

        // Info box
        var infoBox = L.control({{position: 'bottomright'}});
        infoBox.onAdd = function() {{
            var div = L.DomUtil.create('div', 'info-box');
            div.innerHTML =
                '<h4>Case Information</h4>' +
                '<div><strong>Case:</strong> {case_number}</div>' +
                '<div><strong>Item:</strong> {item_number}</div>' +
                '<div><strong>Total Points:</strong> {total_points:,}</div>' +
                '<div><strong>Date:</strong> {date_desc}</div>' +
                '<div style="margin-top:5px;font-size:10px;color:#666;">Zoom: {min_zoom}-{max_zoom} | Offline</div>';
            return div;
        }};
        infoBox.addTo(map);

        // Zoom level display
        var zoomDisplay = L.control({{position: 'topright'}});
        zoomDisplay.onAdd = function() {{
            var div = L.DomUtil.create('div', 'zoom-info');
            div.id = 'zoom-display';
            div.innerHTML = 'Zoom: ' + map.getZoom();
            return div;
        }};
        zoomDisplay.addTo(map);

        map.on('zoomend', function() {{
            document.getElementById('zoom-display').innerHTML = 'Zoom: ' + map.getZoom();
        }});
    </script>
</body>
</html>'''

    return html


def render_event_uploader(event_key, config, uploaded_files, csv_configs):
    """Render file uploader + column config for a single event type."""
    uploaded = st.file_uploader(
        f"{config['name']} CSV",
        type=['csv'],
        key=f"upload_{event_key}",
        help=config.get('description', '')
    )
    if uploaded:
        uploaded_files[event_key] = uploaded
        file_content = uploaded.getvalue()
        headers = get_csv_headers(file_content)
        preview_rows = get_csv_preview(file_content)

        with st.expander(f"⚙️ Configure {config['name']} columns", expanded=True):
            if preview_rows:
                st.caption("Data preview (first 5 rows)")
                st.dataframe(preview_rows, width='stretch')

            st.markdown("**Required column mapping**")
            map_col1, map_col2, map_col3 = st.columns(3)
            with map_col1:
                dt_col = st.selectbox(
                    "DateTime column",
                    options=headers,
                    index=headers.index('DateTime') if 'DateTime' in headers else 0,
                    key=f"dt_col_{event_key}"
                )
            with map_col2:
                lat_col_name = st.selectbox(
                    "Latitude column",
                    options=headers,
                    index=headers.index('Latitude') if 'Latitude' in headers else min(5, len(headers) - 1),
                    key=f"lat_col_{event_key}"
                )
            with map_col3:
                lon_col_name = st.selectbox(
                    "Longitude column",
                    options=headers,
                    index=headers.index('Longitude') if 'Longitude' in headers else min(6, len(headers) - 1),
                    key=f"lon_col_{event_key}"
                )

            st.markdown("**Select columns to show in map popup**")
            non_required = [h for h in headers if h not in (dt_col, lat_col_name, lon_col_name)]
            selected_cols = st.multiselect(
                "Columns",
                options=non_required,
                default=non_required,
                key=f"cols_{event_key}",
                label_visibility="collapsed"
            )

            csv_configs[event_key] = {
                'datetime_col': headers.index(dt_col),
                'lat_col': headers.index(lat_col_name),
                'lon_col': headers.index(lon_col_name),
                'selected_columns': selected_cols,
                'headers': headers
            }


def main():
    st.set_page_config(
        page_title="GPS Map Generator with Events",
        page_icon="📍",
        layout="wide"
    )

    # Initialize session state for custom events
    if 'custom_events' not in st.session_state:
        st.session_state.custom_events = {}  # key -> {name, color, description}

    st.title("📍 GPS Map Generator with Multiple Event Types")
    st.markdown("Upload GPS trackpoints and various event CSVs to create a comprehensive **offline** map.")

    # Sidebar for inputs
    with st.sidebar:
        # Branding
        logo_path = os.path.join(os.path.dirname(__file__), "logo.png")
        if os.path.exists(logo_path):
            st.image(logo_path, width="stretch")
        st.markdown(
            "<p style='text-align: center; color: gray; font-size: 13px; margin-top: -10px;'>"
            "Powered by <a href='https://hashimtech.com' target='_blank' style='color: #27ae60; text-decoration: none;'>HashimTech.com</a>"
            "</p>",
            unsafe_allow_html=True
        )
        st.divider()

        st.header("⚙️ Configuration")

        # Case information
        st.subheader("Case Information")
        case_number = st.text_input("Case Number", value="")
        item_number = st.text_input("Item Number", value="")
        date_desc = st.text_input("Date Description", value="")

        st.divider()

        # Zoom level configuration
        st.subheader("🔍 Zoom Levels")
        st.caption("Higher max zoom = more street detail but larger file & longer generation time")
        min_zoom = st.number_input("Min Zoom", min_value=1, max_value=14, value=6, step=1,
                                   help="Lower = more zoomed out view available")
        max_zoom = st.number_input("Max Zoom", min_value=10, max_value=19, value=14, step=1,
                                   help="Higher = more street detail (15-17 for street names, 18-19 for buildings)")

        if max_zoom <= min_zoom:
            st.error("Max zoom must be greater than min zoom")

        st.divider()

        st.subheader("📋 Select Event Types")
        selected_events = {}

        for event_key, config in EVENT_TYPES.items():
            col1, col2 = st.columns([3, 1])
            with col1:
                selected = st.checkbox(
                    config['name'],
                    value=(event_key == 'gps_trackpoints'),
                    key=f"check_{event_key}",
                    help=config['description']
                )
            with col2:
                color = st.color_picker(
                    "Color",
                    value=config['color'],
                    key=f"color_{event_key}",
                    label_visibility="collapsed"
                )

            if selected:
                selected_events[event_key] = color
                EVENT_TYPES[event_key]['color'] = color

        st.divider()

        st.subheader("➕ Custom Event Types")
        st.caption("Add your own event type (e.g. Speeding, Hard Braking)")

        with st.form("add_custom_event_form", clear_on_submit=True):
            new_event_name = st.text_input("Event Name", placeholder="e.g. Speeding")
            new_event_color = st.color_picker("Marker Color", value="#e74c3c")
            add_btn = st.form_submit_button("➕ Add Event Type")

            if add_btn:
                name_stripped = new_event_name.strip()
                if not name_stripped:
                    st.error("Please enter an event name.")
                else:
                    # Generate a safe unique key from the name
                    custom_key = "custom_" + "".join(
                        c if c.isalnum() else '_' for c in name_stripped.lower()
                    )
                    # Avoid collisions with presets or existing custom events
                    base_key = custom_key
                    counter = 1
                    while custom_key in EVENT_TYPES or custom_key in st.session_state.custom_events:
                        custom_key = f"{base_key}_{counter}"
                        counter += 1

                    st.session_state.custom_events[custom_key] = {
                        'name': name_stripped,
                        'color': new_event_color,
                        'icon': 'circle',
                        'description': f'Custom event: {name_stripped}'
                    }
                    st.success(f"✅ '{name_stripped}' added!")

        # Show existing custom events with enable checkbox + delete button
        if st.session_state.custom_events:
            st.markdown("**Your custom events:**")
            keys_to_delete = []

            for custom_key, config in st.session_state.custom_events.items():
                c1, c2, c3 = st.columns([3, 1, 1])
                with c1:
                    selected_custom = st.checkbox(
                        config['name'],
                        value=True,
                        key=f"check_{custom_key}"
                    )
                with c2:
                    new_color = st.color_picker(
                        "Color",
                        value=config['color'],
                        key=f"color_{custom_key}",
                        label_visibility="collapsed"
                    )
                    # Update color if changed
                    st.session_state.custom_events[custom_key]['color'] = new_color
                with c3:
                    if st.button("🗑️", key=f"delete_{custom_key}", help=f"Remove {config['name']}"):
                        keys_to_delete.append(custom_key)

                if selected_custom:
                    selected_events[custom_key] = new_color

            # Delete outside loop to avoid dict-change-during-iteration
            for k in keys_to_delete:
                del st.session_state.custom_events[k]
            if keys_to_delete:
                st.rerun()

    # ── Build the combined config dict (presets + active custom events) ──────
    all_event_configs = dict(EVENT_TYPES)
    for custom_key, config in st.session_state.custom_events.items():
        all_event_configs[custom_key] = config

    st.header("📁 Upload CSV Files")

    uploaded_files = {}
    csv_configs = {}

    # Preset event uploaders
    for event_key in selected_events.keys():
        if event_key in EVENT_TYPES:
            config = EVENT_TYPES[event_key]
            render_event_uploader(event_key, config, uploaded_files, csv_configs)

    # Custom event uploaders
    for event_key in selected_events.keys():
        if event_key in st.session_state.custom_events:
            config = st.session_state.custom_events[event_key]
            render_event_uploader(event_key, config, uploaded_files, csv_configs)

    st.divider()
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        generate_button = st.button("🚀 Generate Offline Map with Events", type="primary", width='stretch')

    if generate_button:
        if not case_number or not item_number:
            st.error("❌ Please enter both Case Number and Item Number")
            return

        if not uploaded_files:
            st.error("❌ Please upload at least one CSV file")
            return

        if max_zoom <= min_zoom:
            st.error("❌ Max zoom must be greater than min zoom")
            return

        zoom_levels = list(range(min_zoom, max_zoom + 1))

        progress_bar = st.progress(0)
        status_text = st.empty()

        try:
            # Step 1: Parse all CSV files
            status_text.text("🔄 Step 1/4: Parsing CSV files...")
            progress_bar.progress(10)

            events_data = {}
            all_points_combined = []

            for event_key, uploaded_file in uploaded_files.items():
                file_content = uploaded_file.getvalue()
                cfg = csv_configs[event_key]
                points = parse_csv(
                    file_content,
                    cfg['datetime_col'],
                    cfg['lat_col'],
                    cfg['lon_col'],
                    cfg['selected_columns'],
                    cfg['headers']
                )

                if points:
                    events_data[event_key] = points
                    all_points_combined.extend(points)
                    event_name = all_event_configs.get(event_key, {}).get('name', event_key)
                    st.success(f"✅ {event_name}: {len(points)} points loaded")

            if not all_points_combined:
                st.error("❌ No valid points found in any CSV file!")
                return

            progress_bar.progress(20)

            # Step 2: Calculate bounds and tiles
            status_text.text("🔄 Step 2/4: Calculating map bounds...")
            bounds = get_bounds(all_points_combined)

            if not bounds:
                st.error("❌ Could not calculate map bounds!")
                return

            tiles = get_required_tiles(bounds, zoom_levels)
            st.info(f"🗺️ Zoom {min_zoom}-{max_zoom} | Total tiles needed: {len(tiles):,}")
            progress_bar.progress(25)

            # Step 3: Download tiles
            status_text.text(f"🔄 Step 3/4: Downloading {len(tiles):,} map tiles...")
            tile_cache = {}

            tile_progress = st.empty()
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = {executor.submit(download_tile, t): t for t in tiles}
                completed = 0
                for future in as_completed(futures):
                    key, data = future.result()
                    if data:
                        tile_cache[key] = data
                    completed += 1

                    if completed % 20 == 0 or completed == len(tiles):
                        tile_progress.text(f"Downloading: {completed:,}/{len(tiles):,} ({100*completed//len(tiles)}%)")
                        progress_bar.progress(25 + int(55 * completed / len(tiles)))
                    time.sleep(0.05)

            st.success(f"✅ Successfully downloaded {len(tile_cache):,} tiles")
            progress_bar.progress(80)

            # Step 4: Generate HTML
            status_text.text("🔄 Step 4/4: Generating HTML file...")
            leaflet_js, leaflet_css = download_leaflet()

            case_info = {
                'case_number': case_number,
                'item_number': item_number,
                'date_desc': date_desc if date_desc else "See timestamps"
            }

            html = create_html_multi_events(
                events_data, all_event_configs,
                tile_cache, leaflet_js, leaflet_css,
                case_info, zoom_levels
            )
            progress_bar.progress(100)
            status_text.text("✅ Complete!")

            # Generate filename
            output_filename = f"Map_Events_{case_number}_{item_number}.html"
            output_filename = "".join(c for c in output_filename if c not in '<>:"/\\|?*')

            # Display results
            st.success("🎉 Offline map generated successfully!")

            # Stats
            stat_cols = st.columns(len(events_data) + 2)
            for idx, (event_key, points) in enumerate(events_data.items()):
                with stat_cols[idx]:
                    event_name = all_event_configs.get(event_key, {}).get('name', event_key)
                    st.metric(event_name, f"{len(points):,}")

            with stat_cols[-2]:
                st.metric("🗺️ Map Tiles", f"{len(tile_cache):,}")
            with stat_cols[-1]:
                file_size_mb = len(html.encode('utf-8')) / (1024 * 1024)
                st.metric("💾 File Size", f"{file_size_mb:.1f} MB")

            # Download button
            st.divider()
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                st.download_button(
                    label="⬇️ Download Offline Map with Events (HTML)",
                    data=html,
                    file_name=output_filename,
                    mime="text/html",
                    type="primary",
                    width='stretch'
                )

            st.info(f"🌍 This file works 100% OFFLINE! Zoom range: {min_zoom}-{max_zoom}. Open in any browser — no internet needed!")

        except Exception as e:
            st.error(f"❌ Error: {str(e)}")
            import traceback
            st.code(traceback.format_exc())


if __name__ == "__main__":
    main()
