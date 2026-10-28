// Marker and cluster rendering for dash-leaflet's GeoJSON layer.
// Referenced from app.py as {"variable": "dashExtensions.default.<name>"}.
window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        // One charger: a pill with the live price and active sessions / connectors,
        // coloured by occupancy (free / partly busy / full / no events yet).
        pointToLayer: function (feature, latlng) {
            const p = feature.properties;
            const price = p.price == null ? "–" : "A$" + p.price.toFixed(2);
            const html =
                '<div class="ev-pin ev-' + p.occ + '">' +
                '<span class="ev-dot"></span>' +
                '<span class="ev-price">' + price + "</span>" +
                '<span class="ev-busy">⚡' + p.active + "/" + p.connectors + "</span></div>";
            const icon = L.divIcon({ html: html, className: "ev-icon", iconSize: null, iconAnchor: [12, 12] });
            return L.marker(latlng, { icon: icon, riseOnHover: true });
        },
        // A cluster: number of chargers, plus how many of them are in use right now.
        clusterToLayer: function (feature, latlng, index) {
            const n = feature.properties.point_count;
            const leaves = index.getLeaves(feature.properties.cluster_id, Infinity);
            const busy = leaves.filter(function (l) { return l.properties.active > 0; }).length;
            const size = n < 10 ? 34 : n < 100 ? 42 : 50;
            const html =
                '<div class="ev-cluster" style="width:' + size + "px;height:" + size + 'px">' +
                "<b>" + n + "</b>" + (busy > 0 ? '<span class="ev-cluster-busy">⚡' + busy + "</span>" : "") +
                "</div>";
            const icon = L.divIcon({ html: html, className: "ev-icon", iconSize: L.point(size, size) });
            return L.marker(latlng, { icon: icon });
        },
    },
});
