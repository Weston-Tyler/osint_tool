import React, { useEffect, useState, useRef } from 'react';
import { useDispatch } from 'react-redux';
import KeplerGl from 'kepler.gl';
import { addDataToMap } from 'kepler.gl/actions';
import AutoSizer from 'kepler.gl/dist/components/common/auto-sizer';

const MDA_API_URL = import.meta.env.VITE_MDA_API_URL || 'http://localhost:8000';
const WS_URL = import.meta.env.VITE_MDA_WS_URL || 'ws://localhost:8000/v1/ws/alerts';

function App() {
  const dispatch = useDispatch();
  const [alerts, setAlerts] = useState([]);
  const [status, setStatus] = useState('disconnected');
  const wsRef = useRef(null);

  // Fetch vessel data and add to Kepler.gl
  useEffect(() => {
    async function loadVesselData() {
      try {
        const resp = await fetch(`${MDA_API_URL}/v1/vessels?sanctioned_only=true&limit=500`);
        const data = await resp.json();

        if (data.vessels && data.vessels.length > 0) {
          const rows = data.vessels
            .filter((v) => v.last_ais_position_lat && v.last_ais_position_lon)
            .map((v) => [
              v.imo || '',
              v.mmsi || '',
              v.name || 'Unknown',
              v.flag_state || '',
              v.risk_score || 0,
              v.sanctions_status || '',
              v.ais_status || '',
              v.last_ais_position_lat,
              v.last_ais_position_lon,
            ]);

          dispatch(
            addDataToMap({
              datasets: {
                info: { label: 'Sanctioned Vessels', id: 'sanctioned_vessels' },
                data: {
                  fields: [
                    { name: 'imo', type: 'string' },
                    { name: 'mmsi', type: 'string' },
                    { name: 'name', type: 'string' },
                    { name: 'flag_state', type: 'string' },
                    { name: 'risk_score', type: 'real' },
                    { name: 'sanctions_status', type: 'string' },
                    { name: 'ais_status', type: 'string' },
                    { name: 'latitude', type: 'real' },
                    { name: 'longitude', type: 'real' },
                  ],
                  rows,
                },
              },
              option: { centerMap: true, readOnly: false },
            })
          );
        }
      } catch (err) {
        console.error('Failed to load vessel data:', err);
      }
    }

    loadVesselData();
  }, [dispatch]);

  // WebSocket connection for real-time alerts
  useEffect(() => {
    function connectWebSocket() {
      const ws = new WebSocket(`${WS_URL}?severity=CRITICAL,HIGH`);
      wsRef.current = ws;

      ws.onopen = () => {
        setStatus('connected');
        console.log('WebSocket connected');
      };

      ws.onmessage = (event) => {
        try {
          const alert = JSON.parse(event.data);
          setAlerts((prev) => [alert, ...prev].slice(0, 100));

          // Add alert point to map if it has coordinates
          const lat = alert.lat || alert.detection_lat || alert.subject_lat;
          const lon = alert.lon || alert.detection_lon || alert.subject_lon;
          if (lat && lon) {
            dispatch(
              addDataToMap({
                datasets: {
                  info: { label: 'Live Alerts', id: 'live_alerts' },
                  data: {
                    fields: [
                      { name: 'alert_type', type: 'string' },
                      { name: 'severity', type: 'string' },
                      { name: 'message', type: 'string' },
                      { name: 'latitude', type: 'real' },
                      { name: 'longitude', type: 'real' },
                      { name: 'timestamp', type: 'timestamp' },
                    ],
                    rows: [
                      [
                        alert.alert_type || alert.event_type || 'UNKNOWN',
                        alert.severity || 'MEDIUM',
                        alert.message || '',
                        lat,
                        lon,
                        alert.timestamp || new Date().toISOString(),
                      ],
                    ],
                  },
                },
                option: { centerMap: false },
              })
            );
          }
        } catch (err) {
          console.error('WebSocket message error:', err);
        }
      };

      ws.onclose = () => {
        setStatus('disconnected');
        console.log('WebSocket disconnected, reconnecting in 5s...');
        setTimeout(connectWebSocket, 5000);
      };

      ws.onerror = (err) => {
        console.error('WebSocket error:', err);
        ws.close();
      };
    }

    connectWebSocket();

    return () => {
      if (wsRef.current) wsRef.current.close();
    };
  }, [dispatch]);

  return (
    <div style={{ position: 'relative', width: '100%', height: '100%' }}>
      {/* Alert ticker */}
      <div
        style={{
          position: 'absolute',
          top: 10,
          right: 10,
          zIndex: 1000,
          background: 'rgba(0,0,0,0.85)',
          color: '#fff',
          padding: '8px 12px',
          borderRadius: 6,
          maxWidth: 350,
          maxHeight: 300,
          overflow: 'auto',
          fontSize: 12,
          fontFamily: 'monospace',
        }}
      >
        <div style={{ marginBottom: 4 }}>
          <strong>MDA Alerts</strong>
          <span
            style={{
              marginLeft: 8,
              color: status === 'connected' ? '#4caf50' : '#f44336',
            }}
          >
            {status === 'connected' ? '\u25CF Live' : '\u25CB Offline'}
          </span>
        </div>
        {alerts.length === 0 && <div style={{ color: '#888' }}>No alerts yet</div>}
        {alerts.slice(0, 10).map((a, i) => (
          <div
            key={i}
            style={{
              borderTop: '1px solid #333',
              paddingTop: 4,
              marginTop: 4,
              color: a.severity === 'CRITICAL' ? '#f44336' : a.severity === 'HIGH' ? '#ff9800' : '#fff',
            }}
          >
            <strong>[{a.severity || 'INFO'}]</strong> {a.alert_type || a.event_type || 'Alert'}
            <br />
            <span style={{ color: '#aaa' }}>{a.timestamp || ''}</span>
          </div>
        ))}
      </div>

      {/* Kepler.gl map */}
      <AutoSizer>
        {({ height, width }) => (
          <KeplerGl
            id="mda-map"
            mapboxApiAccessToken=""
            width={width}
            height={height}
          />
        )}
      </AutoSizer>
    </div>
  );
}

export default App;
