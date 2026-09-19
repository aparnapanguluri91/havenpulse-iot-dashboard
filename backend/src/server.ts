import express, { NextFunction, Request, Response } from 'express'
import cors from 'cors'
import http from 'node:http'
import fs from 'node:fs/promises'
import path from 'node:path'
import mqtt from 'mqtt'
import { Pool } from 'pg'
import { Server } from 'socket.io'
import { z } from 'zod'

const app = express()
const PORT = Number(process.env.PORT || 5000)
const pool = new Pool({ connectionString: process.env.DATABASE_URL || 'postgres://iot:iot@localhost:5432/iot' })
const server = http.createServer(app)
const io = new Server(server, { cors: { origin: process.env.CORS_ORIGIN || '*' } })

app.use(cors())
app.use(express.json())

type Reading = {
  sensorId: string; location: string; metric: 'temperature' | 'humidity' | 'vibration';
  value: number; unit: string; timestamp: string; source?: string
}

const readingSchema = z.object({
  sensorId: z.string().min(1), location: z.string().min(1),
  metric: z.enum(['temperature', 'humidity', 'vibration']), value: z.number(),
  unit: z.string(), timestamp: z.string().datetime().optional(), source: z.string().optional()
})

async function migrate() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS readings (
      id BIGSERIAL PRIMARY KEY, sensor_id TEXT NOT NULL, location TEXT NOT NULL,
      metric TEXT NOT NULL, value DOUBLE PRECISION NOT NULL, unit TEXT NOT NULL,
      recorded_at TIMESTAMPTZ NOT NULL, source TEXT NOT NULL DEFAULT 'historical'
    );
    CREATE INDEX IF NOT EXISTS idx_readings_metric_time ON readings(metric, recorded_at DESC);
    CREATE TABLE IF NOT EXISTS activity (
      id BIGSERIAL PRIMARY KEY, network_id INTEGER NOT NULL, activity DOUBLE PRECISION NOT NULL,
      recorded_at TIMESTAMPTZ NOT NULL UNIQUE
    );
    CREATE INDEX IF NOT EXISTS idx_activity_time ON activity(recorded_at DESC);
    CREATE TABLE IF NOT EXISTS alert_settings (
      metric TEXT PRIMARY KEY, min_value DOUBLE PRECISION, max_value DOUBLE PRECISION
    );
    INSERT INTO alert_settings(metric,min_value,max_value) VALUES
      ('temperature',18,27),('humidity',30,65),('vibration',NULL,0)
    ON CONFLICT(metric) DO NOTHING;
  `)
}

async function insertReadings(rows: Reading[]) {
  if (!rows.length) return
  const params: unknown[] = []
  const values = rows.map((r, i) => {
    const n = i * 7
    params.push(r.sensorId, r.location, r.metric, r.value, r.unit, r.timestamp, r.source || 'historical')
    return `($${n+1},$${n+2},$${n+3},$${n+4},$${n+5},$${n+6},$${n+7})`
  })
  await pool.query(`INSERT INTO readings(sensor_id,location,metric,value,unit,recorded_at,source) VALUES ${values.join(',')}`, params)
}

async function seed() {
  const { rows: [{ count }] } = await pool.query('SELECT COUNT(*)::int count FROM readings')
  if (count > 0) return
  const dataDir = process.env.DATA_DIR || path.resolve(process.cwd(), '../data')
  const [sensorRaw, activityRaw] = await Promise.all([
    fs.readFile(path.join(dataDir, 'sensors.json'), 'utf8'), fs.readFile(path.join(dataDir, 'activity.json'), 'utf8')
  ])
  const events = JSON.parse(sensorRaw).sensors as Array<Record<string, unknown>>
  const readings: Reading[] = events.map(event => {
    const payload = JSON.parse(String(event.payload || '{}'))
    const metric = 'temperature' in payload ? 'temperature' : 'humidity' in payload ? 'humidity' : 'vibration'
    return { sensorId: String(event.thingName), location: String(event.locationName), metric,
      value: metric === 'vibration' ? 1 : Number(payload[metric]), unit: metric === 'vibration' ? 'event' : String(payload.unit),
      timestamp: String(event.date), source: 'historical' }
  })
  for (let i = 0; i < readings.length; i += 500) await insertReadings(readings.slice(i, i + 500))
  const activities = JSON.parse(activityRaw).activity as Array<{network_id:number;activity:number;time:string}>
  for (let i = 0; i < activities.length; i += 1000) {
    const chunk = activities.slice(i, i + 1000); const params: unknown[] = []
    const values = chunk.map((x, j) => { const n=j*3; params.push(x.network_id,x.activity,x.time); return `($${n+1},$${n+2},$${n+3})` })
    await pool.query(`INSERT INTO activity(network_id,activity,recorded_at) VALUES ${values.join(',')} ON CONFLICT DO NOTHING`, params)
  }
  console.log(`Seeded ${readings.length} readings and ${activities.length} activity buckets`)
}

async function thresholds(metric: string) {
  const { rows } = await pool.query('SELECT min_value, max_value FROM alert_settings WHERE metric=$1', [metric])
  return rows[0] || null
}

function isAnomaly(value: number, limits: {min_value:number|null;max_value:number|null}|null) {
  return !!limits && ((limits.min_value != null && value < limits.min_value) || (limits.max_value != null && value > limits.max_value))
}

async function processReading(reading: Reading) {
  await insertReadings([{ ...reading, source: 'mqtt' }])
  const limits = await thresholds(reading.metric)
  io.emit('reading', { ...reading, anomaly: isAnomaly(reading.value, limits), receivedAt: new Date().toISOString() })
}

app.get('/api/health', async (_req, res) => {
  await pool.query('SELECT 1'); res.json({ status: 'ok', mqtt: mqttClient?.connected || false, time: new Date().toISOString() })
})

app.get('/api/sensors', async (_req, res) => {
  const { rows } = await pool.query(`SELECT DISTINCT ON (metric) sensor_id AS "sensorId", location, metric,
    value, unit, recorded_at AS "timestamp" FROM readings ORDER BY metric, recorded_at DESC`)
  res.json(rows)
})

app.get('/api/readings', async (req, res) => {
  const query = z.object({ metric:z.enum(['temperature','humidity','vibration']), hours:z.coerce.number().min(1).max(8760).default(24), limit:z.coerce.number().min(1).max(1000).default(300) }).parse(req.query)
  const { rows } = await pool.query(`SELECT sensor_id AS "sensorId",location,metric,value,unit,recorded_at AS "timestamp",source
    FROM readings WHERE metric=$1 AND ((source='historical' AND recorded_at >=
      (SELECT MAX(recorded_at) FROM readings WHERE metric=$1 AND source='historical') - ($2 || ' hours')::interval) OR source='mqtt')
    ORDER BY recorded_at DESC LIMIT $3`, [query.metric, query.hours, query.limit])
  res.json(rows.reverse())
})

app.get('/api/activity', async (req, res) => {
  const hours = Math.min(Math.max(Number(req.query.hours || 24), 1), 8760)
  const { rows } = await pool.query(`SELECT activity,recorded_at AS "timestamp" FROM activity
    WHERE recorded_at >= (SELECT MAX(recorded_at) FROM activity) - ($1 || ' hours')::interval ORDER BY recorded_at`, [hours])
  res.json(rows)
})

app.get('/api/insights', async (_req, res) => {
  const { rows } = await pool.query(`SELECT metric, ROUND(AVG(value)::numeric,1) average, MIN(value) minimum, MAX(value) maximum,
    COUNT(*)::int samples FROM readings r WHERE (source='mqtt' OR recorded_at >=
      (SELECT MAX(recorded_at) FROM readings h WHERE h.metric=r.metric AND h.source='historical') - interval '24 hours') GROUP BY metric`)
  const activity = await pool.query(`SELECT ROUND(AVG(activity)::numeric,1) average, ROUND(SUM(activity)::numeric,0) total
    FROM activity WHERE recorded_at >= (SELECT MAX(recorded_at) FROM activity) - interval '24 hours'`)
  res.json({ readings: rows, activity: activity.rows[0] })
})

app.get('/api/settings', async (_req,res) => res.json((await pool.query('SELECT metric,min_value AS "minValue",max_value AS "maxValue" FROM alert_settings ORDER BY metric')).rows))
app.put('/api/settings/:metric', async (req,res) => {
  const metric = z.enum(['temperature','humidity','vibration']).parse(req.params.metric)
  const body = z.object({minValue:z.number().nullable(),maxValue:z.number().nullable()}).parse(req.body)
  const { rows } = await pool.query(`UPDATE alert_settings SET min_value=$2,max_value=$3 WHERE metric=$1 RETURNING metric,min_value AS "minValue",max_value AS "maxValue"`,[metric,body.minValue,body.maxValue])
  res.json(rows[0])
})

app.get('/api/export.csv', async (req,res) => {
  const metric = z.enum(['temperature','humidity','vibration']).default('temperature').parse(req.query.metric)
  const { rows } = await pool.query('SELECT recorded_at,value,unit,location FROM readings WHERE metric=$1 ORDER BY recorded_at DESC LIMIT 5000',[metric])
  res.type('text/csv').attachment(`${metric}-readings.csv`).send(`timestamp,value,unit,location\n${rows.map(r => `${r.recorded_at.toISOString()},${r.value},${r.unit},${r.location}`).join('\n')}`)
})

app.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
  console.error(error); const message = error instanceof Error ? error.message : 'Unexpected error'
  res.status(error instanceof z.ZodError ? 400 : 500).json({ error: message })
})

let mqttClient: mqtt.MqttClient
function connectMqtt() {
  mqttClient = mqtt.connect(process.env.MQTT_URL || 'mqtt://localhost:1883', { reconnectPeriod: 2000 })
  mqttClient.on('connect', () => { console.log('MQTT connected'); mqttClient.subscribe('home/sensors/+/reading') })
  mqttClient.on('message', async (_topic, payload) => {
    try { const parsed = readingSchema.parse(JSON.parse(payload.toString())); await processReading({ ...parsed, timestamp: parsed.timestamp || new Date().toISOString() }) }
    catch (error) { console.error('Rejected MQTT message', error) }
  })
}

function startSimulator() {
  if (process.env.SIMULATE === 'false') return
  let tick = 0
  setInterval(() => {
    if (!mqttClient.connected) return
    tick++
    const now = new Date().toISOString()
    const readings: Reading[] = [
      {sensorId:'SENSOR_7C3E822F6E550000',location:'Bathroom',metric:'temperature',value:+(23+Math.sin(tick/4)*2+Math.random()).toFixed(1),unit:'°C',timestamp:now},
      {sensorId:'SENSOR_7C3E822F6E550000',location:'Bathroom',metric:'humidity',value:Math.round(50+Math.sin(tick/5)*9+Math.random()*3),unit:'%',timestamp:now}
    ]
    if (tick % 4 === 0) readings.push({sensorId:'SENSOR_DOOR_01',location:'Front Door',metric:'vibration',value:1,unit:'event',timestamp:now})
    readings.forEach(r => mqttClient.publish(`home/sensors/${r.sensorId}/reading`, JSON.stringify(r)))
  }, Number(process.env.SIMULATION_INTERVAL_MS || 3000))
}

async function bootstrap() {
  await migrate(); await seed(); connectMqtt(); startSimulator()
  server.listen(PORT, () => console.log(`Backend running on http://localhost:${PORT}`))
}
bootstrap().catch(error => { console.error(error); process.exit(1) })
