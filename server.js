require('dotenv').config()
const { Client, LocalAuth } = require('whatsapp-web.js')
const puppeteer = require('puppeteer')
const qrcode = require('qrcode-terminal')
const supabase = require('./supabase')
const numeral = require('numeral')
const cron = require('node-cron')
const fs = require('fs')
const path = require('path')

const PUPPETEER_ARGS = [
  '--no-sandbox',
  '--disable-setuid-sandbox',
  '--disable-dev-shm-usage',
  '--disable-gpu',
  '--single-process'
]

const client = new Client({
  authStrategy: new LocalAuth({
    dataPath: './session'
  }),
  puppeteer: {
    executablePath: process.env.PUPPETEER_EXECUTABLE_PATH || puppeteer.executablePath(),
    headless: true,
    args: PUPPETEER_ARGS
  }
})

const sessionDir = path.join(process.cwd(), 'session')
if (fs.existsSync(sessionDir)) {
  ;['SingletonLock', 'DevToolsActivePort'].forEach((f) => {
    const p = path.join(sessionDir, f)
    if (fs.existsSync(p)) fs.rmSync(p, { force: true })
  })
}

client.on('qr', (qr) => qrcode.generate(qr, { small: true }))
client.on('ready', () => console.log('WhatsApp conectado'))
client.initialize()

const toWhatsappNumber = (num) => {
  let limpio = String(num || '').replace(/\D/g, '')
  if (limpio.length === 10) limpio = '57' + limpio
  return limpio + '@c.us'
}

console.log('Esperando nuevas ventas...')

let channel = null
let pollingStarted = false

const POLL_INTERVAL_MS = 10000
const TABLE_LIBRIADO = process.env.SUPABASE_VENTAS_TABLE || 'ventas_libriado'
const TABLE_VENTAS = 'ventas'
const TABLE_PAGOS = 'pagos'
const BOT_TIMEZONE = process.env.BOT_TIMEZONE || 'America/Bogota'
const FIADO_STATES = new Set(['fiado', 'pendiente', 'parcial'])
const RECENT_SALE_WINDOW_MS = 2 * 60 * 1000

const statePath = path.join(process.cwd(), '.bot_state.json')
let botState = {
  lastLibriadoId: 0,
  lastVentaId: 0,
  lastPagoId: 0
}
const recentSaleNotifications = new Map()

function loadBotState() {
  try {
    if (!fs.existsSync(statePath)) return
    const parsed = JSON.parse(fs.readFileSync(statePath, 'utf8'))
    botState.lastLibriadoId = Number(parsed?.lastLibriadoId || 0)
    botState.lastVentaId = Number(parsed?.lastVentaId || 0)
    botState.lastPagoId = Number(parsed?.lastPagoId || 0)
  } catch (error) {
    console.error('No se pudo leer .bot_state.json, se usara estado inicial:', error.message)
  }
}

function saveBotState() {
  try {
    fs.writeFileSync(statePath, JSON.stringify(botState, null, 2))
  } catch (error) {
    console.error('No se pudo guardar .bot_state.json:', error.message)
  }
}

function advanceLibriadoCursor(id) {
  const num = Number(id || 0)
  if (num > botState.lastLibriadoId) {
    botState.lastLibriadoId = num
    saveBotState()
  }
}

function advanceVentaCursor(id) {
  const num = Number(id || 0)
  if (num > botState.lastVentaId) {
    botState.lastVentaId = num
    saveBotState()
  }
}

function advancePagoCursor(id) {
  const num = Number(id || 0)
  if (num > botState.lastPagoId) {
    botState.lastPagoId = num
    saveBotState()
  }
}

function markRecentSale(ventaId) {
  if (!ventaId) return
  recentSaleNotifications.set(Number(ventaId), Date.now())
}

function wasRecentlyNotifiedSale(ventaId) {
  if (!ventaId) return false
  const key = Number(ventaId)
  const ts = recentSaleNotifications.get(key)
  if (!ts) return false
  if (Date.now() - ts > RECENT_SALE_WINDOW_MS) {
    recentSaleNotifications.delete(key)
    return false
  }
  return true
}

loadBotState()

const SUPABASE_HOST = (() => {
  try {
    return new URL(process.env.SUPABASE_URL || process.env.VITE_SUPABASE_URL).host
  } catch {
    return 'URL invalida'
  }
})()

console.log(`Supabase host: ${SUPABASE_HOST}`)
console.log(`Tabla libriado: ${TABLE_LIBRIADO}`)
console.log(`Tabla ventas: ${TABLE_VENTAS}`)
console.log(`Zona horaria bot: ${BOT_TIMEZONE}`)
console.log(`Cursor inicial libriado: ${botState.lastLibriadoId}`)
console.log(`Cursor inicial ventas: ${botState.lastVentaId}`)
console.log(`Cursor inicial pagos: ${botState.lastPagoId}`)

if (supabase.realtime?.onOpen) {
  supabase.realtime.onOpen(() => {
    console.log('WebSocket Realtime conectado')
  })
}

if (supabase.realtime?.onClose) {
  supabase.realtime.onClose((event) => {
    console.log('WebSocket Realtime cerrado:', event)
  })
}

if (supabase.realtime?.onError) {
  supabase.realtime.onError((event) => {
    console.error('WebSocket Realtime error:', event)
  })
}

const formatCurrency = (value) => `$${numeral(Number(value || 0)).format('0,0')}`

async function getClienteById(clienteId) {
  const { data: cliente, error } = await supabase
    .from('clientes')
    .select('id, nombre, telefono')
    .eq('id', clienteId)
    .single()

  if (error) throw error
  return cliente
}

async function getClienteResumen(clienteId, options = {}) {
  const excludeLibriadoId = options.excludeLibriadoId || null
  const excludeVentaId = options.excludeVentaId || null
  const excludePagoId = options.excludePagoId || null

  const { data: ventasRows, error: ventasRowsError } = await supabase
    .from(TABLE_VENTAS)
    .select('id, total, tipo_venta')
    .eq('cliente_id', clienteId)
  if (ventasRowsError) throw ventasRowsError

  const ventasDirectas = (ventasRows || [])
    .filter((row) => String(row.tipo_venta || '').toLowerCase() !== 'libriado')
    .filter((row) => !excludeVentaId || Number(row.id) !== Number(excludeVentaId))

  const { data: vts, error: ventasError } = await supabase
    .from(TABLE_LIBRIADO)
    .select('id, subtotal, venta_id')
    .eq('cliente_id', clienteId)
  if (ventasError) throw ventasError

  const vtsFiltradas = (vts || [])
    .filter((row) => !excludeLibriadoId || Number(row.id) !== Number(excludeLibriadoId))

  const totalVentasDirectas = ventasDirectas.reduce((a, b) => a + Number(b.total || 0), 0)
  const totalVentasLibriado = vtsFiltradas.reduce((a, b) => a + Number(b.subtotal || 0), 0)
  const totalVentas = totalVentasDirectas + totalVentasLibriado

  const ventaIds = [
    ...new Set([
      ...ventasDirectas.map((row) => row.id),
      ...vtsFiltradas.map((row) => row.venta_id)
    ].filter(Boolean))
  ]

  const [byClienteResp, byVentaResp] = await Promise.all([
    supabase
      .from('pagos')
      .select('id, monto, venta_id, ventas_libriado_id')
      .eq('cliente_id', clienteId),
    ventaIds.length
      ? supabase
          .from('pagos')
          .select('id, monto, venta_id, ventas_libriado_id')
          .in('venta_id', ventaIds)
      : Promise.resolve({ data: [], error: null })
  ])

  if (byClienteResp.error) throw byClienteResp.error
  if (byVentaResp.error) throw byVentaResp.error

  const pagosMap = new Map()
  for (const p of [...(byClienteResp.data || []), ...(byVentaResp.data || [])]) {
    pagosMap.set(Number(p.id), p)
  }

  const pagosFiltrados = [...pagosMap.values()]
    .filter((row) => !excludeVentaId || Number(row.venta_id) !== Number(excludeVentaId))
    .filter((row) => !excludeLibriadoId || Number(row.ventas_libriado_id) !== Number(excludeLibriadoId))
    .filter((row) => !excludePagoId || Number(row.id) !== Number(excludePagoId))

  const totalPagos = pagosFiltrados.reduce((a, b) => a + Number(b.monto || 0), 0)
  const saldoActual = Math.max(0, totalVentas - totalPagos)

  return { totalVentas, totalPagos, saldoActual }
}

function buildContadoMessage(clienteNombre, payload) {
  const kilosTxt = payload.kilos != null ? `${payload.kilos} kg` : 'No especificada'
  const valorTxt = formatCurrency(payload.valor || 0)

  return (
    `*BURITICA PORCICULTURA*\n\n` +
    `Estimado(a) ${clienteNombre}, gracias por su compra.\n\n` +
    `Hemos registrado su compra pagada de contado:\n` +
    `- Cantidad: ${kilosTxt}\n` +
    `- Valor pagado: ${valorTxt}\n\n` +
    `Agradecemos su preferencia.`
  )
}

function buildFiadoMessage(clienteNombre, payload) {
  const kilosTxt = payload.kilos != null ? `${payload.kilos} kg` : 'No especificada'
  const valorCompra = Math.max(0, Number(payload.valorCompra || payload.deudaNueva || 0))
  const abonoCompra = Math.max(0, Number(payload.abonoCompra || 0))
  const deudaNueva = Math.max(0, Number(payload.deudaNueva || 0))
  const deudaAnterior = Math.max(0, Number(payload.deudaAnterior || 0))
  const deudaTotalActual = deudaAnterior + deudaNueva

  return (
    `*BURITICA PORCICULTURA*\n\n` +
    `Estimado(a) ${clienteNombre}, gracias por su compra.\n\n` +
    `Hemos registrado su compra a credito:\n` +
    `- Cantidad: ${kilosTxt}\n` +
    `- Valor total de compra: ${formatCurrency(valorCompra)}\n` +
    `- Abono en esta compra: ${formatCurrency(abonoCompra)}\n` +
    `- Saldo de esta compra: ${formatCurrency(deudaNueva)}\n\n` +
    `Resumen de cartera:\n` +
    `- Deuda anterior: ${formatCurrency(deudaAnterior)}\n` +
    `- Nueva deuda: ${formatCurrency(deudaNueva)}\n` +
    `- Deuda total actual: ${formatCurrency(deudaTotalActual)}\n\n` +
    `Muchas gracias por su confianza.`
  )
}

async function processVentaLibriado(venta) {
  try {
    const cliente = await getClienteById(venta.cliente_id)
    if (!cliente?.telefono) return

    const estadoVenta = String(venta.estado || '').toLowerCase()
    const esFiado = FIADO_STATES.has(estadoVenta)

    let mensaje = ''
    if (esFiado) {
      const { data: pagosFila, error: pagosFilaError } = await supabase
        .from(TABLE_PAGOS)
        .select('monto')
        .eq('ventas_libriado_id', venta.id)
      if (pagosFilaError) throw pagosFilaError

      const abonoCompra = (pagosFila || []).reduce((acc, p) => acc + Number(p.monto || 0), 0)
      const valorCompra = Number(venta.subtotal || 0)
      const deudaNueva = Number(venta.subtotal || 0)
      const { saldoActual: deudaAnterior } = await getClienteResumen(cliente.id, { excludeLibriadoId: venta.id })
      mensaje = buildFiadoMessage(cliente.nombre, {
        kilos: venta.kilos,
        valorCompra,
        abonoCompra,
        deudaAnterior,
        deudaNueva
      })
    } else {
      mensaje = buildContadoMessage(cliente.nombre, {
        kilos: venta.kilos,
        valor: venta.subtotal
      })
    }

    await client.sendMessage(toWhatsappNumber(cliente.telefono), mensaje)
    markRecentSale(venta.venta_id)
    console.log('Mensaje libriado enviado a ' + cliente.nombre)
  } catch (err) {
    console.error('Error procesando venta libriado:', err)
  }
}

async function processVentaDirecta(venta) {
  try {
    const tipo = String(venta.tipo_venta || '').toLowerCase()
    if (tipo === 'libriado') return

    const cliente = await getClienteById(venta.cliente_id)
    if (!cliente?.telefono) return

    const { data: detalle, error: detalleError } = await supabase
      .from('detalle_venta')
      .select('cantidad')
      .eq('venta_id', venta.id)
      .order('id', { ascending: true })
      .limit(1)
      .maybeSingle()
    if (detalleError) throw detalleError

    const kilos = detalle?.cantidad ?? null
    const estadoVenta = String(venta.estado || '').toLowerCase()
    const esFiado = FIADO_STATES.has(estadoVenta)

    const { saldoActual: deudaAnterior } = await getClienteResumen(cliente.id, { excludeVentaId: venta.id })

    const { data: pagosVentaRows, error: pagosVentaError } = await supabase
      .from('pagos')
      .select('monto')
      .eq('venta_id', venta.id)
    if (pagosVentaError) throw pagosVentaError

    const pagadoVenta = (pagosVentaRows || []).reduce((acc, row) => acc + Number(row.monto || 0), 0)
    const deudaNueva = Math.max(0, Number(venta.total || 0) - pagadoVenta)

    const mensaje = esFiado
      ? buildFiadoMessage(cliente.nombre, {
          kilos,
          valorCompra: Number(venta.total || 0),
          abonoCompra: pagadoVenta,
          deudaAnterior,
          deudaNueva
        })
      : buildContadoMessage(cliente.nombre, { kilos, valor: venta.total })

    await client.sendMessage(toWhatsappNumber(cliente.telefono), mensaje)
    markRecentSale(venta.id)
    console.log(`Mensaje ${tipo || 'venta'} enviado a ${cliente.nombre}`)
  } catch (err) {
    console.error('Error procesando venta directa:', err)
  }
}

async function onVentaLibriadoInsert(payload) {
  const venta = payload?.new
  if (!venta?.id) return

  console.log('Venta libriado detectada por Realtime')
  await processVentaLibriado(venta)
  advanceLibriadoCursor(venta.id)
}

async function onVentaDirectaInsert(payload) {
  const venta = payload?.new
  if (!venta?.id) return

  advanceVentaCursor(venta.id)
  if (String(venta.tipo_venta || '').toLowerCase() === 'libriado') return

  console.log('Venta en pie/canal detectada por Realtime')
  await processVentaDirecta(venta)
}

async function resolvePagoClienteId(pago) {
  if (pago.cliente_id) return Number(pago.cliente_id)

  if (pago.ventas_libriado_id) {
    const { data, error } = await supabase
      .from(TABLE_LIBRIADO)
      .select('cliente_id')
      .eq('id', pago.ventas_libriado_id)
      .maybeSingle()
    if (error) throw error
    if (data?.cliente_id) return Number(data.cliente_id)
  }

  if (pago.venta_id) {
    const { data, error } = await supabase
      .from(TABLE_VENTAS)
      .select('cliente_id')
      .eq('id', pago.venta_id)
      .maybeSingle()
    if (error) throw error
    if (data?.cliente_id) return Number(data.cliente_id)
  }

  return null
}

async function processPagoAbono(pago) {
  try {
    if (Number(pago.monto || 0) <= 0) return
    if (wasRecentlyNotifiedSale(pago.venta_id)) return

    const clienteId = await resolvePagoClienteId(pago)
    if (!clienteId) return

    const cliente = await getClienteById(clienteId)
    if (!cliente?.telefono) return

    const { saldoActual } = await getClienteResumen(cliente.id)
    const abono = Number(pago.monto || 0)
    const deudaAntes = Math.max(0, saldoActual + abono)
    const deudaDespues = Math.max(0, saldoActual)

    if (deudaAntes <= 0 || deudaAntes === deudaDespues) return

    let mensaje = ''
    if (deudaDespues <= 0) {
      mensaje =
        `*BURITICA PORCICULTURA*\n\n` +
        `Estimado(a) ${cliente.nombre}, hemos registrado su pago.\n\n` +
        `Detalle:\n` +
        `- Deuda anterior: ${formatCurrency(deudaAntes)}\n` +
        `- Abono realizado: ${formatCurrency(abono)}\n` +
        `- Saldo pendiente: ${formatCurrency(deudaDespues)}\n\n` +
        `Su deuda ha quedado totalmente cancelada. Muchas gracias por su cumplimiento y confianza.`
    } else {
      mensaje =
        `*BURITICA PORCICULTURA*\n\n` +
        `Estimado(a) ${cliente.nombre}, hemos registrado su abono.\n\n` +
        `Detalle:\n` +
        `- Deuda anterior: ${formatCurrency(deudaAntes)}\n` +
        `- Abono realizado: ${formatCurrency(abono)}\n` +
        `- Saldo pendiente actual: ${formatCurrency(deudaDespues)}\n\n` +
        `Gracias por mantener sus pagos al dia.`
    }

    await client.sendMessage(toWhatsappNumber(cliente.telefono), mensaje)
    console.log('Mensaje de abono enviado a ' + cliente.nombre)
  } catch (error) {
    console.error('Error procesando pago:', error)
  }
}

async function onPagoInsert(payload) {
  const pago = payload?.new
  if (!pago?.id) return

  await processPagoAbono(pago)
  advancePagoCursor(pago.id)
}

async function initPollingBaseline() {
  const [libriadoResp, ventasResp, pagosResp] = await Promise.all([
    supabase.from(TABLE_LIBRIADO).select('id').order('id', { ascending: false }).limit(1),
    supabase.from(TABLE_VENTAS).select('id').order('id', { ascending: false }).limit(1),
    supabase.from(TABLE_PAGOS).select('id').order('id', { ascending: false }).limit(1)
  ])

  if (!botState.lastLibriadoId && !libriadoResp.error) {
    botState.lastLibriadoId = Number(libriadoResp.data?.[0]?.id || 0)
  }
  if (!botState.lastVentaId && !ventasResp.error) {
    botState.lastVentaId = Number(ventasResp.data?.[0]?.id || 0)
  }
  if (!botState.lastPagoId && !pagosResp.error) {
    botState.lastPagoId = Number(pagosResp.data?.[0]?.id || 0)
  }

  saveBotState()
  console.log(`Baseline libriado: ${botState.lastLibriadoId}`)
  console.log(`Baseline ventas: ${botState.lastVentaId}`)
  console.log(`Baseline pagos: ${botState.lastPagoId}`)
}

async function pollVentasLibriado() {
  const { data, error } = await supabase
    .from(TABLE_LIBRIADO)
    .select('id, cliente_id, kilos, subtotal, estado')
    .gt('id', botState.lastLibriadoId)
    .order('id', { ascending: true })
    .limit(500)

  if (error) {
    console.error('Error polling libriado:', error)
    return
  }

  for (const venta of data || []) {
    await processVentaLibriado(venta)
    advanceLibriadoCursor(venta.id)
  }
}

async function pollVentasDirectas() {
  const { data, error } = await supabase
    .from(TABLE_VENTAS)
    .select('id, cliente_id, total, estado, tipo_venta')
    .gt('id', botState.lastVentaId)
    .order('id', { ascending: true })
    .limit(500)

  if (error) {
    console.error('Error polling ventas directas:', error)
    return
  }

  for (const venta of data || []) {
    advanceVentaCursor(venta.id)
    if (String(venta.tipo_venta || '').toLowerCase() === 'libriado') continue
    await processVentaDirecta(venta)
  }
}

async function pollPagos() {
  const { data, error } = await supabase
    .from(TABLE_PAGOS)
    .select('id, cliente_id, venta_id, ventas_libriado_id, monto, fecha')
    .gt('id', botState.lastPagoId)
    .order('id', { ascending: true })
    .limit(500)

  if (error) {
    console.error('Error polling pagos:', error)
    return
  }

  for (const pago of data || []) {
    await processPagoAbono(pago)
    advancePagoCursor(pago.id)
  }
}

async function pollAll() {
  await pollVentasLibriado()
  await pollVentasDirectas()
  await pollPagos()
}

async function startPollingFallback() {
  if (pollingStarted) return
  pollingStarted = true

  console.log(`Activando fallback por polling cada ${POLL_INTERVAL_MS / 1000}s (Realtime no disponible)`)
  await initPollingBaseline()
  setInterval(pollAll, POLL_INTERVAL_MS)
}

async function sendDebtReminders() {
  try {
    const { data: clientes, error: clientesError } = await supabase
      .from('clientes')
      .select('id, nombre, telefono')
      .not('telefono', 'is', null)
    if (clientesError) throw clientesError

    let enviados = 0
    for (const cliente of clientes || []) {
      const { saldoActual } = await getClienteResumen(cliente.id)
      if (saldoActual <= 0) continue

      const mensaje =
        `*BURITICA PORCICULTURA*\n\n` +
        `Estimado(a) ${cliente.nombre}, le enviamos un recordatorio amable de pago.\n\n` +
        `Su saldo pendiente actual es: ${formatCurrency(saldoActual)}\n\n` +
        `Agradecemos su pronta gestion y su comprension.\n\n` +
        `Este mensaje es una automatizacion. Si registra deuda pendiente, recibira recordatorio automatico los dias 1 y 16 de cada mes a las 10:00 a.m.`

      await client.sendMessage(toWhatsappNumber(cliente.telefono), mensaje)
      enviados += 1
    }

    console.log(`Recordatorios enviados: ${enviados}`)
  } catch (error) {
    console.error('Error enviando recordatorios:', error)
  }
}

function startReminderSchedule() {
  cron.schedule(
    '0 10 1,16 * *',
    () => {
      console.log('Ejecutando recordatorios automaticos (1 y 16, 10:00 a.m.)')
      sendDebtReminders()
    },
    { timezone: BOT_TIMEZONE }
  )
}

function subscribeVentas() {
  if (channel) {
    supabase.removeChannel(channel)
  }

  channel = supabase
    .channel('cambios-ventas')
    .on('postgres_changes', { event: 'INSERT', schema: 'public', table: TABLE_LIBRIADO }, onVentaLibriadoInsert)
    .on('postgres_changes', { event: 'INSERT', schema: 'public', table: TABLE_VENTAS }, onVentaDirectaInsert)
    .on('postgres_changes', { event: 'INSERT', schema: 'public', table: TABLE_PAGOS }, onPagoInsert)
    .subscribe((status, err) => {
      console.log('Estado Realtime:', status)
      if (err) console.error('Error Realtime:', err)

      if (status === 'TIMED_OUT' || status === 'CHANNEL_ERROR' || status === 'CLOSED') {
        if (!pollingStarted) {
          console.log('Reintentando suscripcion en 5 segundos...')
        }
        startPollingFallback().catch((e) => {
          console.error('Error activando polling fallback:', e)
        })
        if (!pollingStarted) {
          setTimeout(subscribeVentas, 5000)
        }
      }
    })
}

subscribeVentas()
startReminderSchedule()

