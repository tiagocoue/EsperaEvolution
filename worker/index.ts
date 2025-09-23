// worker/index.ts (Node 18+)
import { createClient } from '@supabase/supabase-js'

/* =========================
   ENV
   ========================= */
const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY
const EVO_BASE = (process.env.EVOLUTION_API_URL || '').replace(/\/+$/, '')
const EVO_KEY = process.env.EVOLUTION_API_KEY

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !EVO_BASE || !EVO_KEY) {
  console.error('[worker] missing env vars')
  console.error({
    SUPABASE_URL: !!SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY: !!SUPABASE_SERVICE_ROLE_KEY,
    EVO_BASE: !!EVO_BASE,
    EVO_KEY: !!EVO_KEY,
  })
  process.exit(1)
}

const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
const WORKER_ID = `worker-${Math.random().toString(36).slice(2, 6)}`

/* =========================
   EVO HELPERS
   ========================= */
function normalizeNumber(input?: string | null): string {
  if (!input) return ''
  // remove JID/sufixos e tudo que não for dígito
  const base = input.split('@')[0].split(':')[0]
  return base.replace(/[^\d]/g, '').trim()
}

async function evoPostRaw(path: string, body: Record<string, unknown>) {
  const url = `${EVO_BASE}${path}`
  return fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', apikey: EVO_KEY as string },
    body: JSON.stringify(body),
  })
}

async function evoPostJson(path: string, body: Record<string, unknown>) {
  const res = await evoPostRaw(path, body)
  if (!res.ok) throw new Error(`${path} ${res.status} ${await res.text().catch(() => '')}`)
  return res.json().catch(() => ({}))
}

function isDataUri(s: string) {
  return typeof s === 'string' && s.startsWith('data:')
}

function inferFilenameAndMime(media: string): { filename: string; mimetype: string } {
  try {
    const u = new URL(media)
    const name = u.pathname.split('/').pop() || 'file.bin'
    const lower = name.toLowerCase()
    if (lower.endsWith('.mp3')) return { filename: name, mimetype: 'audio/mpeg' }
    if (lower.endsWith('.wav')) return { filename: name, mimetype: 'audio/wav' }
    if (lower.endsWith('.ogg') || lower.endsWith('.oga')) return { filename: name, mimetype: 'audio/ogg' }
    return { filename: name, mimetype: 'application/octet-stream' }
  } catch {
    // data URI ou string sem URL válida
    return { filename: 'audio.ogg', mimetype: isDataUri(media) ? guessMimeFromDataUri(media) : 'audio/ogg' }
  }
}

function guessMimeFromDataUri(dataUri: string): string {
  const m = /^data:([^;]+);base64,/.exec(dataUri)
  return m?.[1] || 'audio/ogg'
}

/* =========================
   EVO SEND (texto, imagem, áudio/PTT, presence, notify)
   ========================= */
type SendKind = 'text' | 'image' | 'audio' | 'presence' | 'notify'

async function evoSend(
  kind: SendKind,
  instance: string,
  number: string,
  payload: Record<string, any>
) {
  // para notify o destino vem do payload; para os demais, do arg number
  const num = kind === 'notify' ? normalizeNumber(payload.number) : normalizeNumber(number)

  if (kind === 'notify') {
    const text = (payload.text ?? '').toString().trim()
    if (!num || !text) throw new Error('notify payload inválido (number/text ausentes)')
    const body = { number: num, text, ...(payload.delay ? { delay: payload.delay } : {}) }
    return evoPostJson(`/message/sendText/${instance}`, body)
  }

  if (kind === 'text') {
    const body = { number: num, text: payload.text, ...(payload.delay ? { delay: payload.delay } : {}) }
    return evoPostJson(`/message/sendText/${instance}`, body)
  }

  if (kind === 'image') {
    const media: string = payload.media
    if (!media || typeof media !== 'string') {
      throw new Error('image payload sem media (string URL ou data URI)')
    }

    // Inferir mimetype e nome a partir da URL ou data URI
    const inferImage = (m: string) => {
      // data:image/png;base64,...
      if (isDataUri(m)) {
        const mm = /^data:([^;]+);base64,/i.exec(m)
        const mime = mm?.[1] || 'image/jpeg'
        const ext =
          mime.endsWith('png') ? 'png' :
          mime.endsWith('webp') ? 'webp' :
          mime.endsWith('gif') ? 'gif' :
          'jpg'
        return { mimetype: mime, fileName: `image.${ext}` }
      }
      // URL comum
      try {
        const u = new URL(m)
        const name = u.pathname.split('/').pop() || 'image.jpg'
        const lower = name.toLowerCase()
        const ext = lower.includes('.') ? lower.split('.').pop()! : 'jpg'
        const mime =
          ext === 'png' ? 'image/png' :
          ext === 'webp' ? 'image/webp' :
          ext === 'gif' ? 'image/gif' :
          'image/jpeg'
        return { mimetype: mime, fileName: lower.includes('.') ? name : `image.${ext}` }
      } catch {
        return { mimetype: 'image/jpeg', fileName: 'image.jpg' }
      }
    }

    const { mimetype, fileName } = inferImage(media)

    const body = {
      number: num,
      mediatype: 'image',     // v2.3.x exige isso
      mimetype,
      fileName,
      media,                  // aceita URL pública OU data URI
      ...(payload.caption ? { caption: payload.caption } : {}),
      ...(payload.delay ? { delay: payload.delay } : {}),
    }

    return evoPostJson(`/message/sendMedia/${instance}`, body)
  }

  if (kind === 'presence') {
    // Agora enviando presença de verdade (digitando/gravando)
    const presence = payload.state === 'recording' ? 'recording' : 'composing'
    const duration = Math.max(0, Math.min(60000, Number(payload.durationMs ?? 3000))) // 0..60s
    const body = {
      number: num,
      options: {
        presence,       // "composing" (digitando) | "recording" (gravando)
        delay: duration // duração em ms
      }
    }
    return evoPostJson(`/chat/sendPresence/${instance}`, body)
  }

  if (kind === 'audio') {
    const media: string = payload.media
    const base = { number: num, ...(payload.delay ? { delay: payload.delay } : {}) }
    const { filename, mimetype } = inferFilenameAndMime(media)

    // 0) PTT nativo (documentado): sendWhatsAppAudio { number, audio, delay? }
    {
      const r0 = await evoPostRaw(`/message/sendWhatsAppAudio/${instance}`, {
        number: num,
        audio: media,             // aceita URL pública ou data URI base64
        ...(payload.delay ? { delay: payload.delay } : {}),
      })
      if (r0.ok) return r0.json().catch(() => ({}))
      if (![400, 404].includes(r0.status)) {
        const t = await r0.text().catch(() => '')
        console.error('[evolution][sendWhatsAppAudio] non-4xx', r0.status, t)
        throw new Error(`[evolution] sendWhatsAppAudio ${r0.status} ${t}`)
      }
    }

    // 1) PTT (voice note) legado: sendVoice tentando audio -> media -> base64
    {
      let r = await evoPostRaw(`/message/sendVoice/${instance}`, { ...base, audio: media, mimetype })
      if (r.ok) return r.json().catch(() => ({}))
      if (![400, 404].includes(r.status)) {
        const t = await r.text().catch(() => '')
        console.error('[evolution][sendVoice][audio] non-4xx', r.status, t)
        throw new Error(`[evolution] sendVoice ${r.status} ${t}`)
      }

      r = await evoPostRaw(`/message/sendVoice/${instance}`, { ...base, media: media, mimetype })
      if (r.ok) return r.json().catch(() => ({}))
      if (![400, 404].includes(r.status)) {
        const t = await r.text().catch(() => '')
        console.error('[evolution][sendVoice][media] non-4xx', r.status, t)
        throw new Error(`[evolution] sendVoice ${r.status} ${t}`)
      }

      if (isDataUri(media)) {
        r = await evoPostRaw(`/message/sendVoice/${instance}`, { ...base, base64: media, mimetype })
        if (r.ok) return r.json().catch(() => ({}))
        if (![400, 404].includes(r.status)) {
          const t = await r.text().catch(() => '')
          console.error('[evolution][sendVoice][base64] non-4xx', r.status, t)
          throw new Error(`[evolution] sendVoice ${r.status} ${t}`)
        }
      }
    }

    // 2) Áudio comum (com ptt: true para alguns servidores)
    {
      const r = await evoPostRaw(`/message/sendAudio/${instance}`, { ...base, audio: media, mimetype, ptt: true })
      if (r.ok) return r.json().catch(() => ({}))
      if (![400, 404].includes(r.status)) {
        const t = await r.text().catch(() => '')
        console.error('[evolution][sendAudio][audio+ptt] non-4xx', r.status, t)
        throw new Error(`[evolution] sendAudio ${r.status} ${t}`)
      }
    }

    // 3) Fallback como documento (vai chegar como arquivo)
    {
      const r = await evoPostRaw(`/message/sendMedia/${instance}`, {
        ...base,
        media,
        mimetype,
        mediatype: 'document',
        fileName: filename,
        caption: '',
      })
      if (r.ok) return r.json().catch(() => ({}))
      const t = await r.text().catch(() => '')
      console.error('[evolution][sendMedia][document] failed', r.status, t)
      throw new Error(`[evolution] sendMedia ${r.status} ${t}`)
    }
  }

  throw new Error(`[evolution] unsupported kind: ${kind}`)
}

/* =========================
   JOB QUEUE (fluxo_agendamentos)
   ========================= */
type JobActionKind = 'text' | 'image' | 'audio' | 'presence' | 'notify'

async function enqueueJob(params: {
  conexaoId: string
  fluxoId: string | null
  userId: string | null
  remoteJid: string
  instanceId?: string | null
  instanceName?: string | null
  actionKind: JobActionKind
  payload: Record<string, unknown>
  delayMs?: number
}) {
  const due = new Date(Date.now() + Math.max(0, Number(params.delayMs || 0)))
  await supa.from('fluxo_agendamentos').insert({
    user_id: params.userId,
    whatsapp_conexao_id: params.conexaoId,
    fluxo_id: params.fluxoId,
    remote_jid: params.remoteJid,
    instance_id: params.instanceId ?? null,
    instance_name: params.instanceName ?? null,
    action_kind: params.actionKind,
    payload: params.payload,
    due_at: due.toISOString(),
    status: 'pending',
  })
}

async function claimJobs(limit = 10) {
  const nowIso = new Date().toISOString()
  const { data: candidates, error } = await supa
    .from('fluxo_agendamentos')
    .select('id')
    .eq('status', 'pending')
    .lte('due_at', nowIso)
    .order('due_at', { ascending: true })
    .limit(limit)

  if (error) {
    console.error('[worker][claimJobs] supabase error', error)
    return []
  }
  if (!candidates?.length) return []

  const claimed: any[] = []
  for (const c of candidates) {
    const { data: updated, error: upErr } = await supa
      .from('fluxo_agendamentos')
      .update({ status: 'running', locked_at: new Date().toISOString(), locked_by: WORKER_ID })
      .eq('id', c.id)
      .eq('status', 'pending')
      .select('*')
      .maybeSingle()

    if (upErr) {
      console.error('[worker][claimJobs] update error', upErr)
      continue
    }
    if (updated) claimed.push(updated)
  }
  return claimed
}

async function processJob(job: any) {
  try {
    // payload pode vir como string JSON
    const payload =
      typeof job.payload === 'string' ? JSON.parse(job.payload) : (job.payload || {})

    // para 'notify' o destino é payload.number; para os demais, remote_jid
    const isNotify = job.action_kind === 'notify'
    const number = isNotify ? (payload.number ?? '') : job.remote_jid

    // tenta primeiro pelo NOME da instância, depois pelo ID (fallback)
    const candidates = [job.instance_name, job.instance_id].filter(Boolean) as string[]
    if (candidates.length === 0) throw new Error('sem instance_id/name')

    let sent = false
    let lastErr: unknown = null

    for (const inst of candidates) {
      try {
        await evoSend(job.action_kind as SendKind, inst, number, payload)
        sent = true
        break
      } catch (e) {
        lastErr = e
        const msg = e instanceof Error ? e.message : String(e)
        // Se for erro claro de instância inválida/404, tenta o próximo identificador
        if (!/instance does not exist|Cannot POST|404/.test(msg)) {
          throw e // erro real (payload inválido, etc.) → não adianta trocar instância
        }
      }
    }

    if (!sent) {
      throw (lastErr ?? new Error('Falha ao enviar: nenhum identificador de instância válido.'))
    }

    // log opcional em mensagens (marca notify explicitamente)
    await supa.from('mensagens').insert({
      whatsapp_conexao_id: job.whatsapp_conexao_id,
      fluxo_id: job.fluxo_id,
      user_id: job.user_id,
      de: null,
      para: normalizeNumber(number),
      direcao: 'enviada',
      conteudo: isNotify ? { notify: true, text: payload.text } : payload,
      timestamp: new Date().toISOString(),
    })

    await supa
      .from('fluxo_agendamentos')
      .update({ status: 'done', last_error: null })
      .eq('id', job.id)
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    const attempts = (job.attempts ?? 0) + 1

    if (attempts < (job.max_attempts ?? 5)) {
      const next = new Date(Date.now() + 10_000 * attempts).toISOString()
      await supa
        .from('fluxo_agendamentos')
        .update({
          status: 'pending',
          attempts,
          last_error: msg,
          due_at: next,
          locked_at: null,
          locked_by: null,
        })
        .eq('id', job.id)
    } else {
      await supa
        .from('fluxo_agendamentos')
        .update({ status: 'failed', attempts, last_error: msg })
        .eq('id', job.id)
    }
  }
}

/* =========================
   === AGUARDE === mini-planner
   (rodar do nó target: respeita waits, envia, e para em novo aguarde)
   ========================= */

type DbNode = {
  id: string
  fluxo_id: string
  tipo: string
  conteudo: any
  ordem: number
}

type DbEdge = {
  id: string
  fluxo_id: string
  source: string
  target: string
  data: any
}

function isWaitNode(n: DbNode) {
  return n.tipo === 'mensagem_espera'
}
function isSendNode(n: DbNode) {
  return n.tipo === 'mensagem_texto' || n.tipo === 'mensagem_imagem' || n.tipo === 'mensagem_audio' || n.tipo === 'mensagem_notificada'
}
function isAguardeNode(n: DbNode) {
  return n.tipo === 'aguarde_resposta'
}

function parseWaitSeconds(node: DbNode): number {
  const raw = node?.conteudo?.waitSeconds
  const s = typeof raw === 'number' ? raw : Number(raw)
  return Number.isFinite(s) && s > 0 ? Math.floor(s) : 0
}
function parseText(node: DbNode): string | null {
  const t = typeof node?.conteudo?.text === 'string' ? node.conteudo.text : null
  return t && t.trim() ? t : null
}
function parseImage(node: DbNode): { media: string; caption?: string } | null {
  const c = node.conteudo || {}
  const url = typeof c.url === 'string' && c.url.trim() ? c.url.trim() : undefined
  const b64 = typeof c.base64 === 'string' && c.base64.trim() ? c.base64.trim() : undefined
  const data = typeof c.data === 'string' && c.data.trim() ? c.data.trim() : undefined
  const media = b64 ?? data ?? url ?? null
  if (!media) return null
  const captionRaw = (typeof c.caption === 'string' ? c.caption : undefined) ?? (typeof c.text === 'string' ? c.text : undefined)
  const caption = captionRaw && captionRaw.trim() ? captionRaw.trim() : undefined
  return { media, caption }
}
function parseAudio(node: DbNode): { media: string } | null {
  const c = node.conteudo || {}
  const url = typeof c.url === 'string' ? c.url : undefined
  const b64 = typeof c.base64 === 'string' ? c.base64 : undefined
  const media = b64 ?? url ?? null
  return media ? { media } : null
}
function parseNotify(node: DbNode): { number: string; text: string } | null {
  const c = node.conteudo || {}
  const numero =
    typeof c.numero === 'string'
      ? c.numero.replace(/[^\d]/g, '').trim()
      : typeof c.numero === 'number'
      ? String(c.numero)
      : ''
  const mensagem = typeof c.mensagem === 'string' ? c.mensagem.trim() : ''
  if (!numero || !mensagem) return null
  return { number: numero, text: mensagem }
}
function parseAguarde(node: DbNode): { timeoutSeconds: number; followupText: string } {
  const c = node.conteudo || {}
  const raw = typeof c.timeoutSeconds === 'number' ? c.timeoutSeconds : Number(c.timeoutSeconds)
  const timeoutSeconds = Number.isFinite(raw) && raw > 0 ? Math.min(86400, Math.floor(raw)) : 60
  const followupText = typeof c.followupText === 'string' ? c.followupText.trim() : ''
  return { timeoutSeconds, followupText }
}

function getOutcomeTargets(edges: DbEdge[], nodeId: string): { answered: string | null; no_reply: string | null } {
  let answered: string | null = null
  let no_reply: string | null = null
  for (const e of edges) {
    if (e.source !== nodeId) continue
    const outcome = typeof e?.data?.outcome === 'string' ? e.data.outcome : ''
    if (outcome === 'answered') answered = e.target
    if (outcome === 'no_reply') no_reply = e.target
  }
  return { answered, no_reply }
}

function nextEdge(edges: DbEdge[], nodeId: string): string | null {
  const e = edges.find(ed => ed.source === nodeId)
  return e ? e.target : null
}

type PlannedAction =
  | { kind: 'presence'; delayMs: number; state: 'composing' | 'recording'; durationMs?: number }
  | { kind: 'text'; delayMs: number; text: string }
  | { kind: 'image'; delayMs: number; media: string; caption?: string }
  | { kind: 'audio'; delayMs: number; media: string }
  | { kind: 'notify'; delayMs: number; number: string; text: string }

type PlanResult = {
  actions: PlannedAction[]
  aguarde?: {
    nodeId: string
    timeoutSeconds: number
    followupText: string
    answeredTargetId: string | null
    noReplyTargetId: string | null
  }
}

function planFromNode(nodes: DbNode[], edges: DbEdge[], startId: string): PlanResult {
  const byId = new Map(nodes.map(n => [n.id, n]))
  const actions: PlannedAction[] = []
  const visited = new Set<string>()
  const MAX_STEPS = 20

  let curr: DbNode | undefined = byId.get(startId)
  let elapsedMs = 0
  let steps = 0
  let aguardeInfo: PlanResult['aguarde'] | undefined

  while (curr && steps < MAX_STEPS) {
    if (visited.has(curr.id)) break
    visited.add(curr.id)
    steps++

    if (isWaitNode(curr)) {
      const waitMs = Math.max(0, parseWaitSeconds(curr) * 1000)
      if (waitMs > 0) {
        actions.push({ kind: 'presence', state: 'composing', durationMs: Math.min(waitMs, 60000), delayMs: elapsedMs })
      }
      elapsedMs += waitMs
      const nextId = nextEdge(edges, curr.id)
      curr = nextId ? byId.get(nextId) : undefined
      continue
    }

    if (isAguardeNode(curr)) {
      const { timeoutSeconds, followupText } = parseAguarde(curr)
      const { answered, no_reply } = getOutcomeTargets(edges, curr.id)
      aguardeInfo = {
        nodeId: curr.id,
        timeoutSeconds,
        followupText,
        answeredTargetId: answered,
        noReplyTargetId: no_reply,
      }
      break
    }

    if (isSendNode(curr)) {
      if (curr.tipo === 'mensagem_texto') {
        const t = parseText(curr)
        if (t) actions.push({ kind: 'text', text: t, delayMs: elapsedMs })
      } else if (curr.tipo === 'mensagem_imagem') {
        const img = parseImage(curr)
        if (img) actions.push({ kind: 'image', media: img.media, caption: img.caption, delayMs: elapsedMs })
      } else if (curr.tipo === 'mensagem_audio') {
        const a = parseAudio(curr)
        if (a) actions.push({ kind: 'audio', media: a.media, delayMs: elapsedMs })
      } else if (curr.tipo === 'mensagem_notificada') {
        const n = parseNotify(curr)
        if (n) actions.push({ kind: 'notify', number: n.number, text: n.text, delayMs: elapsedMs })
      }
    }

    const nextId = nextEdge(edges, curr.id)
    curr = nextId ? byId.get(nextId) : undefined
  }

  return { actions, aguarde: aguardeInfo }
}

async function loadGraph(fluxoId: string) {
  const [{ data: nodes }, { data: edges }] = await Promise.all([
    supa.from('fluxo_nos').select('id, fluxo_id, tipo, conteudo, ordem').eq('fluxo_id', fluxoId),
    supa.from('fluxo_edge').select('id, fluxo_id, source, target, data').eq('fluxo_id', fluxoId),
  ])
  return { nodes: (nodes || []) as DbNode[], edges: (edges || []) as DbEdge[] }
}

async function loadConn(conexaoId: string) {
  const { data, error } = await supa
    .from('whatsapp_conexoes')
    .select('id, numero, user_id, instance_id, instance_name')
    .eq('id', conexaoId)
    .maybeSingle()
  if (error) throw error
  return data
}

async function enqueuePlannedActions(params: {
  plan: PlanResult
  fluxoId: string
  conexaoId: string
  remoteJid: string
  userId: string | null
  instanceId: string | null
  instanceName: string | null
}) {
  const { plan, fluxoId, conexaoId, remoteJid, userId, instanceId, instanceName } = params
  for (const act of plan.actions) {
    const kind: JobActionKind =
      act.kind === 'text' ? 'text'
      : act.kind === 'image' ? 'image'
      : act.kind === 'audio' ? 'audio'
      : act.kind === 'presence' ? 'presence'
      : 'notify'

    const payload =
      act.kind === 'text' ? { text: act.text } :
      act.kind === 'image' ? { media: act.media, caption: (act as any).caption ?? null } :
      act.kind === 'audio' ? { media: act.media } :
      act.kind === 'presence' ? { state: act.state, durationMs: act.durationMs ?? 3000 } :
      { number: (act as any).number, text: (act as any).text }

    await enqueueJob({
      conexaoId,
      fluxoId,
      userId,
      remoteJid,
      instanceId,
      instanceName,
      actionKind: kind,
      payload,
      delayMs: act.delayMs,
    })
  }
}

async function createAguardeRow(params: {
  fluxoId: string
  nodeId: string
  remoteJid: string
  conexaoId: string
  userId: string | null
  timeoutSeconds: number
  followupText?: string
  answeredTargetId: string | null
  noReplyTargetId: string | null
}) {
  const now = new Date()
  const expires = new Date(now.getTime() + params.timeoutSeconds * 1000)
  await supa.from('fluxo_esperas').insert({
    status: 'pending',
    created_at: now.toISOString(),
    expires_at: expires.toISOString(),
    fluxo_id: params.fluxoId,
    node_id: params.nodeId,
    remote_jid: params.remoteJid,
    whatsapp_conexao_id: params.conexaoId,
    user_id: params.userId,
    answered_target_id: params.answeredTargetId,
    no_reply_target_id: params.noReplyTargetId,
    followup_text: params.followupText ? { text: params.followupText } : null,
  })
}

/* =========================
   === AGUARDE === processors
   ========================= */

async function claimExpiredEsperas(limit = 20) {
  const nowIso = new Date().toISOString()
  // pega IDs candidatas
  const { data: rows, error } = await supa
    .from('fluxo_esperas')
    .select('id')
    .eq('status', 'pending')
    .lte('expires_at', nowIso)
    .order('expires_at', { ascending: true })
    .limit(limit)
  if (error) {
    console.error('[worker][aguarde][claimExpired] error', error)
    return []
  }
  const claimed: any[] = []
  for (const r of rows || []) {
    // marca como expired de forma atômica
    const { data: upd, error: upErr } = await supa
      .from('fluxo_esperas')
      .update({ status: 'expired' })
      .eq('id', r.id)
      .eq('status', 'pending')
      .select('*')
      .maybeSingle()
    if (upErr) {
      console.error('[worker][aguarde][claimExpired] update error', upErr)
      continue
    }
    if (upd) claimed.push(upd)
  }
  return claimed
}

async function processExpiredEspera(row: any) {
  try {
    const fluxoId = row.fluxo_id as string
    const conexaoId = row.whatsapp_conexao_id as string
    const remoteJid = row.remote_jid as string
    const userId = (row.user_id as string) ?? null
    const followupText = row.followup_text?.text as string | undefined
    const targetId = (row.no_reply_target_id as string) || null

    // carrega conn (pra pegar instance info)
    const conn = await loadConn(conexaoId)

    // follow-up opcional
    if (followupText && followupText.trim()) {
      await enqueueJob({
        conexaoId,
        fluxoId,
        userId,
        remoteJid,
        instanceId: conn?.instance_id ?? null,
        instanceName: conn?.instance_name ?? null,
        actionKind: 'text',
        payload: { text: followupText.trim() },
        delayMs: 0,
      })
    }

    if (targetId) {
      // continua fluxo a partir do nó no_reply
      const { nodes, edges } = await loadGraph(fluxoId)
      const plan = planFromNode(nodes, edges, targetId)

      // enfileira ações
      await enqueuePlannedActions({
        plan,
        fluxoId,
        conexaoId,
        remoteJid,
        userId,
        instanceId: conn?.instance_id ?? null,
        instanceName: conn?.instance_name ?? null,
      })

      // se o plano parar em novo aguarde → cria nova espera
      if (plan.aguarde) {
        await createAguardeRow({
          fluxoId,
          nodeId: plan.aguarde.nodeId,
          remoteJid,
          conexaoId,
          userId,
          timeoutSeconds: plan.aguarde.timeoutSeconds,
          followupText: plan.aguarde.followupText,
          answeredTargetId: plan.aguarde.answeredTargetId,
          noReplyTargetId: plan.aguarde.noReplyTargetId,
        })
      }
    }

    // remove a espera (evita reprocesso)
    await supa.from('fluxo_esperas').delete().eq('id', row.id)
  } catch (e) {
    console.error('[worker][aguarde][expired] process error', e)
    // não remove a linha — fica para nova tentativa num próximo ciclo (ou tratamento manual)
  }
}

async function claimAnsweredEsperas(limit = 20) {
  // pega IDs de esperas já marcadas como answered (pelo webhook de inbound)
  const { data: rows, error } = await supa
    .from('fluxo_esperas')
    .select('id')
    .eq('status', 'answered')
    .order('created_at', { ascending: true })
    .limit(limit)
  if (error) {
    console.error('[worker][aguarde][claimAnswered] error', error)
    return []
  }
  const claimed: any[] = []
  for (const r of rows || []) {
    // “trava” marcando para um estado transitório: aqui podemos só refetch e processar;
    // para evitar corrida, usamos update com eq('status','answered') -> volta o registro completo
    const { data: upd, error: upErr } = await supa
      .from('fluxo_esperas')
      .update({ status: 'answered' }) // mantém answered, mas retorna linha
      .eq('id', r.id)
      .eq('status', 'answered')
      .select('*')
      .maybeSingle()
    if (upErr) {
      console.error('[worker][aguarde][claimAnswered] update error', upErr)
      continue
    }
    if (upd) claimed.push(upd)
  }
  return claimed
}

async function processAnsweredEspera(row: any) {
  try {
    const fluxoId = row.fluxo_id as string
    const conexaoId = row.whatsapp_conexao_id as string
    const remoteJid = row.remote_jid as string
    const userId = (row.user_id as string) ?? null
    const targetId = (row.answered_target_id as string) || null

    if (!targetId) {
      // nada a seguir → apenas remove a espera
      await supa.from('fluxo_esperas').delete().eq('id', row.id)
      return
    }

    const conn = await loadConn(conexaoId)
    const { nodes, edges } = await loadGraph(fluxoId)
    const plan = planFromNode(nodes, edges, targetId)

    await enqueuePlannedActions({
      plan,
      fluxoId,
      conexaoId,
      remoteJid,
      userId,
      instanceId: conn?.instance_id ?? null,
      instanceName: conn?.instance_name ?? null,
    })

    if (plan.aguarde) {
      await createAguardeRow({
        fluxoId,
        nodeId: plan.aguarde.nodeId,
        remoteJid,
        conexaoId,
        userId,
        timeoutSeconds: plan.aguarde.timeoutSeconds,
        followupText: plan.aguarde.followupText,
        answeredTargetId: plan.aguarde.answeredTargetId,
        noReplyTargetId: plan.aguarde.noReplyTargetId,
      })
    }

    // remove a espera (processada)
    await supa.from('fluxo_esperas').delete().eq('id', row.id)
  } catch (e) {
    console.error('[worker][aguarde][answered] process error', e)
  }
}

/* =========================
   LOOP
   ========================= */
async function loop() {
  try {
    // 1) enviar jobs vencidos/pendentes
    const jobs = await claimJobs(10)
    for (const j of jobs) await processJob(j)

    // 2) tratar expiradas (aguarde → no_reply)
    const expired = await claimExpiredEsperas(20)
    for (const r of expired) await processExpiredEspera(r)

    // 3) tratar respondidas (aguarde → answered)
    const answered = await claimAnsweredEsperas(20)
    for (const r of answered) await processAnsweredEspera(r)
  } catch (e) {
    console.error('[worker][loop error]', e)
  } finally {
    setTimeout(loop, 1000)
  }
}

console.log(`[worker] starting ${WORKER_ID}`)
loop()
