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
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

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

function guessMimeFromDataUri(dataUri: string): string {
  const m = /^data:([^;]+);base64,/.exec(dataUri)
  return m?.[1] || 'audio/ogg'
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
    return { filename: 'audio.ogg', mimetype: isDataUri(media) ? guessMimeFromDataUri(media) : 'audio/ogg' }
  }
}

/* =========================
   QUEUE HELPERS (fluxo_agendamentos)
   ========================= */
type QueueActionKind = 'text' | 'image' | 'audio' | 'presence' | 'notify' | 'start_flow';

async function enqueueJob(params: {
  conexaoId: string;
  fluxoId: string | null;
  userId: string | null;
  remoteJid: string;
  instanceId?: string | null;
  instanceName?: string | null;
  actionKind: QueueActionKind;
  payload: Record<string, unknown>;
  delayMs?: number; // default 0
}) {
  const {
    conexaoId, fluxoId, userId, remoteJid,
    instanceId, instanceName, actionKind, payload, delayMs = 0
  } = params;

  const dueAt = new Date(Date.now() + Math.max(0, delayMs)).toISOString();

  const { error } = await supa.from('fluxo_agendamentos').insert({
    user_id: userId,
    whatsapp_conexao_id: conexaoId,
    fluxo_id: fluxoId,
    remote_jid: remoteJid,
    instance_id: instanceId ?? null,
    instance_name: instanceName ?? null,
    action_kind: actionKind,
    payload,
    due_at: dueAt,
    status: 'pending',
  });

  if (error) throw error;
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

    const inferImage = (m: string) => {
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
      mediatype: 'image',
      mimetype,
      fileName,
      media,
      ...(payload.caption ? { caption: payload.caption } : {}),
      ...(payload.delay ? { delay: payload.delay } : {}),
    }

    return evoPostJson(`/message/sendMedia/${instance}`, body)
  }

  if (kind === 'presence') {
    const presence = payload.state === 'recording' ? 'recording' : 'composing'
    const duration = Math.max(0, Math.min(60000, Number(payload.durationMs ?? 3000)))
    const body = {
      number: num,
      options: { presence, delay: duration }
    }
    return evoPostJson(`/chat/sendPresence/${instance}`, body)
  }

  if (kind === 'audio') {
    const media: string = payload.media
    const base = { number: num, ...(payload.delay ? { delay: payload.delay } : {}) }
    const { filename, mimetype } = inferFilenameAndMime(media)

    // 0) PTT nativo
    {
      const r0 = await evoPostRaw(`/message/sendWhatsAppAudio/${instance}`, {
        number: num,
        audio: media,
        ...(payload.delay ? { delay: payload.delay } : {}),
      })
      if (r0.ok) return r0.json().catch(() => ({}))
      if (![400, 404].includes(r0.status)) {
        const t = await r0.text().catch(() => '')
        console.error('[evolution][sendWhatsAppAudio] non-4xx', r0.status, t)
        throw new Error(`[evolution] sendWhatsAppAudio ${r0.status} ${t}`)
      }
    }

    // 1) PTT legado
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

    // 2) Áudio comum (com ptt: true)
    {
      const r = await evoPostRaw(`/message/sendAudio/${instance}`, { ...base, audio: media, mimetype, ptt: true })
      if (r.ok) return r.json().catch(() => ({}))
      if (![400, 404].includes(r.status)) {
        const t = await r.text().catch(() => '')
        console.error('[evolution][sendAudio][audio+ptt] non-4xx', r.status, t)
        throw new Error(`[evolution] sendAudio ${r.status} ${t}`)
      }
    }

    // 3) Fallback documento
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
type JobActionKind = 'text' | 'image' | 'audio' | 'presence' | 'notify' | 'start_flow'

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

/* =========================
   GRAFO + PLANEJAMENTO (start_flow e continuação)
   ========================= */
type DbNode = {
  id: string;
  fluxo_id: string;
  tipo:
    | 'mensagem_texto'
    | 'mensagem_imagem'
    | 'mensagem_audio'
    | 'mensagem_espera'
    | 'mensagem_notificada'
    | 'aguarde_resposta'
    | 'next_flow'
    | string;
  conteudo: unknown; // jsonb
  ordem: number;
};

type DbEdge = {
  id: string;
  fluxo_id: string;
  source: string; // node.id (db)
  target: string; // node.id (db)
  data: unknown;  // jsonb com "outcome" etc (opcional)
};

const isWaitNode = (n: DbNode) => n.tipo === 'mensagem_espera';
const isAguardeNode = (n: DbNode) => n.tipo === 'aguarde_resposta';
const isNextFlowNode = (n: DbNode) => n.tipo === 'next_flow';

function parseWaitSeconds(node: DbNode): number {
  const c = (node.conteudo ?? {}) as { waitSeconds?: unknown };
  const raw = c.waitSeconds;
  const s = typeof raw === 'number' ? raw : Number(raw);
  return Number.isFinite(s) && s > 0 ? Math.floor(s) : 0;
}

function parseText(node: DbNode): string | null {
  const c = (node.conteudo ?? {}) as { text?: unknown };
  const t = typeof c.text === 'string' ? c.text : null;
  return t && t.trim() ? t : null;
}

function parseImage(node: DbNode): { urlOrBase64: string; caption?: string } | null {
  const c = (node.conteudo ?? {}) as {
    url?: unknown; base64?: unknown; data?: unknown;
    caption?: unknown; text?: unknown;
  };
  const url = typeof c.url === 'string' && c.url.trim() ? c.url.trim() : undefined;
  const b64 = typeof c.base64 === 'string' && c.base64.trim() ? c.base64.trim() : undefined;
  const data = typeof c.data === 'string' && c.data.trim() ? c.data.trim() : undefined;
  const media = b64 ?? data ?? url ?? null;
  if (!media) return null;

  const capRaw =
    (typeof c.caption === 'string' ? c.caption : undefined) ??
    (typeof c.text === 'string' ? c.text : undefined);
  const caption = capRaw && capRaw.trim() ? capRaw.trim() : undefined;

  return { urlOrBase64: media, caption };
}

function parseAudio(node: DbNode): { urlOrBase64: string } | null {
  const c = (node.conteudo ?? {}) as { url?: unknown; base64?: unknown };
  const url = typeof c.url === 'string' ? c.url : undefined;
  const b64 = typeof c.base64 === 'string' ? c.base64 : undefined;
  const media = b64 ?? url ?? null;
  return media ? { urlOrBase64: media } : null;
}

function parseNotify(node: DbNode): { number: string; text: string } | null {
  const c = (node.conteudo ?? {}) as { numero?: unknown; mensagem?: unknown };
  const numero =
    typeof c.numero === 'string'
      ? c.numero.replace(/[^\d]/g, '').trim()
      : typeof c.numero === 'number'
      ? String(c.numero)
      : '';
  const mensagem = typeof c.mensagem === 'string' ? c.mensagem.trim() : '';
  if (!numero || !mensagem) return null;
  return { number: numero, text: mensagem };
}

function parseAguarde(node: DbNode): { timeoutSeconds: number; followupText: string } {
  const c = (node.conteudo ?? {}) as { timeoutSeconds?: unknown; followupText?: unknown };
  const rawT = typeof c.timeoutSeconds === 'number' ? c.timeoutSeconds : Number(c.timeoutSeconds);
  const timeoutSeconds = Number.isFinite(rawT) && rawT > 0 ? Math.min(86400, Math.floor(rawT)) : 60;
  const followupText =
    typeof c.followupText === 'string' && c.followupText.trim() ? c.followupText.trim() : '';
  return { timeoutSeconds, followupText };
}

function parseNextFlowTarget(node: DbNode): string | null {
  const c = (node.conteudo ?? {}) as { targetFluxoId?: unknown };
  const raw = typeof c.targetFluxoId === 'string' ? c.targetFluxoId.trim() : '';
  return UUID_RE.test(raw) ? raw : null;
}

function getNextNodeId(edges: DbEdge[], currentNodeId: string): string | null {
  const edge = edges.find((e) => e.source === currentNodeId);
  return edge ? edge.target : null;
}

function getOutcomeTargets(edges: DbEdge[], nodeId: string): {
  answered: string | null;
  no_reply: string | null;
} {
  let answered: string | null | undefined = undefined;
  let no_reply: string | null | undefined = undefined;
  for (const e of edges) {
    if (e.source !== nodeId) continue;
    const d = (e.data ?? {}) as { outcome?: unknown };
    const outcome = typeof d.outcome === 'string' ? d.outcome : '';
    if (outcome === 'answered') answered = e.target;
    if (outcome === 'no_reply') no_reply = e.target;
  }
  return { answered: answered ?? null, no_reply: no_reply ?? null };
}

type PlannedAction =
  | { kind: 'text'; text: string; delayMs: number }
  | { kind: 'image'; urlOrBase64: string; caption?: string; delayMs: number }
  | { kind: 'audio'; urlOrBase64: string; delayMs: number }
  | { kind: 'presence'; state: 'composing' | 'recording'; durationMs?: number; delayMs: number }
  | { kind: 'notify'; number: string; text: string; delayMs: number }
  | { kind: 'next_flow'; targetFluxoId: string; delayMs: number };

type PlanResult = {
  actions: PlannedAction[];
  aguarde?: {
    nodeId: string;
    timeoutSeconds: number;
    followupText: string;
    answeredTargetId: string | null;
    noReplyTargetId: string | null;
  };
};

/** Planeja a partir de um nó inicial:
 * - respeita waits (presence + delay)
 * - para em aguarde_resposta (envia followupText imediato e retorna metadados)
 * - para em next_flow (gera ação de handoff)
 */
function planSends(nodes: DbNode[], edges: DbEdge[], startNodeId: string): PlanResult {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const MAX_STEPS = 20;

  const actions: PlannedAction[] = [];
  let curr: DbNode | undefined = byId.get(startNodeId);
  let elapsedMs = 0;
  let steps = 0;
  const visited = new Set<string>();
  let aguardeInfo: PlanResult['aguarde'] | undefined;

  while (curr && steps < MAX_STEPS) {
    if (visited.has(curr.id)) break;
    visited.add(curr.id);
    steps++;

    if (isWaitNode(curr)) {
      const waitMs = Math.max(0, parseWaitSeconds(curr) * 1000);
      if (waitMs > 0) {
        const presMs = Math.min(waitMs, 60000);
        actions.push({ kind: 'presence', state: 'composing', durationMs: presMs, delayMs: elapsedMs });
      }
      elapsedMs += waitMs;
      const nextId = getNextNodeId(edges, curr.id);
      curr = nextId ? byId.get(nextId) : undefined;
      continue;
    }

    if (isAguardeNode(curr)) {
      const params = parseAguarde(curr);
      const { answered, no_reply } = getOutcomeTargets(edges, curr.id);

      if (params.followupText && params.followupText.trim()) {
        actions.push({ kind: 'text', text: params.followupText.trim(), delayMs: elapsedMs });
      }

      aguardeInfo = {
        nodeId: curr.id,
        timeoutSeconds: params.timeoutSeconds,
        followupText: params.followupText,
        answeredTargetId: answered,
        noReplyTargetId: no_reply,
      };
      break;
    }

    if (isNextFlowNode(curr)) {
      const target = parseNextFlowTarget(curr);
      if (target) actions.push({ kind: 'next_flow', targetFluxoId: target, delayMs: elapsedMs });
      break;
    }

    if (curr.tipo === 'mensagem_texto') {
      const text = parseText(curr);
      if (text) actions.push({ kind: 'text', text, delayMs: elapsedMs });
    } else if (curr.tipo === 'mensagem_imagem') {
      const img = parseImage(curr);
      if (img) actions.push({ kind: 'image', urlOrBase64: img.urlOrBase64, caption: img.caption, delayMs: elapsedMs });
    } else if (curr.tipo === 'mensagem_audio') {
      const aud = parseAudio(curr);
      if (aud) actions.push({ kind: 'audio', urlOrBase64: aud.urlOrBase64, delayMs: elapsedMs });
    } else if (curr.tipo === 'mensagem_notificada') {
      const notif = parseNotify(curr);
      if (notif) actions.push({ kind: 'notify', number: notif.number, text: notif.text, delayMs: elapsedMs });
    }

    const nextId = getNextNodeId(edges, curr.id);
    curr = nextId ? byId.get(nextId) : undefined;
  }

  return { actions, aguarde: aguardeInfo };
}

function pickStartNodeId(nodes: DbNode[]): string | null {
  if (!nodes.length) return null
  // preferir nó "enviável" de menor ordem; fallback: menor ordem geral
  const sendable = nodes
    .filter(n => n.tipo === 'mensagem_texto' || n.tipo === 'mensagem_imagem' || n.tipo === 'mensagem_audio' || n.tipo === 'mensagem_notificada' || n.tipo === 'mensagem_espera' || n.tipo === 'aguarde_resposta' || n.tipo === 'next_flow')
    .sort((a,b) => (a.ordem ?? 0) - (b.ordem ?? 0))
  if (sendable.length) return sendable[0].id
  const sorted = [...nodes].sort((a,b) => (a.ordem ?? 0) - (b.ordem ?? 0))
  return sorted[0].id
}

/** Inicia (ou continua) um fluxo: planeja e enfileira as ações; cria fluxo_esperas se preciso; 
 *  se encontrar next_flow, enfileira outro start_flow com o delay acumulado. */
async function startFlowFromWorker(args: {
  conexaoId: string
  fluxoId: string
  remoteJid: string
  instanceId?: string | null
  instanceName?: string | null
  userId?: string | null
  extraDelayMs?: number // quando vier de um start_flow com delay acumulado
}) {
  const { conexaoId, fluxoId, remoteJid, instanceId, instanceName, userId, extraDelayMs = 0 } = args

  const [{ data: nodes }, { data: edges }] = await Promise.all([
    supa.from('fluxo_nos')
      .select('id, fluxo_id, tipo, conteudo, ordem')
      .eq('fluxo_id', fluxoId)
      .order('ordem', { ascending: true }),
    supa.from('fluxo_edge')
      .select('id, fluxo_id, source, target, data')
      .eq('fluxo_id', fluxoId),
  ])

  if (!nodes?.length) return

  const startId = pickStartNodeId(nodes as DbNode[])
  if (!startId) return

  const plan = planSends(nodes as DbNode[], edges as DbEdge[], startId)

  // criar espera (aguarde_resposta), se houver
  if (plan.aguarde) {
    const now = new Date()
    const expires = new Date(now.getTime() + plan.aguarde.timeoutSeconds * 1000)

    await supa.from('fluxo_esperas').insert({
      status: 'pending',
      created_at: now.toISOString(),
      expires_at: expires.toISOString(),
      fluxo_id: fluxoId,
      node_id: plan.aguarde.nodeId,
      remote_jid: remoteJid,
      whatsapp_conexao_id: conexaoId,
      user_id: userId ?? null,
      answered_target_id: plan.aguarde.answeredTargetId,
      no_reply_target_id: plan.aguarde.noReplyTargetId,
      followup_text: plan.aguarde.followupText ? { text: plan.aguarde.followupText } : null,
      instance_id: instanceId ?? null,
      instance_name: instanceName ?? null,
    })
  }

  // enfileirar ações
  for (const a of plan.actions) {
    const delay = (a.delayMs ?? 0) + extraDelayMs
    if (a.kind === 'presence') {
      await enqueueJob({
        conexaoId,
        fluxoId,
        userId: userId ?? null,
        remoteJid,
        instanceId,
        instanceName,
        actionKind: 'presence',
        payload: { state: a.state, durationMs: a.durationMs ?? 3000 },
        delayMs: delay,
      })
    } else if (a.kind === 'text') {
      await enqueueJob({
        conexaoId,
        fluxoId,
        userId: userId ?? null,
        remoteJid,
        instanceId,
        instanceName,
        actionKind: 'text',
        payload: { text: a.text },
        delayMs: delay,
      })
    } else if (a.kind === 'image') {
      await enqueueJob({
        conexaoId,
        fluxoId,
        userId: userId ?? null,
        remoteJid,
        instanceId,
        instanceName,
        actionKind: 'image',
        payload: { media: a.urlOrBase64, caption: a.caption ?? null },
        delayMs: delay,
      })
    } else if (a.kind === 'audio') {
      await enqueueJob({
        conexaoId,
        fluxoId,
        userId: userId ?? null,
        remoteJid,
        instanceId,
        instanceName,
        actionKind: 'audio',
        payload: { media: a.urlOrBase64 },
        delayMs: delay,
      })
    } else if (a.kind === 'notify') {
      await enqueueJob({
        conexaoId,
        fluxoId,
        userId: userId ?? null,
        remoteJid,
        instanceId,
        instanceName,
        actionKind: 'notify',
        payload: { number: a.number, text: a.text },
        delayMs: delay,
      })
    } else if (a.kind === 'next_flow') {
      // handoff em cadeia: agenda um novo start_flow respeitando o delay acumulado
      await enqueueJob({
        conexaoId,
        fluxoId: a.targetFluxoId, // associa o job ao fluxo de destino
        userId: userId ?? null,
        remoteJid,
        instanceId,
        instanceName,
        actionKind: 'start_flow',
        payload: { targetFluxoId: a.targetFluxoId },
        delayMs: delay,
      })
    }
  }
}

/* =========================
   PROCESSADOR DE JOB
   ========================= */
async function processJob(job: any) {
  try {
    const payload = typeof job.payload === 'string' ? JSON.parse(job.payload) : (job.payload || {})

    // 'notify' → destino em payload.number; demais → remote_jid
    const isNotify = job.action_kind === 'notify'
    const number = isNotify ? (payload.number ?? '') : job.remote_jid

    // start_flow: inicia o fluxo de destino aqui no worker
    if (job.action_kind === 'start_flow') {
      const target: string = payload?.targetFluxoId || job.fluxo_id
      if (!UUID_RE.test(String(target || ''))) {
        throw new Error('start_flow sem targetFluxoId válido')
      }

      await startFlowFromWorker({
        conexaoId: job.whatsapp_conexao_id,
        fluxoId: target,
        remoteJid: job.remote_jid,
        instanceId: job.instance_id,
        instanceName: job.instance_name,
        userId: job.user_id,
        // se quiser “carregar” um atraso já vencido, poderia usar due_at diff; aqui não precisa
      })

      await supa.from('fluxo_agendamentos')
        .update({ status: 'done', last_error: null })
        .eq('id', job.id)

      return
    }

    // envio normal (text/image/audio/presence/notify)
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
        if (!/instance does not exist|Cannot POST|404/.test(msg)) {
          throw e
        }
      }
    }

    if (!sent) throw (lastErr ?? new Error('Falha ao enviar: nenhum identificador de instância válido.'))

    await supa.from('mensagens').insert({
      whatsapp_conexao_id: job.whatsapp_conexao_id,
      fluxo_id: job.fluxo_id,
      user_id: job.user_id,
      de: null,
      para: normalizeNumber(number),
      direcao: 'enviada',
      conteudo: isNotify ? { notify: true, text: (payload as any).text } : payload,
      timestamp: new Date().toISOString(),
    })

    await supa.from('fluxo_agendamentos')
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
   WAIT EXPIRATION (fluxo_esperas)
   ========================= */
async function claimExpiredWaits(limit = 20) {
  const nowIso = new Date().toISOString();
  const { data: candidates, error } = await supa
    .from('fluxo_esperas')
    .select('id')
    .eq('status', 'pending')
    .lte('expires_at', nowIso)
    .order('expires_at', { ascending: true })
    .limit(limit);

  if (error) {
    console.error('[worker][claimExpiredWaits] supabase error', error);
  }
  if (!candidates?.length) return [];

  const claimed: any[] = [];
  for (const c of candidates) {
    const { data: updated, error: upErr } = await supa
      .from('fluxo_esperas')
      .update({ status: 'expired' })
      .eq('id', c.id)
      .eq('status', 'pending')
      .select('*')
      .maybeSingle();

    if (upErr) {
      console.error('[worker][claimExpiredWaits] update error', upErr);
      continue;
    }
    if (updated) claimed.push(updated);
  }
  return claimed;
}

function extractFollowupText(raw: unknown): string {
  if (typeof raw === 'string') {
    const s = raw.trim();
    if (!s) return '';
    try {
      const obj = JSON.parse(s);
      if (obj && typeof obj.text === 'string' && obj.text.trim()) return obj.text.trim();
    } catch {
      return s;
    }
    return '';
  }
  if (raw && typeof raw === 'object' && typeof (raw as any).text === 'string') {
    const s = (raw as any).text.trim();
    return s || '';
  }
  return '';
}

async function resolveInstanceAndUserForWait(wait: any): Promise<{
  instanceId: string | null;
  instanceName: string | null;
  userId: string | null;
}> {
  let instanceId: string | null = wait.instance_id ?? null;
  let instanceName: string | null = wait.instance_name ?? null;
  let userId: string | null = wait.user_id ?? null;

  if (instanceId || instanceName) {
    return { instanceId, instanceName, userId };
  }

  const { data: lastJobs } = await supa
    .from('fluxo_agendamentos')
    .select('instance_id, instance_name, user_id')
    .eq('whatsapp_conexao_id', wait.whatsapp_conexao_id)
    .eq('remote_jid', wait.remote_jid)
    .eq('fluxo_id', wait.fluxo_id)
    .order('created_at', { ascending: false })
    .limit(1);

  if (lastJobs?.length) {
    instanceId = lastJobs[0]?.instance_id ?? null;
    instanceName = lastJobs[0]?.instance_name ?? null;
    userId = userId ?? (lastJobs[0]?.user_id ?? null);
  }

  if (!instanceId && !instanceName) {
    const { data: lastByConn } = await supa
      .from('fluxo_agendamentos')
      .select('instance_id, instance_name, user_id')
      .eq('whatsapp_conexao_id', wait.whatsapp_conexao_id)
      .order('created_at', { ascending: false })
      .limit(1);

    if (lastByConn?.length) {
      instanceId = lastByConn[0]?.instance_id ?? null;
      instanceName = lastByConn[0]?.instance_name ?? null;
      userId = userId ?? (lastByConn[0]?.user_id ?? null);
    }
  }

  return { instanceId, instanceName, userId };
}

async function processExpiredWait(wait: any) {
  try {
    const { instanceId, instanceName, userId } = await resolveInstanceAndUserForWait(wait);

    if (!instanceId && !instanceName) {
      console.error('[worker][processExpiredWait] sem instance_id/name para follow-up', {
        espera_id: wait.id,
        conexao: wait.whatsapp_conexao_id,
        fluxo: wait.fluxo_id,
        remote_jid: wait.remote_jid,
      });
      return;
    }

    const startId: string | null = wait.no_reply_target_id || null;
    if (!startId) return;

    const [{ data: nodes }, { data: edges }] = await Promise.all([
      supa.from('fluxo_nos').select('id, fluxo_id, tipo, conteudo, ordem').eq('fluxo_id', wait.fluxo_id),
      supa.from('fluxo_edge').select('id, fluxo_id, source, target, data').eq('fluxo_id', wait.fluxo_id),
    ]);
    if (!nodes?.length) return;

    const plan = planSends(nodes as DbNode[], edges as DbEdge[], startId)

    // enfileira ações da continuação
    for (const a of plan.actions) {
      if (a.kind === 'presence') {
        await enqueueJob({
          conexaoId: wait.whatsapp_conexao_id,
          fluxoId: wait.fluxo_id ?? null,
          userId: userId ?? null,
          remoteJid: wait.remote_jid,
          instanceId, instanceName,
          actionKind: 'presence',
          payload: { state: a.state, durationMs: a.durationMs ?? 3000 },
          delayMs: a.delayMs,
        });
      } else if (a.kind === 'text') {
        await enqueueJob({
          conexaoId: wait.whatsapp_conexao_id,
          fluxoId: wait.fluxo_id ?? null,
          userId: userId ?? null,
          remoteJid: wait.remote_jid,
          instanceId, instanceName,
          actionKind: 'text',
          payload: { text: a.text },
          delayMs: a.delayMs,
        });
      } else if (a.kind === 'image') {
        await enqueueJob({
          conexaoId: wait.whatsapp_conexao_id,
          fluxoId: wait.fluxo_id ?? null,
          userId: userId ?? null,
          remoteJid: wait.remote_jid,
          instanceId, instanceName,
          actionKind: 'image',
          payload: { media: a.urlOrBase64, caption: a.caption ?? null },
          delayMs: a.delayMs,
        });
      } else if (a.kind === 'audio') {
        await enqueueJob({
          conexaoId: wait.whatsapp_conexao_id,
          fluxoId: wait.fluxo_id ?? null,
          userId: userId ?? null,
          remoteJid: wait.remote_jid,
          instanceId, instanceName,
          actionKind: 'audio',
          payload: { media: a.urlOrBase64 },
          delayMs: a.delayMs,
        });
      } else if (a.kind === 'notify') {
        await enqueueJob({
          conexaoId: wait.whatsapp_conexao_id,
          fluxoId: wait.fluxo_id ?? null,
          userId: userId ?? null,
          remoteJid: wait.remote_jid,
          instanceId, instanceName,
          actionKind: 'notify',
          payload: { number: a.number, text: a.text },
          delayMs: a.delayMs,
        });
      } else if (a.kind === 'next_flow') {
        await enqueueJob({
          conexaoId: wait.whatsapp_conexao_id,
          fluxoId: a.targetFluxoId,
          userId: userId ?? null,
          remoteJid: wait.remote_jid,
          instanceId, instanceName,
          actionKind: 'start_flow',
          payload: { targetFluxoId: a.targetFluxoId },
          delayMs: a.delayMs,
        });
      }
    }
  } catch (e) {
    console.error('[worker][processExpiredWait] error', e);
  }
}

/* =========================
   LOOP
   ========================= */
async function loop() {
  try {
    // 1) processa esperas expiradas (Aguarde)
    const waits = await claimExpiredWaits(20);
    for (const w of waits) {
      await processExpiredWait(w);
    }

    // 2) processa jobs normais (fila de envios + start_flow)
    const jobs = await claimJobs(10);
    for (const j of jobs) await processJob(j);
  } catch (e) {
    console.error('[worker][loop error]', e);
  } finally {
    setTimeout(loop, 1000);
  }
}

console.log(`[worker] starting ${WORKER_ID}`)
loop()
