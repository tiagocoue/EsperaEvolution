// worker/index.ts (Node 18+)
import { createClient } from '@supabase/supabase-js';

/* =========================
   ENV
   ========================= */
const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const EVO_BASE = (process.env.EVOLUTION_API_URL || '').replace(/\/+$/, '');
const EVO_KEY = process.env.EVOLUTION_API_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !EVO_BASE || !EVO_KEY) {
  console.error('[worker] missing env vars');
  console.error({
    SUPABASE_URL: !!SUPABASE_URL,
    SUPABASE_SERVICE_ROLE_KEY: !!SUPABASE_SERVICE_ROLE_KEY,
    EVO_BASE: !!EVO_BASE,
    EVO_KEY: !!EVO_KEY,
  });
  process.exit(1);
}

const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const WORKER_ID = `worker-${Math.random().toString(36).slice(2, 6)}`;

/* =========================
   EVO HELPERS
   ========================= */
function normalizeNumber(input?: string | null): string {
  if (!input) return '';
  if (input.includes('@')) return input.split('@')[0].split(':')[0];
  return input;
}

async function evoPostRaw(path: string, body: Record<string, unknown>) {
  const url = `${EVO_BASE}${path}`;
  return fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', apikey: EVO_KEY as string },
    body: JSON.stringify(body),
  });
}

async function evoPostJson(path: string, body: Record<string, unknown>) {
  const res = await evoPostRaw(path, body);
  if (!res.ok) throw new Error(`${path} ${res.status} ${await res.text().catch(()=> '')}`);
  return res.json().catch(()=> ({}));
}

function isDataUri(s: string) {
  return typeof s === 'string' && s.startsWith('data:');
}

function inferFilenameAndMime(media: string): { filename: string; mimetype: string } {
  try {
    const u = new URL(media);
    const name = u.pathname.split('/').pop() || 'file.bin';
    const lower = name.toLowerCase();
    if (lower.endsWith('.mp3')) return { filename: name, mimetype: 'audio/mpeg' };
    if (lower.endsWith('.wav')) return { filename: name, mimetype: 'audio/wav' };
    if (lower.endsWith('.ogg') || lower.endsWith('.oga')) return { filename: name, mimetype: 'audio/ogg' };
    return { filename: name, mimetype: 'application/octet-stream' };
  } catch {
    // data URI ou string sem URL válida
    return { filename: 'audio.ogg', mimetype: isDataUri(media) ? guessMimeFromDataUri(media) : 'audio/ogg' };
  }
}

function guessMimeFromDataUri(dataUri: string): string {
  const m = /^data:([^;]+);base64,/.exec(dataUri);
  return m?.[1] || 'audio/ogg';
}

/* =========================
   EVO SEND (texto, imagem, áudio/PTT, presence no-op)
   ========================= */
type SendKind = 'text' | 'image' | 'audio' | 'presence';

async function evoSend(
  kind: SendKind,
  instance: string,
  number: string,
  payload: Record<string, any>
) {
  const num = normalizeNumber(number);

  if (kind === 'text') {
    const body = { number: num, text: payload.text, ...(payload.delay ? { delay: payload.delay } : {}) };
    return evoPostJson(`/message/sendText/${instance}`, body);
  }

  if (kind === 'image') {
    const base = {
      number: num,
      ...(payload.caption ? { caption: payload.caption } : {}),
      ...(payload.delay ? { delay: payload.delay } : {}),
    };
    const body = isDataUri(payload.media)
      ? { ...base, base64: payload.media }
      : { ...base, url: payload.media };
    return evoPostJson(`/message/sendImage/${instance}`, body);
  }

  if (kind === 'presence') {
    // opcional: se sua versão suportar Presence; se não, ignore silenciosamente
    return { ok: true, skipped: 'presence' };
  }

  if (kind === 'audio') {
    const media: string = payload.media;
    const base = { number: num, ...(payload.delay ? { delay: payload.delay } : {}) };
    const { filename, mimetype } = inferFilenameAndMime(media);

    // 0) PTT nativo (documentado): sendWhatsAppAudio { number, audio, delay? }
    {
      const r0 = await evoPostRaw(`/message/sendWhatsAppAudio/${instance}`, {
        number: num,
        audio: media,             // aceita URL pública ou data URI base64
        ...(payload.delay ? { delay: payload.delay } : {}),
      });
      if (r0.ok) return r0.json().catch(()=> ({}));
      if (![400, 404].includes(r0.status)) {
        const t = await r0.text().catch(()=> '');
        console.error('[evolution][sendWhatsAppAudio] non-4xx', r0.status, t);
        throw new Error(`[evolution] sendWhatsAppAudio ${r0.status} ${t}`);
      }
    }

    // 1) PTT (voice note) legado: sendVoice tentando audio -> media -> base64
    {
      let r = await evoPostRaw(`/message/sendVoice/${instance}`, { ...base, audio: media, mimetype });
      if (r.ok) return r.json().catch(()=> ({}));
      if (![400, 404].includes(r.status)) {
        const t = await r.text().catch(()=> '');
        console.error('[evolution][sendVoice][audio] non-4xx', r.status, t);
        throw new Error(`[evolution] sendVoice ${r.status} ${t}`);
      }

      r = await evoPostRaw(`/message/sendVoice/${instance}`, { ...base, media: media, mimetype });
      if (r.ok) return r.json().catch(()=> ({}));
      if (![400, 404].includes(r.status)) {
        const t = await r.text().catch(()=> '');
        console.error('[evolution][sendVoice][media] non-4xx', r.status, t);
        throw new Error(`[evolution] sendVoice ${r.status} ${t}`);
      }

      if (isDataUri(media)) {
        r = await evoPostRaw(`/message/sendVoice/${instance}`, { ...base, base64: media, mimetype });
        if (r.ok) return r.json().catch(()=> ({}));
        if (![400, 404].includes(r.status)) {
          const t = await r.text().catch(()=> '');
          console.error('[evolution][sendVoice][base64] non-4xx', r.status, t);
          throw new Error(`[evolution] sendVoice ${r.status} ${t}`);
        }
      }
    }

    // 2) Áudio comum (com ptt: true para alguns servidores)
    {
      const r = await evoPostRaw(`/message/sendAudio/${instance}`, { ...base, audio: media, mimetype, ptt: true });
      if (r.ok) return r.json().catch(()=> ({}));
      if (![400, 404].includes(r.status)) {
        const t = await r.text().catch(()=> '');
        console.error('[evolution][sendAudio][audio+ptt] non-4xx', r.status, t);
        throw new Error(`[evolution] sendAudio ${r.status} ${t}`);
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
      });
      if (r.ok) return r.json().catch(()=> ({}));
      const t = await r.text().catch(()=> '');
      console.error('[evolution][sendMedia][document] failed', r.status, t);
      throw new Error(`[evolution] sendMedia ${r.status} ${t}`);
    }
  }

  throw new Error(`[evolution] unsupported kind: ${kind}`);
}

/* =========================
   CLAIM + PROCESS
   ========================= */
async function claimJobs(limit = 10) {
  const nowIso = new Date().toISOString();
  const { data: candidates, error } = await supa
    .from('fluxo_agendamentos')
    .select('id')
    .eq('status', 'pending')
    .lte('due_at', nowIso)
    .order('due_at', { ascending: true })
    .limit(limit);

  if (error) {
    console.error('[worker][claimJobs] supabase error', error);
    return [];
  }
  if (!candidates?.length) return [];

  const claimed: any[] = [];
  for (const c of candidates) {
    const { data: updated, error: upErr } = await supa
      .from('fluxo_agendamentos')
      .update({ status: 'running', locked_at: new Date().toISOString(), locked_by: WORKER_ID })
      .eq('id', c.id)
      .eq('status', 'pending')
      .select('*')
      .maybeSingle();

    if (upErr) {
      console.error('[worker][claimJobs] update error', upErr);
      continue;
    }
    if (updated) claimed.push(updated);
  }
  return claimed;
}

async function processJob(job: any) {
  try {
    const instance = (job.instance_name || job.instance_id);
    if (!instance) throw new Error('sem instance_id/name');

    const number = job.remote_jid;
    const payload = job.payload || {};

    await evoSend(job.action_kind as SendKind, instance, number, payload);

    // log opcional
    await supa.from('mensagens').insert({
      whatsapp_conexao_id: job.whatsapp_conexao_id,
      fluxo_id: job.fluxo_id,
      user_id: job.user_id,
      de: null,
      para: number,
      direcao: 'enviada',
      conteudo: payload,
      timestamp: new Date().toISOString(),
    });

    await supa.from('fluxo_agendamentos').update({ status: 'done', last_error: null }).eq('id', job.id);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const attempts = (job.attempts ?? 0) + 1;

    if (attempts < (job.max_attempts ?? 5)) {
      const next = new Date(Date.now() + 10_000 * attempts).toISOString();
      await supa
        .from('fluxo_agendamentos')
        .update({ status: 'pending', attempts, last_error: msg, due_at: next, locked_at: null, locked_by: null })
        .eq('id', job.id);
    } else {
      await supa
        .from('fluxo_agendamentos')
        .update({ status: 'failed', attempts, last_error: msg })
        .eq('id', job.id);
    }
  }
}

/* =========================
   LOOP
   ========================= */
async function loop() {
  try {
    const jobs = await claimJobs(10);
    for (const j of jobs) await processJob(j);
  } catch (e) {
    console.error('[worker][loop error]', e);
  } finally {
    setTimeout(loop, 1000);
  }
}

console.log(`[worker] starting ${WORKER_ID}`);
loop();
