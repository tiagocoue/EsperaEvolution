// worker.mjs (Node 18+)
import { createClient } from '@supabase/supabase-js';

// envs
const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const EVO_BASE = (process.env.EVOLUTION_API_URL || '').replace(/\/+$/, '');
const EVO_KEY = process.env.EVOLUTION_API_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !EVO_BASE || !EVO_KEY) {
  console.error('[worker] missing env vars');
  process.exit(1);
}

const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);
const WORKER_ID = `worker-${Math.random().toString(36).slice(2, 8)}`;

async function evoSend(kind, instance, number, payload) {
  const headers = { 'Content-Type': 'application/json', apikey: EVO_KEY };
  let path = '';
  let body = {};

  if (kind === 'text') {
    path = `/message/sendText/${instance}`;
    body = { number, text: payload.text };
  } else if (kind === 'image') {
    path = `/message/sendImage/${instance}`;
    body = { number, media: payload.media, caption: payload.caption ?? '' };
  } else if (kind === 'audio') {
    path = `/message/sendAudio/${instance}`;
    body = { number, media: payload.media };
  }

  const res = await fetch(`${EVO_BASE}${path}`, { method: 'POST', headers, body: JSON.stringify(body) });
  if (!res.ok) throw new Error(`[evolution] ${kind} ${res.status} ${await res.text().catch(()=> '')}`);
  return res.json().catch(()=> ({}));
}

async function claimJobs(limit = 10) {
  // pega jobs pendentes vencidos e "trava" com update atômico (uma rodada por loop)
  const nowIso = new Date().toISOString();

  // 1) pega candidatos
  const { data: candidates } = await supa
    .from('fluxo_agendamentos')
    .select('id')
    .eq('status', 'pending')
    .lte('due_at', nowIso)
    .order('due_at', { ascending: true })
    .limit(limit);

  if (!candidates?.length) return [];

  const claimed = [];
  for (const c of candidates) {
    const { data: updated, error } = await supa
      .from('fluxo_agendamentos')
      .update({ status: 'running', locked_at: new Date().toISOString(), locked_by: WORKER_ID })
      .eq('id', c.id)
      .eq('status', 'pending')        // garante que só pega se ainda estiver pendente
      .select('*')
      .maybeSingle();

    if (!error && updated) claimed.push(updated);
  }

  return claimed;
}

async function processJob(job) {
  try {
    const instance = job.instance_id || job.instance_name;
    if (!instance) throw new Error('sem instance_id/name');

    const number = job.remote_jid;
    const payload = job.payload || {};
    await evoSend(job.action_kind, instance, number, payload);

    // (opcional) registrar em mensagens como 'enviada'
    await supa.from('mensagens').insert({
      whatsapp_conexao_id: job.whatsapp_conexao_id,
      fluxo_id: job.fluxo_id,
      user_id: job.user_id,
      de: null,                // nosso número se quiser (pode buscar depois)
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
      // backoff simples: 10s * attempts
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

async function loop() {
  try {
    const jobs = await claimJobs(10);
    if (jobs.length) {
      for (const j of jobs) await processJob(j);
    }
  } catch (e) {
    console.error('[worker][loop error]', e);
  } finally {
    setTimeout(loop, 1000); // 1s
  }
}

console.log(`[worker] starting ${WORKER_ID}`);
loop();
