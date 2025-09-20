// worker/index.ts
// Node 18+
// - Envia mensagens via Evolution API
// - Consome jobs da tabela fluxo_agendamentos
// - Fallback: tenta instance_id e, se falhar, tenta instance_name
// - Resgata jobs "running" travados (locked_at antigo)
// - Backoff com jitter e retries

import { createClient } from '@supabase/supabase-js';
import fetch from 'cross-fetch'; // garante fetch em runtimes que exigem polyfill

// ===== ENV =====
// Mantive NEXT_PUBLIC_SUPABASE_URL para compat com sua infra atual.
// (Se quiser, troque para SUPABASE_URL em todo o projeto e no painel.)
const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL || process.env.SUPABASE_URL;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
const EVO_BASE = (process.env.EVOLUTION_API_URL || process.env.EVOLUTION_API || '').replace(/\/+$/, '');
const EVO_KEY = process.env.EVOLUTION_API_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY || !EVO_BASE || !EVO_KEY) {
  console.error('[worker] missing env vars', {
    has_SUPABASE_URL: !!SUPABASE_URL,
    has_SERVICE_ROLE: !!SUPABASE_SERVICE_ROLE_KEY,
    has_EVO_BASE: !!EVO_BASE,
    has_EVO_KEY: !!EVO_KEY,
  });
  process.exit(1);
}

const supa = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, { global: { fetch } });
const WORKER_ID = `worker-${Math.random().toString(36).slice(2, 8)}`;

// ===== Evolution API =====
async function evoSend(kind: 'text'|'image'|'audio', instance: string, number: string, payload: any) {
  const headers = { 'Content-Type': 'application/json', apikey: EVO_KEY as string };
  let path = '';
  let body: Record<string, any> = {};

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
  if (!res.ok) {
    const text = await res.text().catch(() => '');
    throw new Error(`[evolution] ${kind} ${res.status} ${text}`);
  }
  return res.json().catch(() => ({}));
}

/** Tenta pelo instance_id; se falhar, tenta instance_name */
async function evoSendWithFallback(kind: 'text'|'image'|'audio', job: any, number: string, payload: any) {
  const attempts: string[] = [];
  if (job.instance_id) attempts.push(job.instance_id);
  if (job.instance_name) attempts.push(job.instance_name);

  let lastErr: any;
  for (const inst of attempts) {
    try {
      return await evoSend(kind, inst, number, payload);
    } catch (e: any) {
      lastErr = e;
      console.error('[worker][evoSendWithFallback] fail for', inst, String(e));

      // Se o erro for "instance does not exist" e ainda temos instance_name,
      // podemos limpar o instance_id para os próximos retries focarem no name.
      const msg = String(e ?? '');
      if (/instance does not exist/i.test(msg) && job.instance_name && inst === job.instance_id) {
        await supa
          .from('fluxo_agendamentos')
          .update({ instance_id: null })
          .eq('id', job.id);
      }
      // tenta a próxima estratégia
    }
  }
  throw lastErr ?? new Error('Sem instance_id/instance_name válidos');
}

// ===== Jobs =====

/** Reabre jobs "running" travados há mais de N ms */
async function rescueStaleRunning(ms = 2 * 60_000) {
  const staleIso = new Date(Date.now() - ms).toISOString();
  const { error } = await supa
    .from('fluxo_agendamentos')
    .update({ status: 'pending', locked_at: null, locked_by: null })
    .eq('status', 'running')
    .lte('locked_at', staleIso);
  if (error) console.error('[rescueStaleRunning] error:', error);
}

/** Busca jobs pendentes vencidos e tenta "travar" com update atômico */
async function claimJobs(limit = 10) {
  const nowIso = new Date().toISOString();

  // 1) resgata "running" travados
  await rescueStaleRunning(2 * 60_000);

  // 2) pega candidatos pendentes vencidos
  const { data: candidates, error: selErr } = await supa
    .from('fluxo_agendamentos')
    .select('id')
    .eq('status', 'pending')
    .lte('due_at', nowIso)
    .order('due_at', { ascending: true })
    .limit(limit);

  if (selErr) {
    console.error('[claimJobs] select error:', selErr);
    return [];
  }
  if (!candidates?.length) return [];

  // 3) tenta travar cada um (atômico)
  const claimed: any[] = [];
  for (const c of candidates) {
    const { data: updated, error: updErr } = await supa
      .from('fluxo_agendamentos')
      .update({ status: 'running', locked_at: new Date().toISOString(), locked_by: WORKER_ID })
      .eq('id', c.id)
      .eq('status', 'pending')
      .select('*')
      .maybeSingle();

    if (!updErr && updated) claimed.push(updated);
  }

  return claimed;
}

async function processJob(job: any) {
  const number = job.remote_jid;
  const payload = job.payload || {};
  try {
    if (!number) throw new Error('sem remote_jid');

    // Envia com fallback (ID → NAME)
    await evoSendWithFallback(job.action_kind as any, job, number, payload);

    // (opcional) registrar em mensagens como 'enviada'
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

    await supa.from('fluxo_agendamentos')
      .update({ status: 'done', last_error: null })
      .eq('id', job.id);
  } catch (err: any) {
    const msg = err instanceof Error ? err.message : String(err);
    const attempts = (job.attempts ?? 0) + 1;
    const max = job.max_attempts ?? 5;

    if (attempts < max) {
      // backoff com jitter
      const base = 10_000 * attempts;
      const jitter = Math.floor(Math.random() * 3_000);
      const next = new Date(Date.now() + base + jitter).toISOString();

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
        .eq('id', job.id);
    } else {
      await supa
        .from('fluxo_agendamentos')
        .update({ status: 'failed', attempts, last_error: msg })
        .eq('id', job.id);
    }
  }
}

// ===== Loop =====
let running = true;

process.on('SIGTERM', () => { running = false; console.log('[worker] SIGTERM recebido, finalizando…'); });
process.on('SIGINT', () => { running = false; console.log('[worker] SIGINT recebido, finalizando…'); });

async function loop() {
  try {
    const jobs = await claimJobs(10);
    if (jobs.length) {
      for (const j of jobs) {
        console.log(`[worker] processing job=${j.id} kind=${j.action_kind} number=${j.remote_jid}`);
        await processJob(j);
      }
    }
  } catch (e) {
    console.error('[worker][loop error]', e);
  } finally {
    if (running) setTimeout(loop, 1000); // 1s
    else process.exit(0);
  }
}

console.log(`[worker] starting ${WORKER_ID}`);
loop();
