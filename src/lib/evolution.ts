// src/lib/evolution.ts
import { getEnv } from './env';

const EVO_BASE = (getEnv('EVOLUTION_API_URL', { optional: true }) || getEnv('EVOLUTION_API', { optional: true })).replace(/\/+$/, '');
const EVO_KEY = getEnv('EVOLUTION_API_KEY');

async function sleep(ms: number) {
  if (ms && ms > 0) await new Promise(r => setTimeout(r, ms));
}

export async function evoSendText(instance: string, number: string, text: string, delayMs?: number) {
  if (delayMs) await sleep(delayMs);
  const res = await fetch(`${EVO_BASE}/message/sendText/${instance}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', apikey: EVO_KEY },
    body: JSON.stringify({ number, text }),
  });
  if (!res.ok) throw new Error(`Evolution sendText ${res.status}: ${await res.text().catch(()=>'')}`);
  return res.json().catch(()=> ({}));
}

export async function evoSendImage(instance: string, number: string, media: string, caption?: string, delayMs?: number) {
  if (delayMs) await sleep(delayMs);
  const res = await fetch(`${EVO_BASE}/message/sendImage/${instance}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', apikey: EVO_KEY },
    body: JSON.stringify({ number, media, caption }),
  });
  if (!res.ok) throw new Error(`Evolution sendImage ${res.status}: ${await res.text().catch(()=>'')}`);
  return res.json().catch(()=> ({}));
}

export async function evoSendAudio(instance: string, number: string, media: string, delayMs?: number) {
  if (delayMs) await sleep(delayMs);
  const res = await fetch(`${EVO_BASE}/message/sendAudio/${instance}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', apikey: EVO_KEY },
    body: JSON.stringify({ number, media }),
  });
  if (!res.ok) throw new Error(`Evolution sendAudio ${res.status}: ${await res.text().catch(()=>'')}`);
  return res.json().catch(()=> ({}));
}

/** Tenta instanceId; se falhar, tenta instanceName. */
export async function evoSendTextWithFallback(opts: { instanceId?: string | null; instanceName?: string | null; number: string; text: string; delayMs?: number }) {
  const { instanceId, instanceName, number, text, delayMs } = opts;
  if (instanceId) {
    try { return await evoSendText(instanceId, number, text, delayMs); } catch {}
  }
  if (instanceName) return evoSendText(instanceName, number, text, delayMs);
  throw new Error('Sem identificador de instância (instanceId/instanceName)');
}
export async function evoSendImageWithFallback(opts: { instanceId?: string | null; instanceName?: string | null; number: string; media: string; caption?: string; delayMs?: number }) {
  const { instanceId, instanceName, number, media, caption, delayMs } = opts;
  if (instanceId) {
    try { return await evoSendImage(instanceId, number, media, caption, delayMs); } catch {}
  }
  if (instanceName) return evoSendImage(instanceName, number, media, caption, delayMs);
  throw new Error('Sem identificador de instância (instanceId/instanceName)');
}
export async function evoSendAudioWithFallback(opts: { instanceId?: string | null; instanceName?: string | null; number: string; media: string; delayMs?: number }) {
  const { instanceId, instanceName, number, media, delayMs } = opts;
  if (instanceId) {
    try { return await evoSendAudio(instanceId, number, media, delayMs); } catch {}
  }
  if (instanceName) return evoSendAudio(instanceName, number, media, delayMs);
  throw new Error('Sem identificador de instância (instanceId/instanceName)');
}
