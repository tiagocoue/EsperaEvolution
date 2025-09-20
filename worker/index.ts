// worker/index.ts
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const EVO_BASE = (process.env.EVOLUTION_API_URL || "").replace(/\/+$/, "");
const EVO_KEY = process.env.EVOLUTION_API_KEY!;

if (!SUPABASE_URL || !SUPABASE_SERVICE_ROLE_KEY) {
  throw new Error("Supabase envs ausentes.");
}
if (!EVO_BASE || !EVO_KEY) {
  throw new Error("Evolution API envs ausentes.");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY);

function normalizeNumber(input: string): string {
  if (!input) return input;
  if (input.includes("@")) return input.split("@")[0].split(":")[0];
  return input;
}

const PREFERRED_INSTANCE = (job: any) =>
  job.instance_name ||
  job.instanceId ||
  job.instance_id ||
  job.instance ||
  job.instanceName;

async function evoSend(
  kind: "text" | "image" | "audio",
  instance: string,
  number: string,
  payload: any
) {
  const headers = { "Content-Type": "application/json", apikey: EVO_KEY };

  const doTry = async (path: string, body: any) => {
    const res = await fetch(`${EVO_BASE}${path}`, {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      if (res.status === 404 || res.status === 400) {
        throw Object.assign(new Error("TRY_NEXT"), {
          code: res.status,
          detail: txt,
        });
      }
      throw new Error(`[evolution] ${path} ${res.status} ${txt}`);
    }
    return res.json().catch(() => ({}));
  };

  const base = { number: normalizeNumber(number) };

  if (kind === "text") {
    return doTry(`/message/sendText/${instance}`, {
      ...base,
      text: payload.text,
    });
  }

  if (kind === "image") {
    const media = payload.media;
    const caption = payload.caption ?? "";
    const bodies = [
      { ...base, url: media, caption },
      { ...base, base64: media, caption },
      { ...base, media, caption },
    ];
    for (const b of bodies) {
      try {
        return await doTry(`/message/sendImage/${instance}`, b);
      } catch (e: any) {
        if (e?.message !== "TRY_NEXT") throw e;
      }
    }
    throw new Error("[evolution] nenhuma forma de imagem aceita");
  }

  if (kind === "audio") {
    const media = payload.media;
    const mimetype = payload.mimetype || payload.mime || "audio/ogg";

    const candidates: Array<{ path: string; body: any }> = [
      { path: `/message/sendAudio/${instance}`, body: { ...base, url: media, mimetype } },
      { path: `/message/sendAudio/${instance}`, body: { ...base, base64: media, mimetype } },
      { path: `/message/sendAudio/${instance}`, body: { ...base, audio: media, mimetype } },

      { path: `/message/sendVoice/${instance}`, body: { ...base, url: media, mimetype } },
      { path: `/message/sendVoice/${instance}`, body: { ...base, base64: media, mimetype } },
      { path: `/message/sendVoice/${instance}`, body: { ...base, audio: media, mimetype } },

      { path: `/message/sendFile/${instance}`, body: { ...base, url: media, mimetype, fileName: "audio.ogg" } },
      { path: `/message/sendFile/${instance}`, body: { ...base, base64: media, mimetype, fileName: "audio.ogg" } },

      { path: `/message/sendMedia/${instance}`, body: { ...base, url: media, mimetype, type: "audio" } },
      { path: `/message/sendMedia/${instance}`, body: { ...base, base64: media, mimetype, type: "audio" } },
    ];

    let lastErr: any;
    for (const c of candidates) {
      try {
        return await doTry(c.path, c.body);
      } catch (e: any) {
        if (e?.message === "TRY_NEXT") {
          lastErr = e;
          continue;
        }
        throw e;
      }
    }
    throw new Error(
      `[evolution] audio: nenhum endpoint/forma aceitou (${lastErr?.code ?? "unknown"})`
    );
  }

  throw new Error("unsupported kind");
}

async function processJob(job: any) {
  const instance = PREFERRED_INSTANCE(job);
  const number = job.remote_jid;
  const payload = job.payload as any;

  try {
    await evoSend(job.action_kind, instance, number, payload);

    await supabase.from("fluxo_agendamentos").update({
      status: "done",
      updated_at: new Date().toISOString(),
    }).eq("id", job.id);
  } catch (err: any) {
    console.error("Erro job", job.id, err);
    const attempts = (job.attempts || 0) + 1;
    await supabase.from("fluxo_agendamentos").update({
      status: attempts >= job.max_attempts ? "failed" : "pending",
      attempts,
      last_error: String(err?.message || err),
      updated_at: new Date().toISOString(),
    }).eq("id", job.id);
  }
}

async function poll() {
  const { data: jobs } = await supabase
    .from("fluxo_agendamentos")
    .select("*")
    .eq("status", "pending")
    .lte("due_at", new Date().toISOString())
    .order("due_at", { ascending: true })
    .limit(10);

  if (jobs?.length) {
    for (const job of jobs) {
      await processJob(job);
    }
  }
}

console.log("[worker] iniciado polling...");
setInterval(poll, 3000);
