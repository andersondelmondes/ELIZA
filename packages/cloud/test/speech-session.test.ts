// spec/cloud/services/speechRelay.md, "Story: Start speech only from a consent I gave"; spec/cloud/services/speechRelay.md, "Story: Keep a live session funded and close it with receipts"
import { describe, expect, it } from "vitest";
import { ConfigProvider, Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/http";
import { driver } from "./drivers/cloud__services__speechRelay.ts";
import { addCredits } from "../src/billing/funds.vibe";
import { deploymentLayer, parseProfile } from "../src/profile/profile.vibe";
import { admitWindow, openSocket, reapSessions, relaySession, retire, sessionEnding } from "../src/services/speech-session.vibe";
import { ledger, within } from "./support/ledger.ts";
import { secrets, testProfile } from "./support/profile.ts";
import { loadRow, stepRow } from "../src/store/rows.vibe";
import type { Session } from "../gen/cloud__services__speechRelay.ts";
import { sessionColumns, sessionRows } from "../src/services/speech-session.vibe";
import type { Store } from "../src/store/connection.vibe";

describe("Speech session (spec/cloud/services/speechRelay.md, \"Story: Start speech only from a consent I gave\"; spec/cloud/services/speechRelay.md, \"Story: Keep a live session funded and close it with receipts\")", () => {
  it("walks the session machine through the driver", async () => {
    await driver.reset();
    const exp = 60_000, cred = 10 ** 13;
    expect(await driver.dispatch("a", { type: "openSocket", now: 1 })).toBe("refused");
    expect(await driver.dispatch("a", { type: "mint", tokenDigest: "d", expiresAt: exp, credentialExpiresAt: cred, now: 0, consentConsumed: true })).toBe("accepted");
    expect(await driver.dispatch("a", { type: "openSocket", now: 1 })).toBe("accepted");
    expect(await driver.dispatch("a", { type: "openSocket", now: 2 })).toBe("refused");
    expect(await driver.dispatch("a", { type: "admitWindow", window: 1, expiresAt: exp, now: 3, funded: true, allowanceAvailable: true })).toBe("accepted");
    expect(await driver.dispatch("a", { type: "admitWindow", window: 2, expiresAt: exp + 60_000, now: 4, funded: false, allowanceAvailable: true })).toBe("refused");
    expect(await driver.dispatch("a", { type: "retire" })).toBe("accepted");
    expect(await driver.dispatch("a", { type: "close" })).toBe("refused");
    expect(await driver.dispatch("a", { type: "settleWindow", window: 1 })).toBe("accepted");
    expect(await driver.dispatch("a", { type: "close" })).toBe("accepted");
    expect((await driver.observe("a")).phase).toBe("Closed");
  });

  it("opens one socket, signals revoke and reaps an expired unopened session", async () => {
    const cloud = ledger();
    await cloud.reset(["org-x"]);
    await cloud.sql("INSERT INTO accounts (key, state, revision, organization_id, status, created_at, updated_at) VALUES ('u', '{}', 1, 'org-x', 'active', 0, 0)");
    await cloud.sql("INSERT INTO sessions (key, state, revision, user_id, access_digest, refresh_digest, live, access_expires_at, refresh_expires_at, auth_at, updated_at) VALUES ('cred', '{\"epoch\":0}', 1, 'u', 'ad', 'rd', true, 10000000000000, 10000000000000, 0, 0)");
    for (const key of ["a", "b"]) {
      await cloud.sql("INSERT INTO speech_consents (nonce, user_id, organization_id, credential_kind, credential_ref, expires_at, consumed_at, created_at) VALUES ($1, 'u', 'org-x', 'session', 'cred', 300000, 0, 0)", [`n-${key}`]);
      const minted = await cloud.run(within((tx) => stepRow(tx, sessionRows, key, { type: "mint", tokenDigest: "d", expiresAt: 60_000, credentialExpiresAt: 10 ** 13, now: 0, consentConsumed: true }, (s: Session) => ({ ...sessionColumns(0)(s), consent_nonce: `n-${key}`, user_id: "u", organization_id: "org-x", credential_kind: "session", credential_ref: "cred", credential_expires_at: 10 ** 13, created_at: 0 }))));
      expect(minted.kind).toBe("done");
    }
    const profile = await Effect.runPromise(parseProfile(testProfile("http://fake.invalid")));
    const deploy = Layer.mergeAll(deploymentLayer(profile, "staging"), FetchHttpClient.layer).pipe(Layer.provide(ConfigProvider.layer(ConfigProvider.fromEnvRecord({ ...secrets }))));
    expect((await cloud.run(addCredits({ organizationId: "org-x", amount: 1_000_000, requestKey: "fund-org-x" }))).kind).toBe("done");
    cloud.at(10);
    expect((await cloud.run(openSocket("a"))).kind).toBe("done");
    const second = await cloud.run(openSocket("a"));
    expect(second.kind === "failed" && (second.error as { refusal: { code: string } }).refusal.code).toBe("SessionClosed");
    const admitted = await cloud.run(Effect.provide(admitWindow("a", 1), deploy));
    expect(admitted.kind === "done" && admitted.value.kind === "admitted").toBe(true);
    const dispatched = await cloud.run(Effect.provide(relaySession.dispatchWindow("a", 1), deploy));
    expect(dispatched.kind).toBe("done");
    const holdPhases = await cloud.sql("SELECT phase FROM holds ORDER BY key");
    expect(holdPhases.map((r) => r.phase)).toEqual(["dispatched", "dispatched"]);
    const settled = await cloud.run(Effect.provide(relaySession.settleWindow("a", 1, { pcmBytes: 0, ttsChars: 0, sttDrained: true, ttsConfirmed: true, dispatched: true }), deploy));
    expect(settled.kind).toBe("done");
    const waiting = cloud.run(sessionEnding("a"));
    await Effect.runPromise(Effect.sleep(50));
    const retired = await cloud.run(retire("a", "Revoked"));
    expect(retired.kind === "done" && retired.value.phase).toBe("Closed");
    const ended = await waiting;
    expect(ended.kind === "done" && ended.value).toBe("SessionOver");
    cloud.at(60_001);
    const reaped = await cloud.run(reapSessions() as Effect.Effect<number, never, Store>);
    expect(reaped.kind === "done" && reaped.value).toBe(1);
    const b = await cloud.run(within((tx) => loadRow(tx, sessionRows, "b", false)));
    expect(b.kind === "done" && b.value.state.phase).toBe("Closed");
    const rows = await cloud.sql("SELECT phase FROM speech_sessions ORDER BY key");
    expect(rows.map((r) => r.phase)).toEqual(["closed", "closed"]);
    await cloud.dispose();
  });
});
