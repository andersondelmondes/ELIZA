// spec/os/voice/liveCapture.md, "Only a speech segment and its short pre-roll leave the capture, and nothing is stored"; spec/os/voice/liveCapture.md, "A routed capture sends its segments only to the owner's agent, in order and bounded, and a stop cuts them off"
package ai.eliza.voice;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

public final class LiveCaptureTest {
  private static List<LiveVadGate.Edge> run(LiveVadGate gate, float... probabilities) {
    List<LiveVadGate.Edge> edges = new ArrayList<>();
    for (float probability : probabilities) edges.add(gate.accept(probability));
    return edges;
  }

  @Test public void aSegmentOpensAfterItsRunAndClosesAfterItsHangover() {
    LiveVadGate gate = new LiveVadGate(2, 3);
    List<LiveVadGate.Edge> edges = run(gate, 0.6f, 0.2f, 0.6f, 0.6f, 0.4f, 0.1f, 0.1f, 0.1f, Float.NaN);
    assertEquals(List.of(
        LiveVadGate.Edge.NONE, LiveVadGate.Edge.NONE, LiveVadGate.Edge.NONE, LiveVadGate.Edge.SPEECH_START,
        LiveVadGate.Edge.NONE, LiveVadGate.Edge.NONE, LiveVadGate.Edge.NONE, LiveVadGate.Edge.SPEECH_END,
        LiveVadGate.Edge.NONE), edges);
    assertFalse(gate.speaking());
  }

  @Test(expected = IllegalArgumentException.class) public void aStartRunLongerThanThePrerollIsRefused() {
    new LiveVadGate(LiveVadGate.PREROLL_WINDOWS + 1, 16);
  }

  private static final class Agent implements LiveSegmentPipe.Transport {
    final List<JSONObject> posted = new ArrayList<>();
    @Override public void check() {}
    @Override public synchronized JSONObject post(JSONObject body) throws Exception {
      posted.add(body);
      JSONObject reply = body.optBoolean("close") ? new JSONObject().put("closed", true) : new JSONObject();
      return new JSONObject().put("status", 200).put("body", reply.toString());
    }
  }

  private static JSONArray frames(int count, long at) throws Exception {
    JSONArray frames = new JSONArray();
    for (int i = 0; i < count; i++) frames.put(new JSONObject().put("pcm16", "AA==").put("timestamp", at + i));
    return frames;
  }

  @Test public void aRoutedCaptureSendsInOrderUntilItsCutoffAndRefusesOverload() throws Exception {
    Agent agent = new Agent();
    List<String> failures = new ArrayList<>();
    LiveSegmentPipe pipe = new LiveSegmentPipe(agent, new LiveCaptureController.Pipe.Sink() {
      @Override public void result(JSONObject value) {}
      @Override public void failed(String code) { failures.add(code); }
    }, "00000000-0000-4000-8000-000000000001", "room-1");
    assertTrue(pipe.start(Executors.newSingleThreadExecutor()));
    assertTrue(pipe.ready().get(5, TimeUnit.SECONDS));
    assertTrue(pipe.offer(frames(2, 1_000), new JSONArray().put(new JSONObject().put("type", "speech-start").put("timestampMs", 1_000))));
    pipe.cutoff(1_500);
    assertTrue(pipe.offer(frames(2, 2_000), new JSONArray()));
    assertTrue(pipe.close(false, 1_500).get(5, TimeUnit.SECONDS));
    List<Integer> sent = new ArrayList<>();
    for (JSONObject body : agent.posted) if (body.has("frames")) sent.add(body.getJSONArray("frames").length());
    assertEquals(List.of(0, 2), sent);
    assertTrue(agent.posted.get(agent.posted.size() - 1).optBoolean("close"));
    assertEquals(List.of(), failures);

    LiveSegmentPipe overloaded = new LiveSegmentPipe(new Agent(), new LiveCaptureController.Pipe.Sink() {
      @Override public void result(JSONObject value) {}
      @Override public void failed(String code) { failures.add(code); }
    }, "00000000-0000-4000-8000-000000000002", null);
    assertFalse(overloaded.offer(frames(LiveSegmentPipe.MAX_QUEUED_FRAMES + 1, 0), new JSONArray()));
    assertEquals(List.of("BACKPRESSURE"), failures);
  }

  @Test public void aRoutedCaptureStartsWithoutBlockingOnPipeReadyBuffersAndFlushesInOrderAndPrewarms() throws Exception {
    CompletableFuture<Boolean> readyFuture = new CompletableFuture<>();
    List<Long> offeredTimestamps = Collections.synchronizedList(new ArrayList<>());
    CountDownLatch prewarmed = new CountDownLatch(1);
    CountDownLatch captureStarted = new CountDownLatch(1);
    CountDownLatch startedCallback = new CountDownLatch(1);
    AtomicReference<LiveCaptureController.Sink> captureSink = new AtomicReference<>();

    LiveCaptureController controller = new LiveCaptureController(new LiveCaptureController.Host() {
      @Override public boolean open(long epoch) { return true; }
      @Override public LiveCaptureController.Capture capture(String id, LiveCaptureController.Sink sink, LiveCaptureOptions options) {
        captureSink.set(sink);
        return new LiveCaptureController.Capture() {
          @Override public String captureId() { return id; }
          @Override public boolean echoCancelled() { return true; }
          @Override public void start() { captureStarted.countDown(); }
          @Override public long stopAndWait() { return 10; }
          @Override public void cancelPending() {}
          @Override public void requestStop() {}
          @Override public void interrupt(String reason) {}
        };
      }
      @Override public void frames(String id, JSONArray frames, JSONArray vad, float peak) {}
      @Override public void ended(String id, String error) {}
      @Override public void prewarm() { prewarmed.countDown(); }
    });

    try {
      assertTrue(controller.prewarm().get(5, TimeUnit.SECONDS));
      assertTrue(prewarmed.await(5, TimeUnit.SECONDS));

      String captureId = "00000000-0000-4000-8000-000000000011";
      LiveCaptureAdmission.Ticket ticket = controller.reserve(captureId, 1L);
      LiveCaptureController.Pipe delayedPipe = new LiveCaptureController.Pipe() {
        @Override public boolean start(Executor executor) { return true; }
        @Override public CompletableFuture<Boolean> ready() { return readyFuture; }
        @Override public boolean offer(JSONArray frames, JSONArray vad) {
          for (int i = 0; i < frames.length(); i++) {
            offeredTimestamps.add(frames.getJSONObject(i).getLong("timestamp"));
          }
          return true;
        }
        @Override public void cutoff(double atMs) {}
        @Override public CompletableFuture<Boolean> close(boolean discard, double atMs) {
          return CompletableFuture.completedFuture(true);
        }
      };

      AtomicReference<Boolean> pipedResult = new AtomicReference<>();
      controller.start(ticket, true, (sink, id, room) -> delayedPipe, "room-1", false, new LiveCaptureController.Started() {
        @Override public void started(String id, int sampleRate, boolean echoCancelled, boolean piped) {
          pipedResult.set(piped);
          startedCallback.countDown();
        }
        @Override public void refused(String message, String code) {}
      });

      assertTrue("capture must open before pipe.ready() completes", captureStarted.await(2, TimeUnit.SECONDS));
      assertTrue("start callback must resolve before pipe.ready() completes", startedCallback.await(2, TimeUnit.SECONDS));
      assertEquals(Boolean.TRUE, pipedResult.get());
      assertFalse(readyFuture.isDone());

      int overflowBatches = LiveCaptureController.PRE_READY_MAX_FRAMES + 4;
      for (int i = 0; i < overflowBatches; i++) {
        captureSink.get().frames(captureId, frames(1, 1000L + i), new JSONArray(), 0.5f);
      }
      assertEquals(0, offeredTimestamps.size());

      readyFuture.complete(true);
      captureSink.get().frames(captureId, frames(1, 9999L), new JSONArray(), 0.5f);

      assertEquals(LiveCaptureController.PRE_READY_MAX_FRAMES + 1, offeredTimestamps.size());
      assertEquals(Long.valueOf(1004L), offeredTimestamps.get(0));
      assertEquals(Long.valueOf(1000L + overflowBatches - 1), offeredTimestamps.get(LiveCaptureController.PRE_READY_MAX_FRAMES - 1));
      assertEquals(Long.valueOf(9999L), offeredTimestamps.get(LiveCaptureController.PRE_READY_MAX_FRAMES));
    } finally {
      controller.shutdown();
    }
  }

  @Test public void vadAvailabilityCachesWithoutChurnAndReusesStandbyHandle() throws Exception {
    File bundle = Files.createTempDirectory("eliza-vad-test-").toFile();
    bundle.deleteOnExit();
    AtomicInteger contextCreates = new AtomicInteger();
    AtomicInteger contextDestroys = new AtomicInteger();
    AtomicInteger vadOpens = new AtomicInteger();
    AtomicInteger vadCloses = new AtomicInteger();
    AtomicInteger nextHandle = new AtomicInteger(10);

    InferenceSession.VadOps fakeOps = new InferenceSession.VadOps() {
      @Override public boolean supported() { return true; }
      @Override public long createContext(String bundlePath) { return contextCreates.incrementAndGet(); }
      @Override public void destroyContext(long context) { contextDestroys.incrementAndGet(); }
      @Override public long open(long context) { vadOpens.incrementAndGet(); return nextHandle.incrementAndGet(); }
      @Override public float[] processBatch(long handle, float[] pcm16kMono) { return new float[] {0.1f}; }
      @Override public void reset(long handle) {}
      @Override public void close(long handle) { vadCloses.incrementAndGet(); }
    };

    InferenceSession session = new InferenceSession(bundle, new File(bundle, "m"), new File(bundle, "l"), null, Collections.emptyMap(), fakeOps);
    for (int i = 0; i < 5; i++) {
      assertTrue(LiveVoiceCapture.vadAvailable(session));
    }
    assertEquals(1, contextCreates.get());
    assertEquals(1, vadOpens.get());
    assertEquals(0, vadCloses.get());

    InferenceSession.Vad first = session.openVad();
    assertEquals("first capture must reuse standby VAD handle", 1, vadOpens.get());
    assertEquals(0, vadCloses.get());
    first.close();
    assertEquals(1, vadCloses.get());

    assertTrue(LiveVoiceCapture.vadAvailable(session));
    assertEquals("cached availability probe must not reopen native VAD", 1, vadOpens.get());

    assertTrue(LiveVoiceCapture.prewarmVad(session));
    assertEquals(2, vadOpens.get());
    session.trimMemory();
    assertEquals(2, vadCloses.get());
    assertEquals(1, contextDestroys.get());

    session.close();
    assertFalse(LiveVoiceCapture.vadAvailable(session));
  }
}
