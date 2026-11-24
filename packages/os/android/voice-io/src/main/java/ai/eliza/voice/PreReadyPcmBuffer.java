// spec/os/voice/cloudSpeech.md, "Story: Talk to the agent through the cloud speech relay"
package ai.eliza.voice;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Bounded in-memory FIFO ring buffer of 16 kHz PCM16 frames captured while cloud speech startup
 * (consent, mint, WebSocket upgrade and STT readiness) runs in parallel. Oldest chunks are evicted
 * and zeroed once the byte cap is reached; {@link #clear} zeroes all retained audio.
 */
public final class PreReadyPcmBuffer {
  private final int maxBytes;
  private final ArrayDeque<byte[]> chunks = new ArrayDeque<>();
  private int bufferedBytes;
  private long droppedBytes;

  public PreReadyPcmBuffer(int maxBytes) {
    if (maxBytes <= 0 || maxBytes % 2 != 0) throw new IllegalArgumentException("maxBytes must be a positive even byte count");
    this.maxBytes = maxBytes;
  }

  public synchronized void offer(byte[] pcm, int length) {
    if (pcm == null || length <= 0) return;
    int count = Math.min(length, pcm.length);
    count -= count % 2;
    if (count <= 0) return;
    int offset = 0;
    if (count > maxBytes) {
      int excess = count - maxBytes;
      excess += excess % 2;
      offset = excess;
      count -= excess;
      droppedBytes += excess;
    }
    while (!chunks.isEmpty() && bufferedBytes + count > maxBytes) {
      byte[] evicted = chunks.removeFirst();
      bufferedBytes -= evicted.length;
      droppedBytes += evicted.length;
      Arrays.fill(evicted, (byte) 0);
    }
    byte[] copy = Arrays.copyOfRange(pcm, offset, offset + count);
    chunks.addLast(copy);
    bufferedBytes += copy.length;
  }

  public synchronized List<byte[]> drain() {
    List<byte[]> out = new ArrayList<>(chunks);
    chunks.clear();
    bufferedBytes = 0;
    return out;
  }

  public synchronized void clear() {
    for (byte[] chunk : chunks) Arrays.fill(chunk, (byte) 0);
    chunks.clear();
    bufferedBytes = 0;
  }

  public synchronized int bufferedBytes() {
    return bufferedBytes;
  }

  public synchronized long droppedBytes() {
    return droppedBytes;
  }
}
