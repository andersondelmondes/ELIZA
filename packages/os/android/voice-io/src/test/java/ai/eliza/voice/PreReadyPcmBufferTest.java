// spec/os/voice/cloudSpeech.md, "Story: Talk to the agent through the cloud speech relay"
package ai.eliza.voice;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.List;
import org.junit.Test;

public final class PreReadyPcmBufferTest {
  @Test public void keepsNewestEvenChunksInFifoOrderAndZeroesOnEvictionAndClear() {
    PreReadyPcmBuffer buffer = new PreReadyPcmBuffer(8);
    buffer.offer(new byte[] {1, 2, 3, 4}, 4);
    buffer.offer(new byte[] {5, 6, 7, 8}, 4);
    assertEquals(8, buffer.bufferedBytes());
    assertEquals(0, buffer.droppedBytes());

    buffer.offer(new byte[] {9, 10, 11, 12}, 4);
    assertEquals(8, buffer.bufferedBytes());
    assertEquals(4, buffer.droppedBytes());

    List<byte[]> drained = buffer.drain();
    assertEquals(2, drained.size());
    assertArrayEquals(new byte[] {5, 6, 7, 8}, drained.get(0));
    assertArrayEquals(new byte[] {9, 10, 11, 12}, drained.get(1));
    assertEquals(0, buffer.bufferedBytes());

    buffer.offer(new byte[] {21, 22, 23, 24}, 4);
    buffer.clear();
    assertEquals(0, buffer.bufferedBytes());
    assertEquals(0, buffer.drain().size());
  }

  @Test public void truncatesOversizedChunkToTrailingBound() {
    PreReadyPcmBuffer buffer = new PreReadyPcmBuffer(4);
    buffer.offer(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, 8);
    assertEquals(4, buffer.bufferedBytes());
    assertEquals(4, buffer.droppedBytes());
    List<byte[]> drained = buffer.drain();
    assertEquals(1, drained.size());
    assertArrayEquals(new byte[] {5, 6, 7, 8}, drained.get(0));
  }
}
