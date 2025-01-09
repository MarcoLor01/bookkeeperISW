package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.ReferenceCounted;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.proto.DataFormats;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.apache.bookkeeper.util.ByteBufList;
import org.apache.bookkeeper.util.IOUtils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.Arrays;

/**
 * Test the bookie journal.
 */

public class BookieUtilJournal {

    public static ByteBuf generateFenceEntry(long ledgerId) {
        ByteBuf bb = Unpooled.buffer();
        bb.writeLong(ledgerId);
        bb.writeLong(BookieImpl.METAENTRY_ID_FENCE_KEY);
        return bb;
    }


    public static ByteBuf generateMetaEntry(long ledgerId, byte[] masterKey) {
        ByteBuf bb = Unpooled.buffer();
        bb.writeLong(ledgerId);
        bb.writeLong(BookieImpl.METAENTRY_ID_LEDGER_KEY);
        bb.writeInt(masterKey.length);
        bb.writeBytes(masterKey);
        return bb;
    }


    private static void moveToPosition(JournalChannel jc, long pos) throws IOException {
        jc.fc.position(pos);
        jc.bc.position = pos;
        jc.bc.writeBufferStartPosition.set(pos);
    }

    private static void updateJournalVersion(JournalChannel jc, int journalVersion) throws IOException {
        long prevPos = jc.fc.position();
        try {
            ByteBuffer versionBuffer = ByteBuffer.allocate(4);
            versionBuffer.putInt(journalVersion);
            versionBuffer.flip();
            jc.fc.position(4);
            IOUtils.writeFully(jc.fc, versionBuffer);
            jc.fc.force(true);
        } finally {
            jc.fc.position(prevPos);
        }
    }


    public static JournalChannel writeV4Journal(File journalDir, int numEntries, byte[] masterKey) throws Exception {
        long logId = System.currentTimeMillis();
        JournalChannel jc = new JournalChannel(journalDir, logId);

        moveToPosition(jc, JournalChannel.VERSION_HEADER_SIZE);

        BufferedChannel bc = jc.getBufferedChannel();

        byte[] data = new byte[1024];
        Arrays.fill(data, (byte) 'X');
        long lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;
        for (int i = 0; i <= numEntries; i++) {
            ByteBuf packet;
            if (i == 0) {
                packet = generateMetaEntry(1, masterKey);
            } else {
                packet = generatePacket(1, i, lastConfirmed, i * data.length, data);
            }
            lastConfirmed = i;
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            lenBuff.putInt(packet.readableBytes());
            lenBuff.flip();
            bc.write(Unpooled.wrappedBuffer(lenBuff));
            bc.write(packet);
            ReferenceCountUtil.release(packet);
        }
        // write fence key
        ByteBuf packet = generateFenceEntry(1);
        ByteBuf lenBuf = Unpooled.buffer();
        lenBuf.writeInt(packet.readableBytes());
        bc.write(lenBuf);
        bc.write(packet);
        bc.flushAndForceWrite(false);
        updateJournalVersion(jc, JournalChannel.V4);
        return jc;
    }


    public static ByteBuf generatePacket(long ledgerId, long entryId, long lastAddConfirmed,
                                         long length, byte[] data) throws GeneralSecurityException {
        return generatePacket(ledgerId, entryId, lastAddConfirmed, length, data, 0, data.length);
    }

    public static ByteBuf generatePacket(long ledgerId, long entryId, long lastAddConfirmed, long length, byte[] data,
                                         int offset, int len) throws GeneralSecurityException {
        DigestManager dm = DigestManager.instantiate(ledgerId, new byte[2], DataFormats.LedgerMetadataFormat.DigestType.CRC32,
                UnpooledByteBufAllocator.DEFAULT, true);
        return copyDataWithSkipHeader(dm.computeDigestAndPackageForSending(entryId, lastAddConfirmed,
                length, Unpooled.wrappedBuffer(data, offset, len), new byte[20], 0));
    }

    public static ByteBuf copyDataWithSkipHeader(ReferenceCounted rc) {
        ByteBuf res;
        if (rc instanceof ByteBuf) {
            res = Unpooled.copiedBuffer((ByteBuf) rc);
        } else {
            res = ByteBufList.coalesce((ByteBufList) rc);
        }

        // Skip headers
        res.skipBytes(28);
        rc.release();

        return res;
    }


    public static JournalChannel writeV5Journal(File journalDir, int numEntries,
                                                byte[] masterKey) throws Exception {
        return writeV5Journal(journalDir, numEntries, masterKey, false);
    }

    static JournalChannel writeV5Journal(File journalDir, int numEntries,
                                         byte[] masterKey, boolean corruptLength) throws Exception {
        long logId = System.currentTimeMillis();
        JournalChannel jc = new JournalChannel(journalDir, logId);

        BufferedChannel bc = jc.getBufferedChannel();

        ByteBuf paddingBuff = Unpooled.buffer();
        paddingBuff.writeZero(2 * JournalChannel.SECTOR_SIZE);
        byte[] data = new byte[4 * 1024 * 1024];
        Arrays.fill(data, (byte) 'X');
        long lastConfirmed = LedgerHandle.INVALID_ENTRY_ID;
        long length = 0;
        for (int i = 0; i <= numEntries; i++) {
            ByteBuf packet;
            if (i == 0) {
                packet = generateMetaEntry(1, masterKey);
            } else {
                packet = generatePacket(1, i, lastConfirmed, length, data, 0, i);
            }
            lastConfirmed = i;
            length += i;
            ByteBuf lenBuff = Unpooled.buffer();
            if (corruptLength) {
                lenBuff.writeInt(-1);
            } else {
                lenBuff.writeInt(packet.readableBytes());
            }
            bc.write(lenBuff);
            bc.write(packet);
            ReferenceCountUtil.release(packet);
            Journal.writePaddingBytes(jc, paddingBuff, JournalChannel.SECTOR_SIZE);
        }
        // write fence key
        ByteBuf packet = generateFenceEntry(1);
        ByteBuf lenBuf = Unpooled.buffer();
        lenBuf.writeInt(packet.readableBytes());
        bc.write(lenBuf);
        bc.write(packet);
        Journal.writePaddingBytes(jc, paddingBuff, JournalChannel.SECTOR_SIZE);
        bc.flushAndForceWrite(false);
        updateJournalVersion(jc, JournalChannel.V5);
        return jc;
    }
}
