package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.apache.bookkeeper.bookie.BookieUtilJournal.generateMetaEntry;

public class BufferedChannelJournalIT {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private static final String SAMPLE_DATA = "BufferedChannelJournal";
    private static final int PADDING_SIZE = 2 * JournalChannel.SECTOR_SIZE;
    private Journal journal;
    private final int journalId = 1;
    private JournalChannel journalChannel;
    private BufferedChannel bufferedChannel;
    private final long ledgerId = 1;
    private final byte[] masterKey = "masterKey".getBytes(StandardCharsets.UTF_8);

    @Before
    public void setUp() throws IOException {
        File tempJournalDirectory = createTemporaryDir();
        File tempLedgerDirectory = createTemporaryDir();
        journal = createJournal(journalId, tempJournalDirectory, tempLedgerDirectory);
        journal.start();
        journalChannel = new JournalChannel(journal.getJournalDirectory(), journalId);
        bufferedChannel = journalChannel.getBufferedChannel();
    }

    private File createTemporaryDir() throws IOException {
        return temporaryFolder.newFolder();
    }

    private Journal createJournal(int journalId, File journalDir, File ledgerDir) throws IOException {
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf
                .setJournalDirsName(new String[]{journalDir.getPath()})
                .setLedgerDirNames(new String[]{ledgerDir.getPath()})
                .setMetadataServiceUri(null);

        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(
                conf, conf.getLedgerDirs(), new DiskChecker(0.99f, 0.98f));
        return new Journal(journalId, journalDir, conf, ledgerDirsManager,
                new NullStatsLogger(), ByteBufAllocator.DEFAULT);
    }

    @Test
    public void shouldWriteAndReadJournalEntryWithPadding() throws IOException {
        ByteBuf paddingBuff = Unpooled.buffer(PADDING_SIZE);
        paddingBuff.writeZero(PADDING_SIZE);

        // Generate meta entry and capture its size before writing
        ByteBuf metaEntry = generateMetaEntry(ledgerId, masterKey);
        int metaEntrySize = metaEntry.readableBytes(); // Capture size before writing

        // Write metadata entry
        ByteBuf lenBuff = Unpooled.buffer(4); // Buffer for length (integer = 4 bytes)
        lenBuff.writeInt(metaEntrySize);
        bufferedChannel.write(lenBuff);
        bufferedChannel.write(metaEntry);
        // Release buffers
        lenBuff.release();
        metaEntry.release();

        // Write content entry
        byte[] contentBytes = SAMPLE_DATA.getBytes(StandardCharsets.UTF_8);
        ByteBuf contentBuffer = Unpooled.wrappedBuffer(contentBytes);
        ByteBuf contentLenBuff = Unpooled.buffer(4);
        contentLenBuff.writeInt(contentBytes.length);
        bufferedChannel.write(contentLenBuff);
        bufferedChannel.write(contentBuffer);
        // Release buffers
        contentLenBuff.release();
        contentBuffer.release();

        // Write padding and flush
        Journal.writePaddingBytes(journalChannel, paddingBuff, JournalChannel.SECTOR_SIZE);
        bufferedChannel.flushAndForceWrite(false);
        paddingBuff.release();

        // Scan journal with correct metaEntrySize
        JournalScan journalScanner = new JournalScan(metaEntrySize);
        journal.scanJournal(journalId, 0, journalScanner, false);

        // Validate
        Assert.assertEquals(ledgerId, journalScanner.getLedgerId());
        Assert.assertEquals(BookieImpl.METAENTRY_ID_LEDGER_KEY, journalScanner.getMetaEntryId());
        Assert.assertArrayEquals(masterKey, journalScanner.getMetaData());
        Assert.assertEquals(SAMPLE_DATA, journalScanner.getData());
        Assert.assertEquals("Padding alignment failed", 0, journalChannel.bc.position % JournalChannel.SECTOR_SIZE);
    }

    private static class JournalScan implements Journal.JournalScanner {
        private final int metaEntrySize;

        @Getter
        private String data;
        @Getter
        private long offset;
        @Getter
        private byte[] metaData;
        @Getter
        private long ledgerId;
        @Getter
        private long metaEntryId;

        public JournalScan(int metaEntrySize) {
            this.metaEntrySize = metaEntrySize;
            this.data = null;
            this.metaData = null;
            this.offset = -1;
        }

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            this.offset = offset;
            try {
                if (metaEntrySize > 0 && entry.remaining() == metaEntrySize) {
                    // Read metadata entry
                    this.ledgerId = entry.getLong();
                    this.metaEntryId = entry.getLong();
                    int masterKeyLength = entry.getInt();
                    byte[] masterKey = new byte[masterKeyLength];
                    entry.get(masterKey);
                    this.metaData = masterKey;
                } else {
                    // Read content entry
                    byte[] dataBytes = new byte[entry.remaining()];
                    entry.get(dataBytes);
                    this.data = new String(dataBytes, StandardCharsets.UTF_8);
                }
            } catch (Exception e) {
                throw new RuntimeException("Error processing journal entry at offset " + offset, e);
            }
        }
    }

    @After
    public void cleanUp() throws IOException {
        if (journalChannel != null) {
            journalChannel.close();
        }
        if (journal != null) {
            journal.shutdown();
        }
    }
}
