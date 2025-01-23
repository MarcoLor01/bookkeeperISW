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
    private static final Logger logger = LoggerFactory.getLogger(BufferedChannelJournalIT.class);
    private final long ledgerId = 1;
    private final byte[] masterKey = "masterKey".getBytes();

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

        // Scrittura metadati
        ByteBuf metaEntry = generateMetaEntry(ledgerId, masterKey);
        ByteBuf lenBuff = Unpooled.buffer();
        lenBuff.writeInt(metaEntry.readableBytes());
        bufferedChannel.write(lenBuff);
        bufferedChannel.write(metaEntry);

        // Scrittura contenuto
        ByteBuf contentBuffer = Unpooled.buffer(SAMPLE_DATA.getBytes().length);
        contentBuffer.writeBytes(SAMPLE_DATA.getBytes());

        lenBuff.clear();
        lenBuff.writeInt(contentBuffer.readableBytes());
        bufferedChannel.write(lenBuff);
        bufferedChannel.write(contentBuffer);

        // Padding e flush
        Journal.writePaddingBytes(journalChannel, paddingBuff, JournalChannel.SECTOR_SIZE);
        bufferedChannel.flushAndForceWrite(false);


        JournalScan journalScanner = new JournalScan(metaEntry.readableBytes());
        journal.scanJournal(journalId, 0, journalScanner, false);

        // Validazione lettura
        Assert.assertEquals(ledgerId, journalScanner.getLedgerId());
        Assert.assertEquals(BookieImpl.METAENTRY_ID_LEDGER_KEY, journalScanner.getMetaEntryId());
        Assert.assertArrayEquals("masterKey".getBytes(), journalScanner.getMetaData());
        Assert.assertEquals(SAMPLE_DATA, journalScanner.getData());
        Assert.assertEquals("Padding alignment failed", 0, journalChannel.bc.position % JournalChannel.SECTOR_SIZE);

    }

    private static class JournalScan implements Journal.JournalScanner {
        private final int metaEntrySize;

        @Getter
        private String data; // Contenuto letto
        @Getter
        private long offset; // Offset della posizione letta
        @Getter
        private byte[] metaData; // Metadati letti
        @Getter
        private long ledgerId; // Ledger ID letto dai metadati
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
                // Lettura metadati
                if (metaEntrySize > 0 && entry.remaining() >= metaEntrySize) {
                    // Ledger ID
                    this.ledgerId = entry.getLong();

                    // MetaEntry ID
                    this.metaEntryId = entry.getLong();

                    // Lunghezza della master key
                    int masterKeyLength = entry.getInt();
                    byte[] masterKey = new byte[masterKeyLength];
                    entry.get(masterKey);

                    this.metaData = masterKey; // Salva la master key nei metadati
                }

                int dataLength = entry.remaining();
                byte[] dataBytes = new byte[dataLength];
                entry.get(dataBytes); // Leggiamo i dati principali
                this.data = new String(dataBytes, StandardCharsets.UTF_8);

            } catch (Exception e) {
                throw new RuntimeException("Errore durante la lettura dell'entry al journal offset " + offset, e);
            }
        }
    }

    @After
    public void cleanUp() throws IOException {
        journalChannel.close();
        journal.shutdown();

    }

}
