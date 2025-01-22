package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.util.ReferenceCountUtil;
import lombok.Getter;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.bookkeeper.bookie.BookieUtilJournal.generateMetaEntry;

public class BufferedChannelJournalIT {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private String dataToWrite = "BufferedChannelJournal";
    private byte[] dataRead;
    private Journal journal;
    private int journalId = 1;
    private JournalChannel journalChannel;
    private BufferedChannel bufferedChannel;
    private static final Logger logger = LoggerFactory.getLogger(BufferedChannelJournalIT.class);


    @Before
    public void setUp() throws IOException {
        File tempJournalDirectory = temporaryFolder.newFolder();
        File tempLedgerDirectory = temporaryFolder.newFolder();
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(tempJournalDirectory));
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(tempLedgerDirectory));
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf
                .setJournalDirsName(new String[] {tempJournalDirectory.getPath()})
                .setLedgerDirNames(new String[] { tempLedgerDirectory.getPath() })
                .setMetadataServiceUri(null);

        LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs(), new DiskChecker(0.99f, 0.98f));

        //Creo Journal
        journal = new Journal(journalId, tempJournalDirectory, conf, ledgerDirsManager, new NullStatsLogger(), ByteBufAllocator.DEFAULT);
        journal.start();
        //Creo Channel
        journalChannel = new JournalChannel(journal.getJournalDirectory(), journalId);
        bufferedChannel = journalChannel.getBufferedChannel();
    }

    @Test
    public void writeJournalEntryTest() throws IOException {
        // Creiamo il buffer per il padding
        ByteBuf paddingBuff = Unpooled.buffer(2 * JournalChannel.SECTOR_SIZE);
        paddingBuff.writeZero(2 * JournalChannel.SECTOR_SIZE); // Riempito di zeri per l'allineamento

        // 1. Scrittura Metadati
        ByteBuf metaEntry = generateMetaEntry(1, "masterKey".getBytes());
        ByteBuf lenBuff = Unpooled.buffer();
        logger.info("Metaentry: {} Lunghezza: {}", metaEntry, metaEntry.readableBytes());
        lenBuff.writeInt(metaEntry.readableBytes()); // Scriviamo la lunghezza dei metadati
        bufferedChannel.write(lenBuff);
        bufferedChannel.write(metaEntry);
        ReferenceCountUtil.release(metaEntry);

        // 2. Scrittura dei dati principali
        ByteBuf buffer = Unpooled.buffer(dataToWrite.getBytes().length);
        buffer.writeBytes(dataToWrite.getBytes());

        lenBuff.clear();
        lenBuff.writeInt(buffer.readableBytes()); // Scriviamo la lunghezza del contenuto
        bufferedChannel.write(lenBuff);
        bufferedChannel.write(buffer);

        // 3. Aggiunta del padding
        Journal.writePaddingBytes(journalChannel, paddingBuff, JournalChannel.SECTOR_SIZE);


        // Flush e aggiornamento del Journal
        bufferedChannel.flushAndForceWrite(false);

        JournalScan journalScanner = new JournalScan(metaEntry.readableBytes());

        //Leggo metadati
        journal.scanJournal(journalId, 0, journalScanner, false);
        Assert.assertEquals(dataToWrite, journalScanner.getData());

        Assert.assertEquals(0, journalChannel.bc.position % JournalChannel.SECTOR_SIZE);

    }

    private static class JournalScan implements Journal.JournalScanner {
        private final int metaEntrySize;

        @Getter
        private String data; // Contenuto letto
        @Getter
        private long offset; // Offset della posizione letta

        public JournalScan(int metaEntrySize) {
            this.metaEntrySize = metaEntrySize;
            this.data = null;
            this.offset = -1;
        }

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            this.offset = offset;
            try {
                
                if (metaEntrySize > 0 && entry.remaining() >= metaEntrySize) {
                    logger.info("Mi sposto in posizione: {}", entry.position() + metaEntrySize);
                    entry.position(entry.position() + metaEntrySize);
                }

                int dataLength = entry.remaining(); // Calcoliamo la lunghezza dei dati rimanenti
                byte[] dataBytes = new byte[dataLength];
                entry.get(dataBytes); // Leggiamo i dati
                logger.info("Lunghezza: {} contenuto in byte: {}", dataBytes.length, dataBytes);
                this.data = new String(dataBytes); // Convertiamo i byte in String

            } catch (Exception e) {
                throw new RuntimeException("Errore durante la lettura dell'entry al journal offset " + offset, e);
            }
        }

    }

}
