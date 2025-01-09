package org.apache.bookkeeper.bookie;

import lombok.Getter;
import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import static org.apache.bookkeeper.bookie.BookieUtilJournal.writeV4Journal;

@RunWith(Parameterized.class)
public class JournalScanJournalTest {
    private long journalId;
    private final long journalPos;
    private final ScannerStatus scannerStatus;
    private final boolean skipInvalidRecord;
    private final Class<? extends Exception> expectedException;
    private JournalScanner journalScanner;
    private Journal journal;
    final List<File> tempDirs = new ArrayList<>();
    private static long BYTES_WRITE = 0;
    private static final long EMPTY_JOURNAL = 516;
    private BookieImpl bookie;
    private final byte[] KEY = "test".getBytes();
    private static final int NUM_ENTRY = 10;
    public JournalScanJournalTest(long journalId, long journalPos, ScannerStatus scanner, boolean skipInvalidRecord,
                                  Class<? extends Exception> expectedException){
        this.journalId = journalId;
        this.journalPos = journalPos;
        this.scannerStatus = scanner;
        this.skipInvalidRecord = skipInvalidRecord;
        this.expectedException = expectedException;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                // TEST: JournalId, journalPos, scanner, skipInvalidRecord -> Expected
                {0, 0, ScannerStatus.VALID, true, null},
                {1, 0, ScannerStatus.VALID, false, null},
                {1, -1, ScannerStatus.VALID, true, null},
                {0, -1, ScannerStatus.VALID, true, null},
                {1, 1, ScannerStatus.VALID, true, null},
                {1, Long.MAX_VALUE, ScannerStatus.VALID, true, null},
                {0, 0, ScannerStatus.NULL, false, NullPointerException.class},
                {0, 0, ScannerStatus.INVALID, false, IOException.class},
                {0, 0, ScannerStatus.INVALID, true, null},
                {1, 0, ScannerStatus.VALID, false, null},
                {0, 1, ScannerStatus.VALID, false, null},
        });
    }

    @Before
    public void setUp() throws Exception {
        setScanner();

        File journalDirectory = createTempDirectory("bookie", "journal");
        File ledgerDirectory = createTempDirectory("bookie", "ledger");

        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(journalDirectory));
        BookieImpl.checkDirectoryStructure(BookieImpl.getCurrentDirectory(ledgerDirectory));

        try (JournalChannel journalChannel = writeV4Journal(BookieImpl.getCurrentDirectory(journalDirectory), NUM_ENTRY, KEY)) {
            BYTES_WRITE += journalChannel.fc.position() + Integer.BYTES; //Tipo di Journal
        }

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf
                .setJournalDirsName(new String[] {journalDirectory.getPath()})
                .setLedgerDirNames(new String[] { ledgerDirectory.getPath() })
                .setMetadataServiceUri(null);

        bookie = new TestBookieImpl(conf);
        this.journal = bookie.journals.get(0);

        if(journalId == 0){
            journalId = Journal.listJournalIds(this.journal.getJournalDirectory(), null).get(0);
        } else {
            journalId = 1;
        }
    }

    File createTempDirectory(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    private void setScanner() {
        switch (scannerStatus) {
            case NULL:
                journalScanner = null;
                break;
            case VALID:
                journalScanner = new ValidJournalScan();
                break;
            case INVALID:
                journalScanner = new InvalidJournalScan();
                break;
        }
    }

    @Test
    public void test() {
        try {
            long bytesRead = journal.scanJournal(journalId, journalPos, journalScanner, skipInvalidRecord);
            Assert.assertNotNull(journal);

            if (expectedException != null) {
                Assert.fail("Expected exception: " + expectedException + " but none was thrown.");
            }
            if(scannerStatus == ScannerStatus.INVALID && journalId != 1){
                Assert.assertEquals(KEY.length + 32, bytesRead);
            } else {
                if (journalPos <= 0) {
                    if (journalId != 1) {
                        Assert.assertEquals(BYTES_WRITE, bytesRead); //Se l'ID è 0 leggo Journal corretto
                        if(journalScanner instanceof ValidJournalScan){
                            Assert.assertEquals(((ValidJournalScan) journalScanner).getProcessCount(), NUM_ENTRY + 2); //Num entry + primo mex + fence mex
                        }
                    } else {
                        Assert.assertEquals(EMPTY_JOURNAL, bytesRead); //Leggo Journal vuoto
                        Assert.assertEquals(((ValidJournalScan) journalScanner).getProcessCount(), 0);
                    }
                } else {
                    if (journalId != 1) {
                        Assert.assertTrue(BYTES_WRITE != bytesRead);
                        Assert.assertEquals(((ValidJournalScan) journalScanner).getProcessCount(), 0);
                    } else {
                        Assert.assertEquals(EMPTY_JOURNAL, bytesRead); //Leggo Journal vuoto
                        Assert.assertEquals(((ValidJournalScan) journalScanner).getProcessCount(), 0);
                    }
                }
            }
        } catch(Exception e){
            Assert.assertEquals(expectedException, e.getClass());
        }
    }

    @After
    public void cleanUp() {
        if (bookie != null) {
            this.bookie.shutdown();
        }

        for (File dir : tempDirs) {
            FileUtils.deleteQuietly(dir);
        }
        tempDirs.clear();

        BYTES_WRITE = 0;
    }

    public enum ScannerStatus {
        NULL,
        VALID,
        INVALID,
    }

    @Getter
    private static class ValidJournalScan implements JournalScanner {
        private int processCount = 0;

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) {
            processCount++;
        }

    }

    private static class InvalidJournalScan implements JournalScanner {

        @Override
        public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
            throw new IOException("Invalid Scanner");
        }
    }
}
